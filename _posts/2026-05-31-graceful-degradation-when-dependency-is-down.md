---
layout: post
title: "Graceful Degradation: What to Return When a Dependency Is Down"
date: 2026-05-31
tags: [reliability, microservices, resilience, production]
read_time: 8
---

Your recommendations service went down at 2 AM. By 2:05 AM, your homepage conversion rate had dropped 40%. Not because users couldn't see recommendations — but because your product API was waiting 30 seconds for a timeout before returning 500 to every client.

This is the graceful degradation failure. The downstream service broke, but the contract failure cascaded upward and took out an unrelated feature. The correct behavior: return a homepage without recommendations, not a broken homepage.

## What Graceful Degradation Actually Means

Graceful degradation means your system continues serving its core function when a non-critical dependency fails. It is not about hiding errors from operations — your monitors should still fire. It is about what you return to users.

The failure modes engineers most often get wrong:

- **Treating all dependencies as equally critical.** A checkout service depends on fraud detection — but fraud detection being slow should delay, not block, checkout (with appropriate risk controls).
- **Propagating timeouts as hard failures.** A 30-second timeout becomes a 30-second hang for every user behind it.
- **No fallback path.** The code calls the dependency or throws. There is no third option.

## Classifying Your Dependencies

Before you can degrade gracefully, you need to decide what each dependency's failure mode should be:

| Dependency type | On failure |
|---|---|
| Hard requirement (auth, payment) | Return error to client |
| Best-effort enrichment (recommendations, ads) | Return empty/default |
| Cached-acceptable (user preferences, feature flags) | Return stale |
| Fire-and-forget (analytics, audit log) | Drop with backlog |

Write this classification down. Put it in your service's runbook. Every new dependency you add should be classified at review time, not during an incident.

## Pattern 1: Stale Cache With Explicit TTL Policies

The most common production-safe pattern is serving stale data from cache when the upstream is unavailable. The key detail engineers miss: cache on success, not on miss.

```python
import time
from typing import Optional, Any, Callable

class StaleWhileRevalidate:
    """Serves stale data when the source is unavailable."""

    def __init__(self, cache, stale_ttl: int = 300):
        self.cache = cache
        self.stale_ttl = stale_ttl  # seconds to serve stale after primary TTL expires

    def get(self, key: str, fetch_fn: Callable, primary_ttl: int = 60) -> Optional[Any]:
        cached = self.cache.get(key)
        now = time.time()

        if cached:
            value, stored_at = cached["value"], cached["stored_at"]
            age = now - stored_at

            if age < primary_ttl:
                return value  # fresh

            if age < primary_ttl + self.stale_ttl:
                # Stale but within grace window — try background refresh
                try:
                    fresh = fetch_fn()
                    self.cache.set(key, {"value": fresh, "stored_at": now})
                    return fresh
                except Exception:
                    return value  # serve stale, upstream is down

        # No cache entry or fully expired — must fetch
        try:
            value = fetch_fn()
            self.cache.set(key, {"value": value, "stored_at": now})
            return value
        except Exception:
            return None  # caller handles None as degraded mode
```

The caller decides what `None` means — an empty recommendations list, a default feature flag value, a zero balance. The contract is explicit: this function returns `None` when fully degraded, not an exception.

## Pattern 2: Circuit Breaker With Explicit Fallback

A circuit breaker stops calling a failing dependency, which prevents timeout accumulation from pile-up. But the breaker state should feed your fallback logic rather than just raising:

```python
import threading
import time
from enum import Enum

class BreakerState(Enum):
    CLOSED = "closed"       # Normal operation
    OPEN = "open"           # Not calling downstream
    HALF_OPEN = "half_open" # Testing if downstream recovered

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._failures = 0
        self._state = BreakerState.CLOSED
        self._opened_at = None
        self._lock = threading.Lock()

    @property
    def state(self) -> BreakerState:
        with self._lock:
            if self._state == BreakerState.OPEN:
                if time.time() - self._opened_at > self.recovery_timeout:
                    self._state = BreakerState.HALF_OPEN
            return self._state

    def call(self, fn: Callable, fallback: Callable = None):
        if self.state == BreakerState.OPEN:
            if fallback:
                return fallback()
            raise RuntimeError("circuit open, no fallback provided")

        try:
            result = fn()
            with self._lock:
                self._failures = 0
                self._state = BreakerState.CLOSED
            return result
        except Exception as e:
            with self._lock:
                self._failures += 1
                if self._failures >= self.failure_threshold:
                    self._state = BreakerState.OPEN
                    self._opened_at = time.time()
            if fallback:
                return fallback()
            raise
```

The critical design point: pass the `fallback` callable into `call()`. This keeps the fallback logic next to the call site, making it explicit in code review that this path has been thought through. A circuit breaker that just raises `CircuitOpenError` is incomplete — it defers the degradation decision to callers who may not implement it consistently.

## Pattern 3: Timeouts That Actually Work

Graceful degradation requires timeouts short enough to actually protect users. A 30-second HTTP timeout is not a timeout — it is a resource hold. Set timeouts at the call site based on what the user experience can tolerate:

```python
import httpx

async def get_user_recommendations(user_id: str) -> list:
    # Homepage render: user sees recommendations or nothing.
    # 200ms is enough to try; more harms the page load SLA.
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"http://recommendations-svc/user/{user_id}",
                timeout=httpx.Timeout(connect=0.05, read=0.2),
            )
            resp.raise_for_status()
            return resp.json()["items"]
    except (httpx.TimeoutException, httpx.HTTPStatusError, httpx.RequestError):
        return []  # Empty list → homepage renders without recommendations
```

If the recommendation service cannot respond in 200ms, it is already unhealthy — waiting longer does not help users, it harms them. The connect timeout (50ms) catches DNS and network failures early. The read timeout (200ms) catches slow responses. Each should be tuned independently.

## Signaling Degraded State Without Breaking Contracts

Clients should be able to distinguish "empty result" from "degraded response." Two conventions that work in practice:

**HTTP header approach** — works well for proxies and centralized logging:
```
X-Service-Degraded: recommendations
X-Degraded-Reason: upstream-timeout
```

**Response envelope approach** — works for clients that need to render differently:
```json
{
  "data": [],
  "meta": {
    "degraded": true,
    "degraded_services": ["recommendations"],
    "fallback": "empty"
  }
}
```

Either way, record every degraded response in your metrics with labels for the affected service and degradation type. `degraded_responses_total{service="recommendations", reason="timeout"}` gives you a real-time signal during incidents and a baseline for capacity planning.

## The Kill Switch

Graceful degradation does not eliminate the incident. It buys time. When a dependency is flapping — recovering briefly every few minutes — a circuit breaker oscillates in and out of OPEN state, creating unpredictable behavior. A manual kill switch is more valuable than you might expect:

```python
# In your config or feature flag store:
# degraded_mode:recommendations = false  (normal)
# degraded_mode:recommendations = true   (forced degraded)

def get_recommendations(user_id: str) -> list:
    if feature_flags.get("degraded_mode:recommendations"):
        return []  # Manually forced degraded

    return circuit_breaker.call(
        fn=lambda: recommendations_client.fetch(user_id),
        fallback=lambda: [],
    )
```

A single config change that forces degraded mode for one service beats an hour on an incident bridge waiting for a flapping upstream to stabilize.

## What Your Runbook Needs

For each non-critical dependency:

1. Which features degrade and how (empty, stale, default value)
2. Which metric signals that degraded mode is active
3. How to manually force degraded mode
4. Acceptable duration in degraded mode before escalating to the owning team

Without this documentation, engineers on call at 3 AM will make the wrong call — blocking on the downstream service instead of cutting it out.

## The One Thing to Do Today

Pick the three most critical user-facing flows in your service. For each one, trace every downstream call and classify it: hard requirement, best-effort, cached-acceptable, or fire-and-forget. For anything classified "best-effort" that currently has no fallback, add an explicit timeout and an empty return path. That work takes an afternoon and will prevent your next incident from taking down features that had nothing to do with the service that actually failed.
