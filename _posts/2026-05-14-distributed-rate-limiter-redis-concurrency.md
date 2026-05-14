---
layout: post
title: "Building a Distributed Rate Limiter with Redis That Handles Concurrency Correctly"
date: 2026-05-14
tags: [redis, rate-limiting, distributed-systems, backend]
read_time: 9
---

Your rate limiter is broken. Not obviously broken — it passes every unit test and holds up fine in staging. But under real production load with multiple API replicas, users are blasting through their limits while your counters say they shouldn't be. You're getting abuse reports. The naive implementation that worked on one instance falls apart the moment you scale horizontally.

Here's why, and how to fix it.

## Why Simple Counters Fail Under Concurrency

The most common implementation people reach for first:

```python
def is_rate_limited(user_id: str, limit: int, window_seconds: int) -> bool:
    key = f"rate:{user_id}"
    count = redis.get(key)
    if count is None:
        redis.setex(key, window_seconds, 1)
        return False
    if int(count) >= limit:
        return True
    redis.incr(key)
    return False
```

This has a textbook race condition. Two requests arrive at the same millisecond. Both read `count = 49` (limit is 50). Both decide they're under the limit. Both call `incr`. Now the counter is 51 and both requests went through. At high throughput, this race fires constantly.

The fix most engineers reach for next — wrapping it in a Lua script or using `INCR` directly — solves the atomicity problem, but introduces a different one: the fixed window. A user can send 100 requests in the last second of one window and 100 in the first second of the next, effectively getting 200 requests in a two-second span with no throttling.

## The Sliding Window Log Algorithm

A proper sliding window tracks *when* each request happened, not just how many:

```python
import time
import redis

r = redis.Redis()

def is_rate_limited(user_id: str, limit: int, window_seconds: int) -> bool:
    key = f"rate:log:{user_id}"
    now = time.time()
    window_start = now - window_seconds

    pipe = r.pipeline()
    # Remove timestamps outside the window
    pipe.zremrangebyscore(key, 0, window_start)
    # Count remaining entries in window
    pipe.zcard(key)
    # Add current request timestamp
    pipe.zadd(key, {str(now): now})
    # Expire the key slightly after window to auto-cleanup
    pipe.expire(key, window_seconds + 1)
    results = pipe.execute()

    request_count = results[1]
    return request_count >= limit
```

The sorted set stores request timestamps as both member and score. `ZREMRANGEBYSCORE` evicts anything outside the current window, `ZCARD` gives an accurate count, and the whole pipeline executes atomically.

**The catch**: memory. Each request creates one entry in the sorted set. At 1,000 requests per second per user, that's 1,000 entries per user per second. With thousands of users, this becomes significant. For most APIs this is fine; for very high throughput with large windows, it's not.

## The Sliding Window Counter: Best of Both Worlds

The sliding window counter approximates the sliding log at a fraction of the memory cost. It uses two fixed-window buckets and weights them by how far you are through the current window:

```python
import time
import redis
import math

r = redis.Redis()

def is_rate_limited(user_id: str, limit: int, window_seconds: int) -> bool:
    now = time.time()
    current_window = math.floor(now / window_seconds)
    previous_window = current_window - 1

    current_key = f"rate:{user_id}:{current_window}"
    previous_key = f"rate:{user_id}:{previous_window}"

    # How far into the current window are we? (0.0 to 1.0)
    window_offset = (now % window_seconds) / window_seconds

    pipe = r.pipeline()
    pipe.get(current_key)
    pipe.get(previous_key)
    results = pipe.execute()

    current_count = int(results[0] or 0)
    previous_count = int(results[1] or 0)

    # Weight the previous window by how much of it overlaps the sliding window
    weighted_count = previous_count * (1 - window_offset) + current_count

    if weighted_count >= limit:
        return True

    # Atomically increment current window counter
    pipe2 = r.pipeline()
    pipe2.incr(current_key)
    pipe2.expire(current_key, window_seconds * 2)
    pipe2.execute()

    return False
```

If you're 30% into the current window, 70% of the previous window's requests still fall within the sliding window. The approximation error is at most `rate * (1 - window_offset)` — in practice, less than 1% for typical window sizes.

This is what Cloudflare's rate limiting uses internally. It's O(1) memory per user (two keys) and handles millions of users efficiently.

## Making Increment Atomic with Lua

The sliding window counter above has a subtle race: the read and increment aren't atomic. Under high concurrency, two requests can both read `weighted_count = 99` (limit 100) and both increment. Use a Lua script to make the check-and-increment atomic:

```lua
-- rate_limit.lua
local current_key = KEYS[1]
local previous_key = KEYS[2]
local limit = tonumber(ARGV[1])
local window_seconds = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local window_offset = tonumber(ARGV[4])

local current_count = tonumber(redis.call('GET', current_key) or 0)
local previous_count = tonumber(redis.call('GET', previous_key) or 0)

local weighted = previous_count * (1 - window_offset) + current_count

if weighted >= limit then
    return 0  -- rate limited
end

redis.call('INCR', current_key)
redis.call('EXPIRE', current_key, window_seconds * 2)
return 1  -- allowed
```

```python
import time
import math
import redis

r = redis.Redis()

with open("rate_limit.lua") as f:
    LUA_SCRIPT = f.read()

rate_limit_script = r.register_script(LUA_SCRIPT)

def is_rate_limited(user_id: str, limit: int, window_seconds: int) -> bool:
    now = time.time()
    current_window = math.floor(now / window_seconds)
    previous_window = current_window - 1
    window_offset = (now % window_seconds) / window_seconds

    result = rate_limit_script(
        keys=[
            f"rate:{user_id}:{current_window}",
            f"rate:{user_id}:{previous_window}",
        ],
        args=[limit, window_seconds, now, window_offset],
    )
    return result == 0
```

Redis executes Lua scripts atomically — no other commands run between the GET and INCR. This is the correct implementation.

## Handling Redis Failures Safely

What happens when Redis goes down? You have two options, and you need to decide which failure mode you want:

**Fail open** (allow traffic): prefer this for most internal APIs and user-facing features where a brief surge is acceptable. Downtime should never block all users.

**Fail closed** (deny traffic): prefer this for payment APIs, authentication endpoints, or anywhere abuse has real financial consequence.

```python
def is_rate_limited(user_id: str, limit: int, window_seconds: int) -> bool:
    try:
        return _check_redis_rate_limit(user_id, limit, window_seconds)
    except redis.RedisError:
        # Fail open: log the error, allow the request
        logger.error("Rate limiter Redis unavailable, failing open", extra={"user_id": user_id})
        return False
        # Fail closed: return True here instead
```

Set a tight socket timeout on the Redis connection (50–100ms). A rate limiter that adds 500ms to every request is worse than no rate limiter.

```python
r = redis.Redis(
    host="redis-host",
    socket_connect_timeout=0.1,
    socket_timeout=0.05,
    retry_on_timeout=False,
)
```

## Tracking Rate Limit Headers

Return rate limit state in response headers so clients can back off intelligently. The standard is `RateLimit-Limit`, `RateLimit-Remaining`, and `RateLimit-Reset`:

```python
def get_rate_limit_headers(user_id: str, limit: int, window_seconds: int) -> dict:
    now = time.time()
    current_window = math.floor(now / window_seconds)
    previous_window = current_window - 1
    window_offset = (now % window_seconds) / window_seconds

    current_count = int(r.get(f"rate:{user_id}:{current_window}") or 0)
    previous_count = int(r.get(f"rate:{user_id}:{previous_window}") or 0)
    weighted = previous_count * (1 - window_offset) + current_count

    remaining = max(0, limit - int(weighted))
    reset_at = (current_window + 1) * window_seconds

    return {
        "RateLimit-Limit": str(limit),
        "RateLimit-Remaining": str(remaining),
        "RateLimit-Reset": str(int(reset_at)),
        "X-RateLimit-Policy": f"{limit};w={window_seconds}",
    }
```

## What to Instrument

A rate limiter with no visibility is a liability. Track these:

- `rate_limit.checked` — every call (counter by user tier or endpoint)
- `rate_limit.limited` — every rejection (counter; alert if > 5% of traffic on non-abuse paths)
- `rate_limit.redis_error` — Redis unavailability events
- Redis command latency for `EVALSHA` (your Lua script calls) — p99 should be under 5ms

If you're seeing the limited percentage spike for a specific `user_id`, that's abuse. If it spikes across many users, you've misconfigured limits or have a traffic incident.

## The Actionable Takeaway

Replace any rate limiter that uses `GET` + conditional `SET`/`INCR` with an atomic Lua script. Use the sliding window counter algorithm (two fixed-window keys, weighted by window position) for O(1) memory with accurate approximation. Set a 50ms Redis socket timeout and decide explicitly whether to fail open or closed — don't let an exception handler make that choice for you implicitly by propagating the error to the caller.
