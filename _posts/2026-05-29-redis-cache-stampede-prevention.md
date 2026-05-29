---
layout: post
title: "Redis Cache Stampede: Why It Happens in Production and How to Prevent It"
date: 2026-05-29
tags: [redis, caching, production, backend]
read_time: 9
---

Your Redis cluster is running fine. Hit rate is 98%. Then a high-traffic key expires — a product listing page, a user permissions object, a popular leaderboard — and within 50ms, 400 goroutines are all executing the same database query. Database CPU spikes to 100%. Your p99 latency jumps from 20ms to 8 seconds. Some requests time out. The database falls over. Now your cache is cold, and every new request continues hitting the database.

This is a cache stampede. It's one of the most common failure modes in Redis-backed systems, and it almost always surfaces at 2am on a Friday.

## Why Standard TTL Expiry Is the Problem

The typical caching pattern looks like this:

```python
def get_product(product_id: str) -> dict:
    key = f"product:{product_id}"
    cached = redis.get(key)
    if cached:
        return json.loads(cached)

    product = db.query("SELECT * FROM products WHERE id = %s", product_id)
    redis.setex(key, 300, json.dumps(product))  # 5-minute TTL
    return product
```

This works fine under normal load. The problem is atomic expiry. When the TTL hits zero, Redis removes the key instantly. All concurrent requests that arrive in the following milliseconds see a cache miss simultaneously and race to repopulate the same key.

For a key receiving 200 requests/second, that 5-minute TTL expiry is about 200 simultaneous cache misses racing to the database. If that query takes 100ms, you've just serialized 200 connections into the database at once.

## Pattern 1: Locking with Stale Fallback

The most broadly applicable approach is a distributed mutex. One process holds the lock and repopulates the cache; others return slightly stale data while waiting.

```python
import time
import uuid

LOCK_TTL = 10  # seconds — must exceed your worst-case DB query time

def get_product_with_lock(product_id: str) -> dict:
    key = f"product:{product_id}"
    lock_key = f"lock:product:{product_id}"
    stale_key = f"stale:product:{product_id}"

    # Fast path: valid cache hit
    cached = redis.get(key)
    if cached:
        return json.loads(cached)

    lock_token = str(uuid.uuid4())
    acquired = redis.set(lock_key, lock_token, nx=True, ex=LOCK_TTL)

    if acquired:
        try:
            # Re-check under lock — another worker may have populated while we waited
            cached = redis.get(key)
            if cached:
                return json.loads(cached)

            product = db.query("SELECT * FROM products WHERE id = %s", product_id)
            pipeline = redis.pipeline()
            pipeline.setex(key, 300, json.dumps(product))
            # Stale copy lives longer so waiters have something to return
            pipeline.setex(stale_key, 600, json.dumps(product))
            pipeline.execute()
            return product
        finally:
            # Release only if we still own the lock — atomic Lua prevents a race
            # between GET and DEL if our lock expired and another process acquired it
            lua = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
            """
            redis.eval(lua, 1, lock_key, lock_token)
    else:
        # Lock held elsewhere — return stale data immediately rather than queuing
        stale = redis.get(stale_key)
        if stale:
            return json.loads(stale)

        # No stale data (cold start) — brief wait then retry
        time.sleep(0.05)
        cached = redis.get(key)
        if cached:
            return json.loads(cached)

        # Last resort: direct DB hit (rare, only on first population of a new key)
        return db.query("SELECT * FROM products WHERE id = %s", product_id)
```

Two details most implementations miss:

**Release the lock atomically with Lua.** A plain GET + DEL has a race: if your lock TTL expires between those two calls, you'll delete a lock another process just acquired. The Lua script executes as a single atomic unit.

**Always keep a stale copy.** Setting `stale_key` with a longer TTL means non-lock-holding processes serve slightly stale data instead of piling up in a sleep loop or going straight to the database.

## Pattern 2: Probabilistic Early Expiration (XFetch)

Locking handles stampedes reactively — it limits the damage once a key expires, but there's still one expensive recomputation. XFetch (from the 2015 paper "Optimal Probabilistic Cache Stampede Prevention") prevents the key from ever going cold.

The idea: as expiry approaches, each request independently decides whether to recompute, with probability weighted by how expensive the last recomputation was.

```python
import math
import random
import time

def xfetch_get(redis_client, key: str, recompute_fn, ttl: int, beta: float = 1.0):
    """
    beta: higher = more aggressive early refresh.
    1.0 is a good default; increase if recomputation is cheap and fast.
    """
    raw = redis_client.get(key)

    if raw is not None:
        value, delta, expiry = json.loads(raw)
        gap = expiry - time.time()

        # Early refresh decision: probability rises as gap shrinks.
        # delta (last recompute time in seconds) scales the aggressiveness —
        # expensive queries get refreshed earlier.
        if -delta * beta * math.log(random.random()) < gap:
            return value  # still fresh enough

    # Cache miss or lost the probabilistic draw
    t_start = time.time()
    value = recompute_fn()
    delta = time.time() - t_start  # actual recompute cost, stored with the value

    expiry = time.time() + ttl
    redis_client.setex(key, ttl, json.dumps([value, delta, expiry]))
    return value


# Usage:
def get_product(product_id: str) -> dict:
    return xfetch_get(
        redis,
        key=f"product:{product_id}",
        recompute_fn=lambda: db.query(
            "SELECT * FROM products WHERE id = %s", product_id
        ),
        ttl=300,
        beta=1.0,
    )
```

XFetch requires no locking and no coordination. Each process independently makes the refresh decision; statistical averaging means the key gets refreshed before expiry with overwhelming probability, while no single request carries all the work.

The tradeoff: two or three processes may occasionally recompute in parallel near the end of the TTL window. For most systems this is far preferable to a full stampede of hundreds.

## Pattern 3: Background Refresh

For keys that are both very expensive to recompute and read on every request — user permissions, feature flags, global configuration — remove TTL-based expiry entirely. Serve stale data unconditionally; refresh in the background on a schedule.

```python
import threading

class BackgroundRefreshCache:
    def __init__(self, redis_client, refresh_interval: int):
        self.redis = redis_client
        self.refresh_interval = refresh_interval
        self._in_flight = set()
        self._lock = threading.Lock()

    def get(self, key: str, compute_fn):
        raw = self.redis.hgetall(f"brc:{key}")

        if not raw:
            # Cold start only — compute synchronously
            value = compute_fn()
            self._store(key, value)
            return value

        value = json.loads(raw[b"value"])
        last_refresh = float(raw[b"last_refresh"])

        if time.time() - last_refresh > self.refresh_interval:
            self._trigger_refresh(key, compute_fn)

        return value  # always returns something immediately

    def _trigger_refresh(self, key: str, compute_fn):
        with self._lock:
            if key in self._in_flight:
                return
            self._in_flight.add(key)

        def refresh():
            try:
                self._store(key, compute_fn())
            finally:
                with self._lock:
                    self._in_flight.discard(key)

        threading.Thread(target=refresh, daemon=True).start()

    def _store(self, key: str, value):
        self.redis.hset(
            f"brc:{key}",
            mapping={"value": json.dumps(value), "last_refresh": time.time()},
        )
        # Safety-net expiry in case the service stops refreshing
        self.redis.expire(f"brc:{key}", self.refresh_interval * 10)
```

The tradeoff is serving data that's up to `refresh_interval` seconds stale, which is acceptable for anything that doesn't change frequently or where brief staleness doesn't cause correctness issues.

## Choosing the Right Pattern

| Scenario | Pattern |
|---|---|
| High cardinality, strict freshness | Locking + stale fallback |
| High traffic, slight staleness acceptable | XFetch |
| Shared global data, very high read rate | Background refresh |
| Any pattern, bulk cache pre-warm | Add TTL jitter |

TTL jitter deserves a mention even when stampedes on a single key aren't your concern. If you pre-warm or populate many keys at deploy time, they all expire together. Two lines of code prevent this:

```python
BASE_TTL = 300
redis.setex(key, BASE_TTL + random.randint(0, 60), value)
```

Spreading expiry events across 60 seconds eliminates correlated misses without changing anything else.

## Detecting Stampedes Before They Cause Outages

```bash
# Real-time cache miss rate (Redis 4+)
redis-cli info stats | grep -E "keyspace_(hits|misses)"

# Watch instantaneous miss ratio — alert if > 5% in a 30s window
watch -n1 "redis-cli info stats | grep keyspace"

# See which keys are expiring right now
redis-cli --no-auth-warning monitor 2>/dev/null | grep expired
```

On the database side, watch for connection count spikes that are correlated with Redis key expirations. If your Postgres `pg_stat_activity` shows a sudden burst of identical queries within a 100ms window, you've found a stampede in progress.

A useful alert: if `keyspace_misses / (keyspace_hits + keyspace_misses)` exceeds 5% over a 30-second rolling window on a key that normally has a 99%+ hit rate, page someone.

## One Thing to Do Today

Audit the three highest-traffic cache keys in your system. Add TTL jitter to each — it's two lines and zero architectural change. Then for any key that your p99 latency depends on directly, add either the XFetch wrapper or the stale-fallback locking pattern. Both are self-contained and can be dropped into an existing service in an afternoon. You don't need to rewrite your caching layer; you need to change how three specific keys behave when they expire.
