---
layout: post
title: "Multi-Tenant SaaS Rate Limiting and Quota Systems"
date: 2026-06-07
tags: [rate-limiting, redis, postgresql, saas]
read_time: 11
---

Your on-call page fires at 2 AM: API response times have jumped from 50ms to 4 seconds. You dig in and find one tenant is hammering your webhook processing endpoint with 50,000 requests per minute—a runaway retry loop on their end. Every other tenant is degraded. You had a global rate limiter, but it was set to 100,000 req/min across all tenants. That one tenant consumed it all.

This is the defining failure mode of multi-tenant rate limiting: treating all tenants as a single pool. The fix isn't just adding a per-tenant limit—it's building a system that enforces isolation, tracks quotas across billing periods, and doesn't fall apart under burst traffic or Redis failures.

## Why Per-Tenant Limits Are Not Enough

Most teams start with a simple Redis counter per tenant. That handles burst rate limiting—"tenant X can send at most N requests per second." But SaaS products usually have two distinct constraints:

**Rate limits**: Requests per second/minute. Short window, prevents traffic storms.  
**Quotas**: Total requests per day/month. Billing constraint, tracks consumption.

These require different data structures and different enforcement strategies. A token bucket handles rate limits. Quotas need a persistent counter that survives Redis restarts, resets on a billing schedule, and can be audited when a customer disputes their invoice.

Conflating them produces systems that either over-throttle paying customers or silently let quota violations slide until billing reconciliation.

## The Token Bucket for Per-Tenant Rate Limiting

The token bucket algorithm is the right model for burst-tolerant rate limiting. Each tenant starts with a full bucket of N tokens. Each request consumes one token. Tokens refill at a fixed rate. Burst capacity is the bucket size; sustained throughput is the refill rate.

In Redis, the critical requirement is atomicity—read and write must happen together. A Lua script running on Redis is the right primitive:

```python
import redis
import time
import math

RATE_LIMIT_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])       -- max tokens (bucket size)
local rate = tonumber(ARGV[3])        -- tokens refilled per second
local requested = tonumber(ARGV[4])  -- cost of this request

-- Read current state: tokens remaining and last refill timestamp
local state = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local last_ts = tonumber(state[2])

if tokens == nil then
    tokens = burst
    last_ts = now
end

-- Refill based on elapsed time since last request
local elapsed = now - last_ts
local refill = math.floor(elapsed * rate)
tokens = math.min(burst, tokens + refill)
-- Advance ts only by the time consumed by actual refill
last_ts = last_ts + math.floor(refill / rate)

-- Consume tokens or deny
if tokens >= requested then
    tokens = tokens - requested
    redis.call('HMSET', key, 'tokens', tokens, 'ts', last_ts)
    redis.call('EXPIRE', key, math.ceil(burst / rate) * 2)
    return {1, tokens}  -- allowed; second value is remaining tokens
else
    redis.call('HMSET', key, 'tokens', tokens, 'ts', last_ts)
    redis.call('EXPIRE', key, math.ceil(burst / rate) * 2)
    return {0, tokens}  -- denied
end
"""

class TenantRateLimiter:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.script = redis_client.register_script(RATE_LIMIT_SCRIPT)

    def check(self, tenant_id: str, burst: int, rate: float, cost: int = 1):
        key = f"rl:tenant:{tenant_id}"
        allowed, remaining = self.script(
            keys=[key],
            args=[time.time(), burst, rate, cost]
        )
        return bool(allowed), int(remaining)
```

The Lua script runs atomically in Redis. Without this, two concurrent requests from the same tenant can both see sufficient tokens and both be permitted—meaning your burst limit is meaningless under load.

Set `burst` to 2–5x the per-second `rate`. This lets a tenant fire a burst of requests after a quiet period without hitting limits, while still bounding sustained throughput. A tenant on a 100 rps plan with a burst of 300 can absorb a 3-second spike from a retry storm before throttling kicks in.

## Quota Tracking with PostgreSQL

Rate limits are operational—a brief spike over the limit is recoverable. Quotas are contractual—a tenant on the Starter plan genuinely cannot exceed 10,000 API calls per month.

Track quotas in PostgreSQL, not Redis. You need durability, auditability, and the ability to query usage for billing. Redis is suitable for caching the quota state to avoid hitting Postgres on every request, not for owning the source of truth.

```sql
-- Core quota table, one row per tenant per billing period
CREATE TABLE tenant_quotas (
    tenant_id     UUID    NOT NULL,
    period_start  DATE    NOT NULL,  -- first day of the billing month
    api_calls     BIGINT  NOT NULL DEFAULT 0,
    limit_calls   BIGINT  NOT NULL,
    PRIMARY KEY (tenant_id, period_start)
);

CREATE INDEX ON tenant_quotas (tenant_id, period_start DESC);

-- Single upsert increments the counter and returns the new total.
-- Safe under concurrent writes: ON CONFLICT acquires a row lock.
INSERT INTO tenant_quotas (tenant_id, period_start, api_calls, limit_calls)
VALUES ($1, date_trunc('month', now())::date, 1, $2)
ON CONFLICT (tenant_id, period_start)
DO UPDATE SET api_calls = tenant_quotas.api_calls + 1
RETURNING api_calls, limit_calls;
```

For high-throughput tenants, calling this upsert on every request creates write contention on the same row. The fix is buffered writes: accumulate increments in Redis for a few seconds, then flush to Postgres in batches.

```python
import asyncio

async def increment_quota_buffered(redis_client, tenant_id: str, count: int = 1):
    """Buffer quota increments in Redis; a background job flushes to Postgres."""
    buffer_key = f"quota:buffer:{tenant_id}"
    await redis_client.incrby(buffer_key, count)
    await redis_client.expire(buffer_key, 300)  # safety TTL if flush job dies

async def flush_quota_buffers(redis_client, pg_pool, get_tenant_limit):
    """Run every 5-10 seconds as a background task."""
    async for key in redis_client.scan_iter("quota:buffer:*"):
        tenant_id = key.split(":")[-1]
        # GETDEL reads and deletes atomically—concurrent flushes won't double-count
        buffered = await redis_client.getdel(key)
        if not buffered or int(buffered) == 0:
            continue
        async with pg_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO tenant_quotas (tenant_id, period_start, api_calls, limit_calls)
                VALUES ($1, date_trunc('month', now())::date, $2, $3)
                ON CONFLICT (tenant_id, period_start)
                DO UPDATE SET api_calls = tenant_quotas.api_calls + EXCLUDED.api_calls
            """, tenant_id, int(buffered), get_tenant_limit(tenant_id))
```

`GETDEL` is the key primitive here. A `GET` followed by `DEL` has a race: a write between those two commands loses increments. `GETDEL` is atomic and eliminates that window.

## Three-Tier Enforcement

Naively checking Postgres on every API request adds 5–20ms of latency and collapses under load. The right enforcement model is a three-tier check:

**Tier 1 — Redis cache**: Cache each tenant's current usage with a 60-second TTL. Check this first. If `cached_usage + 1 > limit`, reject with 429 immediately. A cache miss falls through to Tier 2.

**Tier 2 — Postgres read**: On cache miss, read the actual row, populate the cache, then enforce. This runs rarely—only when cache entries expire.

**Tier 3 — Async grace window**: Accept the request, increment the buffer asynchronously, and hard-reject only when usage exceeds the limit by a small margin (typically 1–5%). With buffered writes, the cache is always slightly stale; a tenant sitting at 9,999 out of 10,000 will have some requests accepted before the flush runs.

The grace window prevents a specific support ticket: "Your API rejected my request but your dashboard says I only used 9,998 calls." Set the hard cutoff at 10,500 for a 10,000-call plan. Document it as a 500-call burst allowance if you want. The alternative is a flood of billing disputes every month.

## Redis Failure Modes

Your rate limiter sits in the hot path of every API request. If Redis is down and you fail closed (reject all requests), your API availability is now bounded by Redis availability. If you fail open (allow everything), your limits are useless during the outage.

Separate the failure behavior by constraint type:

- **Rate limits**: fail open. During a Redis outage you're already degraded; accepting more traffic is better than taking down your API entirely. Alert on the Redis failure.
- **Quotas**: fall back to a direct Postgres check. Slower, but correct. If Postgres is also unavailable, return 503—you cannot safely make a billing decision without the current count.

## Storing Limits in Postgres

Hardcoding tier limits in config files breaks down when you have enterprise customers with custom contracts. Store limits with the tenant:

```sql
CREATE TABLE tenant_plans (
    tenant_id        UUID PRIMARY KEY,
    plan_name        TEXT   NOT NULL,
    rate_limit_rps   INT    NOT NULL,   -- sustained requests per second
    rate_limit_burst INT    NOT NULL,   -- burst capacity (tokens)
    quota_monthly    BIGINT NOT NULL    -- total monthly API calls
);
```

Load this into a local in-process cache on startup and refresh every 5 minutes. Never query this table inline on hot paths—a misconfigured index or a plan change running `UPDATE tenant_plans SET ...` on 50,000 rows will lock your API.

To temporarily increase a tenant's limits for a big demo or incident recovery, update the row and manually invalidate the cache via an internal endpoint. The 5-minute refresh cycle is fine for normal operations; force-refresh is for emergencies.

## The Actionable Takeaway

If you have a global rate limiter today and are migrating to per-tenant enforcement: deploy the Redis Lua token bucket first and load-test it with concurrent requests hitting the same tenant ID—atomicity failures appear immediately as overage under load. Then add the PostgreSQL quota table with buffered writes, wire up the three-tier check, and set your hard cutoff 1–5% above the plan limit to absorb flush lag.

The piece most teams skip is the audit trail. When a tenant disputes their bill, you need per-tenant usage by day. Build the Postgres quota table on day one, even before you enforce it. You cannot reconstruct historical usage from Redis keys that expired three months ago.
