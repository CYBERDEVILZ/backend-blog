---
layout: post
title: "Sizing Thread Pools and Connection Pools for Real Workloads"
date: 2026-05-30
tags: [performance, postgresql, python, concurrency]
read_time: 11
---

Your service handles 100 req/s without complaint. You push a deploy, traffic climbs to 300 req/s, and within seconds your dashboards light up: p99 jumps to 10s, then 30s, then requests start failing. The database CPU is at 20%. No long-running queries. No locks. Your app logs say one thing: `QueuePool limit of size 10 overflow 5 reached, connection timed out`.

The database was never the problem. Your pool was.

Getting pool sizing wrong is one of the most common causes of load-dependent failures that look like database problems but aren't. The fix isn't "set a bigger number." It's understanding the relationship between concurrency, latency, and throughput—and then sizing to that relationship.

## Why Bigger Pools Backfire

The intuitive response to pool exhaustion is to increase `pool_size`. In many cases this makes things worse.

Every connection to PostgreSQL costs real resources: memory (~5–10 MB per backend), a file descriptor, and a slot in `max_connections`. PostgreSQL processes connections with one backend process per connection. When you have 200 active backends all competing for shared memory, lock tables, and the autovacuum workers, you pay a coordination tax that grows nonlinearly.

There's a well-documented PostgreSQL anti-pattern: `max_connections = 1000` with no pooler, every app thread holding a connection. Under that setup, a query that takes 5ms at low concurrency takes 40ms at high concurrency because the kernel is scheduling 1000 postgres processes on 16 cores, and every shared resource acquisition becomes a bottleneck.

The correct target for PostgreSQL is roughly **2–4× the number of CPU cores** for active connections doing CPU-bound work, slightly higher for I/O-bound workloads where backends spend time waiting on disk. For a 16-core instance: 32–64 active connections is usually the sweet spot. A connection pooler (PgBouncer in transaction mode) then multiplies that out to however many app threads you need.

## Little's Law Is the Math You Need

For any stable system: **L = λ × W**

- **L** = average number of requests in the system (concurrency)
- **λ** = throughput (requests per second)
- **W** = average time each request spends in the system (latency in seconds)

If your service handles 500 req/s and each request holds a database connection for an average of 20ms:

```
L = 500 × 0.020 = 10 connections needed on average
```

For headroom against bursts, multiply by 2–3. So 20–30 connections is a reasonable pool size for this workload, not 5 (too small) and not 200 (wastes resources, damages PostgreSQL performance).

The failure mode: engineers size pools based on peak request rate, not on the product of rate × hold time. A pool of 100 for 100 req/s sounds generous—but if each request holds a connection for 500ms (maybe it's doing a complex report query), you need 50 connections just to sustain steady state, and 100 the moment you get a burst.

## Diagnosing Your Actual Hold Time

Don't guess. Measure where your service actually spends time holding connections.

```python
import time
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine, event
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)

engine = create_engine(
    "postgresql://user:pass@host/db",
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=10,
    pool_timeout=5,  # fail fast—don't queue forever
    pool_pre_ping=True,
)

# Track how long each checkout holds a connection
@event.listens_for(engine, "checkout")
def on_checkout(dbapi_conn, connection_record, connection_proxy):
    connection_record.checkout_time = time.monotonic()

@event.listens_for(engine, "checkin")
def on_checkin(dbapi_conn, connection_record):
    hold_time = time.monotonic() - getattr(connection_record, "checkout_time", time.monotonic())
    if hold_time > 0.1:  # log connections held longer than 100ms
        logger.warning("long_connection_hold_seconds=%.3f", hold_time)
    # Emit to your metrics system
    metrics.histogram("db.connection.hold_seconds", hold_time)
```

Run this for a few days and look at the p50, p95, p99 of hold time. Plug your p95 hold time into Little's Law with your peak req/s. That's your minimum pool size before you start queueing. Add 50% overhead for safety.

## Thread Pool Sizing: A Different Problem

Thread pools for CPU-bound work and I/O-bound work have different optimal sizes.

**CPU-bound workers** (image processing, JSON parsing, crypto): you want at most `N` threads where `N` = number of CPU cores. Beyond that you pay context-switch overhead with no gain. In Python this matters less because of the GIL, but in Go and Java it matters a lot.

**I/O-bound workers** (HTTP calls to upstream services, file reads, slow queries): threads spend most of their time waiting. The optimal count is:

```
thread_count = N_cores × (1 + wait_ratio)
```

Where `wait_ratio` = (time waiting for I/O) / (time actually computing). If your handler spends 80ms waiting on an upstream API and 20ms computing, wait_ratio = 4, and your optimal thread count per core is 5. For a 4-core machine: 20 threads.

This is a starting point, not gospel. Measure actual CPU utilization. If CPU is below 60% but threads are queueing, add more. If CPU is pegged and latency is high, adding threads won't help—you've hit a compute limit.

Here's a concrete Go example that applies this to an HTTP server worker pool:

```go
package main

import (
    "context"
    "net/http"
    "runtime"
    "time"

    "golang.org/x/sync/semaphore"
)

// WorkerPool limits concurrency into the database layer.
// Sized using Little's Law: peak_rps * avg_db_hold_time_seconds * headroom_factor
type WorkerPool struct {
    sem *semaphore.Weighted
}

func NewWorkerPool(size int) *WorkerPool {
    return &WorkerPool{sem: semaphore.NewWeighted(int64(size))}
}

func (p *WorkerPool) Do(ctx context.Context, fn func()) error {
    // Acquire a slot with a timeout—don't queue forever under overload
    ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()

    if err := p.sem.Acquire(ctx, 1); err != nil {
        return err // caller returns 503, not 500—this is overload, not a bug
    }
    defer p.sem.Release(1)
    fn()
    return nil
}

func recommendedDBPoolSize(peakRPS float64, avgHoldSeconds float64) int {
    // Little's Law + 50% headroom
    needed := peakRPS * avgHoldSeconds * 1.5
    // Cap at what PostgreSQL can sanely handle
    maxSane := float64(runtime.NumCPU() * 4)
    if needed > maxSane {
        needed = maxSane
    }
    if needed < 5 {
        needed = 5
    }
    return int(needed)
}
```

The semaphore pattern is important: when the pool is exhausted, you want to return a `503 Service Unavailable` quickly rather than queue requests until they time out. Queueing under overload amplifies load—each queued request holds memory and eventually times out anyway, but it takes longer, so more requests pile up behind it. Fail fast and let the caller retry with backoff.

## PgBouncer Configuration That Actually Works

If you're running PostgreSQL directly and hitting connection limits, PgBouncer in transaction mode is the right tool. Key settings:

```ini
[databases]
mydb = host=127.0.0.1 port=5432 dbname=mydb

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 6432
auth_type = scram-sha-256
auth_file = /etc/pgbouncer/userlist.txt

pool_mode = transaction          ; one connection per transaction, not per session
max_client_conn = 1000           ; app threads can connect freely
default_pool_size = 40           ; connections TO postgres—size this with Little's Law
min_pool_size = 10               ; keep some warm
reserve_pool_size = 5            ; emergency overflow
reserve_pool_timeout = 3         ; seconds before using reserve pool
server_idle_timeout = 300        ; close idle server connections after 5min
client_idle_timeout = 60         ; disconnect idle app connections
query_wait_timeout = 10          ; fail fast if no connection available in 10s
```

`default_pool_size` is your actual PostgreSQL connection count. Size it with Little's Law based on your database query hold time, not your app thread count. `max_client_conn` is how many app threads can hold a PgBouncer connection simultaneously—set this high. The pool manages the ratio.

One gotcha: transaction mode breaks `SET LOCAL`, advisory locks, and prepared statements that span multiple queries. If your ORM uses server-side prepared statements, you'll need `pool_mode = session` and a much smaller `max_client_conn`.

## Reading the Metrics That Tell You When You're Wrong

Set these alerts:

```
# Pool utilization above 80% sustained is a warning
pool_used / pool_size > 0.8 for 5 minutes → PagerDuty warning

# Pool exhausted (timeout waiting for connection) is an immediate alert
db_connection_timeout_total rate > 0 → PagerDuty critical

# Queue depth growing means you're under-provisioned
db_pool_queue_depth > 0 sustained → investigate pool size
```

On the PostgreSQL side, `pg_stat_activity` shows you what's actually connected:

```sql
SELECT
    state,
    wait_event_type,
    wait_event,
    count(*) AS count
FROM pg_stat_activity
WHERE datname = 'mydb'
GROUP BY state, wait_event_type, wait_event
ORDER BY count DESC;
```

If you see many connections in `idle in transaction`, your application is opening transactions and not committing promptly—these hold connections and database locks the entire time. Fix the application code; increasing pool size just lets more of them pile up.

## The Actionable Takeaway

Before you touch any pool size configuration: measure your average database connection hold time in production using checkout/checkin instrumentation. Multiply your peak req/s by that hold time. That's your minimum pool size. Double it for headroom, cap it at 4× your PostgreSQL CPU cores (routing through PgBouncer if needed), and configure your pool to fail fast on exhaustion with a 2–5 second timeout rather than queueing indefinitely. A pool that fails fast under overload is a pool that recovers when the spike passes; one that queues turns a 30-second spike into a 5-minute outage.
