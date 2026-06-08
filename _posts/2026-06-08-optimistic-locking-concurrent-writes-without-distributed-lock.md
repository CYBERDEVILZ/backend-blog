---
layout: post
title: "Optimistic Locking for Concurrent Writes Without a Distributed Lock"
date: 2026-06-08
tags: [postgresql, concurrency, distributed-systems, database]
read_time: 12
---

Your inventory service starts losing stock counts under load. Orders succeed, stock decrements happen, but the final number is wrong. You add logging, confirm the reads and writes are happening — and then you find it: two requests read the same row at the same time, both see `quantity = 5`, both decrement by 1, and both write `quantity = 4`. One decrement was lost.

You reach for a distributed lock. Redis `SET NX EX`, a lock key per SKU, acquire-update-release. It works in staging. In production, under 2,000 RPS, the locks become a bottleneck: tail latency spikes, timeouts cascade, and now you have a new failure mode — lock acquisition failures that must be retried, and a Redis instance that is now in the critical path of every order.

Optimistic locking solves lost updates without any external coordination.

## The Core Idea

Optimistic locking assumes conflicts are rare. Instead of preventing concurrent access, it detects conflicts at write time and lets the caller retry. The mechanism is a version counter on every row.

```sql
CREATE TABLE inventory (
    sku_id     UUID PRIMARY KEY,
    quantity   INT NOT NULL CHECK (quantity >= 0),
    version    INT NOT NULL DEFAULT 0
);
```

Every write includes the version the caller read. The database only applies the write if the version still matches. If another writer committed between your read and your write, the version won't match, and your update returns zero rows.

```sql
-- Read phase
SELECT quantity, version FROM inventory WHERE sku_id = $1;
-- Returns: quantity=5, version=42

-- Write phase — only succeeds if no one else committed since your read
UPDATE inventory
   SET quantity = $new_qty,
       version  = version + 1
 WHERE sku_id = $1
   AND version = $expected_version;  -- $expected_version = 42
-- Returns rowcount=0 if someone else updated first
```

If `rowcount = 0`, you re-read and retry the entire operation. No lock was held, no external service was consulted, and the database did all the work with a single index lookup on the WHERE clause.

## Why This Is Safer Than It Looks

The correctness guarantee comes from the database's own transaction isolation. The `UPDATE ... WHERE version = $v` is an atomic test-and-set at the row level. Even if two processes submit this query simultaneously, the database serializes the writes to the row — only one will see `rowcount = 1`. The other will see `rowcount = 0` and must retry.

This works correctly under `READ COMMITTED` (PostgreSQL's default). You don't need `SERIALIZABLE` or `REPEATABLE READ`.

What it doesn't protect against is the **ABA problem**: version goes from 42 → 43 → 42 again. In practice this doesn't happen with a monotonically incrementing counter, but if you ever use a non-monotonic identifier (a timestamp, for example), be aware. Use integers.

## Production Implementation in Python

```python
import psycopg2
from psycopg2.extras import RealDictCursor

MAX_RETRIES = 5

def decrement_stock(conn, sku_id: str, qty: int) -> dict:
    for attempt in range(MAX_RETRIES):
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Read current state
            cur.execute(
                "SELECT quantity, version FROM inventory WHERE sku_id = %s FOR NO KEY UPDATE SKIP LOCKED",
                (sku_id,)
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"SKU {sku_id} not found")
            if row["quantity"] < qty:
                raise ValueError(f"Insufficient stock: have {row['quantity']}, need {qty}")

            new_qty = row["quantity"] - qty
            expected_version = row["version"]

            # Conditional write
            cur.execute(
                """
                UPDATE inventory
                   SET quantity = %s,
                       version  = version + 1
                 WHERE sku_id = %s
                   AND version = %s
                RETURNING version
                """,
                (new_qty, sku_id, expected_version)
            )
            updated = cur.fetchone()
            conn.commit()

            if updated is not None:
                return {"sku_id": sku_id, "new_quantity": new_qty, "version": updated["version"]}

            # Conflict — retry
            conn.rollback()

    raise RuntimeError(f"Optimistic lock failed after {MAX_RETRIES} retries for SKU {sku_id}")
```

A few things worth noting here:

**`FOR NO KEY UPDATE SKIP LOCKED`** on the read is optional but useful under high contention. Without it, two transactions can read the same row and both proceed to the UPDATE — one wins, one retries. With `SKIP LOCKED`, a transaction that can't immediately acquire a row-level advisory skip locks will move on rather than wait; this is useful for queue-like workloads. For pure optimistic locking with retries, you can drop it entirely.

**`conn.rollback()` before retry** is required. If you don't reset the transaction state, subsequent queries in the same connection may still be in a failed or inconsistent transaction block.

**Retry count matters.** Five retries is enough for hot rows under 2,000 RPS. Under extreme write concurrency (hundreds of processes competing for the same row), optimistic locking degrades — every retry consumes a full round trip, and the effective throughput of the hot row is bounded by the database's single-row update rate. At that scale, a queue or serialized actor model (one worker owns updates for a given key) performs better.

## Detecting Real Contention

Add a counter to understand whether retries are normal background noise or a signal of pathological contention:

```python
import time
from prometheus_client import Counter, Histogram

optimistic_retries = Counter(
    "inventory_optimistic_lock_retries_total",
    "Optimistic lock retries",
    ["sku_id"]
)
optimistic_latency = Histogram(
    "inventory_decrement_duration_seconds",
    "Time to complete a stock decrement including retries"
)

def decrement_stock_instrumented(conn, sku_id: str, qty: int) -> dict:
    start = time.monotonic()
    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            optimistic_retries.labels(sku_id=sku_id).inc()
        try:
            result = _try_decrement(conn, sku_id, qty)
            optimistic_latency.observe(time.monotonic() - start)
            return result
        except ConflictError:
            continue
    raise RuntimeError(f"Exceeded retries for {sku_id}")
```

A healthy system will show retry rates below 5%. If a single SKU is consistently above 20%, that row is a bottleneck. Candidates for relief: pre-aggregated reservation pools, a per-SKU queue, or coalescing multiple reservations into batched updates.

## When to Use It and When Not To

Optimistic locking is the right tool when:

- Conflicts are genuinely rare (< 10% of writes)
- Your write logic fits in a single `UPDATE` with a version predicate
- You can tolerate retries and have idempotent read-modify-write logic

It's the wrong tool when:

- A single row is written hundreds of times per second by competing processes (high contention flattens throughput)
- Your write operation spans multiple rows or tables and you need all-or-nothing semantics with conflict detection (use `SERIALIZABLE` isolation or explicit locking instead)
- You can't retry — for example, a payment operation that already sent an external request before detecting the conflict

For multi-row operations, wrap everything in a transaction and use `SELECT ... FOR UPDATE` on all rows you intend to modify, in a consistent order. Optimistic locking's version predicate on a single row does not compose cleanly across multiple tables without careful design.

## The Version Column Across Services

If you're exposing resources over a REST API, surface the version in the response and require it on writes:

```
GET /inventory/sku-abc
{
  "sku_id": "sku-abc",
  "quantity": 5,
  "version": 42
}

PUT /inventory/sku-abc
{
  "quantity": 4,
  "version": 42        ← client echoes back what it read
}

409 Conflict
{
  "error": "version_conflict",
  "message": "Resource was modified since you last read it. Re-fetch and retry."
}
```

The `409 Conflict` response tells the client exactly what happened and what to do. Compare this to a pessimistic lock timeout, which typically returns a generic `503` or `500` with no actionable information.

HTTP ETags are the standardized header form of this pattern — the ETag is a version token, and `If-Match` on a `PUT` is the conditional write predicate.

## Takeaway

Before reaching for a distributed lock, try a version column and a conditional `UPDATE ... WHERE version = $v`. Check the rowcount. If it's zero, re-read and retry. The database provides the coordination, the failure mode is a retry rather than a lock timeout, and your critical path stays inside a single SQL statement. Measure your retry rate in production — if it stays below 5%, you've solved the lost-update problem without adding any new infrastructure.
