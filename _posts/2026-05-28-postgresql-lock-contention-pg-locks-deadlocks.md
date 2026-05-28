---
layout: post
title: "Lock Contention in PostgreSQL: pg_locks, Deadlocks, and Patterns to Avoid"
date: 2026-05-28
tags: [postgresql, performance, concurrency, backend]
read_time: 12
---

Your deploy window is 11 PM on a Tuesday. You run `ALTER TABLE orders ADD COLUMN processed_at TIMESTAMPTZ`. The command hangs. Your monitoring shows a spike in `waiting` queries. Within 90 seconds, your connection pool is exhausted and the service is down—not because the migration broke anything, but because a single `AccessExclusiveLock` queued behind a long-running read, and every subsequent query stacked behind it waiting for that lock.

This is PostgreSQL lock contention. It is quiet until it is catastrophic.

## How PostgreSQL Locking Actually Works

Every SQL statement acquires one or more locks. Most are weak locks that only conflict with DDL—a `SELECT` takes a `AccessShareLock`, an `UPDATE` takes a `RowExclusiveLock`. These coexist fine. The problem is when you introduce a lock that conflicts with something already held, and something else conflicts with that.

PostgreSQL does not skip the queue. If a lock request cannot be granted immediately, it waits. Everything behind it also waits, regardless of whether those later requests would conflict with the held lock. This **lock queue effect** means a brief DDL operation can cause a minutes-long outage on a busy table.

Lock hierarchy from weakest to strongest (simplified):

```
AccessShareLock       — SELECT
RowShareLock          — SELECT FOR SHARE
RowExclusiveLock      — INSERT, UPDATE, DELETE
ShareUpdateExclusiveLock — VACUUM, CREATE INDEX CONCURRENTLY
ShareLock             — CREATE INDEX (non-concurrent)
ExclusiveLock         — (rare, application-level)
AccessExclusiveLock   — ALTER TABLE, DROP TABLE, TRUNCATE, LOCK TABLE
```

`AccessExclusiveLock` conflicts with everything. `RowExclusiveLock` conflicts only with `ShareLock` and stronger.

## Diagnosing with pg_locks

When you suspect lock contention, the first step is `pg_locks` joined with `pg_stat_activity`:

```sql
SELECT
    blocked.pid                   AS blocked_pid,
    blocked.query                 AS blocked_query,
    blocked.wait_event_type       AS wait_type,
    blocked.wait_event,
    blocking.pid                  AS blocking_pid,
    blocking.query                AS blocking_query,
    blocking.state                AS blocking_state,
    now() - blocking.query_start  AS blocking_duration
FROM  pg_stat_activity blocked
JOIN  pg_locks         bl  ON  bl.pid = blocked.pid AND NOT bl.granted
JOIN  pg_locks         kl  ON  kl.transactionid = bl.transactionid
                            OR (kl.relation = bl.relation AND kl.locktype = bl.locktype)
JOIN  pg_stat_activity blocking ON blocking.pid = kl.pid AND kl.granted
WHERE blocked.pid != blocking.pid;
```

This shows you the lock chain: who is blocked, who is blocking them, and how long the blocker has been running. A blocker that has been running for 30 minutes holding a `RowExclusiveLock` is your first target.

For deadlocks specifically, PostgreSQL logs them automatically at `log_min_messages = log`. Look for lines like:

```
ERROR:  deadlock detected
DETAIL:  Process 12345 waits for ShareLock on transaction 7890;
         blocked by process 67890.
         Process 67890 waits for ShareLock on transaction 12345;
         blocked by process 12345.
HINT:  See server log for query details.
```

The `DETAIL` block gives you the exact process IDs and transaction IDs involved. Combine with `pg_stat_activity` snapped at that moment (or from a slow-query log) to reconstruct which queries caused the cycle.

## The Three Lock Anti-Patterns

### 1. DDL Without a Lock Timeout

Running `ALTER TABLE` without a timeout means it will wait indefinitely for an `AccessExclusiveLock`. On a busy table, that wait can be long—and every query arriving after it queues behind it.

**Wrong:**
```sql
ALTER TABLE orders ADD COLUMN notes TEXT;
```

**Right:**
```sql
SET lock_timeout = '2s';
ALTER TABLE orders ADD COLUMN notes TEXT;
```

If the lock cannot be acquired within 2 seconds, the command fails. You retry during a quieter window rather than silently blocking your entire application. Pair this with `statement_timeout` in your application connection settings so rogue queries do not become indefinite blockers themselves.

### 2. Long Transactions Holding Row Locks

An `UPDATE` acquires `RowExclusiveLock` on affected rows. That lock is held until the transaction commits or rolls back. If your application starts a transaction, updates a row, then does something slow (an HTTP call, a job queue push, a sleep), that lock is held for the duration of that slow thing.

Pattern that causes this:

```python
# BAD: HTTP call inside a transaction holding row locks
with db.transaction():
    order = db.execute("UPDATE orders SET status='processing' WHERE id=%s RETURNING *", [order_id]).fetchone()
    # This HTTP call may take 5-30 seconds
    payment_result = payment_client.charge(order["amount"])
    db.execute("UPDATE orders SET payment_id=%s WHERE id=%s", [payment_result["id"], order_id])
```

Any concurrent `SELECT FOR UPDATE` or `UPDATE` on the same order row blocks for the duration of that HTTP call.

**Fix: minimize what lives inside the transaction.**

```python
# GOOD: fetch outside transaction, minimal critical section
order = db.execute("SELECT * FROM orders WHERE id=%s", [order_id]).fetchone()
payment_result = payment_client.charge(order["amount"])

# Only the database work lives in the transaction
with db.transaction():
    updated = db.execute(
        "UPDATE orders SET status='processing', payment_id=%s "
        "WHERE id=%s AND status='pending'",  # optimistic guard
        [payment_result["id"], order_id]
    ).rowcount
    if updated == 0:
        payment_client.refund(payment_result["id"])  # compensate
        raise OrderAlreadyProcessed()
```

The lock is now held for microseconds, not seconds.

### 3. Lock Ordering Inconsistency Leading to Deadlocks

Deadlocks happen when two transactions each hold a lock the other needs. The classic form:

```
Transaction A: UPDATE accounts SET balance = balance - 100 WHERE id = 1
Transaction B: UPDATE accounts SET balance = balance - 100 WHERE id = 2
Transaction A: UPDATE accounts SET balance = balance + 100 WHERE id = 2  -- waits for B
Transaction B: UPDATE accounts SET balance = balance + 100 WHERE id = 1  -- waits for A → deadlock
```

PostgreSQL detects this and aborts one transaction. But your application must handle the `ERROR 40P01: deadlock detected` and retry. Most ORM layers do not do this automatically.

**Structural fix: always acquire locks in a consistent order.**

```python
def transfer(from_id: int, to_id: int, amount: Decimal):
    # Sort IDs to ensure consistent lock acquisition order
    first_id, second_id = sorted([from_id, to_id])

    with db.transaction():
        # Lock in sorted order — deadlock is now impossible between two transfers
        accounts = db.execute(
            "SELECT id, balance FROM accounts WHERE id = ANY(%s) FOR UPDATE",
            [[first_id, second_id]]
        ).fetchall()

        by_id = {a["id"]: a for a in accounts}
        if by_id[from_id]["balance"] < amount:
            raise InsufficientFunds()

        db.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", [amount, from_id])
        db.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", [amount, to_id])
```

By locking both rows in a deterministic order, two concurrent transfers can never create a cycle regardless of which direction they move money.

## Tuning Lock Timeouts in Production

Three settings that should be in every production PostgreSQL configuration:

```sql
-- Per-connection: abort if waiting for a lock more than N ms
SET lock_timeout = '5s';

-- Per-connection: abort if a query runs more than N ms total
SET statement_timeout = '30s';

-- Per-transaction: abort if a transaction is idle (holding locks) more than N ms
SET idle_in_transaction_session_timeout = '60s';
```

`idle_in_transaction_session_timeout` is the most commonly missed. It kills sessions that open a transaction and then disappear—a crashed application server leaving a connection with an open transaction will otherwise hold locks until the TCP keepalive fires, which can be hours.

Set these in `postgresql.conf` as defaults and override per-connection where workloads require longer windows (bulk jobs, migrations):

```ini
# postgresql.conf
lock_timeout = '5s'
statement_timeout = '30s'
idle_in_transaction_session_timeout = '60s'
```

## Monitoring Lock Wait Time

Add this query to your monitoring stack as a scheduled job. Alert if rows appear:

```sql
SELECT
    pid,
    usename,
    application_name,
    wait_event_type,
    wait_event,
    state,
    now() - state_change AS time_in_state,
    left(query, 120)     AS query_snippet
FROM pg_stat_activity
WHERE wait_event_type = 'Lock'
  AND now() - state_change > interval '5 seconds'
ORDER BY time_in_state DESC;
```

Anything waiting on a lock for more than 5 seconds in a healthy system is worth investigating. Sustained lock waits above 30 seconds are a production incident in the making.

For deadlock frequency, parse your PostgreSQL logs or use `pg_stat_database`:

```sql
SELECT datname, deadlocks, conflicts
FROM pg_stat_database
WHERE datname = current_database();
```

A deadlock counter incrementing steadily indicates a consistent lock ordering problem in application code—not an occasional race, but a structural issue.

## The One Actionable Fix for Today

If you do nothing else: add `SET lock_timeout = '3s'` to every DDL migration script you run, and add `idle_in_transaction_session_timeout = '60s'` to your `postgresql.conf`. The first prevents a migration from silently queueing behind a long-running query and taking down your service. The second kills crashed application connections that would otherwise hold locks until TCP gives up. Both can be deployed without a restart (use `ALTER SYSTEM SET` + `SELECT pg_reload_conf()`) and both eliminate the two most common sources of unexpected outages from lock contention.
