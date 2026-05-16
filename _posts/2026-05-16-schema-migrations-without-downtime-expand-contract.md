---
layout: post
title: "Schema Migrations Without Downtime: The Expand-Contract Pattern on Large Tables"
date: 2026-05-16
tags: [postgresql, migrations, database, backend]
read_time: 9
---

The alarm fires at 02:15. Your ALTER TABLE statement has been running for 47 minutes on a 600 GB orders table, blocking every write behind it. The migration was supposed to be quick—just add a non-nullable column with a default. You cancel it, roll back, and spend the next three hours in incident review explaining how a routine migration became an outage.

This is not a rare failure. It is the default outcome when you apply standard schema migration patterns to tables with millions of rows.

## Why Standard Migrations Fail at Scale

PostgreSQL's `ALTER TABLE` takes an `AccessExclusiveLock` on the full table for many operations. Adding a `NOT NULL` column with a `DEFAULT`? Every row must be rewritten. On a 600 GB table with concurrent writes, that lock queues behind active transactions and blocks every new query from entering the table—including reads in newer PostgreSQL versions when the lock queue backs up.

The failure compounds: your connection pool exhausts as queries pile up waiting for the lock. Your API starts returning 503s. The migration finishes eventually, but not before cascading failures hit downstream services.

The fix is the expand-contract pattern: decompose the dangerous migration into safe, independent steps that never hold an exclusive lock longer than milliseconds.

## The Three Phases

Instead of transforming a column in place, you run three distinct phases across multiple deployments:

- **Expand**: Add the new structure without removing anything. New columns are nullable. Code writes to both old and new simultaneously.
- **Migrate**: Backfill historical data in small batches. No schema locks, just data writes.
- **Contract**: Remove the old structure after all code has migrated.

Here's what this looks like for a real migration: rename `user_id` (integer) to `account_uuid` (UUID) on a 200M-row `events` table.

## Phase 1: Expand

Add the new column as nullable with no default that touches existing rows:

```sql
-- Adding a nullable column with no stored default is instant in PostgreSQL 11+.
-- PostgreSQL writes the default at the table metadata level only when
-- the default is immutable and storable; NULL always is.
ALTER TABLE events ADD COLUMN account_uuid UUID;

-- CONCURRENTLY never takes an exclusive lock.
-- The build runs alongside live reads and writes; it just takes longer.
CREATE INDEX CONCURRENTLY idx_events_account_uuid ON events(account_uuid);
```

The `CONCURRENTLY` flag is critical. Without it, `CREATE INDEX` takes a `ShareLock` that blocks all writes for the duration of the index build—potentially tens of minutes on a large table.

Deploy application code that writes to **both** columns on every insert and update:

```python
def insert_event(conn, user_id: int, account_uuid: str, event_type: str):
    conn.execute(
        """
        INSERT INTO events (user_id, account_uuid, event_type, created_at)
        VALUES (%s, %s, %s, NOW())
        """,
        (user_id, account_uuid, event_type),
    )
```

Reads still use `user_id`. No behavior changes. The expand is done.

## Phase 2: Backfill

Never backfill with a single `UPDATE` across the full table. That statement holds row-level locks for its entire duration and generates a write-ahead log spike that can saturate replication.

Batch the backfill by primary key range:

```python
import psycopg2
import time

def backfill_account_uuid(conn_string: str, batch_size: int = 5000, sleep_ms: int = 100):
    """
    Backfill account_uuid from the users table.
    Safe to pause and resume—filters on IS NULL for idempotency.
    """
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    cursor.execute("SELECT MIN(id), MAX(id) FROM events WHERE account_uuid IS NULL")
    min_id, max_id = cursor.fetchone()

    if min_id is None:
        print("Nothing to backfill")
        return

    current_id = min_id
    total_updated = 0

    while current_id <= max_id:
        cursor.execute(
            """
            UPDATE events e
            SET account_uuid = u.uuid
            FROM users u
            WHERE u.id = e.user_id
              AND e.id >= %s
              AND e.id < %s
              AND e.account_uuid IS NULL
            """,
            (current_id, current_id + batch_size),
        )
        rows_updated = cursor.rowcount
        conn.commit()

        total_updated += rows_updated
        current_id += batch_size

        # Throttle to avoid overwhelming the primary and widening replication lag.
        # Start at 100ms and tune upward if lag climbs above your SLO.
        time.sleep(sleep_ms / 1000)

        if total_updated % 100_000 == 0:
            print(f"Progress: {total_updated} rows updated, current_id={current_id}")

    conn.close()
    print(f"Backfill complete: {total_updated} total rows")
```

Three decisions here that matter:

**Batch by primary key range, not `LIMIT/OFFSET`.** Offset pagination degrades to O(n²) as the offset grows because PostgreSQL must scan all skipped rows on every batch. A range scan on the indexed primary key is O(1) per batch regardless of where you are in the table.

**Commit per batch.** Each transaction holds row locks only during that batch's execution—milliseconds, not the duration of the full backfill. This keeps the lock wait queue clear for application traffic.

**Sleep between batches.** Without throttling, the backfill will saturate your primary's I/O and push replication lag past your SLO. Monitor lag on the replica during the run: `SELECT now() - pg_last_xact_replay_timestamp() AS lag;`. If lag climbs, increase the sleep interval.

The script is safe to kill and restart at any point. The `IS NULL` filter ensures already-backfilled rows are skipped.

## Phase 3: Contract

Only begin phase 3 after:
1. Backfill shows `SELECT COUNT(*) FROM events WHERE account_uuid IS NULL` returns 0.
2. All application reads use `account_uuid` exclusively.
3. At least one full deploy cycle has elapsed with no rollbacks.

Add the `NOT NULL` constraint without a table rewrite:

```sql
-- NOT VALID adds the constraint and enforces it on new rows immediately,
-- but skips scanning existing rows—so no full-table lock.
ALTER TABLE events
  ADD CONSTRAINT events_account_uuid_not_null
  CHECK (account_uuid IS NOT NULL)
  NOT VALID;

-- VALIDATE scans existing rows but only takes ShareUpdateExclusiveLock,
-- which does not block reads or writes. Safe to run during business hours.
ALTER TABLE events VALIDATE CONSTRAINT events_account_uuid_not_null;

-- Dropping a column is a metadata-only operation in PostgreSQL.
-- The column space is reclaimed lazily by future VACUUM runs.
ALTER TABLE events DROP COLUMN user_id;

-- Optionally promote the CHECK to a real NOT NULL (PostgreSQL 12+).
-- This is a metadata-only change; it takes a brief AccessExclusiveLock
-- but releases it in microseconds since no rows are rewritten.
ALTER TABLE events ALTER COLUMN account_uuid SET NOT NULL;
ALTER TABLE events DROP CONSTRAINT events_account_uuid_not_null;
```

The `NOT VALID` + `VALIDATE CONSTRAINT` sequence is the critical technique. The constraint is enforced immediately for new writes, and the full-table validation uses a lock that does not block application traffic. This is the only safe way to add a `NOT NULL` constraint to a large table in production.

## What to Monitor During Each Phase

**Replication lag** is your primary signal during the backfill. Keep it below 5 seconds. If it climbs, double the sleep interval.

**Lock waits** reveal if anything is blocking. Check with:
```sql
SELECT pid, wait_event_type, wait_event, query, now() - query_start AS duration
FROM pg_stat_activity
WHERE wait_event_type = 'Lock'
ORDER BY duration DESC;
```

**Table bloat** accumulates as batch updates create dead row versions. After the backfill completes, run `VACUUM ANALYZE events;` explicitly, or confirm autovacuum has processed the table with:
```sql
SELECT last_vacuum, last_autovacuum, n_dead_tup
FROM pg_stat_user_tables
WHERE relname = 'events';
```

**Index usage** on the new column should be verified before you drop the old one:
```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT event_type FROM events WHERE account_uuid = '550e8400-e29b-41d4-a716-446655440000';
```
If the planner chooses a sequential scan where you expect an index scan, the statistics may be stale. Run `ANALYZE events;` to refresh them.

## The Real Timeline

For a 200M-row table on modern cloud hardware:

- Phase 1 (expand + concurrent index build): 20–40 minutes of background index build, zero application impact
- Phase 2 (backfill): 2–8 hours depending on row size, network, and throttle settings
- Phase 3 (contract): seconds per statement

The total elapsed clock time is much longer than a naive `ALTER TABLE`, but **none of that time takes the table offline**. The cost is coordination across deployment cycles—typically two to three separate deploys separated by the backfill window.

Teams that skip this and run the naive migration on large tables are not cutting corners; they genuinely believe it will be fast. Check the table size first: `SELECT pg_size_pretty(pg_total_relation_size('events'));`. Above 10 GB, use the expand-contract pattern. Below that, measure lock duration in a staging environment with production-sized data before deciding.

---

The one takeaway: before writing `ALTER TABLE ... ADD COLUMN ... NOT NULL DEFAULT 'x'` on any production table over 10 GB, stop. That single statement will lock the table for minutes and queue every application query behind it. Split the migration into expand, backfill, and contract phases—three deployment cycles where each schema change takes milliseconds of lock time—and your 02:15 alarm stays silent.
