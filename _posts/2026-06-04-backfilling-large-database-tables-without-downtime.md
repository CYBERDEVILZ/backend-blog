---
layout: post
title: "Backfilling Large Database Tables Without Taking Down Your Service"
date: 2026-06-04
tags: [postgresql, database, migrations, production]
read_time: 8
---

Your analytics team needs a `normalized_email` column on the `users` table. The column exists — you added it in a migration — but it's NULL for all 180 million existing rows. You run:

```sql
UPDATE users SET normalized_email = lower(trim(email));
```

Database CPU spikes to 100%. Connection pool exhausts. Replication lag climbs to 40 seconds. You kill the query, but the damage is done: 3 minutes of elevated error rates in production.

This is the backfill trap. It doesn't only affect computed columns — it hits any time you need to populate data for rows that existed before a new column, lookup table, or denormalized field was added. The naive single-statement approach makes your database unavailable while it runs.

## Why Bulk UPDATE Destroys Production

A single `UPDATE users SET ...` with no WHERE clause does several harmful things in parallel:

- Takes row-level exclusive locks on every row it touches. No concurrent transaction can update those rows until the statement commits.
- Writes a new heap tuple for every row, doubling table bloat during the operation.
- Generates WAL for every row. On a 180M row table with streaming replicas, replication lag grows for the entire duration — potentially 20–40 minutes.
- Holds all those locks until the transaction commits, which happens at the very end.

The transaction is a single unit. The locks don't release incrementally. You either wait for all 180 million rows, or you kill it and retry.

## The Batched Backfill Pattern

Process rows in small batches, committing after each one, and pace the work to stay within your I/O and lock budget.

```python
import psycopg2
import time
import json
import pathlib
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 1000
SLEEP_SECONDS = 0.05   # 50ms between batches — limits WAL rate and replica lag
PROGRESS_FILE = pathlib.Path("/tmp/backfill_progress.json")
DSN = "postgresql://app:secret@primary/mydb"


def load_last_id():
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())["last_id"]
    return 0


def save_last_id(last_id: int):
    PROGRESS_FILE.write_text(json.dumps({"last_id": last_id}))


def backfill_normalized_email():
    conn = psycopg2.connect(dsn=DSN)
    conn.autocommit = False
    last_id = load_last_id()
    total_updated = 0

    while True:
        with conn.cursor() as cur:
            # Select the next batch by primary key — O(BATCH_SIZE), not O(offset)
            cur.execute(
                """
                SELECT id FROM users
                WHERE id > %s AND normalized_email IS NULL
                ORDER BY id
                LIMIT %s
                """,
                (last_id, BATCH_SIZE),
            )
            ids = [row[0] for row in cur.fetchall()]

        if not ids:
            break

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE users
                SET normalized_email = lower(trim(email))
                WHERE id = ANY(%s)
                  AND normalized_email IS NULL
                """,
                (ids,),
            )
            conn.commit()

        last_id = max(ids)
        total_updated += len(ids)
        save_last_id(last_id)   # persist progress so a crash can resume here
        logger.info("updated through id=%d  total=%d", last_id, total_updated)
        time.sleep(SLEEP_SECONDS)

    logger.info("backfill complete — %d rows updated", total_updated)
    conn.close()


backfill_normalized_email()
```

Four details that matter:

**Keyset pagination instead of OFFSET.** `OFFSET 500000` forces PostgreSQL to scan and throw away 500,000 rows on each batch. Using `WHERE id > $last_id ORDER BY id LIMIT $n` keeps each batch O(BATCH\_SIZE) regardless of where you are in the table. For 180M rows that difference is the gap between a 2-hour run and an 18-hour run.

**`normalized_email IS NULL` as a guard on the UPDATE.** This makes the backfill idempotent. If the script crashes and restarts, rows that were already updated are skipped instead of re-written. It also means you can run multiple passes safely.

**Commit after every batch.** Row-level locks are held for the duration of the UPDATE statement, not the backfill session. Committing after 1,000 rows means locks are held for milliseconds, not hours.

**The sleep.** Without pacing, the backfill will saturate disk I/O and WAL throughput. 50ms per 1,000-row batch caps throughput at roughly 20,000 rows/second — fast enough to finish in 2–3 hours, slow enough not to blow out replication lag.

## Tuning Batch Size and Sleep

1,000 rows / 50ms is a conservative starting point. The correct numbers depend on row width, index count, and your replica lag tolerance.

Start a test run and watch these during the first 10 minutes:

```sql
-- Replication lag on each standby
SELECT client_addr, write_lag, flush_lag, replay_lag
FROM pg_stat_replication;

-- Active lock waits (sign of batch size too large)
SELECT pid, wait_event, query
FROM pg_stat_activity
WHERE wait_event_type = 'Lock'
  AND state = 'active';

-- WAL generation rate (run twice, 10s apart; divide delta by interval)
SELECT pg_current_wal_lsn();
```

If `replay_lag` is trending upward, halve the batch size or double the sleep. If both metrics are flat well under your thresholds, double the batch size. Find the largest stable batch size — that minimizes SELECT overhead per updated row.

## Parallelizing Across ID Ranges

Once the single-threaded version is stable, you can cut wall-clock time by running workers on non-overlapping ranges:

```sql
-- Divide the unfilled rows into 4 equal buckets by primary key
SELECT
    bucket,
    min(id) AS range_start,
    max(id) AS range_end,
    count(*) AS rows
FROM (
    SELECT id, ntile(4) OVER (ORDER BY id) AS bucket
    FROM users
    WHERE normalized_email IS NULL
) sub
GROUP BY bucket
ORDER BY bucket;
```

Each worker receives `(range_start, range_end)` and uses `WHERE id > $last_id AND id <= $range_end`. Because the ranges don't overlap, there is zero lock contention between workers. Four workers at the same pacing finishes in roughly one quarter of the time.

Give each worker a separate progress file:

```python
PROGRESS_FILE = pathlib.Path(f"/tmp/backfill_progress_worker_{worker_id}.json")
```

## Adding a NOT NULL Constraint After Backfill

If the column eventually needs to be NOT NULL, doing it in one `ALTER TABLE` after the backfill takes an `AccessExclusiveLock` and blocks all reads and writes while PostgreSQL scans the table. Instead:

```sql
-- 1. Add the constraint without scanning the table (catalog change only)
ALTER TABLE users
  ADD CONSTRAINT users_normalized_email_not_null
  CHECK (normalized_email IS NOT NULL) NOT VALID;

-- 2. Validate in a separate transaction
--    This takes ShareUpdateExclusiveLock, which does NOT block reads or writes
ALTER TABLE users VALIDATE CONSTRAINT users_normalized_email_not_null;

-- 3. Convert to NOT NULL (now the scan is skipped — PostgreSQL trusts the constraint)
ALTER TABLE users ALTER COLUMN normalized_email SET NOT NULL;
ALTER TABLE users DROP CONSTRAINT users_normalized_email_not_null;
```

`NOT VALID` means "enforce on new rows, don't scan existing rows yet." `VALIDATE CONSTRAINT` does the scan under a non-blocking lock. By the time you reach step 3, PostgreSQL knows the constraint is already satisfied, so `SET NOT NULL` is a catalog update only — no scan, no blocking lock.

## What Happens When You Skip the Test

The most common failure mode: the backfill script is tested on a staging database with 50,000 rows, works instantly, and gets shipped to production against 180 million. Without load testing at scale, you have no data on what batch size actually does to replica lag on production hardware with production WAL volume.

Run a 10-minute test on a read replica that has production-scale data. Use `pg_basebackup` or your snapshot tooling to create one if you don't already have it. Use the exact batch size and sleep values you plan to use in production. Watch the three metrics above. If lag stabilizes under 2 seconds and no lock waits appear, you have a safe configuration. If not, tune before touching primary.

The backfill that takes 6 hours instead of 20 minutes is the one your on-call rotation sleeps through.
