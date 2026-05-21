---
layout: post
title: "Write-Ahead Logging: Crash Recovery and What It Means for Your Backup Strategy"
date: 2026-05-21
tags: [postgresql, databases, backup, reliability]
read_time: 9
---

Your replica is four hours behind. A node failed at 2:47 AM, and your on-call engineer is staring at a 300GB base backup from midnight — already stale by nearly three hours. The question is: can you recover to 2:47 AM, or are you losing data?

The answer depends on whether you understood WAL before you designed your backup strategy, not after.

## What WAL Actually Does in Production

PostgreSQL's Write-Ahead Log is a sequential, append-only journal of every change made to the database. Before any data page is modified in the buffer pool, the change is written to WAL. This is the "write-ahead" guarantee: the log always reflects what the data pages will look like after recovery.

When a transaction commits, PostgreSQL writes a COMMIT record to WAL and flushes it to disk (controlled by `synchronous_commit`). Only then does the client receive the acknowledgment. This is what makes durability guarantees possible — even if the process crashes immediately after acknowledging the commit, the WAL record exists and the change can be replayed.

The buffer pool (`shared_buffers`) holds dirty pages that haven't been written to heap files yet. PostgreSQL's checkpointing process periodically flushes those dirty pages to disk, then writes a checkpoint record to WAL. During crash recovery, PostgreSQL finds the latest checkpoint, replays WAL records from that point forward, and returns to a consistent state.

## The Checkpoint–WAL Relationship Determines Your Recovery Time

This is where production engineers get burned. The `checkpoint_completion_target` and `max_wal_size` settings interact in ways that directly affect crash recovery time.

```sql
-- Check your current checkpoint configuration and WAL size
SELECT name, setting, unit, short_desc
FROM pg_settings
WHERE name IN (
  'checkpoint_completion_target',
  'max_wal_size',
  'min_wal_size',
  'checkpoint_timeout',
  'wal_level',
  'archive_mode',
  'archive_command'
);

-- See how often checkpoints fire, and whether timeout or WAL size is driving them.
-- checkpoints_req >> checkpoints_timed means max_wal_size is too small.
SELECT checkpoints_timed,
       checkpoints_req,
       checkpoint_write_time,
       checkpoint_sync_time,
       buffers_checkpoint,
       buffers_clean,
       buffers_backend
FROM pg_stat_bgwriter;
```

If `checkpoints_req` dwarfs `checkpoints_timed`, your `max_wal_size` is too small and checkpoints are being forced early. PostgreSQL writes checkpoint records more frequently, which reduces crash recovery time but hammers storage with random I/O during peak write periods.

The rule: **crash recovery time ≈ time to replay WAL from the last checkpoint to the crash point**. With `checkpoint_timeout = 5min` and a write-heavy workload, expect recovery to take up to five minutes if the node crashed just before a checkpoint would have fired.

## WAL Archiving Is Not Optional for Real Point-in-Time Recovery

Base backups alone cannot give you PITR. A base backup is a consistent snapshot of the data directory, but without the WAL segments generated *after* that snapshot, you can only recover to the moment the backup was taken — not to 2:47 AM.

Here is the minimum configuration that enables real PITR:

```bash
# postgresql.conf
wal_level = replica           # or logical — never minimal
archive_mode = on
archive_command = 'pgbackrest --stanza=main archive-push %p'

# For low-write databases: force a WAL segment switch every 60 seconds
# so the archive stays fresh even if 16MB segments fill slowly.
archive_timeout = 60
```

The `archive_command` runs every time a WAL segment is completed (default segment size: 16MB). The `%p` placeholder is the path to the WAL file. The command must return exit code 0 on success — PostgreSQL will retry indefinitely on any non-zero exit.

**The silent killer**: setting `archive_command` to something that fails quietly. If your archive target is full, unreachable, or misconfigured, PostgreSQL retries and WAL files accumulate in `pg_wal/` until `max_wal_size` is hit — at which point PostgreSQL overwrites segments. You lose your PITR window with no visible error in application logs.

Monitor this continuously:

```sql
-- A non-zero and increasing failed_count is an emergency.
-- It means your PITR window is closing right now.
SELECT archived_count,
       failed_count,
       last_archived_wal,
       last_archived_time,
       last_failed_wal,
       last_failed_time
FROM pg_stat_archiver;
```

Alert on `failed_count` increasing. If archiving is failing, you are losing your recovery window in real time.

## Designing a Backup Strategy Around WAL

A production-grade backup strategy for PostgreSQL has four layers:

1. **Base backup** — full consistent snapshot via `pgbackrest backup --type=full` or `pg_basebackup`, taken daily or weekly. This is your recovery anchor.
2. **Incremental backups** — pgbackrest tracks which 8KB pages have changed using WAL and backs up only those. Full backups that take hours become incremental runs that take minutes.
3. **Continuous WAL archiving** — every completed segment shipped to S3, GCS, or Azure Blob. This fills the gap between base backups and gives you second-level recovery granularity.
4. **Verified restores** — automated weekly restore tests into a separate environment that runs schema validation and spot-checks row counts. An untested restore is not a backup.

Here is the pgbackrest restore command you will run at 3 AM:

```bash
# 1. Stop PostgreSQL
systemctl stop postgresql

# 2. Restore base backup and replay WAL through the target time.
#    --delta skips files that already match the backup — critical when the
#    data directory is not empty.
pgbackrest --stanza=main \
           --delta \
           --type=time \
           "--target=2026-05-21 02:47:00+00" \
           --target-action=promote \
           restore

# 3. Start PostgreSQL. It enters recovery mode, replays WAL to the target
#    timestamp, then promotes to a writable primary.
systemctl start postgresql

# 4. Confirm the recovery landed where you expected.
psql -c "SELECT pg_last_xact_replay_timestamp();"
```

The `--target-action=promote` flag tells PostgreSQL to promote to a primary after reaching the target time, rather than pausing and waiting for manual intervention.

## Replica Lag Is a WAL Problem

Streaming replication ships WAL from primary to replica in near-real-time. When your replica falls hours behind, you have a WAL delivery or application problem. Check this on the primary:

```sql
-- Per-replica lag broken down into write, flush, and replay components.
-- lag_bytes growing means the replica cannot keep up with WAL generation rate.
SELECT client_addr,
       state,
       sent_lsn,
       write_lsn,
       flush_lsn,
       replay_lsn,
       write_lag,
       flush_lag,
       replay_lag,
       pg_size_pretty(pg_wal_lsn_diff(sent_lsn, replay_lsn)) AS lag_bytes
FROM pg_stat_replication;
```

Common root causes when `lag_bytes` grows:

- **Network saturation**: WAL is generated faster than the replica can receive it. Check bandwidth between primary and replica.
- **Replica I/O pressure**: The replica applies WAL serially. If its disk is saturated, replay falls behind even if delivery is fine.
- **Query conflicts**: With `hot_standby_feedback = off`, the primary vacuums rows that long-running queries on the replica still need. WAL replay pauses to avoid violating visibility. The symptom is `replay_lag` spiking during bulk deletes or aggressive autovacuum runs on the primary.

The immediate lever for conflict-driven lag is `max_standby_streaming_delay` — how long WAL replay waits before canceling a conflicting query on the replica. Increasing it reduces cancellations but allows lag to grow. The structural fix is to run analytical queries on a logical replication target, not a streaming standby under heavy write load.

## The One Check That Tells You Where You Actually Stand

Run this right now:

```sql
SELECT failed_count,
       last_failed_wal,
       last_failed_time,
       now() - last_archived_time AS archive_lag
FROM pg_stat_archiver;
```

If `failed_count` is non-zero and growing, or `archive_lag` exceeds your `archive_timeout`, your PITR window is already compromised. Fix the archive command, verify a segment lands in your archive target, reset the stats with `SELECT pg_stat_reset_shared('archiver');`, and wire an alert to `last_failed_time` so the next failure pages someone within minutes — not at 3 AM when you need the recovery to work.

Your backup strategy is only as strong as your last successfully archived WAL segment.
