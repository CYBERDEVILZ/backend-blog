---
layout: post
title: "Practical Database Index Design: Covering Indexes, Partial Indexes, and Index Bloat"
date: 2026-05-23
tags: [postgresql, performance, indexing, database]
read_time: 11
---

Your query runs in 4ms in staging. In production, with 80 million rows and concurrent writes, it's hitting 600ms p99 and occasionally timing out. The index exists — you can see it in `\d` — but `EXPLAIN ANALYZE` shows a Bitmap Heap Scan fetching 40,000 rows through it when you expected a handful. You added the index to fix slowness six months ago. It helped then. It's not helping now.

Most index problems are not "we need an index" problems. They're "we have the wrong index" or "our index has bloated into uselessness" problems. This post covers the three techniques that solve the majority of real production index failures: covering indexes that eliminate table heap fetches, partial indexes that stay small and fast, and diagnosing and recovering from index bloat.

## Why Heap Fetches Kill Index Performance

When PostgreSQL uses a standard B-tree index, finding rows is a two-step process. Step one: walk the B-tree to find TIDs (tuple IDs — physical row locations). Step two: fetch each TID from the heap (the actual table). That second step is where performance collapses at scale.

Consider this table and query:

```sql
-- 80M row orders table
CREATE TABLE orders (
    id          BIGINT PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    status      TEXT NOT NULL,       -- 'pending','processing','shipped','delivered','cancelled'
    created_at  TIMESTAMPTZ NOT NULL,
    total_cents BIGINT NOT NULL,
    metadata    JSONB
);

-- The index we added six months ago
CREATE INDEX idx_orders_user_id ON orders(user_id);

-- The query that's now slow
SELECT id, status, total_cents, created_at
FROM orders
WHERE user_id = 42
ORDER BY created_at DESC
LIMIT 20;
```

`EXPLAIN (ANALYZE, BUFFERS)` on this reveals the problem:

```
Index Scan using idx_orders_user_id on orders
  Index Cond: (user_id = 42)
  Rows Removed by Filter: 0
  Buffers: shared hit=12 read=4821   <-- 4821 page reads from heap
  Planning Time: 0.3 ms
  Execution Time: 187.4 ms
```

User 42 has 5,000 orders. The index found all 5,000 TIDs instantly. But then PostgreSQL made 4,821 random page reads into the heap to retrieve `status`, `total_cents`, and `created_at` — columns the index doesn't contain. Each random page read on a loaded production disk costs 0.1–2ms. The index is doing its job and still producing a slow query.

## Covering Indexes: Eliminate the Heap Fetch

A covering index stores additional columns inside the index itself. When the query only needs those columns, PostgreSQL never touches the heap — it reads the full result from the index pages, which are smaller, frequently cached, and sequentially organized.

```sql
-- Drop the old index, replace with a covering index
DROP INDEX idx_orders_user_id;

CREATE INDEX idx_orders_user_covering
    ON orders(user_id, created_at DESC)
    INCLUDE (status, total_cents, id);
```

The key distinction: columns in the main index key (`user_id`, `created_at`) are used for B-tree traversal and range scans. Columns in `INCLUDE` are stored in the leaf pages but not traversed. This matters — including wide columns in the B-tree key itself would bloat every level of the tree.

Now `EXPLAIN (ANALYZE, BUFFERS)` shows:

```
Index Only Scan using idx_orders_user_covering on orders
  Index Cond: (user_id = 42)
  Heap Fetches: 0
  Buffers: shared hit=9 read=3
  Planning Time: 0.3 ms
  Execution Time: 1.1 ms
```

`Index Only Scan` with `Heap Fetches: 0` means zero heap I/O. The entire result came from index pages. Query went from 187ms to 1ms under the same data.

Two caveats. First, `INCLUDE` columns add size to leaf pages only — the index is larger but the B-tree height stays the same. Benchmark before and after on your actual data. Second, `Heap Fetches > 0` on an Index Only Scan means some pages have dead tuples not yet vacuumed. Run `VACUUM` if you see this on a new covering index.

For queries that touch `metadata JSONB` (too wide to include), the covering index won't help — you'll still hit the heap. That's fine. Use covering indexes for the tight query patterns that matter: list views, dashboards, API responses that project a fixed set of columns.

## Partial Indexes: Stay Selective, Stay Small

A partial index covers only rows that match a `WHERE` predicate. If your slow queries always filter on a column with low cardinality (status, type, active flag), a partial index dramatically reduces index size while being more selective per-query than a full index.

Real scenario: a jobs table in a task queue system.

```sql
CREATE TABLE jobs (
    id          BIGSERIAL PRIMARY KEY,
    queue       TEXT NOT NULL,
    status      TEXT NOT NULL,  -- 'pending','running','done','failed'
    priority    INT NOT NULL DEFAULT 5,
    payload     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 50M rows total. 49.8M have status IN ('done','failed').
-- Only ~200K rows are 'pending' or 'running' at any time.
-- But the worker query looks like this:
SELECT id, queue, payload
FROM jobs
WHERE status = 'pending'
  AND queue = 'email'
ORDER BY priority DESC, created_at ASC
LIMIT 10;
```

A full index on `(status, queue, priority, created_at)` would be huge — 50M entries — and mostly wasted on 49.8M completed jobs. A partial index targets only the rows that matter to active workers:

```sql
CREATE INDEX idx_jobs_active_queue
    ON jobs(queue, priority DESC, created_at ASC)
    WHERE status IN ('pending', 'running');
```

This index contains ~200K rows instead of 50M. It fits in shared_buffers. Selects from it are fast because the index is always hot in cache. Inserts and updates only touch the index for rows matching the predicate — completed jobs stop touching it the moment they transition out of `pending`/`running`.

Check whether your partial index is being used:

```sql
SELECT indexname, idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
WHERE relname = 'jobs'
  AND indexname = 'idx_jobs_active_queue';
```

If `idx_scan` is zero after several hours of traffic, the planner isn't choosing it. Common cause: the query's `WHERE` clause doesn't exactly match the index predicate. In this case `WHERE status = 'pending'` does satisfy `WHERE status IN ('pending', 'running')` — PostgreSQL can infer this. But `WHERE status != 'done'` would not be recognized as a subset. Use `IN (...)` or explicit equality when defining partial index predicates.

## Index Bloat: The Slow Decay

Indexes degrade over time on tables with frequent updates and deletes. PostgreSQL's MVCC model never overwrites tuples in place — updates write a new version and mark the old one dead. B-tree indexes accumulate dead entries pointing at dead tuples. `VACUUM` reclaims heap space, but B-tree pages don't shrink — they stay allocated and fill with dead entries. This is index bloat.

Bloated indexes are larger than necessary, slower to scan, and waste cache. An index that was 2GB at creation can grow to 8GB after a year of updates with insufficient vacuuming.

Measure bloat before taking any action:

```sql
-- pgstattuple extension gives accurate bloat data
CREATE EXTENSION IF NOT EXISTS pgstattuple;

SELECT
    indexrelid::regclass AS index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    (pgstattuple(indexrelid)).dead_tuple_percent AS dead_pct,
    (pgstattuple(indexrelid)).free_percent AS free_pct
FROM pg_index
WHERE indrelid = 'orders'::regclass
ORDER BY pg_relation_size(indexrelid) DESC;
```

A healthy index has `dead_tuple_percent < 5%` and `free_percent < 30%`. If you're seeing `dead_tuple_percent > 20%` or `free_percent > 50%`, the index is bloated.

**Recovering from bloat without downtime:**

Option 1: `REINDEX CONCURRENTLY` (PostgreSQL 12+). Rebuilds the index from scratch without taking a lock that blocks reads or writes. Takes 2–5x longer than a regular REINDEX but is safe on production.

```bash
# Run from psql or a migration script
REINDEX INDEX CONCURRENTLY idx_orders_user_covering;
```

Option 2: swap with a concurrent build. More flexible — lets you change the index definition at the same time.

```sql
-- Build new index concurrently (no write lock)
CREATE INDEX CONCURRENTLY idx_orders_user_covering_new
    ON orders(user_id, created_at DESC)
    INCLUDE (status, total_cents, id);

-- Verify it's valid
SELECT indexname, indisvalid
FROM pg_indexes
JOIN pg_class ON pg_class.relname = pg_indexes.indexname
JOIN pg_index ON pg_index.indexrelid = pg_class.oid
WHERE tablename = 'orders';

-- Swap atomically (takes brief lock but only for catalog update)
BEGIN;
DROP INDEX idx_orders_user_covering;
ALTER INDEX idx_orders_user_covering_new RENAME TO idx_orders_user_covering;
COMMIT;
```

**Prevention:** tune autovacuum for high-write tables. The defaults are designed for average tables — a table with millions of updates/hour needs more aggressive settings:

```sql
ALTER TABLE orders SET (
    autovacuum_vacuum_scale_factor = 0.01,   -- vacuum when 1% of rows are dead (default 20%)
    autovacuum_vacuum_cost_delay   = 2,      -- less throttling (default 20ms)
    autovacuum_vacuum_cost_limit   = 400     -- more work per vacuum cycle (default 200)
);
```

Monitor vacuum activity to confirm it's keeping up:

```sql
SELECT relname, last_vacuum, last_autovacuum, n_dead_tup, n_live_tup,
       round(n_dead_tup::numeric / nullif(n_live_tup, 0) * 100, 2) AS dead_pct
FROM pg_stat_user_tables
WHERE relname = 'orders';
```

## Putting It Together

Three diagnostic questions for any slow query:

1. **Does `EXPLAIN (ANALYZE, BUFFERS)` show `Heap Fetches > 0` on an Index Only Scan, or a regular Index Scan fetching far more rows than you expect?** Add an `INCLUDE` covering index for the projected columns.

2. **Does the query always filter on a low-cardinality column where most rows don't qualify?** Build a partial index scoped to the rows that actually get queried.

3. **Is the index older than six months on a table with heavy write traffic?** Measure bloat with `pgstattuple`. If `dead_tuple_percent > 20%`, run `REINDEX CONCURRENTLY` and tune autovacuum to prevent recurrence.

The actionable starting point: run this on your production database today:

```sql
SELECT
    schemaname,
    relname AS table_name,
    indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY pg_relation_size(indexrelid) DESC
LIMIT 20;
```

Any index with `idx_scan = 0` and size over 100MB is dead weight — it's slowing down every write to that table while helping no read. Drop it. Any index where `idx_tup_read` is orders of magnitude larger than `idx_tup_fetch` is fetching far more entries than it returns rows — a candidate for a partial index. Start there.
