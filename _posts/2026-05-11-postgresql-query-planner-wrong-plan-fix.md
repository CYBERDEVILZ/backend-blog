---
layout: post
title: "PostgreSQL Query Planner Mistakes: When It Picks the Wrong Plan and How to Fix It"
date: 2026-05-11
tags: [postgresql, performance, database, debugging]
read_time: 8
---

Your query ran in 12ms for months. This morning it's taking 8 seconds and paging your on-call rotation. Nothing changed in the application code. The table got bigger, but not dramatically so. You run `EXPLAIN ANALYZE` and see a Seq Scan on a 20-million-row table where you know a B-tree index exists.

The PostgreSQL query planner made the wrong call. This happens in production more than most engineers expect, and the fix is rarely "add an index."

## Why the Planner Gets It Wrong

PostgreSQL's planner is a cost-based optimizer. It estimates the cheapest execution plan by estimating row counts for each operation, then multiplying by per-row costs. The key word is *estimates*—it doesn't know the actual data; it reads statistics collected by `ANALYZE` (or autovacuum's analyze phase).

When those statistics are stale or structurally misleading, estimates go wrong. Wrong estimates cause wrong plan choices.

The three most common root causes in production:

**1. Stale statistics after bulk loads.**
You loaded 10M rows via COPY or an ETL job. Autovacuum hadn't run yet. The planner still thinks the table has 500K rows, so the cost of an index scan looks proportionally higher than it is—and it picks a sequential scan instead.

**2. Correlated columns with independent statistics.**
PostgreSQL's default statistics treat columns independently. If you have `(country, city)` and query `WHERE country = 'US' AND city = 'Portland'`, the planner estimates `rows(US) * rows(Portland) / total`, which wildly overestimates the matches for a correlated pair.

**3. High variation in value frequency.**
A column like `status` with values 99% `completed` and 0.1% `pending` can mislead the planner. If `statistics_target` is low, it may not have enough samples to notice the skew, and it estimates `pending` has the same frequency as `completed`.

## Diagnosing the Wrong Plan

Start with `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)`:

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT o.id, o.created_at, u.email
FROM orders o
JOIN users u ON u.id = o.user_id
WHERE o.status = 'pending'
  AND o.created_at > now() - interval '7 days';
```

Look for these red flags:

- **Rows estimate vs actual rows diverge by >10x**: `rows=50 actual rows=47823` means the planner was flying blind.
- **Seq Scan on a large table where an index exists**: The planner thought it was cheaper, usually because it underestimated selectivity.
- **Hash Join when one side is tiny**: If the inner relation has 3 rows, a Nested Loop is nearly always cheaper, but a wildly overestimated inner count pushes the planner toward Hash Join.

Here's a real example of a bad estimate causing the wrong join strategy:

```
Hash Join  (cost=1842.30..9201.44 rows=12800 width=48)
           (actual time=5832.443..5834.201 rows=12 loops=1)
  Hash Cond: (o.user_id = u.id)
  ->  Seq Scan on orders o  (cost=0.00..6144.00 rows=12800 width=32)
                             (actual time=0.043..4211.332 rows=12 loops=1)
        Filter: ((status = 'pending') AND (created_at > ...))
        Rows Removed by Filter: 307188
```

The planner estimated 12,800 matching rows. Actual: 12. It did a full sequential scan (307K rows examined, 307K removed by filter) and built a hash table, all to return 12 rows. An index scan plus nested loop would have cost microseconds.

## Fix 1: Force Fresh Statistics

If this follows a bulk load:

```sql
ANALYZE orders;
```

Or target a specific skewed column with higher granularity:

```sql
ALTER TABLE orders ALTER COLUMN status SET STATISTICS 500;
ANALYZE orders (status);
```

The default `statistics_target` is 100 (roughly 300 rows sampled per column). For columns with high cardinality or heavy skew, push it to 500 or 1000. The tradeoff is slightly slower `ANALYZE` runs and larger `pg_statistic` entries—acceptable for hot columns.

After analyzing, re-run your `EXPLAIN ANALYZE`. If estimates are now close to actual, you're done.

## Fix 2: Extended Statistics for Correlated Columns

For the correlated-column problem, PostgreSQL 10+ has extended statistics:

```sql
CREATE STATISTICS orders_status_created (dependencies)
ON status, created_at
FROM orders;

ANALYZE orders;
```

The planner now uses the functional dependency between these columns to produce a better combined estimate. You can also create `ndistinct` statistics if you have multi-column `GROUP BY` queries with bad cardinality estimates.

Verify what statistics exist and when they were last refreshed:

```sql
SELECT stxname, stxkeys, stxkind, last_analyzed
FROM pg_statistic_ext
JOIN pg_statistic_ext_data ON pg_statistic_ext.oid = pg_statistic_ext_data.stxoid
WHERE stxrelid = 'orders'::regclass;
```

## Fix 3: Recalibrate Cost Parameters for Your Hardware

Sometimes the planner has accurate estimates but still picks the wrong physical plan because `random_page_cost` and `seq_page_cost` are set for spinning disks when you're on SSD or NVMe.

Default values:
```
seq_page_cost    = 1.0
random_page_cost = 4.0
```

On NVMe, random reads are nearly as fast as sequential. A miscalibrated `random_page_cost` makes every index scan look 4× more expensive than it is. Set in `postgresql.conf`:

```ini
random_page_cost = 1.1
effective_cache_size = 24GB   # how much OS page cache is available
```

`effective_cache_size` doesn't allocate memory—it's a hint that tells the planner how much of the working set is likely already in cache, which raises the value of index scans relative to sequential scans.

If you need to test without changing server config, disable plan types per-session:

```sql
SET enable_seqscan = off;
EXPLAIN ANALYZE SELECT ...;  -- forces the index-based plan, shows its real cost
SET enable_seqscan = on;
```

Never leave `enable_seqscan = off` in production. Use it only to see the cost of the alternative plan and understand why the planner avoided it.

## Fix 4: pg_hint_plan for Surgical Overrides

When you need to force a plan in production without changing planner parameters globally, `pg_hint_plan` lets you embed hints in query comments:

```sql
/*+ IndexScan(o orders_status_created_at_idx) NestLoop(o u) */
SELECT o.id, o.created_at, u.email
FROM orders o
JOIN users u ON u.id = o.user_id
WHERE o.status = 'pending'
  AND o.created_at > now() - interval '7 days';
```

The hint is read from the comment—no schema change needed, and it can be injected at the query layer. This forces an index scan on `orders` and a nested loop join regardless of what the planner estimates.

Use this as a temporary production fix while you identify why the planner's statistics are wrong. Hints couple your query to the current index names and break if the schema changes, so treat them as a stabilizing patch, not an architecture decision.

## Catching Regressions Before They Hit Production

The `auto_explain` extension logs slow query plans automatically:

```ini
# postgresql.conf
shared_preload_libraries = 'auto_explain'
auto_explain.log_min_duration = '100ms'
auto_explain.log_analyze = true
auto_explain.log_buffers = true
```

Every query slower than 100ms gets its full `EXPLAIN ANALYZE` output written to the PostgreSQL log. Pair this with log parsing or a tool like pgBadger to alert when estimate error ratios exceed 10× on critical queries. You'll catch planner regressions before users do.

## The One Thing to Do Right Now

Run this against any table that's received significant writes in the last 24 hours:

```sql
SELECT
  schemaname,
  relname,
  last_analyze,
  last_autoanalyze,
  n_live_tup,
  n_dead_tup
FROM pg_stat_user_tables
WHERE (last_analyze < now() - interval '1 day' OR last_analyze IS NULL)
  AND n_live_tup > 100000
ORDER BY n_live_tup DESC
LIMIT 20;
```

Any large table with stale statistics is a query planner time bomb. Run `ANALYZE` on it before the planner makes a catastrophic plan choice under load—because the next time it does, it will be at peak traffic, not at 3am on a Tuesday when the table was quiet.
