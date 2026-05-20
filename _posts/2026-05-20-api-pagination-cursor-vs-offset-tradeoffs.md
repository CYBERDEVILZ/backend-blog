---
layout: post
title: "API Pagination That Holds Under Real Load: Cursor vs Offset Tradeoffs"
date: 2026-05-20
tags: [postgresql, api, performance, backend]
read_time: 9
---

Your `/events?page=47&per_page=100` endpoint is quietly destroying your database. At low traffic you'll never notice. Then a customer builds a sync pipeline that hammers every page of their 2 million rows, and suddenly you have a `LIMIT 100 OFFSET 4600` query holding a row-exclusive lock while PostgreSQL scans 4,700 rows to return 100.

This is the offset pagination trap, and it shows up in every sufficiently old codebase.

## Why OFFSET Pagination Breaks Under Load

The SQL is deceptively simple:

```sql
SELECT id, created_at, payload
FROM events
WHERE user_id = $1
ORDER BY created_at DESC
LIMIT 100 OFFSET 4600;
```

What PostgreSQL actually does: it evaluates the full predicate, sorts the result, then discards the first 4,600 rows. The `OFFSET` value is not a bookmark—it's an instruction to scan and throw away. Your index on `(user_id, created_at)` helps with the predicate and sort, but PostgreSQL still has to traverse 4,700 index entries to hand you the last 100.

As pages get deeper the work grows linearly. Page 1 reads 100 rows. Page 500 reads 50,100 rows and returns 100. Under concurrent load, these deep-page queries pile up, each one scanning overlapping index ranges. Add a write-heavy table where rows shift between page boundaries during a scan and you have another problem: duplicate or missing rows as callers page through.

```sql
-- Show the actual rows read for a deep offset query
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, created_at, payload
FROM events
WHERE user_id = 42
ORDER BY created_at DESC
LIMIT 100 OFFSET 4600;

-- Rows Removed by Limit: 4600  <-- this is your problem
-- Buffers: shared hit=4832     <-- ~48x more buffer reads than returned rows
```

If you see `Rows Removed by Limit` climbing proportionally with your offset value, you are paying the full scan cost on every deep-page request.

## Keyset Pagination: The Fix That Actually Works

Keyset pagination (also called cursor-based pagination) replaces the offset with a boundary condition on the data itself:

```sql
-- First page (no cursor)
SELECT id, created_at, payload
FROM events
WHERE user_id = $1
ORDER BY created_at DESC, id DESC
LIMIT 100;

-- Subsequent pages (cursor = last row from previous page)
SELECT id, created_at, payload
FROM events
WHERE user_id = $1
  AND (created_at, id) < ($last_created_at, $last_id)
ORDER BY created_at DESC, id DESC
LIMIT 100;
```

PostgreSQL can now use the index on `(user_id, created_at DESC, id DESC)` to seek directly to the cursor position. Every page request reads exactly 100 rows regardless of depth. Page 1 and page 500 have identical query plans and identical I/O cost.

The composite cursor `(created_at, id)` handles ties on `created_at` without skipping rows. If you only cursor on `created_at`, two rows with the same timestamp will cause inconsistent pagination. Always include a tiebreaker column that is unique—`id` works if it's monotonic, otherwise use a `(created_at, uuid_pk)` pair with the correct sort.

## Encoding the Cursor for Clients

Expose the cursor as an opaque string. Clients should not be able to construct cursors manually—that defeats the point and makes your API brittle.

```python
import base64
import json
from datetime import datetime

def encode_cursor(created_at: datetime, row_id: int) -> str:
    payload = {
        "t": created_at.isoformat(),
        "id": row_id,
    }
    return base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":")).encode()
    ).decode()

def decode_cursor(cursor: str) -> tuple[datetime, int]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        data = json.loads(raw)
        return datetime.fromisoformat(data["t"]), int(data["id"])
    except Exception:
        raise ValueError("invalid cursor")

# API response shape
def paginate_events(user_id: int, cursor: str | None, limit: int = 100):
    conn = get_db_connection()
    if cursor:
        last_ts, last_id = decode_cursor(cursor)
        rows = conn.execute("""
            SELECT id, created_at, payload
            FROM events
            WHERE user_id = %s
              AND (created_at, id) < (%s, %s)
            ORDER BY created_at DESC, id DESC
            LIMIT %s
        """, (user_id, last_ts, last_id, limit + 1)).fetchall()
    else:
        rows = conn.execute("""
            SELECT id, created_at, payload
            FROM events
            WHERE user_id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT %s
        """, (user_id, limit + 1)).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = None
    if has_more and page:
        last = page[-1]
        next_cursor = encode_cursor(last["created_at"], last["id"])

    return {"items": page, "next_cursor": next_cursor}
```

Fetching `limit + 1` rows is a standard trick to determine `has_more` without a separate `COUNT(*)` query.

## The Index You Actually Need

The query above requires a specific composite index. If you don't have it, PostgreSQL will fall back to a full index scan on `(user_id)` and then sort in memory:

```sql
-- Create the index that makes keyset pagination O(1) per page
CREATE INDEX CONCURRENTLY idx_events_user_pagination
ON events (user_id, created_at DESC, id DESC);

-- Verify it's being used
EXPLAIN (ANALYZE)
SELECT id, created_at, payload
FROM events
WHERE user_id = 42
  AND (created_at, id) < ('2026-05-01 12:00:00', 99999)
ORDER BY created_at DESC, id DESC
LIMIT 100;
-- Expected: Index Scan using idx_events_user_pagination
-- Actual Rows: 100, Rows Removed by Filter: 0
```

If you see `Rows Removed by Filter` or a sort node above the index scan, your index column order or sort direction doesn't match the query. The `DESC` on the index must match the `ORDER BY DESC` in the query, otherwise PostgreSQL scans forward and sorts.

## When Offset Pagination Is Acceptable

Keyset pagination has real constraints. It doesn't support random access—you can't jump to page 47 without walking pages 1 through 46. It doesn't trivially support `COUNT(*)` for "showing 4,601–4,700 of 52,341 results." And it requires a stable sort key, which rules out sorts on non-indexed columns.

Use offset pagination when:
- The result set is genuinely small and bounded (under ~1,000 rows total)
- You need human-readable page numbers in the UI and the dataset never grows large
- You're building an admin interface where correctness under concurrent writes is less critical than simplicity

Use keyset pagination when:
- Result sets are unbounded or grow with user data
- Clients are machines (sync pipelines, export scripts, data consumers)
- You need consistent, non-skipping reads under concurrent writes
- Deep-page latency is already showing up in your p95/p99 metrics

## Migrating an Existing Endpoint

You can support both simultaneously during a transition:

```python
def get_events(user_id, page=None, cursor=None, per_page=100):
    if cursor:
        # New path: keyset
        return paginate_events(user_id, cursor, per_page)
    elif page is not None:
        # Legacy path: offset (log a deprecation warning)
        import logging
        logging.warning("offset pagination used for user_id=%s page=%s", user_id, page)
        offset = (page - 1) * per_page
        rows = db.execute(
            "SELECT ... FROM events WHERE user_id=%s ORDER BY created_at DESC LIMIT %s OFFSET %s",
            (user_id, per_page, offset)
        ).fetchall()
        return {"items": rows, "next_cursor": None}
    else:
        return paginate_events(user_id, None, per_page)
```

Track which clients still use `page=` via the deprecation log. Once you've migrated them, drop the offset path and the query cost goes with it.

## The Concrete Takeaway

Add `EXPLAIN (ANALYZE, BUFFERS)` to your deepest-offset query in production today. If `Rows Removed by Limit` is more than 10x your `LIMIT` value, you are paying hidden scan costs that scale with your data. Replace those endpoints with keyset pagination: a composite cursor on `(sort_column DESC, id DESC)`, a matching composite index, and an opaque base64-encoded cursor token in your API response. The query plan becomes a constant-cost index seek regardless of page depth.
