---
layout: post
title: "Diagnosing and Fixing N+1 Query Problems in Production ORMs"
date: 2026-05-10
tags: [postgresql, orm, performance, django]
read_time: 9
---

Your API endpoint takes 50ms in staging. In production with 10,000 users it regularly hits 4 seconds. APM shows the slow transactions have 300+ database queries. You are hitting N+1.

## What N+1 Looks Like in Production

The classic form: load a list of N objects, then issue one extra query per object. With 300 users on a page, that is 301 queries. Your database handles each in 5ms, but 301 serial roundtrips add up to 1.5 seconds before you have done anything else.

What makes it insidious is that it does not show up in development. With 10 rows in your fixtures, 11 queries is fast. With 50,000 users in production, the same code path is your worst bottleneck.

The failure mode in your metrics: p50 is fine, p99 is brutal. The queries themselves are fast—no slow query log entries. APM shows the request spending most of its time in the database layer, but no individual query is slow. Many fast queries adding up to a slow request is the N+1 signature.

## Finding It Without Guessing

**Django: query count middleware**

The fastest way to find N+1 in Django is to count queries per request and log when you cross a threshold:

```python
# middleware.py
from django.db import connection
import logging

logger = logging.getLogger(__name__)

class QueryCountMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        query_count = len(connection.queries)
        if query_count > 20:
            logger.warning(
                "High query count on %s %s: %d queries",
                request.method,
                request.path,
                query_count,
            )
        return response
```

With `DEBUG = True` in staging, `connection.queries` captures every query. Set the threshold conservatively and watch the logs after a deploy.

**PostgreSQL: pg_stat_statements in production**

In production you cannot enable `DEBUG` logging. Instead, `pg_stat_statements` gives you aggregated query stats without overhead:

```sql
-- Find the N+1 pattern: same simple query called thousands of times
-- High calls-to-rows ratio is the tell
SELECT
    query,
    calls,
    total_exec_time,
    rows,
    round(total_exec_time::numeric / calls, 2) AS avg_ms,
    -- calls >> rows means you are fetching one row at a time
    round(calls::numeric / NULLIF(rows, 0), 2) AS calls_per_row
FROM pg_stat_statements
WHERE calls > 100
  AND rows > 0
ORDER BY calls DESC
LIMIT 20;
```

N+1 queries leave a distinctive pattern: a simple lookup like `SELECT * FROM orders WHERE user_id = $1` with call counts that track your traffic exactly, often at N × (page size) per minute. The `avg_ms` is low, but total wall time is enormous. That query at the top of the `calls` column with sub-millisecond latency is almost always your N+1.

## Fixing It: Django ORM

The most common fix is `select_related` for foreign keys and `prefetch_related` for reverse FKs and many-to-many relations.

**Before (N+1):**

```python
def get_orders_with_users(request):
    orders = Order.objects.filter(status='pending')  # 1 query
    result = []
    for order in orders:
        result.append({
            'id': order.id,
            'user_email': order.user.email,           # +1 query per order
            'items': [i.sku for i in order.items.all()],  # +1 query per order
        })
    return JsonResponse({'orders': result})
```

With 500 pending orders, this runs 1 + 500 + 500 = 1,001 queries.

**After:**

```python
def get_orders_with_users(request):
    orders = (
        Order.objects
        .filter(status='pending')
        .select_related('user')        # JOIN users in the same query
        .prefetch_related('items')     # 1 additional query for all items
    )
    result = []
    for order in orders:
        result.append({
            'id': order.id,
            'user_email': order.user.email,            # no query, already joined
            'items': [i.sku for i in order.items.all()],  # no query, prefetched
        })
    return JsonResponse({'orders': result})
```

Now: 2 queries total—one JOIN for orders and users, one IN-clause query for all items.

**The prefetch_related gotcha:** calling `.filter()` on a prefetched relation discards the prefetch and issues a new query:

```python
# This discards the prefetch and fires a new query for every order:
for order in orders:
    urgent = order.items.filter(priority='high')  # NEW QUERY each iteration

# Filter in Python instead—the data is already loaded:
for order in orders:
    urgent = [i for i in order.items.all() if i.priority == 'high']
```

## Fixing It: SQLAlchemy

SQLAlchemy's default `lazy='select'` is the N+1 trap. The fix is explicit loading strategies:

```python
from sqlalchemy.orm import joinedload, selectinload

# Before: accessing order.user inside the loop fires SELECT user WHERE id = ?
orders = (
    session.query(Order)
    .filter(Order.status == 'pending')
    .all()
)

# After: declare how to load each relationship upfront
orders = (
    session.query(Order)
    .filter(Order.status == 'pending')
    .options(
        joinedload(Order.user),      # JOIN for single FK — one round trip
        selectinload(Order.items),   # SELECT ... WHERE order_id IN (...) for collections
    )
    .all()
)
```

Use `joinedload` for single-object relationships and `selectinload` for collections. Avoid `subqueryload`; `selectinload` is faster in PostgreSQL for most workloads.

**Catching it in tests before it ships:**

```python
# Block all lazy loading during tests so N+1 is a test failure, not a production incident
from sqlalchemy import event
from sqlalchemy.orm import Session

def no_lazy_loading(session, query_context):
    if query_context.lazy_loaded_from:
        raise AssertionError(
            f"Lazy load on {query_context.lazy_loaded_from.mapper.class_.__name__}. "
            "Add explicit eager loading."
        )

event.listen(Session, 'do_orm_execute', no_lazy_loading)
```

Run this fixture in your test suite. Any new code that lazy-loads a relationship fails immediately in CI.

## When ORM Magic Is Not Enough

Sometimes the relationship is complex or conditional and you cannot express it cleanly with eager loading. The production-safe fallback is a manual batch fetch:

```python
def enrich_orders(order_ids: list[int]) -> dict:
    """Fetch all enrichment data in one query, return keyed by order_id."""
    if not order_ids:
        return {}

    rows = db.execute("""
        SELECT
            o.id,
            u.email,
            u.tier,
            COUNT(oi.id)        AS item_count,
            SUM(oi.unit_price)  AS total_value
        FROM orders o
        JOIN users u         ON u.id  = o.user_id
        LEFT JOIN order_items oi ON oi.order_id = o.id
        WHERE o.id = ANY(%s)
        GROUP BY o.id, u.email, u.tier
    """, [order_ids])

    return {row['id']: row for row in rows}

# Caller:
orders    = get_pending_orders()
order_ids = [o.id for o in orders]
enriched  = enrich_orders(order_ids)  # exactly 1 query regardless of N

result = [
    {**order_to_dict(o), **enriched.get(o.id, {})}
    for o in orders
]
```

One query regardless of N. This is the escape hatch when the ORM cannot express what you need cleanly, and it is always correct.

## Staying Clean Over Time

N+1 regresses silently. A developer adds a field to the API response, accesses a new relation, and nobody notices because the query count per request jumps from 2 to 2+N. Staging still looks fast.

Two practices that prevent regression:

**Query count assertions in integration tests.** Django provides `django_assert_num_queries`; build something similar for your stack:

```python
def test_orders_endpoint_query_count(client, django_assert_num_queries):
    # This test fails if a future change adds any lazy-loaded access.
    with django_assert_num_queries(3):
        response = client.get('/api/orders/')
    assert response.status_code == 200
```

Pin the expected query count at the lowest correct value. When a developer accidentally introduces a new lazy load, CI catches it before review.

**APM alert on queries-per-transaction.** Datadog, New Relic, and most APMs let you alert on database query count by endpoint. Set the alarm at 3× your current baseline per endpoint. When a deploy causes query count to spike, you catch it within minutes rather than in a postmortem.

**The single most useful thing you can do today:** run the `pg_stat_statements` query above against your production replica. Find the query with the highest `calls` count that takes under 10ms each. Pull the calling code and add `select_related` or `selectinload`. That one change commonly cuts 20–40% off total database CPU for services that have never had N+1 audits.
