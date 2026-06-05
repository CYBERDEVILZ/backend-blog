---
layout: post
title: "Safely Deprecating an API Endpoint Without Breaking Clients"
date: 2026-06-05
tags: [api, backend, http, infrastructure]
read_time: 11
---

## The Incident

You shipped `/api/v2/payments` two months ago. It fixes the currency rounding bugs in `/api/v1/payments`, and your mobile apps and internal frontend both migrated weeks ago. You've emailed the partner team. You flip the kill switch—and your on-call pager fires at 2am because a payment partner you forgot about is 404ing on their checkout flow.

This is the canonical deprecation failure mode: you thought you'd told everyone, and you were wrong. Safely deprecating an endpoint means making it technically impossible for a client to call your API without receiving a machine-readable warning—and keeping the endpoint alive long enough for every real consumer to migrate. Here's how to do it operationally.

## Step 1: Find Every Real Consumer Before You Do Anything Else

Don't start from "who should be calling this." Start from "who *is* calling this, right now, in your logs."

Pull 90 days of access logs and group by API key, user-agent, and source IP:

```bash
# Parse structured nginx/envoy logs for the deprecated path
zcat /var/log/nginx/access.log.*.gz | \
  grep '"GET /api/v1/payments' | \
  awk '{print $1, $7, $12}' | \
  sort | uniq -c | sort -rn | head -60
```

If you use a log aggregation store (BigQuery, Redshift, Clickhouse), this is more useful as a query:

```sql
SELECT
  DATE(timestamp)                         AS day,
  JSON_VALUE(labels, '$.api_key')         AS api_key,
  http_request.user_agent                 AS client,
  COUNT(*)                                AS request_count,
  MIN(timestamp)                          AS first_seen,
  MAX(timestamp)                          AS last_seen
FROM access_logs
WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  AND http_request.request_url LIKE '/api/v1/payments%'
  AND http_request.status != 401  -- exclude unauthenticated probes
GROUP BY 1, 2, 3
ORDER BY request_count DESC;
```

Save every `api_key` in this result set. You cannot sunset the endpoint until every one of these shows zero traffic. That list is your migration checklist—not your Slack thread, not your email thread.

## Step 2: Signal Deprecation in Every Response

Two HTTP headers exist specifically for this. Use both.

**`Deprecation`** (RFC 9745): the date the endpoint was officially deprecated.  
**`Sunset`** (RFC 8594): the date after which the endpoint *will not respond*.  
**`Link`**: a machine-readable pointer to the replacement or migration docs.

Any competent API client library, monitoring tool, or engineer tailing response headers will see these immediately. Add them via middleware so you don't have to touch individual handlers:

```python
# Django deprecation middleware
import datetime
from django.utils.http import http_date

DEPRECATED_ENDPOINTS = {
    '/api/v1/payments': {
        'deprecated': datetime.datetime(2026, 6, 5, tzinfo=datetime.timezone.utc),
        'sunset':     datetime.datetime(2026, 9, 5, tzinfo=datetime.timezone.utc),
        'successor':  'https://api.example.com/api/v2/payments',
        'docs':       'https://docs.example.com/migration/payments-v2',
    },
}

class DeprecationHeaderMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        for path, meta in DEPRECATED_ENDPOINTS.items():
            if request.path.startswith(path):
                response['Deprecation'] = http_date(meta['deprecated'].timestamp())
                response['Sunset']      = http_date(meta['sunset'].timestamp())
                response['Link'] = (
                    f'<{meta["successor"]}>; rel="successor-version", '
                    f'<{meta["docs"]}>; rel="deprecation"'
                )
                break
        return response
```

The `Sunset` header is the important one. It's not a promise that you'll remove the endpoint—it's a contract. Set it, honor it.

## Step 3: Instrument Calls at the Endpoint Level

Headers are the signal. Metrics are the proof that clients are receiving and acting on the signal.

Add a labeled counter to every deprecated endpoint handler:

```python
# FastAPI + Prometheus
from prometheus_client import Counter
from fastapi import Request, Depends

deprecated_calls = Counter(
    'api_deprecated_calls_total',
    'Requests to deprecated API endpoints',
    ['path', 'api_key', 'user_agent'],
)

@app.get('/api/v1/payments')
async def legacy_payments(
    request: Request,
    api_key: str = Depends(authenticate),
):
    deprecated_calls.labels(
        path='/api/v1/payments',
        api_key=api_key,
        user_agent=request.headers.get('User-Agent', 'unknown'),
    ).inc()

    # ... existing handler logic unchanged
```

Wire this counter to a dashboard. Add an alert: if any `api_key` that appeared in your initial log query is *still generating calls* 60 days after your deprecation notice, that integration has not migrated and you cannot sunset without breaking them. The metric is your gate, not the calendar.

## Step 4: Run the Deprecation Timeline

Compress this and you will break someone. Extend it indefinitely and the endpoint never goes away.

| Week | Action |
|------|--------|
| 0    | Ship deprecation headers. Email every `api_key` owner from your log query with the sunset date and migration docs link. |
| 2    | Confirm receipt from each integration owner. Block the sunset date until you have confirmation. |
| 8    | Send a second notice to any `api_key` still generating calls. Attach their current call volume from your metrics. |
| 12   | Start returning HTTP `429 Too Many Requests` with `Retry-After: <sunset_date>` to the highest-volume remaining callers. This is a warning, not a break—but it forces the conversation. |
| 16   | Sunset. Return HTTP `410 Gone`. |

The week-12 step matters. A `410` at week 16 is a surprise. A `429` at week 12 is an alarm that doesn't break the integration but makes ignoring the migration impossible. The `Retry-After` date gives them a deadline they can act on.

## Step 5: Return 410, Not 404, at Sunset

`404 Not Found` implies the resource doesn't exist at this URL right now—a client may retry or use a different path. `410 Gone` is permanent: this resource is gone, stop asking. Return `410` at sunset with a body and link header that still directs clients to the migration path:

```python
import datetime
from django.http import JsonResponse

SUNSET = datetime.datetime(2026, 9, 5, tzinfo=datetime.timezone.utc)

def legacy_payments_view(request):
    if datetime.datetime.now(datetime.timezone.utc) >= SUNSET:
        response = JsonResponse(
            {
                'error': 'endpoint_removed',
                'message': 'This endpoint was removed on 2026-09-05. '
                           'See https://docs.example.com/migration/payments-v2',
            },
            status=410,
        )
        response['Link'] = '<https://docs.example.com/migration/payments-v2>; rel="deprecation"'
        return response

    # ... normal handler
```

Keep the `410` handler running for at least six months after sunset. Someone will still be hitting it—a vendor integration that rarely runs, a cron job that fires monthly—and a `410` with a `Link` header pointing to migration docs is far more useful than a connection reset or nginx `404`.

## The Pre-Sunset Gate

Before you merge the `410` rollout, your monitoring must show that `api_deprecated_calls_total` for every api_key in your original log query has reached zero—or that you've explicitly documented why you're willing to break that client (the integration is dead, the owner is unreachable, the use case no longer exists).

Make this a written checklist item in your deploy runbook:

```
[ ] api_deprecated_calls_total{path="/api/v1/payments"} == 0 for all api_keys
    OR documented exception per api_key with owner sign-off
```

The traffic data is ground truth. "I think everyone has migrated" is not. Every API deprecation incident I've seen was caused by someone skipping this check because they were confident. Run the query. Require sign-off. Then pull the switch.
