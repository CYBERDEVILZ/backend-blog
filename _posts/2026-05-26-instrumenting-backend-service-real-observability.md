---
layout: post
title: "Instrumenting a Backend Service for Real Observability, Not Just Uptime Pings"
date: 2026-05-26
tags: [observability, metrics, tracing, production]
read_time: 10
---

Your service is "up." The health check returns 200. Uptime is 99.9%. And yet, users are filing tickets, engineers are paging, and nobody can pinpoint why 3% of checkout requests take 12 seconds instead of 200ms.

This is the gap between *monitoring* and *observability*. Monitoring tells you something is wrong. Observability lets you ask arbitrary questions about your system's internal state using its external outputs—and answer them without deploying new code.

Most services are instrumented for the former. This post is about building the latter.

---

## The Three Signals That Actually Matter

Uptime checks and error rate thresholds are table stakes. Real observability requires three correlated signal types:

- **Metrics**: numeric time-series data (request rate, latency percentiles, queue depth)
- **Logs**: structured, queryable events with context about individual operations
- **Traces**: causally linked records that show what happened inside a single request, across service boundaries

The key word is *correlated*. A spike in p99 latency is a metric. The trace that shows which downstream call caused it is what lets you act. If your metrics, logs, and traces don't share a common request identifier, you have three isolated dashboards instead of an observability system.

---

## Metrics: Measure the Right Things

The RED method (Rate, Errors, Duration) per service endpoint is the minimum. But most implementations stop at averages, which hide the distribution.

**Measure percentiles, not averages.** A p99 latency of 4 seconds with a p50 of 80ms means 1 in 100 requests is dramatically worse—and an average of 120ms hides it entirely.

Here's a Prometheus instrumentation pattern in Python using `prometheus_client`:

```python
from prometheus_client import Histogram, Counter, start_http_server
import time, functools

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "Request latency in seconds",
    ["method", "endpoint", "status_code"],
    # Buckets tuned for a web service: 5ms to 10s
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status_code"],
)

def track_request(method, endpoint):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            status = "500"
            try:
                result = func(*args, **kwargs)
                status = str(result.status_code)
                return result
            except Exception:
                raise
            finally:
                duration = time.perf_counter() - start
                labels = {"method": method, "endpoint": endpoint, "status_code": status}
                REQUEST_LATENCY.labels(**labels).observe(duration)
                REQUEST_COUNT.labels(**labels).inc()
        return wrapper
    return decorator
```

Two critical decisions here: **label cardinality** and **bucket placement**.

High-cardinality labels (`user_id`, `order_id`) will explode your metric storage—use only categorical dimensions. For bucket placement, put them where your SLOs live: if your p99 SLO is 500ms, you need resolution around 250ms-750ms, not just at 1s and 10s.

---

## Structured Logs: Context That Survives a 3 AM Page

Unstructured logs are write-only. You can tail them; you can't query them. Every log line should be a JSON object that a query engine can filter and aggregate.

```python
import logging, json, time, uuid

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": "checkout-api",
            "version": "v2.4.1",
        }
        # Merge any extra fields passed to the logger
        for key in ("request_id", "user_id", "order_id", "duration_ms", "db_query_count"):
            if hasattr(record, key):
                log_entry[key] = getattr(record, key)
        return json.dumps(log_entry)

# Usage in a request handler
logger = logging.getLogger("checkout")

def process_checkout(request_id, order_id, user_id):
    log = logging.LoggerAdapter(logger, {
        "request_id": request_id,
        "order_id": order_id,
        "user_id": user_id,
    })
    start = time.perf_counter()
    try:
        result = _do_checkout(order_id)
        log.info("checkout.success", extra={
            "duration_ms": int((time.perf_counter() - start) * 1000),
            "db_query_count": result.query_count,
        })
        return result
    except PaymentDeclined as e:
        log.warning("checkout.payment_declined", extra={"reason": e.reason})
        raise
    except Exception:
        log.exception("checkout.error")
        raise
```

The `request_id` field is what makes logs useful in correlation. It must be the same value you propagate through traces and surface in error responses to users. When a user reports "my checkout failed at 14:32," you query logs by `request_id` and see every event that touched that request—including the database query count that jumped from 4 to 47 due to an ORM regression.

---

## Distributed Tracing: Following a Request Across Services

A trace is a DAG of spans. Each span records one unit of work: an HTTP handler, a database query, a cache lookup. The trace ties them together with a shared `trace_id`.

OpenTelemetry is the standard instrumentation library. Here's Go instrumentation for a service that calls a downstream API:

```go
package checkout

import (
    "context"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/codes"
    "net/http"
)

var tracer = otel.Tracer("checkout-service")

func (s *Service) AuthorizePayment(ctx context.Context, orderID string, amount int64) error {
    ctx, span := tracer.Start(ctx, "payment.authorize")
    defer span.End()

    span.SetAttributes(
        attribute.String("order.id", orderID),
        attribute.Int64("payment.amount_cents", amount),
    )

    req, _ := http.NewRequestWithContext(ctx, "POST", s.paymentURL+"/authorize", nil)
    // The otel HTTP transport injects W3C traceparent header automatically
    resp, err := s.httpClient.Do(req)
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, err.Error())
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode != 200 {
        span.SetStatus(codes.Error, "payment declined")
        span.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))
        return ErrPaymentDeclined
    }

    span.SetStatus(codes.Ok, "")
    return nil
}
```

The `otel` HTTP transport injects the `traceparent` header (W3C Trace Context format) into every outbound request. The downstream payment service extracts it and creates child spans under the same `trace_id`. The result: in Jaeger or Tempo, you see a single flame graph from the original user request through every service it touched, with timing at each hop.

**Tag spans with business context**, not just technical identifiers. `order.id` and `payment.amount_cents` let you search for traces by order in addition to searching by trace ID. When a customer reports a failed order, you find the trace without needing them to capture a request ID.

---

## Correlating Across All Three

The plumbing that makes this useful is a single `trace_id` threaded through all three signals:

```python
# FastAPI middleware that extracts or generates trace context
# and injects it into logs and metrics
from opentelemetry import trace
from fastapi import Request
import logging

async def observability_middleware(request: Request, call_next):
    span = trace.get_current_span()
    ctx = span.get_span_context()

    # Format as 32-char hex if valid, else generate
    if ctx.is_valid:
        trace_id = format(ctx.trace_id, "032x")
    else:
        trace_id = uuid.uuid4().hex

    # Inject into log context for all logs in this request
    with logging_context(trace_id=trace_id, request_id=trace_id):
        response = await call_next(request)

    # Return trace_id in response header so clients can report it
    response.headers["X-Trace-Id"] = trace_id
    return response
```

Now when Prometheus fires a p99 alert, you:
1. Open the latency histogram, filter to the affected endpoint
2. Query structured logs for requests in that time window with `duration_ms > 2000`
3. Take a `trace_id` from those logs and open it in Jaeger
4. See the exact span that caused the slowdown (db query? downstream call? lock wait?)

Without correlation, step 3 requires guessing.

---

## What to Alert On

Instrument first, then alert—not the other way around. The signal you want for SLO-based alerting:

```yaml
# Prometheus alerting rule: SLO burn rate alert
# Fires when error budget is burning 14x faster than sustainable
- alert: CheckoutErrorBudgetBurnRateHigh
  expr: |
    (
      sum(rate(http_requests_total{endpoint="/checkout", status_code=~"5.."}[1h]))
      /
      sum(rate(http_requests_total{endpoint="/checkout"}[1h]))
    ) > 14 * (1 - 0.999)
  for: 2m
  labels:
    severity: page
  annotations:
    summary: "Checkout SLO error budget burning at >14x rate"
```

A 14x burn rate on a 99.9% SLO means you'll exhaust the entire monthly error budget in 2 hours. This is worth waking someone up. "Error rate > 1%" is not—it doesn't tell you how much of your budget is gone.

---

## The Actionable Takeaway

Pick one endpoint in your most critical service. Add a latency histogram with correct bucket placement, structured JSON logging with a `request_id` field, and an OpenTelemetry span for every downstream call. Wire them together with the same `trace_id`. Then write one alert based on SLO burn rate instead of raw error percentage.

That single endpoint, instrumented correctly, will teach you more about your system in one incident than months of uptime dashboards. Expand from there.
