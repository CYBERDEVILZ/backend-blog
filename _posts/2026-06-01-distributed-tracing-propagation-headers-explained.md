---
layout: post
title: "Distributed Tracing from Scratch: What Propagation Headers Actually Do"
date: 2026-06-01
tags: [observability, distributed-systems, tracing, microservices]
read_time: 10
---

Your p99 latency jumped to 800ms. The dashboard shows the API gateway is slow, but the upstream services each report sub-100ms. The time has to be somewhere — connection setup? A retry? A queued request waiting on a saturated thread pool? Without distributed tracing, you're reading tea leaves from separate log streams and mentally joining them by timestamp. That's how you spend three hours on a one-hour problem.

Distributed tracing solves a specific structural issue: when a single user request fans out across multiple services, how do you reconstruct the causal chain of work that happened? The answer is propagation headers — a handful of bytes threaded through every hop that let you stitch together a complete picture after the fact. Understanding what those bytes actually encode, and what happens when they're missing or wrong, is the difference between tracing that works and tracing that lies.

## The Data Model

Every tracing system, whether Jaeger, Zipkin, or OpenTelemetry, builds on the same primitives:

- **Trace ID**: A globally unique identifier for the entire request. Every service involved in handling one user request shares the same trace ID.
- **Span ID**: A unique identifier for a single unit of work within the trace. One service call = one span.
- **Parent Span ID**: The span ID of the caller. This is what lets you reconstruct the tree structure of calls.
- **Sampling flag**: A bit (or bits) indicating whether this trace is being recorded.

When service A calls service B, A creates a child span, serializes `{trace_id, span_id, parent_span_id, flags}` into HTTP headers, and sends the request. Service B reads those headers, starts its own span using the extracted context as the parent, does work, and propagates further when it calls service C.

The result is a directed tree of spans, all sharing one trace ID, that you can reassemble into a waterfall view.

## What W3C TraceContext Actually Sends

The W3C TraceContext spec (now the standard; supported by every major tracing backend) uses two headers:

```
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
tracestate:  vendor1=value1,vendor2=value2
```

The `traceparent` format is `version-trace_id-parent_id-flags`:
- `00` — spec version (always 00 for now)
- `4bf92f3577b34da6a3ce929d0e0e4736` — 128-bit trace ID as 32 hex chars
- `00f067aa0ba902b7` — 64-bit parent span ID as 16 hex chars
- `01` — flags byte; bit 0 is the "sampled" flag

The `tracestate` carries vendor-specific metadata — Datadog might put its own span ID here, B3 might carry its sampling state here. This is how multiple tracing systems can coexist without colliding.

The `flags` byte deserves attention. When it's `01`, the trace is sampled: every downstream service should record spans. When it's `00`, they shouldn't (but must still propagate the headers). The critical invariant is **consistent sampling**: once a head-based decision is made at the entry point, every service in the call tree must respect it. If service B decides to sample independently of service A's decision, you get incomplete traces — some spans present, some missing — which is worse than no traces at all.

## Instrumenting a Service Correctly

Here's a minimal Python implementation using the OpenTelemetry SDK, showing what the libraries actually do under the hood:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, inject
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# One-time setup at service startup
provider = TracerProvider()
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://otel-collector:4317"))
)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("orders-service")
propagator = TraceContextTextMapPropagator()

# In your HTTP handler (e.g., Flask/FastAPI middleware):
def handle_request(request):
    # Extract incoming context from headers.
    # If there's no traceparent header, extract() returns an empty context
    # and a new root span will be created below.
    ctx = propagator.extract(carrier=dict(request.headers))

    with tracer.start_as_current_span(
        "orders.create",
        context=ctx,
        kind=trace.SpanKind.SERVER,
    ) as span:
        span.set_attribute("http.method", request.method)
        span.set_attribute("http.url", str(request.url))
        span.set_attribute("order.customer_id", request.json.get("customer_id"))

        result = process_order(request.json)

        span.set_attribute("http.status_code", 200)
        return result

# When calling a downstream service:
def call_inventory_service(order_items: list) -> dict:
    headers = {}
    # inject() writes traceparent/tracestate into the dict using current span context
    propagator.inject(headers)

    response = requests.post(
        "http://inventory-service/reserve",
        json={"items": order_items},
        headers=headers,
        timeout=2.0,
    )
    response.raise_for_status()
    return response.json()
```

The two critical lines are `propagator.extract()` in the receiver and `propagator.inject()` in the sender. Forget either one and you break the causal chain. Forget `inject()` and downstream spans become orphaned root spans — they appear in the backend as separate unrelated traces. This is the most common tracing bug in production, and it's invisible until someone tries to trace a specific request and can't find all the spans.

## Async and Queue Propagation

HTTP is straightforward. The harder case is propagation across message queues, async workers, and scheduled jobs.

When you publish a message to Kafka or RabbitMQ, you need to carry the trace context as message metadata, not as a message body field. OpenTelemetry defines a `TextMapPropagator` interface specifically for this: the "carrier" is whatever key-value store you have available — HTTP headers, gRPC metadata, message attributes.

```python
# Producer: inject trace context into Kafka message headers
from opentelemetry.propagate import inject

def publish_order_event(order: dict):
    headers = {}
    inject(headers)  # adds traceparent, tracestate

    producer.produce(
        topic="order-events",
        value=json.dumps(order).encode(),
        # Kafka headers are List[Tuple[str, bytes]]
        headers=[(k, v.encode()) for k, v in headers.items()],
    )

# Consumer: extract and create a *linked* span, not a child span
from opentelemetry.propagate import extract
from opentelemetry.trace import Link

def consume_order_event(message):
    # Extract the producer's context
    carrier = {k: v.decode() for k, v in (message.headers() or [])}
    producer_ctx = extract(carrier)
    producer_span_ctx = trace.get_current_span(producer_ctx).get_span_context()

    # Use a Link rather than a parent — the consumer is a separate trace
    # causally related to the producer, but not a synchronous child
    with tracer.start_as_current_span(
        "orders.process",
        links=[Link(producer_span_ctx)],
        kind=trace.SpanKind.CONSUMER,
    ) as span:
        span.set_attribute("messaging.kafka.partition", message.partition())
        span.set_attribute("messaging.kafka.offset", message.offset())
        process(message.value())
```

The `Link` instead of parent is intentional. In async processing, the producer and consumer run in separate traces that may be seconds or minutes apart. A parent-child relationship implies the child's latency is part of the parent's. A Link says "this work was caused by that work" without contaminating the latency view.

## Sampling Strategies That Don't Lie

Head-based sampling (decide at trace entry whether to record) is cheap but blind — you sample uniformly without knowing whether the trace will be interesting. A 1% sample rate will capture roughly 1% of errors, which may be far too few when your error rate is already 0.1%.

The production-grade approach is **tail-based sampling**: buffer all spans for a trace until the trace completes (or times out), then apply sampling rules based on the full trace — keep all error traces, keep all traces above 500ms, keep 1% of everything else. The OpenTelemetry Collector supports this natively:

```yaml
# otel-collector-config.yaml
processors:
  tail_sampling:
    decision_wait: 10s          # wait up to 10s for all spans to arrive
    num_traces: 50000           # max traces in memory at once
    policies:
      - name: errors-policy
        type: status_code
        status_code: {status_codes: [ERROR]}
      - name: slow-traces-policy
        type: latency
        latency: {threshold_ms: 500}
      - name: probabilistic-policy
        type: probabilistic
        probabilistic: {sampling_percentage: 1}
```

The catch with tail sampling is that you need to route all spans for a given trace to the same collector instance — either by using consistent hashing on trace ID at the load balancer in front of your collectors, or by using a collector agent locally and a small collector cluster for tail sampling.

## The One Thing That Breaks Everything

Incorrect clock synchronization. Spans carry wall-clock timestamps. If your service pods have drifted NTP clocks — which happens — spans from service B will appear to start before service A sent the request. The waterfall view becomes nonsensical.

Fix: run `chrony` or `timedatectl` on your nodes, verify with `chronyc tracking`, and alert when `System time offset` exceeds 10ms. In Kubernetes, node clock sync is inherited from the hypervisor — verify it's actually configured, don't assume it is. A trace where the database query appears to finish before the application sent the SQL is not showing you a time machine; it's showing you NTP drift.

## Start Here

Pick one service — ideally one that's on the critical path for a high-value user flow. Add OpenTelemetry SDK instrumentation, confirm spans appear in your backend, then verify that `traceparent` is being injected into every outbound call and extracted from every inbound one. Get that one service right before spreading to others. Incomplete propagation produces misleading traces; it's better to have one fully-instrumented service than ten that all drop the context at different points.

The goal is not coverage. The goal is that when p99 spikes, you can open one trace from that window and read exactly where the time went.
