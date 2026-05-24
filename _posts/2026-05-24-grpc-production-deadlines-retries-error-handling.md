---
layout: post
title: "gRPC in Production: Deadlines, Retries, and Error Handling That Prevents Cascades"
date: 2026-05-24
tags: [grpc, distributed-systems, resilience, backend]
read_time: 11
---

Your payment service starts throwing 503s. You check the gRPC logs: `DEADLINE_EXCEEDED` on every call to the inventory service. The inventory service looks fine — 99th percentile latency is 40ms. But the payment service is piling up goroutines, all blocked waiting for inventory responses that never arrive. Within two minutes you've got a cascade: payment, checkout, and order history are all down. The inventory service is healthy the whole time.

This is the most common failure pattern in gRPC-based microservices, and it has a specific, preventable cause: missing or misconfigured deadlines, combined with retry behavior that amplifies load at exactly the wrong moment.

## Why gRPC Cascades Happen

gRPC exposes four things you must get right: deadlines, status codes, retries, and interceptors. Most teams handle status codes but leave the other three on defaults. The defaults will kill you.

**The default deadline is no deadline.** A gRPC call without a deadline can block forever. If your downstream is slow — due to a GC pause, a hot mutex, a slow database query — every goroutine (or thread) waiting on that call is stuck. When those goroutines exhaust your server's capacity, your service stops accepting new requests. You are now down, even though the downstream is technically responding.

**The default retry behavior in most client libraries is none.** That sounds safe, but it pushes retry logic to the application layer where teams often implement it without backoff, jitter, or budget constraints. A retry loop with no backoff under load is a DoS attack on your own infrastructure.

## Set Deadlines at Every Hop

The correct model is: each service sets a deadline on calls it makes to its dependencies. The deadline should reflect how long *you* can wait, given your own SLA, not some optimistic guess at downstream latency.

In Go, the pattern looks like this:

```go
func (s *PaymentService) ProcessPayment(ctx context.Context, req *pb.PaymentRequest) (*pb.PaymentResponse, error) {
    // Inherit deadline from incoming context, but cap it.
    // If the caller gave us 5s, we give inventory 1s max.
    // This ensures we always have budget left for our own work.
    inventoryCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
    defer cancel()

    inv, err := s.inventoryClient.Reserve(inventoryCtx, &inventorypb.ReserveRequest{
        ItemID:    req.ItemID,
        Quantity:  req.Quantity,
    })
    if err != nil {
        st, ok := status.FromError(err)
        if !ok {
            return nil, status.Errorf(codes.Internal, "non-gRPC error from inventory: %v", err)
        }
        switch st.Code() {
        case codes.DeadlineExceeded, codes.Unavailable:
            // These are transient. Signal to the caller they can retry.
            return nil, status.Errorf(codes.Unavailable, "inventory temporarily unavailable")
        case codes.NotFound:
            return nil, status.Errorf(codes.FailedPrecondition, "item %s not in inventory", req.ItemID)
        default:
            return nil, status.Errorf(codes.Internal, "inventory error: %s", st.Message())
        }
    }
    // ... process payment using inv
}
```

Key points:
- `context.WithTimeout(ctx, 1*time.Second)` — inherits cancellation from the parent but adds its own deadline. If the upstream context is already past its deadline, this call fails immediately.
- The timeout is 1s, not "however long inventory takes." Your service's SLA drives this number.
- Error translation: `DEADLINE_EXCEEDED` from inventory becomes `UNAVAILABLE` to your caller. You expose your internal topology to callers only when necessary; usually you don't want them to know which downstream failed.

**Deadline propagation in the wire format**: gRPC transmits deadlines as a `grpc-timeout` HTTP/2 header. If you set a 5s deadline on an inbound request and then make an outbound call with `context.WithTimeout(ctx, 1s)`, the outbound call will have at most 1s, but also honors the parent's remaining budget. If the parent context has 200ms left, the 1s timeout is irrelevant — the call gets 200ms.

## Configure Server-Side Max Deadline

Even if your clients set deadlines, malicious or buggy callers may not. Add a server-side maximum:

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(
        maxDeadlineInterceptor(5 * time.Second),
    ),
)

func maxDeadlineInterceptor(max time.Duration) grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
        deadline, ok := ctx.Deadline()
        if !ok || time.Until(deadline) > max {
            var cancel context.CancelFunc
            ctx, cancel = context.WithTimeout(ctx, max)
            defer cancel()
        }
        return handler(ctx, req)
    }
}
```

This interceptor runs before your handler. If the client sends no deadline or an absurd one, you cap it. This prevents individual slow calls from holding goroutines indefinitely even when clients misbehave.

## Retries: Service Config, Not Application Code

The correct place to configure gRPC retries is the [gRPC service config](https://github.com/grpc/grpc/blob/master/doc/service_config.md), not your application code. Service config can be delivered via DNS (if your name server supports it) or hardcoded into the client. It gives you retries with proper backoff without cluttering call sites.

```go
serviceConfig := `{
  "methodConfig": [{
    "name": [{"service": "inventory.InventoryService"}],
    "retryPolicy": {
      "maxAttempts": 3,
      "initialBackoff": "0.1s",
      "maxBackoff": "1s",
      "backoffMultiplier": 2,
      "retryableStatusCodes": ["UNAVAILABLE", "RESOURCE_EXHAUSTED"]
    },
    "timeout": "5s"
  }]
}`

conn, err := grpc.Dial(
    "inventory-service:50051",
    grpc.WithDefaultServiceConfig(serviceConfig),
    grpc.WithTransportCredentials(insecure.NewCredentials()),
)
```

Critical decisions in this config:
- `retryableStatusCodes`: Only `UNAVAILABLE` and `RESOURCE_EXHAUSTED`. Never retry `DEADLINE_EXCEEDED` — the operation may have already completed; retrying risks double-execution. Never retry `INTERNAL`, `DATA_LOSS`, or `UNIMPLEMENTED`.
- `maxAttempts: 3`: 3 attempts total (1 original + 2 retries). Beyond 3, you're adding load during an incident, not helping.
- `timeout: "5s"`: This is a per-RPC deadline applied when the caller doesn't set one. It acts as a backstop.

Retries are only safe if the underlying operations are idempotent. For mutations, either ensure the operation is idempotent (accept a client-supplied idempotency key in the request proto) or do not add mutation methods to the retryable status codes list.

## Observing What's Actually Happening

Deadlines and retries interact in ways that are hard to see without instrumentation. You need two metrics minimum:

```python
# Prometheus example: add these to your gRPC server interceptor

from prometheus_client import Counter, Histogram

grpc_requests_total = Counter(
    'grpc_requests_total',
    'Total gRPC requests',
    ['method', 'status_code']
)

grpc_request_duration_seconds = Histogram(
    'grpc_request_duration_seconds',
    'gRPC request duration',
    ['method'],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

def metrics_interceptor(request, context, method_name):
    start = time.time()
    try:
        response = method_handler(request, context)
        grpc_requests_total.labels(method=method_name, status_code='OK').inc()
        return response
    except grpc.RpcError as e:
        grpc_requests_total.labels(method=method_name, status_code=e.code().name).inc()
        raise
    finally:
        grpc_request_duration_seconds.labels(method=method_name).observe(time.time() - start)
```

The metric that catches cascade precursors is the rate of `DEADLINE_EXCEEDED` on outbound calls. When that rate climbs, you have a downstream problem that will eventually exhaust your capacity. Alert on it before it escalates:

```yaml
# Prometheus alert
- alert: GrpcHighDeadlineExceededRate
  expr: |
    rate(grpc_requests_total{status_code="DEADLINE_EXCEEDED"}[5m]) /
    rate(grpc_requests_total[5m]) > 0.05
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "{{ $labels.method }} has >5% DEADLINE_EXCEEDED over 5 minutes"
```

5% is a reasonable threshold. At 1-2% you likely have legitimate slow callers; at 5%+ you have a systemic problem forming.

## The Load Shedding Backstop

Deadlines and retries protect against slow downstreams. But when your service itself is overloaded — when the goroutine pool is full or CPU is saturated — you need to shed load, not accept more work.

```go
// Attach to the gRPC server as an interceptor, before any business logic
func loadSheddingInterceptor(maxInFlight int64) grpc.UnaryServerInterceptor {
    var inFlight int64
    return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
        current := atomic.AddInt64(&inFlight, 1)
        defer atomic.AddInt64(&inFlight, -1)

        if current > int64(maxInFlight) {
            return nil, status.Errorf(codes.ResourceExhausted, "server overloaded, try again later")
        }
        return handler(ctx, req)
    }
}
```

`RESOURCE_EXHAUSTED` is in the retryable status codes list above. The client sees the error, waits with backoff, and retries — which is exactly the behavior you want. You've communicated "I'm alive but busy" rather than timing out silently.

The right value for `maxInFlight` depends on your service. Start at 2× your expected peak concurrency and tune downward if you observe latency degradation before the limit is reached.

## The Concrete Checklist

If you're running gRPC services in production today, verify these four things:

1. **Every outbound gRPC call has a context with a deadline.** Grep your codebase for `grpc.Dial` and `client.Method(` — any call passing `context.Background()` without a subsequent `WithTimeout` or `WithDeadline` is a liability.

2. **Retries are configured via service config, not ad-hoc loops.** The service config approach enforces consistent backoff and respects the deadline budget; application-level retry loops almost never do.

3. **Only `UNAVAILABLE` and `RESOURCE_EXHAUSTED` are retried.** `DEADLINE_EXCEEDED` and anything indicating a definitive server-side error must not be retried automatically.

4. **You alert on elevated `DEADLINE_EXCEEDED` rates before they hit 5%.** By the time calls are failing at 10%, you are already in an incident; the alert should fire at 5% so you investigate while there's still capacity to respond.

Getting these four right won't eliminate all gRPC-related incidents, but it will prevent the entire class of cascade failures where a degraded dependency takes down services that should have been shielded from it.
