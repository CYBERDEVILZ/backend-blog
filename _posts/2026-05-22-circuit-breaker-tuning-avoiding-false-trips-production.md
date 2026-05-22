---
layout: post
title: "Circuit Breaker Tuning: Avoiding False Trips in Production"
date: 2026-05-22
tags: [reliability, microservices, production, resilience]
read_time: 8
---

Your order service is returning 503s. The payment service recovered from a GC pause four minutes ago, but the circuit breaker never noticed—it is still open. Every request to payment fails immediately. The payment team is watching healthy metrics while your users get errors.

This is a false-trip-and-stuck-open failure. The circuit breaker did exactly what it was configured to do. The configuration was wrong.

## Why Circuit Breakers Stay Open Too Long

The failure threshold to *open* a circuit gets tuned obsessively. The recovery path gets ignored.

A typical misconfigured circuit breaker:
- Opens on 50% errors over 10 seconds ✓
- Waits 30 seconds before probing ✓
- Allows 1 probe in half-open ✓
- Returns to open if the probe fails ✗

A single slow probe request—one that hits the dependency at the worst moment during recovery—puts you back to open. You wait another 30 seconds. The dependency has been healthy for three minutes but your circuit will not close.

The fix is not "allow more failures." The fix is changing what you measure and how many probes you require.

## Measuring the Right Things

Circuit breakers operate on two signals: **error rate** and **slow call rate**. Most teams only configure error rate. Slow calls that succeed still damage your service, but more importantly, a recovering dependency will often have elevated latency before its error rate drops.

If you only track errors, the recovery window looks like this:

1. Dependency has a problem → error rate spikes → circuit opens
2. Dependency recovers → latency still elevated → your probe times out → counted as failure → circuit stays open
3. Latency normalizes → probe succeeds → circuit closes

You have been failing requests for the entire elevated-latency window for no reason. Adding a **slow call threshold** lets you use latency as an early indicator during half-open probing without using it as a trip signal during normal operation.

## A Properly Tuned Implementation

Here is a production-ready circuit breaker setup using Sony's gobreaker, extended to handle half-open probe logic correctly:

```go
package breaker

import (
    "context"
    "errors"
    "sync"
    "time"

    "github.com/sony/gobreaker"
)

type Config struct {
    Name             string
    MaxRequests      uint32        // probe requests allowed in half-open
    Interval         time.Duration // sliding window for error counting
    Timeout          time.Duration // how long to stay open before probing
    FailureRatio     float64       // fraction of failures required to trip
    SlowCallDuration time.Duration // what counts as a "slow" call
}

func New(cfg Config) *gobreaker.CircuitBreaker {
    var mu sync.Mutex
    _ = mu

    return gobreaker.NewCircuitBreaker(gobreaker.Settings{
        Name:        cfg.Name,
        MaxRequests: cfg.MaxRequests, // use 5, not 1—explained below
        Interval:    cfg.Interval,
        Timeout:     cfg.Timeout,

        ReadyToTrip: func(c gobreaker.Counts) bool {
            // Don't trip on tiny sample sizes. A single failure on startup
            // or after a rolling deploy should not open the circuit.
            if c.Requests < 10 {
                return false
            }
            failureRatio := float64(c.TotalFailures) / float64(c.Requests)
            return failureRatio >= cfg.FailureRatio
        },

        OnStateChange: func(name string, from, to gobreaker.State) {
            // Emit to your metrics system here.
            // circuitBreakerState.WithLabelValues(name, to.String()).Set(1)
        },
    })
}

// CallWithTimeout wraps Execute with an explicit deadline. The timeout here
// must be shorter than your upstream caller's context—you need the breaker
// to record a failure before the caller's context cancels and the result
// is discarded.
func CallWithTimeout(cb *gobreaker.CircuitBreaker, timeout time.Duration, fn func() error) error {
    _, err := cb.Execute(func() error {
        ctx, cancel := context.WithTimeout(context.Background(), timeout)
        defer cancel()

        done := make(chan error, 1)
        go func() { done <- fn() }()

        select {
        case err := <-done:
            return err
        case <-ctx.Done():
            // Typed error so callers distinguish circuit-level timeouts
            // from dependency-level errors.
            return ErrCallTimeout
        }
    })
    return err
}

var ErrCallTimeout = errors.New("circuit breaker: call timeout")
```

Key decisions here:

**`MaxRequests: 5` instead of 1.** Requiring a majority of 5 probes to succeed before closing eliminates false-closure on a lucky probe and false-stay-open on one unlucky one. With a single probe, you are flipping a coin on whether the circuit recovers.

**`Requests < 10` guard in `ReadyToTrip`.** Without this floor, a service that restarts against a recovering dependency opens its circuit on the first two failures and then thrashes: open → half-open → open → half-open, indefinitely, while the dependency is actually fine.

**Timeout shorter than caller's context.** If the upstream hangs and your caller cancels the request, `Execute` returns before the goroutine finishes—the circuit breaker never records a failure. This is a common silent bug where network hangs accumulate without ever tripping the circuit. Put your timeout inside `CallWithTimeout`, not outside it.

## Sliding Window vs. Count-Based Windows

The window type is the other major source of false trips.

A **count-based window** trips when the last N requests had more than X% failures. Under low traffic, 3 failures out of 5 requests opens the circuit. That is 2 unhappy users causing degraded experience for everyone.

A **time-based sliding window** with a minimum request threshold is more robust. In Resilience4j:

```yaml
resilience4j:
  circuitbreaker:
    instances:
      payment-service:
        sliding-window-type: TIME_BASED
        sliding-window-size: 30           # seconds of history
        minimum-number-of-calls: 20       # require statistical confidence
        failure-rate-threshold: 50        # percentage to trip open
        slow-call-rate-threshold: 80      # percentage of calls counted as slow
        slow-call-duration-threshold: 2s  # definition of "slow"
        wait-duration-in-open-state: 30s
        permitted-number-of-calls-in-half-open-state: 5
        automatic-transition-from-open-to-half-open-enabled: true
```

`minimum-number-of-calls: 20` is the single most important setting that most configs omit. Without it, every cold start and every restart against an imperfect dependency risks nuisance trips.

`automatic-transition-from-open-to-half-open-enabled: true` is critical for low-traffic services. Without it, the circuit only transitions to half-open when an incoming request arrives. If you get one request per minute and your open-state timeout is 30 seconds, you may wait a full minute before the first probe—and have no observability into the fact that the circuit is stuck.

## Diagnosing a Stuck-Open Circuit in Production

When a circuit will not close, check these in order:

```bash
# 1. Confirm what state the circuit is actually in.
#    Never assume—check the metric directly.
curl -s localhost:9090/metrics | grep circuit_breaker_state

# 2. Is the dependency actually healthy?
#    Don't trust the circuit. Hit it directly with a realistic request.
curl -w "\n%{http_code} in %{time_total}s\n" https://payment-service/healthz

# 3. What are the half-open probe results?
#    This is the most useful signal and the one most teams don't emit.
circuit_breaker_half_open_calls_total{name="payment-service",result="success"}
circuit_breaker_half_open_calls_total{name="payment-service",result="failure"}

# 4. Are probes timing out before succeeding?
#    Compare p99 probe duration against your configured timeout.
histogram_quantile(
  0.99,
  rate(circuit_breaker_call_duration_seconds_bucket{name="payment-service"}[5m])
)

# 5. Check for goroutine leaks eating your probe budget.
#    A blocked goroutine from a previous probe can count against MaxRequests.
curl -s localhost:6060/debug/pprof/goroutine?debug=1 | grep -A 5 payment
```

If probes return HTTP 200 but the circuit refuses to close, the cause is almost always one of:
- `MaxRequests` is 1 and a single flaky probe keeps resetting the timeout
- Your `Execute` inner function swallows errors and returns `nil`, so failures never register
- The probe hits a different instance or load balancer path than the one that was failing, giving false-positive success—confirm with service mesh telemetry or request tracing

## The Actionable Takeaway

Set `minimum-number-of-calls` (or an equivalent `Requests < N` guard in your custom implementation) before touching anything else. Without a floor on sample size, every low-traffic service and every restart is at risk of nuisance trips. Then raise your half-open probe count from 1 to at least 5. These two changes eliminate the majority of false-trip and stuck-open incidents without weakening your actual failure detection.
