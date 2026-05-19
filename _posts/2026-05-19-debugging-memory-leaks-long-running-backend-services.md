---
layout: post
title: "Debugging Memory Leaks in Long-Running Backend Services"
date: 2026-05-19
tags: [memory, debugging, python, go]
read_time: 11
---

Your service starts at 200 MB RSS on Monday. By Thursday it's at 1.8 GB and the OOM killer is circling. You restart it, and the cycle begins again. The cause isn't obvious—no single request is pathologically allocating memory, heap profiling during a snapshot looks fine, and the leak only manifests after days of traffic. This is the signature of a slow, structural memory leak.

This post covers a systematic approach for diagnosing and fixing memory leaks in production backend services, using Python and Go as examples.

## Establish a Baseline First

Before you can find a leak, you need metrics that tell you *when* it started, *how fast* it grows, and *which workload* triggers it. Relying on OOM alerts means you're always reacting after the fact.

Instrument resident set size (RSS) and heap allocation at the process level:

```python
# Python: expose memory metrics via a /metrics endpoint
import os
import resource
import tracemalloc
from prometheus_client import Gauge

RSS_GAUGE = Gauge("process_rss_bytes", "Resident set size in bytes")
HEAP_GAUGE = Gauge("process_heap_bytes", "Python heap allocation in bytes")

def update_memory_metrics():
    # RSS from /proc/self/status — more reliable than resource.getrusage on Linux
    rss = 0
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1]) * 1024
                    break
    except OSError:
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024

    RSS_GAUGE.set(rss)

    # tracemalloc tracks Python-level heap; won't catch C extension allocations
    if tracemalloc.is_tracing():
        current, _ = tracemalloc.get_traced_memory()
        HEAP_GAUGE.set(current)
```

Call this function from a background thread every 30 seconds and expose it via `/metrics`. Graph RSS over time in Grafana. If it rises monotonically between restarts, you have a leak. If it plateaus after a traffic spike and stays there, you're looking at a cache that never evicts.

The distinction matters: a true leak grows without bound; cache accumulation can be bounded with a TTL or size cap.

## Narrowing the Leak to a Component

Once you confirm a leak exists, the fastest way to localize it is to disable subsystems temporarily and watch whether the growth rate changes. This works in staging if you can reproduce the leak in under an hour with a realistic traffic replay.

Create a synthetic workload with `k6` or `hey` that replays your top-10 endpoints by traffic share. Run the service normally for 30 minutes, measure RSS slope (bytes/minute). Then disable one subsystem—a caching layer, a background worker, a WebSocket handler—and repeat. If disabling a component changes the slope, you've found the guilty subsystem.

A common culprit in Python services: unbounded `asyncio` task queues, module-level dicts acting as caches with no eviction, and `threading.local()` values on thread pools that outlive individual requests.

In Go services, look for goroutine leaks first:

```go
// Go: expose a /debug/goroutines endpoint to count goroutines over time
import (
    "fmt"
    "net/http"
    "runtime"
)

func goroutineHandler(w http.ResponseWriter, r *http.Request) {
    // runtime.NumGoroutine() is cheap and safe to call in production
    count := runtime.NumGoroutine()
    fmt.Fprintf(w, "goroutines: %d\n", count)

    // For a full stack dump (expensive, use only during incident diagnosis):
    // buf := make([]byte, 1<<20)
    // n := runtime.Stack(buf, true)
    // w.Write(buf[:n])
}
```

If goroutine count grows alongside RSS, you have goroutines that are blocking indefinitely—typically on channel reads with no sender, or `http.Client` calls with no timeout that are stuck waiting on a dead upstream.

## Python: Finding the Actual Objects

Once you've isolated the subsystem, you need to find which Python objects are accumulating. `tracemalloc` gives you allocation call stacks. `gc.get_objects()` lets you count by type.

The most practical approach for production is to enable `tracemalloc` with a small buffer and expose a snapshot endpoint behind authentication:

```python
import gc
import tracemalloc
from collections import Counter
from flask import Blueprint, jsonify

memory_bp = Blueprint("memory", __name__)

@memory_bp.route("/debug/memory")
def memory_snapshot():
    # Take a snapshot of the top 20 allocation sites
    if not tracemalloc.is_tracing():
        return jsonify({"error": "tracemalloc not active"}), 503

    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics("lineno")

    result = []
    for stat in top_stats[:20]:
        result.append({
            "file": stat.traceback[0].filename,
            "line": stat.traceback[0].lineno,
            "size_kb": stat.size // 1024,
            "count": stat.count,
        })

    # Also count live objects by type — catches leaks tracemalloc misses
    # (e.g., objects kept alive by __del__ preventing GC)
    gc.collect()
    type_counts = Counter(type(obj).__name__ for obj in gc.get_objects())
    top_types = type_counts.most_common(15)

    return jsonify({
        "top_allocations": result,
        "top_object_types": top_types,
    })
```

Enable tracing at startup: `tracemalloc.start(25)` (25-frame stack depth). Hit this endpoint on a freshly started service, wait 2 hours under load, hit it again, and diff the `size_kb` values. Whatever grew is the leak.

A real example: a data processing service was leaking `dict` objects. The `top_object_types` output showed 400,000 live dicts after 3 hours, up from 12,000 at startup. The allocation stack pointed to a request-context object that was stored in a module-level list for "audit logging" but never pruned. The fix: replace the list with a `collections.deque(maxlen=10000)`.

## Go: Heap Profiling Without Stopping Traffic

Go's `net/http/pprof` package is the standard tool. The critical insight is to take *two* heap profiles 10 minutes apart and diff them—a single snapshot tells you where memory is, not where it's growing.

```bash
# Take baseline heap profile
curl -s http://localhost:6060/debug/pprof/heap > heap1.prof

# Wait 10 minutes under load
sleep 600

# Take second profile
curl -s http://localhost:6060/debug/pprof/heap > heap2.prof

# Diff them: shows what grew between the two snapshots
go tool pprof -diff_base heap1.prof heap2.prof
# Inside pprof:
#   (pprof) top20
#   (pprof) web   # opens a flame graph if graphviz is installed
```

The `inuse_space` metric shows allocations still live. The diff focuses you only on what accumulated between snapshots, eliminating one-time initialization noise.

Common Go leak patterns:

**Goroutine leak via context not propagated:**
```go
// BUG: goroutine leaks if parent context is cancelled before HTTP call completes
func fetchUser(userID string) (*User, error) {
    // context.Background() ignores the request's cancellation signal
    req, _ := http.NewRequestWithContext(context.Background(), "GET",
        fmt.Sprintf("http://user-service/users/%s", userID), nil)
    resp, err := httpClient.Do(req)
    // ...
}

// FIX: thread the request context through
func fetchUser(ctx context.Context, userID string) (*User, error) {
    req, _ := http.NewRequestWithContext(ctx, "GET",
        fmt.Sprintf("http://user-service/users/%s", userID), nil)
    resp, err := httpClient.Do(req)
    // ...
}
```

**String interning accumulation:** In Go, converting `[]byte` to `string` allocates. If you're converting large response bodies to strings and storing them in a map—even temporarily—the GC may not reclaim them promptly under sustained load.

## The Difference Between a Leak and a Cache

Many "memory leaks" are actually unbounded caches. These are fixable without touching core logic. The key properties to audit in any in-memory cache:

1. **Max size cap** — does it have one? If not, add `maxsize` to your LRU.
2. **TTL or access-based eviction** — can entries live forever?
3. **Key cardinality** — if the key includes user ID or request ID, the cache will grow with your user base indefinitely.

```python
# Dangerous: module-level dict keyed on arbitrary strings
_compiled_templates = {}

def render(template_name: str, context: dict) -> str:
    if template_name not in _compiled_templates:
        _compiled_templates[template_name] = compile_template(template_name)
    return _compiled_templates[template_name].render(context)

# Safe: bounded LRU cache
from functools import lru_cache

@lru_cache(maxsize=512)
def _get_compiled_template(template_name: str):
    return compile_template(template_name)

def render(template_name: str, context: dict) -> str:
    return _get_compiled_template(template_name).render(context)
```

`functools.lru_cache` is thread-safe for reads but uses a single lock for writes. For high-concurrency services, consider `cachetools.TTLCache` with an explicit lock, or an external cache like Redis for anything shared across workers.

## Validating the Fix in Production

After deploying a fix, don't just watch for OOM events—measure the RSS slope directly. A fixed leak should show flat RSS growth after traffic stabilizes. Define a concrete pass/fail criterion before deploying: "RSS must not exceed 400 MB after 24 hours of production traffic."

If your service runs in Kubernetes, add a memory limit and a liveness probe that fails when `/proc/self/status` VmRSS exceeds your threshold. This forces a restart before the OOM killer does it for you, giving you a cleaner signal and protecting neighbor pods on the same node.

## The Actionable Takeaway

Add RSS and heap metrics to your service today if they're not already there. Export them to your metrics system and create an alert for monotonically increasing RSS over a 6-hour window. The next time a memory issue surfaces, you'll have trend data going back weeks, and you'll be able to correlate the onset with a specific deployment or traffic event—cutting hours off the diagnosis.
