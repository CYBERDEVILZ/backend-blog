---
layout: post
title: "Debugging Slow Kafka Consumers: Lag, Rebalancing, and Partition Skew in Production"
date: 2026-05-15
tags: [kafka, distributed-systems, messaging, performance]
read_time: 11
---

Your Kafka consumer group is falling behind. The lag metric on your dashboard is climbing—50,000 messages, then 200,000, then a million. Your on-call engineer gets paged at 2 AM. Where do you start?

Slow Kafka consumers fail in three distinct ways, and conflating them sends you chasing the wrong fix. Lag from rebalancing looks identical to lag from a genuinely slow consumer if you only look at aggregate numbers. Partition skew looks like a throughput problem until you see that 11 of your 12 consumers are idle. Each failure mode has a different root cause and a different fix.

## Consumer Lag: What the Number Actually Means

Consumer lag is the difference between the latest offset in a partition and the committed offset of your consumer group. High lag means your consumers are processing slower than producers are writing—but "slow" can mean at least three different things before you've looked at any detail.

The first mistake is watching total group lag as an aggregate. A consumer group with 12 partitions, 11 of which are current and one of which is 2 million messages behind, shows a misleading average. You need partition-level lag from the start.

```shell
# Get per-partition lag for a consumer group
kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group my-consumer-group

# TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID          HOST
# events          0          4821903         4821903         0     consumer-1-abc       /10.0.0.1
# events          1          3109421         3109421         0     consumer-2-def       /10.0.0.2
# events          2          491203          2509841         2018638  consumer-3-ghi    /10.0.0.3
```

Uniform lag across all partitions: throughput problem. Lag concentrated in specific partitions: skew. Lag that spikes and then partially recovers repeatedly across all partitions simultaneously: rebalances.

## Diagnosing Rebalance Storms

A consumer group rebalance reassigns partition ownership. During a rebalance, all consumption stops. If rebalances happen frequently, you can have near-zero throughput even with completely healthy consumers.

Common causes:
- `max.poll.interval.ms` exceeded: the consumer took too long between `poll()` calls
- `session.timeout.ms` exceeded: the broker stopped receiving heartbeats
- Consumers joining and leaving the group because of deploys, crashes, or autoscaling
- JVM GC pauses that outlast timeout thresholds

The signature in your lag graph is a sawtooth: lag climbs sharply during the rebalance pause, then drops as processing resumes, then climbs again on the next rebalance. If your rebalances are frequent enough, the drops never catch up.

Instrument your consumer to detect and log rebalances so you can correlate events with lag spikes:

```python
from confluent_kafka import Consumer
import time
import logging

logger = logging.getLogger(__name__)

class InstrumentedConsumer:
    def __init__(self, bootstrap_servers):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'my-consumer-group',
            'auto.offset.reset': 'earliest',
            # Set higher than your worst-case processing time per batch
            'max.poll.interval.ms': 300000,   # 5 minutes
            # Heartbeat interval should be 1/3 of session timeout
            'heartbeat.interval.ms': 3000,
            'session.timeout.ms': 10000,
            # Disable auto-commit so you control exactly when offsets advance
            'enable.auto.commit': False,
        })
        self._rebalance_count = 0

    def _on_assign(self, consumer, partitions):
        self._rebalance_count += 1
        logger.warning(
            "Rebalance assign #%d: partitions=%s",
            self._rebalance_count,
            [f"{p.topic}[{p.partition}]" for p in partitions],
        )

    def _on_revoke(self, consumer, partitions):
        logger.warning(
            "Rebalance revoke: losing partitions=%s",
            [f"{p.topic}[{p.partition}]" for p in partitions],
        )
        # Commit before losing ownership—otherwise you re-process on rejoin
        consumer.commit(asynchronous=False)

    def run(self, topic):
        self.consumer.subscribe(
            [topic], on_assign=self._on_assign, on_revoke=self._on_revoke
        )
        while True:
            poll_start = time.monotonic()
            msg = self.consumer.poll(timeout=1.0)
            poll_elapsed = time.monotonic() - poll_start

            # Log when poll takes suspiciously long—a sign you're near the interval limit
            if poll_elapsed > 10.0:
                logger.error(
                    "Slow poll: %.2fs — approaching max.poll.interval.ms limit", poll_elapsed
                )

            if msg is None or msg.error():
                continue

            self.process(msg)
            self.consumer.commit(asynchronous=False)

    def process(self, msg):
        raise NotImplementedError
```

If your processing genuinely exceeds `max.poll.interval.ms`, the answer is not to increase the timeout to a large number and hope for the best. The fix is to move slow work off the hot path: poll, acknowledge to Kafka, then process asynchronously with a bounded in-process queue. Only do this if your downstream is idempotent or you accept at-least-once delivery with duplicate handling.

Alternatively, if deploys are causing rebalances, use cooperative sticky rebalancing (`partition.assignment.strategy=CooperativeStickyAssignor`). Unlike the default eager rebalance which stops all consumers and reassigns everything, cooperative rebalancing only migrates the partitions that actually need to move—surviving consumers keep processing during the transition.

## Identifying Partition Skew

Partition skew is when some partitions receive dramatically more messages than others, leaving the consumer assigned to the hot partition as your permanent bottleneck while other consumers sit idle.

Skew happens when:
- Your partition key has uneven cardinality (user IDs where 0.1% of users generate 50% of events)
- You're partitioning by an enum with few values and many partitions
- You have intentional ordering by key but low key diversity

Diagnose by comparing partition sizes:

```shell
# Get per-partition size and offset lag directly from broker log dirs
kafka-log-dirs.sh \
  --bootstrap-server kafka:9092 \
  --topic-list events \
  --describe \
  | grep -o '"partition":"[^"]*","size":[0-9]*,"offsetLag":[0-9]*' \
  | awk -F'[":,]' '{printf "partition=%-20s size=%-12s lag=%s\n", $4, $7, $10}' \
  | sort -t= -k3 -rn

# partition=events-2         size=8421376    lag=2018638
# partition=events-0         size=1024000    lag=0
# partition=events-1         size=998400     lag=0
```

If you confirm skew from a hot key, your options depend on root cause:

**Hot user IDs or entity IDs**: Switch to a composite partition key. Instead of `user_id`, use `(user_id, shard_suffix)` where `shard_suffix = random.randint(0, 9)`. This breaks strict per-user ordering, which is usually acceptable if your consumer handles reordering or your downstream is idempotent.

**Low cardinality enum keys**: If you have 6 event types and 24 partitions, you can never distribute evenly. Either add entropy to the key or accept the skew and assign more consumers to hot partitions using static membership and manual partition assignment.

**Permanently hot partitions**: More partitions is the long-term answer. Kafka doesn't retroactively rebalance existing messages when you add partitions, so plan partition counts with growth in mind. Changing partition count is a one-way operation that breaks key-based ordering for existing consumers.

## Tuning for Throughput When the Consumer Is Genuinely Slow

If lag is growing uniformly across all partitions and you've ruled out rebalances, your consumers are just processing slower than the write rate. Three levers:

**Batch fetching**: Fetch more data per poll call to reduce per-message overhead.

```shell
# kafka consumer config: reduce per-message overhead by fetching larger batches
fetch.min.bytes=65536         # wait for 64KB before returning a fetch response
fetch.max.wait.ms=500         # but never wait more than 500ms
max.partition.fetch.bytes=10485760  # up to 10MB per partition per fetch
```

**Batch processing**: Don't send records downstream one at a time when your backend supports bulk operations.

```python
def consume_in_batches(consumer, batch_size=500, max_wait_seconds=1.0):
    """
    Accumulate messages up to batch_size or max_wait_seconds, whichever comes first,
    then process as a single unit—ideal for bulk database inserts or batch API calls.
    """
    batch = []
    deadline = time.monotonic() + max_wait_seconds

    while len(batch) < batch_size and time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        msg = consumer.poll(timeout=max(0, remaining))
        if msg and not msg.error():
            batch.append(msg)

    if not batch:
        return

    # One database transaction for the entire batch instead of N individual writes
    with db.transaction():
        db.bulk_insert([deserialize(m.value()) for m in batch])

    # Commit the highest offset; all preceding offsets are implicitly committed
    consumer.commit(asynchronous=False)
```

**Scale consumers to partition count**: The hard ceiling is one active consumer per partition in a group. If you have 12 partitions and 20 consumer instances, 8 are idle. Add partitions first, then add consumers. Partition count is permanent—size it for your projected peak, not your current load.

## One Fix That Applies Regardless of Root Cause

Emit per-partition lag as a time-series metric, not just group-level totals. Alert on any single partition exceeding your SLO lag threshold. When you get paged, your first query should be: which partition? That answer immediately tells you whether you're looking at a rebalance (all partitions spike simultaneously and recover), skew (one partition is permanently behind while others are current), or a throughput problem (all partitions grow at the same steady rate).

Most Kafka monitoring setups instrument at the group level because it's the default output of `kafka-consumer-groups.sh`. Add partition-level lag to your metrics pipeline before the next incident—it turns a 2 AM debugging session into a five-minute diagnosis.
