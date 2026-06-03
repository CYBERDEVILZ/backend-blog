---
layout: post
title: "Kafka Exactly-Once Semantics: How It Works and When You Actually Need It"
date: 2026-06-03
tags: [kafka, distributed-systems, messaging, reliability]
read_time: 9
---

Your payment processing pipeline debited the customer's account twice. The Kafka consumer crashed mid-flight after committing the side effect but before committing the offset. On restart it reprocessed the message. Your at-least-once delivery guarantee did exactly what it promised — and it cost you a duplicate charge, an incident, and a support ticket queue.

This is the failure mode that makes engineers reach for Kafka's exactly-once semantics (EOS). The question is whether you actually need EOS, or whether idempotent consumers plus careful offset management would have been enough and far cheaper operationally.

## What "Exactly Once" Actually Means

Kafka's EOS guarantee is scoped to a specific boundary: **produce → broker → consume → produce**. It guarantees that a message produced to Kafka is stored exactly once on the broker even if the producer retries, and that within a consume-transform-produce loop (read-process-write), the output record appears exactly once in the output topic.

It does *not* guarantee that your database write, HTTP call to an external API, or any side effect outside Kafka happens exactly once. That boundary is yours to own.

The three components that combine to give EOS:

1. **Idempotent producer** — the broker deduplicates retries using a producer ID and sequence number. If a network failure causes the producer to retry a batch already written, the broker drops the duplicate.
2. **Transactions** — a producer can atomically write records to multiple partitions and commit or abort the whole batch. Consumers configured with `isolation.level=read_committed` only see records from committed transactions.
3. **Transactional consumer-producer loop** — when you consume from one topic and produce to another, the offset commit for the source partition is included in the same transaction as the output records. Either both commit or neither does.

## The Kafka Transaction Protocol Under the Hood

When you call `producer.initTransactions()`, the client registers with a **Transaction Coordinator** — a broker elected as the owner of a `transactional.id`. That coordinator tracks the transaction state in an internal topic (`__transaction_state`).

The flow for a consume-transform-produce cycle looks like this:

```
Producer                     Transaction Coordinator         Output Broker
   |                                  |                            |
   |--- initTransactions() ---------->|                            |
   |<-- pid=42, epoch=1 -------------|                            |
   |                                  |                            |
   |--- beginTransaction() (local) ---|                            |
   |                                  |                            |
   |--- send(output-topic) ---------->| (broker notes pid+epoch)  |
   |--- sendOffsetsToTransaction() -->| (offsets included in txn) |
   |                                  |                            |
   |--- commitTransaction() --------->|                            |
   |                         writes "Prepare" to __transaction_state
   |                         then writes to output partitions
   |                         then writes "Complete"               |
   |<-- ack ----------------------------------- ------------------|
```

If the producer dies between "Prepare" and "Complete", the coordinator's epoch mechanism ensures a restarted producer with the same `transactional.id` either rolls forward (completes) or aborts the previous transaction before starting new work. This is why `transactional.id` must be stable and unique per logical producer instance.

## Implementing EOS in Python

```python
from confluent_kafka import Consumer, Producer, KafkaError, TopicPartition

BOOTSTRAP = "kafka:9092"
INPUT_TOPIC = "payments.raw"
OUTPUT_TOPIC = "payments.processed"
GROUP_ID = "payment-processor"
TRANSACTIONAL_ID = "payment-processor-instance-0"  # must be unique per instance

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP_ID,
    "enable.auto.commit": False,          # manual offset control
    "isolation.level": "read_committed",  # skip aborted/pending records
    "auto.offset.reset": "earliest",
})

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "transactional.id": TRANSACTIONAL_ID,
    # enable.idempotence is automatically set to True with transactional.id
    "acks": "all",
})

producer.init_transactions()
consumer.subscribe([INPUT_TOPIC])

while True:
    messages = consumer.consume(num_messages=100, timeout=1.0)
    if not messages:
        continue

    producer.begin_transaction()
    try:
        for msg in messages:
            if msg.error():
                raise KafkaError(msg.error())

            result = transform(msg.value())

            producer.produce(
                OUTPUT_TOPIC,
                key=msg.key(),
                value=result,
            )

        # Flush produce buffer before committing offsets into the transaction.
        # Without this, the offset commit and record writes may land in different
        # internal batches, breaking the atomicity guarantee.
        producer.flush()

        # Include the consumer group offsets in the transaction.
        # This is what makes the consume-side exactly-once: the committed offset
        # only becomes visible if this transaction commits.
        offsets = [
            TopicPartition(m.topic(), m.partition(), m.offset() + 1)
            for m in messages
        ]
        producer.send_offsets_to_transaction(
            offsets,
            consumer.consumer_group_metadata(),
        )

        producer.commit_transaction()

    except Exception as e:
        producer.abort_transaction()
        # Do NOT commit the consumer offsets. On restart, we'll reprocess
        # the same batch and try again.
        raise
```

Key points in this implementation:

- **`isolation.level=read_committed`** on the consumer is mandatory. Without it, the consumer reads records from uncommitted (or aborted) transactions, undoing the EOS guarantee entirely.
- **`send_offsets_to_transaction`** binds the consumer offset commit inside the transaction. If the transaction aborts, the offsets are not advanced.
- **`flush()` before `send_offsets_to_transaction`** ensures all produced records are staged before the coordinator seals the transaction. Skipping this is a common bug.
- **`transactional.id` is per instance**, not per consumer group. If you run three instances of this service, they must each have a distinct `transactional.id` (e.g., append the partition assignment or a stable instance identifier). Reusing the same `transactional.id` across instances causes the epoch bump to fence off the other producer mid-transaction.

## The Performance Cost

EOS is not free. The transaction protocol adds two extra round-trips to the broker (BeginTransaction markers and EndTransaction markers written to each affected partition). `isolation.level=read_committed` on consumers introduces read-side overhead because the broker must filter uncommitted records.

Rough numbers from Kafka's own benchmarks: EOS throughput is **approximately 20% lower** than at-least-once under high load, with higher tail latency at p99 due to the synchronous coordinator round-trip on commit.

For most pipelines processing thousands of events per second, this is completely acceptable. For pipelines pushing millions of events per second with strict latency SLAs, you need to measure it in your environment.

## When You Don't Actually Need EOS

Before enabling transactions, ask: **can your downstream already tolerate duplicates?**

If the output topic feeds a consumer that writes to a database using upserts keyed on `event_id`, or a service that deduplicates on a stable key — you already have exactly-once *effective* semantics at a fraction of the operational cost. Idempotent consumers are often the right answer.

EOS is worth its cost when:
- The output of the pipeline is another Kafka topic consumed by systems you don't control (and cannot make idempotent)
- You're doing aggregations or stateful operations where duplicate inputs corrupt the result (e.g., a running total that double-counts)
- Regulatory requirements demand that every event be processed exactly once at the infrastructure level, not just the application level

EOS is overkill when:
- Your consumer's side effects are naturally idempotent (set operations, upserts, cache invalidation)
- You can add a deduplication key and check it at the write boundary
- You're doing fan-out enrichment where duplicates are filtered downstream anyway

## Operational Gotchas

**Zombie producers and epoch fencing.** If an instance crashes and restarts with the same `transactional.id`, the coordinator bumps the epoch and fences the old instance. Any in-flight `commit_transaction()` call on the old instance raises `ProducerFencedException`. Handle this by shutting down and restarting clean — do not attempt to recover in-place.

**Transaction timeout.** The broker-side `transaction.timeout.ms` (default 60 seconds) controls how long an open transaction can live. If your processing loop stalls — slow downstream call, GC pause, slow transform — the coordinator will expire and abort the transaction. The producer sees `InvalidProducerEpochException`. Keep transaction timeout longer than your 99th-percentile batch processing time, and keep `num_messages` per transaction small enough that batches complete fast.

**`__transaction_state` replication.** This internal topic's replication factor determines transaction durability. In production, it should be 3. Verify: `kafka-topics.sh --describe --topic __transaction_state`.

## The One Thing to Take Away

Enable Kafka EOS only after you've confirmed that idempotent consumers can't solve the problem — most duplicate-sensitivity issues can be fixed at the write boundary with a deduplication key, which is operationally simpler and has zero throughput penalty. When you do need EOS, the three non-negotiable settings are `transactional.id` (unique per instance), `isolation.level=read_committed` on all consumers of the output topic, and `send_offsets_to_transaction` inside every transaction — miss any one of these and you have the illusion of exactly-once, not the guarantee.
