---
layout: post
title: "Event-Driven Systems That Never Lose Events: At-Least-Once with Idempotent Consumers"
date: 2026-05-27
tags: [kafka, event-driven, idempotency, reliability]
read_time: 9
---

Your payment service just charged a customer twice. The Kafka consumer committed its offset after processing successfully, but before the downstream payment API confirmed. A network timeout caused the consumer to retry, and the payment went through again. Your at-least-once delivery guarantee worked exactly as designed — and cost you a chargeback.

This is the core tension in event-driven systems: you can guarantee delivery, or you can guarantee exactly-once processing, but not both cheaply. At-least-once delivery is the default in Kafka, RabbitMQ, and most message systems because it's achievable without distributed consensus. Exactly-once semantics exist in Kafka (idempotent producers + transactional consumers) but they come with significant constraints: same broker cluster, specific Kafka versions, and they don't help if your processing touches external APIs.

The practical solution for most production systems: **accept at-least-once delivery, build idempotent consumers**.

## What "Idempotent Consumer" Actually Means

An idempotent consumer produces the same final state regardless of how many times it processes the same event. This is different from ignoring duplicates — idempotency means the second invocation is a no-op, not an error or a skip.

The distinction matters. A consumer that silently drops events it thinks are duplicates but actually aren't (because your deduplication key is wrong) is harder to debug than one that processes duplicates. You want correctness proofs, not silent failures.

## The Deduplication Table Pattern

The most reliable approach is a deduplication table in your database. Every event carries a unique ID. Before processing, you attempt to insert that ID. If the insert succeeds, you process. If it fails with a duplicate key error, you skip. The entire operation — dedup insert + business logic — must be atomic inside one transaction.

```python
import psycopg2
from kafka import KafkaConsumer
import json
import logging

def process_payment_event(conn, event_id: str, event: dict) -> bool:
    """
    Returns True if the event was processed, False if it was a duplicate.
    Uses a single transaction to atomically record the event ID and
    execute business logic. If any step fails, both roll back.
    """
    with conn.cursor() as cur:
        try:
            # Attempt to claim this event_id. ON CONFLICT DO NOTHING
            # returns rowcount=0 on a duplicate — no exception raised.
            cur.execute("""
                INSERT INTO processed_events (event_id, processed_at)
                VALUES (%s, NOW())
                ON CONFLICT (event_id) DO NOTHING
            """, (event_id,))

            if cur.rowcount == 0:
                # Another worker already handled this event.
                conn.rollback()
                return False

            # Business logic runs inside the same transaction.
            # If this raises, the dedup insert also rolls back —
            # the event remains unprocessed and safe to retry.
            cur.execute("""
                INSERT INTO payments (user_id, amount_cents, idempotency_key, status)
                VALUES (%s, %s, %s, 'pending')
                ON CONFLICT (idempotency_key) DO NOTHING
            """, (event['user_id'], event['amount_cents'], event_id))

            conn.commit()
            return True

        except Exception:
            conn.rollback()
            raise  # Caller decides retry logic


def run_consumer():
    consumer = KafkaConsumer(
        'payment-events',
        bootstrap_servers=['kafka:9092'],
        group_id='payment-processor',
        enable_auto_commit=False,   # CRITICAL: never let Kafka commit for you
        auto_offset_reset='earliest',
        value_deserializer=lambda b: json.loads(b.decode('utf-8'))
    )

    conn = psycopg2.connect(dsn="postgresql://payments-db/payments")

    for message in consumer:
        event = message.value
        event_id = event.get('event_id')

        if not event_id:
            # Malformed event — log and skip so the consumer doesn't stall
            logging.warning(f"Event missing ID at offset {message.offset}")
            consumer.commit()
            continue

        try:
            processed = process_payment_event(conn, event_id, event)
            status = "processed" if processed else "duplicate (skipped)"
            logging.info(f"Event {event_id}: {status}")

            # Commit offset only after the DB transaction is durable.
            # A crash here causes redelivery; the dedup table handles it.
            consumer.commit()

        except Exception as e:
            logging.error(f"Error on event {event_id}: {e}")
            # Do not commit — let Kafka redeliver after consumer restart.
```

The schema for the deduplication table:

```sql
CREATE TABLE processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Supports fast cleanup queries without scanning the whole table
CREATE INDEX idx_processed_events_age
    ON processed_events (processed_at);
```

## The Three Failure Modes You Must Handle

**Consumer crashes between DB commit and offset commit**

This is the scenario your system must survive. The database committed, but Kafka didn't receive the offset commit. On restart, Kafka redelivers the message. Your dedup table returns `rowcount == 0`, the event is skipped, and the offset is committed. No duplicate processing. This is the entire point of the pattern.

**External side effects before the transaction commits**

If your consumer calls an external API (send email, charge card) *before* committing the database transaction, you lose atomicity. The API call may succeed, but a subsequent database failure means you'll retry and call it again — with no dedup protection, since the dedup record was never committed.

Fix this with the outbox pattern: write the intent to an outbox table inside the same transaction that records the event ID. A separate process reads the outbox and makes the external call. The outbox row is your durable state; the external call is the retry-safe delivery step.

```sql
-- Both writes happen in a single transaction with the dedup insert
INSERT INTO outbox_events (idempotency_key, event_type, payload, created_at)
VALUES ($1, 'send_receipt_email', $2, NOW())
ON CONFLICT (idempotency_key) DO NOTHING;
```

A background worker polls the outbox and marks rows delivered after the external call confirms. This decouples "we decided to send an email" from "the email was sent" — the first is transactional, the second is eventual.

**Deduplication table growing unbounded**

Events older than your retention window can never be redelivered. Records beyond that age serve no purpose. Clean them up on a schedule:

```sql
-- Run as a cron job, NOT inline with consumer processing
DELETE FROM processed_events
WHERE processed_at < NOW() - INTERVAL '7 days';
```

On large tables, batch this to avoid lock contention:

```sql
WITH old_events AS (
    SELECT event_id FROM processed_events
    WHERE processed_at < NOW() - INTERVAL '7 days'
    LIMIT 5000
)
DELETE FROM processed_events
WHERE event_id IN (SELECT event_id FROM old_events);
```

Run this in a loop until it deletes zero rows, ideally during off-peak hours.

## Choosing the Right Event ID

Your deduplication key is only as good as your event ID schema. Most duplicate-processing bugs trace back here.

**What makes a good event ID:**
- Globally unique across all time — UUID v4 or ULID
- Assigned by the producer, not derived by the consumer on receipt
- Stable across redeliveries — the same logical event must have the same ID on retry number one and retry number ten

**What breaks deduplication:**
- Using Kafka partition offsets as event IDs — offsets repeat if topics are recreated
- Generating a new UUID on consumer receipt — every delivery looks novel
- Composite keys based on payload fields that can legitimately repeat (`user_id + amount` is not unique across time)

If you don't control the producer and it sends no stable event IDs, derive a deterministic ID from the fields that define the event's identity — a SHA-256 of the canonical fields. Document this derivation explicitly and test it against real payloads. It's fragile, but it's better than no deduplication.

## When Kafka's Transactional API Is the Right Tool

Kafka's exactly-once semantics (EOS) are appropriate for one specific scenario: your consumer reads from one Kafka topic and writes results to another Kafka topic, with no external state. In that case, Kafka's transactional producer can atomically commit both the output record and the consumer offset, giving true exactly-once within the broker cluster.

EOS does not help when your consumer writes to a relational database, calls REST APIs, or interacts with any state outside Kafka. For those cases — which is most production systems — the deduplication table pattern above is simpler, more debuggable, and works across any message broker.

## Actionable Takeaway

Add a `processed_events` table to your database and wrap every consumer's business logic in a single transaction that atomically inserts the event ID and applies the state change. Disable auto-commit in your Kafka consumer and commit offsets only after the database transaction succeeds. This gives you at-least-once delivery with idempotent processing: events are never lost and never applied twice, regardless of consumer crashes, network partitions, or broker restarts. For consumers that call external APIs, pair this with an outbox table to restore atomicity across the boundary.
