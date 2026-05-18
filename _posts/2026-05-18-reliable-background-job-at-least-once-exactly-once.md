---
layout: post
title: "Reliable Background Job Processing: At-Least-Once vs Exactly-Once Delivery"
date: 2026-05-18
tags: [background-jobs, distributed-systems, reliability, postgres]
read_time: 10
---

Your billing service sends duplicate invoices. Your email service fires the same welcome message three times. Your order processing service silently drops payment captures under load. These failures share a root cause: picking the wrong delivery semantic for background jobs, or picking the right one and implementing it incorrectly.

Background job systems fail at two points—before the job completes (worker crashes, process is killed, OOM) or after it completes but before the queue registers the acknowledgment. Both lead to re-delivery. The question is never whether your jobs will be retried. They will. The question is whether your system handles that gracefully.

## At-Least-Once Is the Default

Most job queues—SQS, Redis Streams, Sidekiq, Celery with AMQP—default to at-least-once delivery. The mechanism:

1. Worker fetches the job (visibility timeout or lease begins)
2. Worker processes the job
3. Worker acknowledges completion; queue removes the job

If the worker crashes at step 2, the queue eventually requeues the job once the visibility timeout expires. The job runs again. At-least-once guarantees jobs eventually complete, but some will run more than once.

The standard fix is making job handlers **idempotent**: processing the same job N times produces the same observable result as processing it once. This sounds straightforward until you try to implement it.

Here's a payment capture handler that looks idempotent but isn't:

```python
def capture_payment(order_id: str, amount_cents: int):
    order = Order.get(order_id)
    if order.status == "captured":
        return  # early exit—looks safe

    result = stripe.charge.create(amount=amount_cents, source=order.payment_method)
    Order.update(order_id, status="captured", charge_id=result.id)
```

The race: two workers fetch the same job simultaneously under queue duplication. Both check `order.status`, both see `"pending"`, both call Stripe, both write `"captured"`. The customer is charged twice.

The real fix requires an atomic state transition at the database level:

```python
def capture_payment(order_id: str, amount_cents: int):
    # Atomic conditional update: only one worker wins this transition
    rows_updated = db.execute("""
        UPDATE orders
        SET status = 'capturing', updated_at = NOW()
        WHERE id = %s AND status = 'pending'
    """, [order_id])

    if rows_updated == 0:
        logger.info("Order %s already processing or captured, skipping", order_id)
        return

    try:
        result = stripe.charge.create(
            amount=amount_cents,
            source=Order.get(order_id).payment_method,
            idempotency_key=f"order-capture-{order_id}",  # Stripe deduplicates on this
        )
        db.execute("""
            UPDATE orders SET status = 'captured', charge_id = %s WHERE id = %s
        """, [result.id, order_id])
    except Exception:
        # Release the lock so another worker can retry
        db.execute("""
            UPDATE orders SET status = 'pending'
            WHERE id = %s AND status = 'capturing'
        """, [order_id])
        raise
```

Two mechanisms working together: the `WHERE status = 'pending'` guard makes the state transition atomic at the DB level, and `idempotency_key` tells Stripe to deduplicate at their end. If the worker crashes after charging but before writing, the retry hits Stripe's idempotency cache and returns the original charge object.

## When Idempotency Isn't Enough: Transactional Outbox

Some operations cannot be made idempotent at all—third-party webhooks with no idempotency support, analytics pipelines that must append exactly once, downstream services you don't control. For these, the transactional outbox pattern is the standard answer.

The core idea: write the external side effect as a row in an outbox table within the same database transaction as your business state change. Separate delivery infrastructure handles fan-out with its own retry logic.

```python
def process_order(order_id: str):
    with db.transaction():
        order = Order.get_for_update(order_id)
        Order.update(order_id, status="processed")

        # Both writes commit or both roll back—atomically
        db.execute("""
            INSERT INTO outbox_events (id, event_type, payload, created_at, delivered_at)
            VALUES (%s, 'order.processed', %s, NOW(), NULL)
            ON CONFLICT (id) DO NOTHING
        """, [f"order-processed-{order_id}", json.dumps({"order_id": order_id})])
```

A separate polling process delivers outbox events:

```sql
-- Concurrent-safe poll: multiple workers without row contention
SELECT id, event_type, payload
FROM outbox_events
WHERE delivered_at IS NULL
  AND created_at < NOW() - INTERVAL '2 seconds'
ORDER BY created_at
LIMIT 50
FOR UPDATE SKIP LOCKED;
```

`FOR UPDATE SKIP LOCKED` is essential. Without it, multiple pollers block each other and create a thundering herd on the outbox table. With it, each poller grabs its own disjoint set of rows instantly.

After delivery succeeds, mark the row:

```sql
UPDATE outbox_events SET delivered_at = NOW() WHERE id = ANY(%s);
```

## Exactly-Once Is Narrower Than You Think

"Exactly-once delivery" as a guarantee doesn't exist end-to-end in distributed systems. What vendors mean by "exactly-once" is exactly-once *processing*—state changes commit once, not that the job never executes twice.

Kafka's exactly-once semantics (EOS) are real but strictly scoped to Kafka-to-Kafka transformations. A producer-consumer loop can atomically commit offsets and write output within a Kafka transaction. The moment you write to PostgreSQL or call an HTTP API, you're back to at-least-once.

The most common EOS misconfiguration:

```python
consumer_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'order-processor',
    'isolation.level': 'read_committed',  # critical—reads only committed transaction output
    'enable.auto.commit': False,           # manual offset commit inside the transaction
}

producer_config = {
    'bootstrap.servers': 'kafka:9092',
    'transactional.id': 'order-processor-1',  # unique per producer instance
    'enable.idempotence': True,
}

producer = Producer(producer_config)
producer.init_transactions()

def process_batch(consumer, messages):
    producer.begin_transaction()
    try:
        for msg in messages:
            output = transform(msg.value())
            producer.produce('output-topic', value=output)

        # Commit offsets atomically inside the transaction
        offsets = {
            TopicPartition(m.topic(), m.partition(), m.offset() + 1)
            for m in messages
        }
        producer.send_offsets_to_transaction(offsets, consumer_config['group.id'])
        producer.commit_transaction()
    except Exception:
        producer.abort_transaction()
        raise
```

If the consumer doesn't set `isolation.level: read_committed`, it reads uncommitted messages from aborted transactions and processes them. Teams discover this by seeing "impossible" duplicate processing in their metrics after enabling EOS. The producer configuration is correct; the consumer configuration is not.

## Choosing the Right Semantic

| Scenario | Recommendation |
|---|---|
| Sending email | At-least-once + idempotency key at provider |
| Charging payments | At-least-once + DB state guard + charge idempotency key |
| Updating internal state | At-least-once + conditional UPDATE with WHERE clause |
| Calling external webhook | Transactional outbox + delivery tracking |
| Kafka stream transformation | Kafka EOS (exactly-once within Kafka only) |
| Writing to external DB from Kafka | At-least-once + upsert |

## The At-Most-Once Trap

Teams focus on at-least-once vs exactly-once and accidentally implement **at-most-once**—jobs that can be silently lost. At-most-once happens when the system removes a job from the queue before confirming the work is done.

Common manifestations:

- **Redis LPOP before processing**: `LPOP` removes the job immediately. Worker crash means permanent loss.
- **RabbitMQ auto-ack mode**: acknowledges the moment the message is delivered to the worker, not when processing completes.
- **SQS delete before return**: deleting the message inside the handler before it finishes processing.

The fix in all cases is identical: acknowledge or delete **only after successful processing returns**.

```python
def worker_loop(queue_url: str):
    while True:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            VisibilityTimeout=300,  # 5 minutes to complete; tune to p99 job duration
        )
        for msg in resp.get('Messages', []):
            try:
                process(json.loads(msg['Body']))
                # Delete only after process() returns successfully
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=msg['ReceiptHandle'],
                )
            except Exception:
                # Don't delete—let it reappear after VisibilityTimeout and retry
                logger.exception("Failed to process job %s, will retry", msg['MessageId'])
```

Set `VisibilityTimeout` to at least your p99 job processing duration plus 20%. If your jobs routinely take longer than the timeout, the queue redelivers them while a worker is still processing—creating duplicates even in a correctly-implemented system. This is one of the most common sources of unexpected at-least-once behavior in SQS-backed services.

## The Concrete Takeaway

Before optimizing for exactly-once, audit your job processing code for at-most-once bugs. Production job loss nearly always comes from accidental ack-before-process patterns, not from missing exactly-once semantics. Fix those first, then make your handlers idempotent with atomic state transitions, and you will handle 95% of real-world background job reliability failures without complex infrastructure changes.
