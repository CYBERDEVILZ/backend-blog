---
layout: post
title: "Handling Partial Failures in Distributed Transactions Without Two-Phase Commit"
date: 2026-05-17
tags: [distributed-systems, databases, reliability, patterns]
read_time: 9
---

Your order service charged the customer. Then the inventory service timed out. Now you have a $200 charge with no order to fulfill, an angry customer, and an on-call alert at 2am.

This is the distributed transaction problem at its most unpleasant. Two-phase commit (2PC) is the academic answer: coordinate a prepare phase across all participants, then commit only if all agree. In practice, 2PC is a reliability trap. It requires a transaction coordinator that becomes a single point of failure, holds locks across services during network latency, and blocks indefinitely if any participant goes down during the prepare phase. At the scale where you need distributed transactions, 2PC will become your outage, not your protection.

The working solution is the Saga pattern combined with the transactional outbox. This post shows exactly how to implement it for the failure mode above.

## Why 2PC Fails in Production

The fundamental problem is that 2PC requires all participants to be available and responsive simultaneously. In a distributed system under real load:

- Your inventory service has a 99.5% monthly uptime SLA—that's 3.6 hours of downtime.
- Your payment service has a separate 99.5% SLA.
- Combined availability: ~99.0%.
- 2PC holds locks in both systems while awaiting the coordinator's commit decision.

During those locked windows, every query touching those records blocks. Under high load, this cascades into connection pool exhaustion within seconds. A 200ms inventory lock during a 2PC stall can take down an entire checkout system in under a minute.

## The Saga Pattern

A saga is a sequence of local transactions, each with a compensating transaction that can undo it. Instead of one atomic commit across services, you chain single-service transactions. If step N fails, you run compensating transactions for steps N-1 through 1 in reverse.

For order placement:
1. Reserve inventory → compensate: release reservation
2. Charge payment → compensate: refund
3. Create order record (last step, no compensation needed)
4. Confirm inventory reservation and ship

The key insight: compensating transactions don't need to achieve perfect rollback. They need to return the system to a consistent, observable state. A refund is not the same as "charge never happened"—there's a brief window where both occurred—but for business purposes, it's sufficient.

## Orchestrated Sagas with a Compensation Stack

There are two saga implementations: choreography (services emit events and react to each other) and orchestration (a central coordinator drives the steps). For complex multi-step flows, orchestration is easier to debug and reason about.

Here's a production-grade orchestrated saga in Python, using PostgreSQL to persist saga state:

```python
import uuid
import json
import psycopg2
from dataclasses import dataclass, field
from typing import Callable, List
from enum import Enum

class SagaStatus(Enum):
    RUNNING       = "running"
    COMPLETED     = "completed"
    COMPENSATING  = "compensating"
    FAILED        = "failed"

@dataclass
class SagaStep:
    name:         str
    action:       Callable  # forward step
    compensation: Callable  # undo step

@dataclass
class SagaContext:
    saga_id:         str
    data:            dict
    completed_steps: List[str] = field(default_factory=list)

class SagaOrchestrator:
    def __init__(self, conn, steps: List[SagaStep]):
        self.conn  = conn
        self.steps = steps

    def run(self, data: dict) -> tuple[bool, dict]:
        saga_id = str(uuid.uuid4())
        ctx     = SagaContext(saga_id=saga_id, data=data)

        # Persist saga state before starting—crash recovery depends on this.
        self._save_saga(ctx, SagaStatus.RUNNING)

        for step in self.steps:
            try:
                # Each action mutates ctx.data with results;
                # e.g., the payment step adds {"charge_id": "ch_xxx"}.
                step.action(ctx)
                ctx.completed_steps.append(step.name)
                self._save_saga(ctx, SagaStatus.RUNNING)
            except Exception as e:
                self._compensate(ctx, e)
                return False, ctx.data

        self._save_saga(ctx, SagaStatus.COMPLETED)
        return True, ctx.data

    def _compensate(self, ctx: SagaContext, original_error: Exception):
        self._save_saga(ctx, SagaStatus.COMPENSATING)

        # Reverse only the steps that actually completed.
        completed = list(reversed([
            s for s in self.steps if s.name in ctx.completed_steps
        ]))

        for step in completed:
            try:
                step.compensation(ctx)
            except Exception as comp_error:
                # Do NOT raise here—an exception would abort remaining
                # compensations and leave the system in a worse partial state.
                # Route to a dead-letter table for manual remediation instead.
                self._record_compensation_failure(ctx, step.name, comp_error)

        self._save_saga(ctx, SagaStatus.FAILED)

    def _save_saga(self, ctx: SagaContext, status: SagaStatus):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sagas
                    (saga_id, status, data, completed_steps, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (saga_id) DO UPDATE SET
                    status          = EXCLUDED.status,
                    data            = EXCLUDED.data,
                    completed_steps = EXCLUDED.completed_steps,
                    updated_at      = EXCLUDED.updated_at
            """, (
                ctx.saga_id,
                status.value,
                json.dumps(ctx.data),
                json.dumps(ctx.completed_steps),
            ))
            self.conn.commit()

    def _record_compensation_failure(self, ctx, step_name, error):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO saga_compensation_failures
                    (saga_id, step_name, error, created_at)
                VALUES (%s, %s, %s, NOW())
            """, (ctx.saga_id, step_name, str(error)))
            self.conn.commit()
```

Three production details worth internalizing:

**Compensation failure handling**: When a compensation fails, you cannot raise—it aborts the remaining compensations and leaves the system in a worse partial state. Write to a `saga_compensation_failures` table and page ops. Every production saga needs a human-review queue for failed compensations; there is no way around this.

**Saga state persistence**: Writing `completed_steps` before advancing means a crashed coordinator can resume or recompensate from a known state. Without this, coordinator restarts lose track of what happened.

**Idempotent actions**: Both forward and compensating actions must be idempotent. A crashed coordinator may replay a step. Assign IDs before calling downstream services; use `INSERT ... ON CONFLICT DO NOTHING` for deduplication on the receiving end.

## The Transactional Outbox for Reliable Event Emission

The saga pattern solves coordination logic, but there is a subtler problem: how does step 1 reliably trigger step 2? If you update the database and then publish a message to a queue, a crash between those two operations leaves the database updated with no event published.

The transactional outbox pattern fixes this. Instead of publishing directly, write to an `outbox` table in the same database transaction as your business data change. A separate relay process reads the outbox and publishes to the queue.

```sql
-- Reserve inventory and emit the event atomically—no gap for a crash.
BEGIN;

UPDATE inventory
   SET reserved  = reserved + $2,
       available = available - $2
 WHERE product_id = $1
   AND available >= $2;

INSERT INTO outbox (event_id, event_type, payload, created_at)
VALUES (
    gen_random_uuid(),
    'inventory.reserved',
    jsonb_build_object(
        'order_id',   $3,
        'product_id', $1,
        'quantity',   $2
    ),
    NOW()
);

COMMIT;
```

A relay process—or a CDC tool like Debezium reading the WAL—polls the outbox and publishes rows to Kafka or SQS, then marks them sent. If the relay crashes mid-publish, it republishes on restart. Consumers must be idempotent; duplicate delivery is guaranteed, not prevented.

The relay query looks like this:

```sql
-- Claim a batch of unsent events for publishing.
UPDATE outbox
   SET status     = 'processing',
       locked_at  = NOW(),
       locked_by  = $1          -- relay instance ID
 WHERE id IN (
     SELECT id FROM outbox
      WHERE status = 'pending'
        AND (locked_at IS NULL OR locked_at < NOW() - INTERVAL '30 seconds')
      ORDER BY created_at
      LIMIT 100
        FOR UPDATE SKIP LOCKED
 )
 RETURNING *;
```

`SKIP LOCKED` ensures multiple relay instances claim non-overlapping batches without contention.

## When Sagas Don't Fit

Sagas work well when:
- Individual service transactions are fast (under one second)
- Business logic tolerates a brief inconsistency window between steps
- Compensations are expressible in business terms: refund, release reservation, cancel shipment

They break down when:
- Compensation is physically impossible (the truck already left the warehouse)
- Strict read isolation is required across services during the saga window
- You have more than five or six steps and the compensation graph becomes unmaintainable

In those cases, the better fix is usually redesigning the service boundary so the entire operation lives in one service with a single ACID transaction. Splitting a naturally atomic operation across service boundaries to satisfy an org chart is a common source of distributed transaction pain.

## The One Thing to Do Today

Find your most complex multi-service write flow—the one with the most "what happens if this step fails?" comments. Add two tables to that service's database: `sagas` (saga_id, status, data, completed_steps, updated_at) and `saga_compensation_failures` (saga_id, step_name, error, created_at). Instrument the flow to write saga state transitions to those tables. You don't need to rewrite the flow yet. But the next time a partial failure occurs in production, you will have a complete record of exactly where the saga stopped and what needs manual remediation—instead of reconstructing the crime scene from scattered service logs.
