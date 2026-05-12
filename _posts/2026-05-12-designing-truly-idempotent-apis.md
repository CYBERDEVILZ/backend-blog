---
layout: post
title: "Designing Truly Idempotent APIs: Patterns, Pitfalls, Real Implementation"
date: 2026-05-12
tags: [api-design, distributed-systems, databases, backend]
read_time: 9
---

Your payment service just double-charged 847 customers. The root cause: a mobile client timed out at 3 seconds, retried, and the backend processed both requests. Your endpoint was not idempotent. The network did exactly what it is supposed to do.

This happens on every payment system that has not explicitly solved it, and the patterns that fix it extend far beyond payments—order creation, account provisioning, email delivery, job scheduling. Any operation that modifies state and can be retried needs to be idempotent.

## What Idempotency Actually Means

An idempotent operation produces the same result whether it is called once or N times. GET is trivially idempotent. DELETE is idempotent by HTTP semantics. POST is not by default, and that is what you have to fix.

The goal is not to prevent retries. Retries are correct and necessary behavior from clients. The goal is to make retries safe.

## The Idempotency Key Pattern

The industry-standard approach: require clients to send a unique key per logical operation. Stripe, Braintree, and every serious payments API use this. The client generates a UUID before the request and sends it as a header:

```
POST /payments HTTP/1.1
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000
Content-Type: application/json

{"amount": 4999, "currency": "usd", "customer_id": "cus_123"}
```

On the backend, you store the key with its response. Any subsequent request with the same key returns the stored response without re-executing the operation.

## The Race Condition That Breaks Naive Implementations

Here is where most implementations fail. Two retries arrive simultaneously. Both check for an existing key. Both find nothing. Both execute the operation. You have prevented sequential duplicates but not concurrent ones.

The fix requires an atomic check-and-insert at the database level—no application lock, no Redis lock, no two-step check-then-insert. PostgreSQL handles this cleanly:

```sql
-- Idempotency keys table
CREATE TABLE idempotency_keys (
    key          TEXT PRIMARY KEY,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL,
    request_hash TEXT NOT NULL,      -- SHA-256 of the canonicalized request body
    status       TEXT NOT NULL DEFAULT 'in_flight',  -- in_flight | complete
    response     JSONB,
    http_status  INT
);

-- Fast cleanup for the background expiry job
CREATE INDEX ON idempotency_keys (expires_at)
    WHERE status = 'complete';
```

```python
import hashlib, json
from psycopg2.extras import Json, RealDictCursor

def handle_payment(conn, idempotency_key: str, request_body: dict):
    request_hash = hashlib.sha256(
        json.dumps(request_body, sort_keys=True).encode()
    ).hexdigest()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Atomic insert: only one concurrent caller wins.
        # ON CONFLICT at the row level — no external lock needed.
        cur.execute("""
            INSERT INTO idempotency_keys (key, expires_at, request_hash)
            VALUES (%s, now() + interval '24 hours', %s)
            ON CONFLICT (key) DO NOTHING
            RETURNING key
        """, (idempotency_key, request_hash))

        inserted = cur.fetchone() is not None
        conn.commit()

        if not inserted:
            cur.execute("""
                SELECT status, response, http_status, request_hash
                FROM idempotency_keys WHERE key = %s
            """, (idempotency_key,))
            row = cur.fetchone()

            # Same key, different body: client bug — reject loudly
            if row['request_hash'] != request_hash:
                return 422, {
                    "error": "idempotency_key_reuse",
                    "message": "Key reused with a different request body"
                }

            # Still executing on another thread/process — tell client to back off
            if row['status'] == 'in_flight':
                return 409, {
                    "error": "request_in_flight",
                    "message": "A request with this key is still processing"
                }

            # Return the cached deterministic response
            return row['http_status'], row['response']

    # This process owns the key. Execute the real operation.
    try:
        result = charge_customer(request_body)
        response = {"charge_id": result.id, "status": "succeeded"}
        http_status = 201

    except InsufficientFundsError as e:
        # Business logic failure: deterministic, safe to cache
        response = {"error": "insufficient_funds", "message": str(e)}
        http_status = 402

    except Exception:
        # Transient failure: delete in-flight record so client can retry
        # with the same key without getting stuck on 409 forever
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM idempotency_keys WHERE key = %s AND status = 'in_flight'",
                (idempotency_key,)
            )
            conn.commit()
        raise

    # Persist the response before returning it
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE idempotency_keys
            SET status = 'complete', response = %s, http_status = %s
            WHERE key = %s
        """, (Json(response), http_status, idempotency_key))
        conn.commit()

    return http_status, response
```

The critical line is `ON CONFLICT (key) DO NOTHING RETURNING key`. PostgreSQL's `INSERT … ON CONFLICT` is atomic at the row level—exactly one concurrent insert wins. The losers check the row status and return appropriately. No application-level locking, no `SELECT FOR UPDATE`, no Redis distributed lock needed.

## Transient vs Deterministic Failures

Do not cache responses for transient failures: network timeouts, database errors, upstream rate limits. Cache only deterministic outcomes—business logic rejections (invalid card, insufficient funds) and successes.

For transient failures, delete the in-flight record. If you do not, the client is stuck: they cannot reuse the key (it shows `in_flight`) and should not generate a new key (they do not know whether the first attempt succeeded). Deleting the record and letting the client retry with the same idempotency key is the correct behavior.

## The Request Hash Check

This step is frequently skipped and it causes real incidents. A client reuses an idempotency key across two different requests—different amount, different account ID—possibly due to a UUID generation bug or state reset. If you do not check the request hash, you silently return the first response for the second request. Wrong amount charged, wrong account provisioned.

The hash is a SHA-256 of the canonicalized (sorted-key) JSON body. It is cheap to compute and catches client-side key reuse bugs before they mutate production state incorrectly.

## Key Expiry and Cleanup

Idempotency keys do not need to live forever. 24 hours covers typical retry windows for most APIs; payments workflows may warrant 7 days. A background job cleans up expired complete records:

```sql
-- Run this as a scheduled job every few minutes
DELETE FROM idempotency_keys
WHERE expires_at < now()
  AND status = 'complete';
```

The partial index on `(expires_at) WHERE status = 'complete'` keeps this O(deleted rows), not O(table size). Handle abandoned in-flight records separately with a longer window—anything `in_flight` for more than an hour is almost certainly dead:

```sql
DELETE FROM idempotency_keys
WHERE status = 'in_flight'
  AND created_at < now() - interval '1 hour';
```

## Where This Pattern Applies

Any POST endpoint that:

- Creates a resource that must not be created twice
- Triggers an external side effect (email, SMS, webhook)
- Modifies money, inventory, or any non-reversible state

The pattern is identical whether you are scheduling a job, provisioning a cloud resource, or initiating a bank transfer. The database schema and the check-and-insert wrapper are reusable across endpoints.

## What Idempotency Does Not Solve

Idempotency guarantees that same-key requests return the same response. It does not make your underlying operation atomic across multiple tables. If your payment processing writes a charge record, then fails before updating the order record, you have a partial failure problem—that is distributed transaction territory, not idempotency. Outbox pattern and saga orchestration address that separately.

Idempotency handles retries. Sagas handle partial failures. These are distinct problems with distinct solutions.

## The Change to Make Today

Add the `idempotency_keys` table to your database before you need it. The table is simple, the wrapper is under 50 lines, and retrofitting stateless POST endpoints takes an afternoon. Doing it under incident pressure after a double-charge event takes much longer and produces worse results.

Wire in idempotency key validation on your three highest-risk write endpoints this week. The next time a client retries under a network partition, your backend returns the same response it gave the first time—no duplicate charges, no ghost provisioning, no double-sends.
