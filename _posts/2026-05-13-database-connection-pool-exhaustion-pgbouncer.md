---
layout: post
title: "Database Connection Pool Exhaustion: Root Causes, Diagnosis, and Tuning PgBouncer"
date: 2026-05-13
tags: [postgresql, pgbouncer, performance, production]
read_time: 11
---

Your service is returning 500s. Latency has spiked to 30 seconds. The database CPU is idle at 4%. Engineers are paging each other. Someone restarts the app and it recovers — until it doesn't. The culprit is almost always connection pool exhaustion, and it's one of the most misdiagnosed failures in backend production systems.

This post walks through how to confirm the diagnosis, understand exactly why it happened, and tune PgBouncer to prevent it from recurring.

## The Failure Pattern

PostgreSQL connections are expensive. Each connection spawns a backend process on the server, consuming roughly 5–10 MB of memory and adding overhead to every lock check, autovacuum decision, and WAL operation. PostgreSQL itself caps connections with `max_connections` (default: 100).

When your application layer holds too many open connections — whether due to a traffic spike, a slow query blocking connection release, a misconfigured pool, or a connection leak — the pool fills up. New requests block waiting for a connection. Those requests time out. Users see errors.

The insidious part: the database is fine. Your slow query dashboards show nothing because queries aren't even reaching the database.

## Step 1: Confirm Connection Exhaustion

Connect to PostgreSQL as a superuser and run:

```sql
-- How many connections exist, grouped by state and client
SELECT
    client_addr,
    state,
    wait_event_type,
    wait_event,
    COUNT(*) AS count,
    MAX(EXTRACT(EPOCH FROM (now() - state_change))) AS max_age_seconds
FROM pg_stat_activity
WHERE datname = 'your_database'
GROUP BY client_addr, state, wait_event_type, wait_event
ORDER BY count DESC;
```

If you see dozens of rows in `idle` state with `max_age_seconds` in the hundreds, your pool is oversized and connections are not being returned. If you see rows in `idle in transaction` state, you have a worse problem: transactions that were started and never committed or rolled back.

Check the hard ceiling:

```sql
SELECT
    current_setting('max_connections') AS max_connections,
    COUNT(*) AS current_connections,
    COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_txn,
    COUNT(*) FILTER (WHERE state = 'active') AS active
FROM pg_stat_activity;
```

If `current_connections` is near `max_connections`, you're at the wall. New connection attempts will return:

```
FATAL: sorry, too many clients already
```

## Step 2: Find the Root Cause

Connection exhaustion has four common root causes. Identify yours before tuning anything.

**Root cause 1: Pool misconfiguration.** Your app is configured with a max pool size that, multiplied by the number of app instances, exceeds `max_connections`. This is the most common cause after scaling from one app server to many. With 10 app instances each configured for `max_pool_size=20`, you need 200 PostgreSQL connections. If `max_connections=100`, you're over the limit at full utilization.

**Root cause 2: Idle in transaction.** A query acquired a connection, started a transaction, and the connection was never released — due to an exception that wasn't caught, or an ORM that opened a transaction implicitly and the handler crashed before committing. These pile up silently.

Find long-running idle transactions:

```sql
SELECT pid, usename, client_addr, state, query, state_change
FROM pg_stat_activity
WHERE state = 'idle in transaction'
  AND state_change < now() - interval '30 seconds'
ORDER BY state_change;
```

**Root cause 3: Lock contention.** A query is waiting to acquire a lock held by another query. The waiting connection stays open, blocking a pool slot.

```sql
SELECT
    blocked.pid AS blocked_pid,
    blocked.query AS blocked_query,
    blocking.pid AS blocking_pid,
    blocking.query AS blocking_query,
    blocking.state AS blocking_state
FROM pg_stat_activity AS blocked
JOIN pg_stat_activity AS blocking
    ON blocking.pid = ANY(pg_blocking_pids(blocked.pid))
WHERE cardinality(pg_blocking_pids(blocked.pid)) > 0;
```

**Root cause 4: Connection leaks.** Your application code opens connections and fails to close them — a missing `conn.close()` in an error path, or a library that doesn't release connections back to the pool after use. These accumulate over hours and cause a slow-burn exhaustion, not an instantaneous one.

## Step 3: Install and Configure PgBouncer

PgBouncer is a lightweight connection pooler that sits between your app and PostgreSQL. It multiplexes many application connections onto far fewer server-side PostgreSQL connections.

Install on Ubuntu/Debian:

```bash
apt-get install pgbouncer
```

The key config file is `/etc/pgbouncer/pgbouncer.ini`. Here is a production-ready configuration:

```ini
[databases]
your_database = host=127.0.0.1 port=5432 dbname=your_database

[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6432
auth_type = md5
auth_file = /etc/pgbouncer/userlist.txt

# Use transaction pooling unless you rely on session-level features
pool_mode = transaction

# Max connections PgBouncer will open to PostgreSQL
server_pool_size = 20

# Max clients that can wait for a connection before being rejected
max_client_conn = 500

# How long a client will wait for a server connection before timeout
query_wait_timeout = 5

# How long an idle server connection is kept open (seconds)
server_idle_timeout = 600

# Drop server connections that are older than this (seconds)
server_lifetime = 3600

# Log disconnections — useful for debugging leaks
log_disconnections = 1

# Minimum number of server connections to keep ready
min_pool_size = 5
```

Create `/etc/pgbouncer/userlist.txt` with your credentials:

```
"app_user" "md5<hash>"
```

Generate the md5 hash:

```bash
echo -n "passwordapp_user" | md5sum
# prefix with 'md5' when writing to userlist.txt
```

## Understanding Pool Modes

PgBouncer has three pool modes. Picking the wrong one is the most common configuration mistake:

- **Session pooling**: A server connection is held for the duration of the client session. Equivalent to a simple proxy — provides no real multiplexing benefit.
- **Transaction pooling**: A server connection is held only for the duration of a transaction. Released immediately after `COMMIT` or `ROLLBACK`. This is the mode you almost always want.
- **Statement pooling**: A server connection is held only for a single statement. Incompatible with multi-statement transactions.

**Transaction pooling incompatibilities to know about:** Prepared statements, `SET` session variables, advisory locks, and `LISTEN/NOTIFY` don't work correctly in transaction mode because the connection returned to the pool may be given to a different client. If your ORM uses prepared statements, either disable them or use session pooling.

For SQLAlchemy, disable server-side prepared statements:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg2://user:password@pgbouncer_host:6432/your_database",
    # Required for PgBouncer transaction mode
    connect_args={"options": "-c statement_timeout=30000"},
    execution_options={"no_parameters": True},
)
```

For psycopg2 directly:

```python
import psycopg2

conn = psycopg2.connect(
    "host=pgbouncer_host port=6432 dbname=your_database user=app_user password=secret",
    # Disable named prepared statements
    options="-c plan_cache_mode=force_generic_plan"
)
conn.autocommit = False
```

## Tuning the Pool Size

`server_pool_size` is the number of server-side PostgreSQL connections PgBouncer will open per database/user pair. How do you size it?

The right number is determined by the number of CPU cores on your database server, not the number of app instances. Connections beyond the number of cores don't execute in parallel — they contend for CPU. A practical starting formula:

```
server_pool_size = (num_db_cores * 2) + effective_spindle_count
```

For a 4-core RDS instance with SSD storage (`effective_spindle_count = 1`):

```
server_pool_size = (4 * 2) + 1 = 9  → round to 10
```

This will feel too small, but it's correct. The goal is to queue work at the PgBouncer layer — where the wait is microseconds — rather than at the PostgreSQL layer where waiting means a process sleeping and holding memory.

Monitor PgBouncer's internal stats to validate:

```bash
# Connect to PgBouncer's admin interface
psql -h 127.0.0.1 -p 6432 -U pgbouncer pgbouncer

-- Show pool utilization
SHOW POOLS;

-- Show current client and server connection counts
SHOW STATS;

-- Show per-client info
SHOW CLIENTS;
```

Key columns in `SHOW POOLS`:
- `cl_active`: clients with an assigned server connection
- `cl_waiting`: clients waiting for a connection — if this is nonzero at steady state, increase `server_pool_size`
- `sv_idle`: server connections sitting idle — if this is consistently high, decrease `server_pool_size`

## Killing Stuck Connections

When you're in an active incident and connections are stuck, you can terminate them without restarting PostgreSQL:

```sql
-- Kill idle connections older than 5 minutes
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state = 'idle'
  AND state_change < now() - interval '5 minutes'
  AND pid <> pg_backend_pid();

-- Kill all idle-in-transaction connections
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state = 'idle in transaction'
  AND pid <> pg_backend_pid();
```

Use `pg_cancel_backend(pid)` first if you want a softer touch — it sends `SIGINT`, which cancels the current query but keeps the connection open. `pg_terminate_backend(pid)` sends `SIGTERM` and closes the connection.

## The Concrete Takeaway

Set `max_connections` in PostgreSQL to `server_pool_size * num_pgbouncer_instances + 5` (the 5 is for superuser connections and monitoring), run PgBouncer in transaction mode, and size `server_pool_size` to `(db_cores * 2) + 1`. Your application's pool size ceiling becomes irrelevant — PgBouncer enforces the server-side limit. When `cl_waiting` in `SHOW POOLS` is nonzero under normal load, raise `server_pool_size` by 5 and re-measure. When `sv_idle` is consistently above 30% of your pool, lower it. Start there, measure for a week, and you'll have empirical numbers for your workload instead of guesses.
