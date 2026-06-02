---
layout: post
title: "Secrets Management: Rotation, Injection, and Avoiding Leaks in Backend Services"
date: 2026-06-02
tags: [security, devops, postgresql, kubernetes]
read_time: 11
---

Your on-call phone rings at 2 AM. A security scanner found your database password in a GitHub commit from six months ago. The password has been valid this whole time. You have no idea whether anyone used it. Now you need to rotate credentials across twelve services without dropping production traffic, and you have to do it in the next four hours because your security team is filing an incident report.

This is not a hypothetical. It happens constantly — and the failure is almost never "someone was careless." It's structural: secrets get hardcoded because the injection mechanism is too painful to use, rotation gets skipped because there's no safe way to do it without downtime, and leaks go undetected for months because nobody instruments for them.

Here's how to build a secrets infrastructure that makes the right path the easy path.

## Why Env Vars Alone Are Not Enough

Environment variables are the de-facto standard for secret injection, but they leak in more ways than most engineers realize.

**Process listings**: On Linux, `/proc/<pid>/environ` is readable by any process running as the same user. In a containerized environment with a shared PID namespace, this exposes all env vars of all processes in the pod.

**Crash dumps and core files**: When a process crashes and dumps core, the entire address space — including env var strings — ends up on disk. Even without core dumps, unhandled exception handlers in frameworks like Express, Django, or Spring Boot will log the full environment when `DEBUG=true`.

**Health and debug endpoints**: The single most common secret leak vector I see in production is a `/debug/vars`, `/actuator/env`, or `/metrics` endpoint that exposes environment variables. Spring Boot Actuator with default config exposes every property including passwords.

**Child processes**: When your application shells out — to `pg_dump`, `aws s3`, any subprocess — the child inherits the parent's environment. If that subprocess logs its own argv or environment for debugging, your secrets are in two places now.

The solution is not to abandon env vars but to use them as a short-lived handoff mechanism, not as long-term storage.

## The Injection Pattern That Actually Works

The safest injection model: secrets live in a secret store (HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager), get pulled at container start into a tmpfs-mounted file, and the application reads the file — not the env var.

Here's a Kubernetes init container pattern that does this with Vault:

```yaml
# pod spec excerpt
initContainers:
  - name: vault-init
    image: hashicorp/vault:1.15
    command:
      - sh
      - -c
      - |
        vault login -method=kubernetes role=my-service
        vault kv get -field=password secret/my-service/db \
          > /secrets/db_password
        vault kv get -field=api_key secret/my-service/stripe \
          > /secrets/stripe_api_key
        chmod 400 /secrets/*
    env:
      - name: VAULT_ADDR
        value: "https://vault.internal:8200"
    volumeMounts:
      - name: secrets-vol
        mountPath: /secrets

containers:
  - name: app
    volumeMounts:
      - name: secrets-vol
        mountPath: /secrets
        readOnly: true

volumes:
  - name: secrets-vol
    emptyDir:
      medium: Memory  # tmpfs — never hits disk
```

The Vault Kubernetes auth method uses the pod's service account JWT to authenticate without any pre-shared secret. The app container gets read-only access to an in-memory filesystem that was never an env var.

Reading secrets from files in Python:

```python
import os
from pathlib import Path
from functools import lru_cache

SECRETS_DIR = Path(os.getenv("SECRETS_DIR", "/secrets"))

@lru_cache(maxsize=None)
def get_secret(name: str) -> str:
    secret_path = SECRETS_DIR / name
    if not secret_path.exists():
        raise RuntimeError(f"Secret {name} not found at {secret_path}")
    # Strip trailing newline from file write
    value = secret_path.read_text().strip()
    if not value:
        raise RuntimeError(f"Secret {name} is empty")
    return value

# Usage — no secret in module-level variables
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=get_secret("db_password"),  # read at call time
    )
```

The `lru_cache` means we read the file once per process lifetime. If you need live rotation (more on this below), remove the cache and add a stat-based invalidation check.

## Preventing Leaks in Application Code

**Logging middleware is the biggest risk.** Request logging that captures headers will capture `Authorization` headers. Exception trackers that serialize request context will capture anything in `request.environ`. Audit these explicitly:

```python
import logging
import re

# Patterns that indicate a value should be redacted
SECRET_PATTERNS = re.compile(
    r'(password|passwd|secret|token|api[-_]?key|auth|credential)',
    re.IGNORECASE
)

class SecretRedactingFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        # Redact k=v patterns where k looks like a secret name
        return re.sub(
            r'(' + SECRET_PATTERNS.pattern + r')=[^\s&"\']+',
            r'\1=REDACTED',
            msg
        )
```

Add this formatter to every handler in your logging config, not just production. Secrets leak in dev too, and dev logs often end up in Slack or JIRA tickets.

**Sanitize error responses**: Framework error handlers that return stack traces in 500 responses can expose variable names and values that are in scope. In Go:

```go
// Bad — stack trace may contain secret variable values in some frameworks
http.Error(w, err.Error(), http.StatusInternalServerError)

// Good — log the full error server-side, return a reference ID
errorID := ulid.Make().String()
log.WithError(err).WithField("error_id", errorID).Error("request failed")
http.Error(w, fmt.Sprintf("internal error (id: %s)", errorID), 500)
```

**Disable actuator/debug endpoints in production**: This is a config line in Spring Boot:

```yaml
# application-production.yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info  # NOT env, configprops, beans
  endpoint:
    env:
      enabled: false
```

## Rotation Without Downtime

Rotation is where most teams fail. The problem: your database has one password, fifteen services use it, you rotate it, and nine of them crash before they pick up the new password.

The expand-contract pattern for credential rotation:

**Step 1 — Expand**: Add the new credential alongside the old one. For Postgres, create a new role or add the new password as a second option (Postgres doesn't natively support multiple passwords per user, so you'd either use a role alias or accept a brief window where both are valid via the auth method).

For API keys, this is easier — most providers let you have two active keys simultaneously for exactly this reason.

**Step 2 — Roll services**: Deploy each service with the new credential. Because the old one is still valid, there's no outage window. Watch error rates after each rollout before proceeding.

**Step 3 — Contract**: Once all services are confirmed on the new credential, revoke the old one.

With Vault's dynamic secrets, this pattern becomes automatic for databases:

```bash
# Vault generates short-lived credentials on demand
# No rotation needed — they expire automatically

# Configure a database secrets engine
vault secrets enable database
vault write database/config/my-postgres \
    plugin_name=postgresql-database-plugin \
    connection_url="postgresql://{{username}}:{{password}}@postgres:5432/mydb" \
    allowed_roles="my-service" \
    username="vault" \
    password="$VAULT_DB_PASSWORD"

vault write database/roles/my-service \
    db_name=my-postgres \
    creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}'; GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO \"{{name}}\";" \
    default_ttl="1h" \
    max_ttl="24h"
```

Each service gets a unique Postgres credential that expires after an hour. Rotation is implicit — the service calls Vault for a new lease before the old one expires. No rotation event, no coordination, no outage.

For services that maintain long-lived connection pools, implement lease renewal:

```python
import threading
import time

class RotatingCredential:
    def __init__(self, vault_client, role_path: str, renew_before_seconds: int = 300):
        self._vault = vault_client
        self._role_path = role_path
        self._renew_before = renew_before_seconds
        self._credential = None
        self._lease_id = None
        self._expires_at = 0
        self._lock = threading.Lock()
        self._refresh()

    def _refresh(self):
        secret = self._vault.secrets.database.generate_credentials(self._role_path)
        with self._lock:
            self._credential = secret["data"]
            self._lease_id = secret["lease_id"]
            self._expires_at = time.time() + secret["lease_duration"]
        threading.Timer(
            secret["lease_duration"] - self._renew_before,
            self._refresh
        ).start()

    @property
    def password(self) -> str:
        with self._lock:
            return self._credential["password"]

    @property
    def username(self) -> str:
        with self._lock:
            return self._credential["username"]
```

## Detecting Leaks Before They Become Incidents

Preventive controls catch most leaks at the source:

**Git pre-commit hooks with secret scanning**:

```bash
# .git/hooks/pre-commit
#!/bin/bash
# Fail commit if secrets are detected
if command -v trufflehog &>/dev/null; then
    trufflehog git file://. --since-commit HEAD --only-verified --fail
elif command -v gitleaks &>/dev/null; then
    gitleaks protect --staged --redact
fi
```

Install `gitleaks` or `trufflehog` as a dev dependency and wire them into your CI pipeline as a mandatory check. Both have tunable entropy thresholds and allowlists for known false positives.

**Audit log alerting**: If you're using Vault, enable audit logging and alert on unexpected credential access patterns — a service reading a secret it has never read before, or accessing secrets outside business hours.

**Canary tokens**: Inject fake high-entropy API keys into places secrets shouldn't be (public S3 buckets, documentation repos, error logs). Use a service like canarytokens.org or build your own: any time the token is used, you get an alert. If it fires, you know where the leak is.

## The One Change That Prevents Most Incidents

If you do nothing else from this post: wire up secret scanning in your CI pipeline before merges, not as an advisory warning but as a blocking check. The majority of production secret leaks I have seen postmortems for were committed to git and would have been caught by a five-minute setup of `gitleaks` as a required status check.

Everything else — Vault, dynamic credentials, rotation automation — is correct and worth building. But the largest risk reduction per hour of engineering effort is preventing secrets from entering source control in the first place. Start there.
