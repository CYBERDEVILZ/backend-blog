#!/usr/bin/env python3
"""
Daily blog post generator.
Generates a backend engineering post via Claude API and opens a GitHub PR.
Requires env vars: ANTHROPIC_API_KEY, GITHUB_TOKEN, GITHUB_REPO (owner/repo)
"""

import os
import re
import sys
import json
import datetime
import subprocess
import anthropic

TOPICS = [
    "database indexing internals (B-trees, LSM trees)",
    "HTTP/2 and HTTP/3 multiplexing and head-of-line blocking",
    "two-phase commit vs Saga pattern for distributed transactions",
    "write-ahead logging (WAL) and crash recovery",
    "connection pooling strategies and PgBouncer internals",
    "CAP theorem and the PACELC extension",
    "bloom filters and their use in databases",
    "MVCC (Multi-Version Concurrency Control) in PostgreSQL",
    "gRPC vs REST: when to use each",
    "event sourcing and CQRS in practice",
    "Kafka consumer group rebalancing internals",
    "rate limiting algorithms: token bucket, leaky bucket, sliding window",
    "TLS handshake deep dive",
    "DNS resolution and caching",
    "zero-downtime deployments: blue-green vs canary vs rolling",
    "circuit breaker pattern implementation",
    "idempotency keys and exactly-once delivery",
    "PostgreSQL query planner and EXPLAIN ANALYZE",
    "Redis data structures and when to use each",
    "sidecar proxy pattern and service mesh internals",
    "container networking: CNI plugins and overlay networks",
    "columnar storage formats: Parquet and Apache Arrow",
    "vector clocks and conflict resolution in distributed systems",
    "graceful shutdown and signal handling in servers",
    "backpressure mechanisms in streaming systems",
    "JWT internals and common security pitfalls",
    "hot reload and live migration of database schemas",
    "lock-free data structures and CAS operations",
    "S3-compatible object storage internals",
    "observability: the three pillars (metrics, logs, traces)",
]


def pick_topic(existing_titles: list[str]) -> str:
    """Return a topic not already covered."""
    used = {t.lower() for t in existing_titles}
    for topic in TOPICS:
        if not any(word in used for word in topic.split()[:3]):
            return topic
    # If all topics are exhausted, let the model pick freely
    return "an advanced backend engineering topic not yet covered in this blog"


def get_existing_post_titles(repo: str, token: str) -> list[str]:
    import urllib.request
    url = f"https://api.github.com/repos/{repo}/contents/_posts"
    req = urllib.request.Request(url, headers={"Authorization": f"token {token}", "Accept": "application/vnd.github+json"})
    try:
        with urllib.request.urlopen(req) as resp:
            files = json.loads(resp.read())
            return [f["name"] for f in files if f["name"].endswith(".md")]
    except Exception:
        return []


def generate_post(topic: str, date_str: str) -> tuple[str, str]:
    """Returns (filename_slug, markdown_content)."""
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    system = """You are a senior backend engineer writing a technical blog post.
Write in clear, direct prose. No fluff, no filler. Use concrete examples and real code.
The audience is working engineers — skip basics, go deep.
Format: Jekyll-compatible Markdown with YAML front matter."""

    user = f"""Write a backend engineering blog post about: **{topic}**

Rules:
- YAML front matter must include: layout, title, date ({date_str}), tags (array), read_time (integer minutes)
- 800–1400 words of body content
- At least one well-commented code block (Python, Go, or pseudocode)
- Structure with ## headings, no introduction fluff
- End with a clear, practical takeaway
- Do not add any commentary outside the Markdown

Respond with ONLY the raw Markdown file content (including front matter). No preamble."""

    message = client.messages.create(
        model="claude-opus-4-7",
        max_tokens=4096,
        system=system,
        messages=[{"role": "user", "content": user}],
    )

    content = message.content[0].text.strip()

    # Extract title for slug
    title_match = re.search(r'^title:\s*["\']?(.+?)["\']?\s*$', content, re.MULTILINE)
    title = title_match.group(1) if title_match else topic
    slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')[:60]

    return slug, content


def push_pr(repo: str, token: str, date_str: str, slug: str, content: str) -> str:
    """Creates branch, commits file, opens PR. Returns PR URL."""
    import urllib.request
    import urllib.parse

    filename = f"_posts/{date_str}-{slug}.md"
    branch = f"post/{date_str}-{slug}"

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
    }

    def gh(method: str, path: str, body: dict | None = None):
        url = f"https://api.github.com/repos/{repo}{path}"
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())

    # Get default branch SHA
    ref_data = gh("GET", "/git/ref/heads/main")
    base_sha = ref_data["object"]["sha"]

    # Create branch
    gh("POST", "/git/refs", {"ref": f"refs/heads/{branch}", "sha": base_sha})

    # Get tree SHA for base commit
    commit_data = gh("GET", f"/git/commits/{base_sha}")
    tree_sha = commit_data["tree"]["sha"]

    # Create blob
    import base64
    blob = gh("POST", "/git/blobs", {
        "content": base64.b64encode(content.encode()).decode(),
        "encoding": "base64",
    })

    # Create tree
    tree = gh("POST", "/git/trees", {
        "base_tree": tree_sha,
        "tree": [{"path": filename, "mode": "100644", "type": "blob", "sha": blob["sha"]}],
    })

    # Create commit
    title_match = re.search(r'^title:\s*["\']?(.+?)["\']?\s*$', content, re.MULTILINE)
    post_title = title_match.group(1) if title_match else slug.replace('-', ' ').title()
    commit = gh("POST", "/git/commits", {
        "message": f"post: {post_title}",
        "tree": tree["sha"],
        "parents": [base_sha],
    })

    # Update branch ref
    gh("PATCH", f"/git/refs/heads/{branch}", {"sha": commit["sha"]})

    # Open PR
    pr = gh("POST", "/pulls", {
        "title": f"Daily post: {post_title}",
        "head": branch,
        "base": "main",
        "body": f"Auto-generated daily backend engineering post.\n\n**Topic:** {post_title}\n**Date:** {date_str}",
    })

    return pr["html_url"]


def main():
    repo = os.environ.get("GITHUB_REPO")
    token = os.environ.get("GITHUB_TOKEN")
    if not repo or not token:
        print("ERROR: GITHUB_REPO and GITHUB_TOKEN must be set.", file=sys.stderr)
        sys.exit(1)

    date_str = datetime.date.today().isoformat()
    existing = get_existing_post_titles(repo, token)
    topic = pick_topic(existing)

    print(f"Generating post for: {topic}")
    slug, content = generate_post(topic, date_str)

    print(f"Pushing PR for: {date_str}-{slug}.md")
    pr_url = push_pr(repo, token, date_str, slug, content)

    print(f"PR opened: {pr_url}")


if __name__ == "__main__":
    main()
