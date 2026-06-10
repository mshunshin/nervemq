# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "boto3>=1.35",
#     "requests>=2.32",
# ]
# ///
"""Seeds a NerveMQ queue with test messages via the SQS SendMessageBatch API.

Run against a local server with uv (https://docs.astral.sh/uv/):

    uv run seed_messages.py --namespace hello --queue dave --count 5000
    uv run seed_messages.py --namespace hello --queue dave --count 100 \
        --body "Order {n}"

The queue is created if it does not exist. Messages are numbered 1..count;
each carries a `Seq` (Number) attribute, and `{n}` / `{count}` placeholders
in --body are substituted.

Credentials resolve like test_sqs.py and benchmark.py: AWS_ACCESS_KEY_ID /
AWS_SECRET_ACCESS_KEY if set (the key must be scoped to the target
namespace), otherwise a temporary API key is minted for the namespace via
the admin API (NERVEMQ_ADMIN_EMAIL / NERVEMQ_ADMIN_PASSWORD, defaulting to
the dev server's admin@example.com / password) and deleted afterwards.

Transient server-fault batch failures are retried with backoff; sender
faults abort.
"""

import argparse
import os
import sys
import time
import uuid

import boto3
import requests

DEFAULT_ENDPOINT = os.environ.get("NERVEMQ_ENDPOINT", "http://localhost:8080/api/sqs")
ADMIN_EMAIL = os.environ.get("NERVEMQ_ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.environ.get("NERVEMQ_ADMIN_PASSWORD", "password")

BATCH_SIZE = 10  # AWS SQS maximum entries per SendMessageBatch.
RETRIES = 5


def admin_base(endpoint: str) -> str:
    """The admin API base, derived from the SQS endpoint (…/api/sqs)."""
    return endpoint.rsplit("/api/", 1)[0] + "/api/admin"


def admin_request(session: requests.Session, method: str, url: str, **kwargs):
    """A session request that tolerates the server's Secure session cookie.

    The admin API (re-)issues its session cookie with the Secure flag on
    every response. Browsers treat localhost as a secure context and send it
    anyway; `requests` does not, so unflag it after each call.
    """
    res = session.request(method, url, timeout=10, **kwargs)
    for cookie in session.cookies:
        cookie.secure = False
    return res


def admin_login(endpoint: str) -> requests.Session:
    session = requests.Session()
    res = admin_request(
        session,
        "POST",
        f"{admin_base(endpoint)}/auth/login",
        json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    if res.status_code != 200:
        sys.exit(
            "admin login failed; either set AWS_ACCESS_KEY_ID/"
            "AWS_SECRET_ACCESS_KEY to a namespace-scoped NerveMQ API key or "
            "set NERVEMQ_ADMIN_EMAIL/NERVEMQ_ADMIN_PASSWORD"
        )
    return session


def ensure_queue(session: requests.Session, base: str, namespace: str, queue: str):
    res = admin_request(session, "GET", f"{base}/queue/{namespace}")
    res.raise_for_status()
    if not any(q["name"] == queue for q in res.json()["queues"]):
        admin_request(
            session,
            "POST",
            f"{base}/queue/{namespace}/{queue}",
            json={"attributes": {}, "tags": {}},
        ).raise_for_status()
        print(f"created queue {namespace}/{queue}")


def send_all(sqs, queue_url: str, count: int, body_template: str) -> int:
    start = time.monotonic()
    sent = 0
    retried = 0
    for first in range(1, count + 1, BATCH_SIZE):
        entries = [
            {
                "Id": str(i),
                "MessageBody": body_template.format(n=n, count=count),
                "MessageAttributes": {
                    "Seq": {"DataType": "Number", "StringValue": str(n)}
                },
            }
            for i, n in enumerate(range(first, min(first + BATCH_SIZE, count + 1)))
        ]
        # The server runs each batch in one transaction, so entries fail
        # together; retry the whole batch on transient server faults.
        for attempt in range(RETRIES):
            res = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
            failed = res.get("Failed", [])
            if not failed:
                break
            if any(f.get("SenderFault") for f in failed):
                sys.exit(f"sender fault at batch {first}: {failed}")
            retried += 1
            time.sleep(0.2 * (attempt + 1))
        else:
            sys.exit(f"batch {first} kept failing after {RETRIES} tries: {failed}")
        sent += len(entries)
        if sent % 1000 == 0:
            print(f"{sent}/{count} sent")
    elapsed = time.monotonic() - start
    print(
        f"sent {sent} messages in {elapsed:.1f}s "
        f"({sent / elapsed:.0f} msg/s, {retried} batch retries)"
    )
    return sent


def main():
    parser = argparse.ArgumentParser(
        description="Seed a NerveMQ queue with numbered test messages."
    )
    parser.add_argument("--namespace", required=True, help="target namespace")
    parser.add_argument("--queue", required=True, help="target queue")
    parser.add_argument(
        "--count", type=int, default=1000, help="messages to send (default: 1000)"
    )
    parser.add_argument(
        "--body",
        default="Test message {n} of {count}",
        help="body template; {n} and {count} are substituted",
    )
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    args = parser.parse_args()

    access = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    session = None
    token_name = None

    if not (access and secret):
        session = admin_login(args.endpoint)
        base = admin_base(args.endpoint)
        ensure_queue(session, base, args.namespace, args.queue)
        token_name = f"seed{uuid.uuid4().hex[:8]}"
        res = admin_request(
            session,
            "POST",
            f"{base}/tokens",
            json={"name": token_name, "namespace": args.namespace},
        )
        res.raise_for_status()
        creds = res.json()
        access, secret = creds["access_key"], creds["secret_key"]

    try:
        sqs = boto3.client(
            "sqs",
            endpoint_url=args.endpoint,
            region_name="us-east-1",
            aws_access_key_id=access,
            aws_secret_access_key=secret,
        )
        queue_url = sqs.get_queue_url(QueueName=args.queue)["QueueUrl"]
        send_all(sqs, queue_url, args.count, args.body)
    finally:
        if session is not None and token_name is not None:
            admin_request(
                session,
                "DELETE",
                f"{admin_base(args.endpoint)}/tokens",
                json={"name": token_name},
            ).raise_for_status()
            print(f"temporary API key {token_name} deleted")

    if session is not None:
        stats = admin_request(
            session,
            "GET",
            f"{admin_base(args.endpoint)}/queue/{args.namespace}/{args.queue}",
        ).json()
        print(
            f"queue {args.namespace}/{args.queue}: "
            f"{stats['message_count']} messages ({stats['pending']} pending)"
        )


if __name__ == "__main__":
    main()
