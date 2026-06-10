# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "boto3>=1.35",
#     "requests>=2.32",
# ]
# ///
"""Benchmarks the NerveMQ SQS-compatible API with boto3, the official AWS SDK
for Python.

Run against a local server with uv (https://docs.astral.sh/uv/):

    uv run benchmark.py
    uv run benchmark.py --messages 1000 --concurrency 16 --payload-bytes 4096

Scenarios (each on a fresh, purged queue):

    send_message  (sequential)   one message per request, single thread
    send_message_batch           10 messages per request
    send_message  (concurrent)   one message per request, N threads
    receive + delete drain       pre-filled queue, receive 10 / delete each
    round trip                   send -> receive -> delete, single thread

Credentials resolve like test_sqs.py: AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY
if set, otherwise a throwaway namespace + API key is minted via the admin API
(NERVEMQ_ADMIN_EMAIL / NERVEMQ_ADMIN_PASSWORD, defaulting to the dev server's
admin@example.com / password) and removed afterwards.

Latency percentiles are per-request (per-batch for batch send); msg/s is
messages — not requests — per wall-clock second.
"""

import argparse
import concurrent.futures
import os
import sys
import time
import uuid
from dataclasses import dataclass

import boto3
import requests
from botocore.client import Config as BotoConfig

DEFAULT_ENDPOINT = os.environ.get("NERVEMQ_ENDPOINT", "http://localhost:8080/api/sqs")
ADMIN_EMAIL = os.environ.get("NERVEMQ_ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.environ.get("NERVEMQ_ADMIN_PASSWORD", "password")

BATCH_SIZE = 10  # AWS SQS maximum entries per SendMessageBatch.


@dataclass
class Result:
    scenario: str
    messages: int
    wall_seconds: float
    latencies: "list[float]"  # Per-request, in seconds.

    @property
    def throughput(self) -> float:
        return self.messages / self.wall_seconds if self.wall_seconds else 0.0

    def percentile_ms(self, pct: float) -> float:
        ordered = sorted(self.latencies)
        index = min(len(ordered) - 1, int(round(pct / 100 * (len(ordered) - 1))))
        return ordered[index] * 1000


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


def bootstrap_credentials(endpoint: str):
    """Returns (access_key, secret_key, cleanup) — see module docstring."""
    access = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access and secret:
        return access, secret, lambda: None

    base = endpoint.rsplit("/api/", 1)[0] + "/api/admin"
    session = requests.Session()
    res = admin_request(
        session,
        "POST",
        f"{base}/auth/login",
        json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    if res.status_code != 200:
        sys.exit(
            "admin login failed; either set AWS_ACCESS_KEY_ID/"
            "AWS_SECRET_ACCESS_KEY to a NerveMQ API key or set "
            "NERVEMQ_ADMIN_EMAIL/NERVEMQ_ADMIN_PASSWORD"
        )

    suffix = uuid.uuid4().hex[:10]
    namespace = f"sqsbench{suffix}"
    token_name = f"sqsbench{suffix}"
    admin_request(session, "POST", f"{base}/ns/{namespace}").raise_for_status()
    res = admin_request(
        session,
        "POST",
        f"{base}/tokens",
        json={"name": token_name, "namespace": namespace},
    )
    res.raise_for_status()
    key = res.json()

    def cleanup():
        admin_request(session, "DELETE", f"{base}/tokens", json={"name": token_name})
        admin_request(session, "DELETE", f"{base}/ns/{namespace}")

    return key["access_key"], key["secret_key"], cleanup


def make_client(endpoint: str, access: str, secret: str, pool_size: int):
    return boto3.client(
        "sqs",
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name=os.environ.get("AWS_REGION", "us-west-1"),
        endpoint_url=endpoint,
        config=BotoConfig(
            retries={"max_attempts": 1},
            connect_timeout=5,
            read_timeout=60,
            max_pool_connections=pool_size,
        ),
    )


def fill_queue(sqs, queue_url: str, count: int, body: str) -> None:
    """Pre-loads a queue using batch sends (untimed)."""
    for start in range(0, count, BATCH_SIZE):
        entries = [
            {"Id": str(i), "MessageBody": body}
            for i in range(min(BATCH_SIZE, count - start))
        ]
        sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)


def bench_sequential_send(sqs, queue_url: str, count: int, body: str) -> Result:
    latencies = []
    started = time.perf_counter()
    for _ in range(count):
        t = time.perf_counter()
        sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        latencies.append(time.perf_counter() - t)
    return Result(
        "send_message (sequential)", count, time.perf_counter() - started, latencies
    )


def bench_batch_send(sqs, queue_url: str, count: int, body: str) -> Result:
    latencies = []
    sent = 0
    started = time.perf_counter()
    while sent < count:
        size = min(BATCH_SIZE, count - sent)
        entries = [{"Id": str(i), "MessageBody": body} for i in range(size)]
        t = time.perf_counter()
        sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
        latencies.append(time.perf_counter() - t)
        sent += size
    return Result(
        f"send_message_batch ({BATCH_SIZE}/req)",
        count,
        time.perf_counter() - started,
        latencies,
    )


def bench_concurrent_send(
    sqs, queue_url: str, count: int, body: str, workers: int
) -> Result:
    def send_one(_index: int) -> float:
        t = time.perf_counter()
        sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        return time.perf_counter() - t

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        latencies = list(pool.map(send_one, range(count)))
    return Result(
        f"send_message ({workers} threads)",
        count,
        time.perf_counter() - started,
        latencies,
    )


def bench_drain(sqs, queue_url: str, count: int, body: str) -> Result:
    """receive_message (up to 10) + delete_message for each, until empty."""
    fill_queue(sqs, queue_url, count, body)

    latencies = []
    drained = 0
    started = time.perf_counter()
    while True:
        t = time.perf_counter()
        res = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=BATCH_SIZE)
        messages = res.get("Messages", [])
        for msg in messages:
            sqs.delete_message(
                QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"]
            )
        latencies.append(time.perf_counter() - t)
        if not messages:
            break
        drained += len(messages)
    return Result(
        "receive + delete drain", drained, time.perf_counter() - started, latencies
    )


def bench_round_trip(sqs, queue_url: str, count: int, body: str) -> Result:
    latencies = []
    started = time.perf_counter()
    for _ in range(count):
        t = time.perf_counter()
        sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        res = sqs.receive_message(QueueUrl=queue_url)
        (msg,) = res.get("Messages", [])
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
        latencies.append(time.perf_counter() - t)
    return Result(
        "send -> receive -> delete", count, time.perf_counter() - started, latencies
    )


def print_report(endpoint: str, payload_bytes: int, results: "list[Result]") -> None:
    print()
    print(f"NerveMQ SQS benchmark — {endpoint}")
    print(f"payload: {payload_bytes} bytes per message")
    print()
    header = (
        f"{'scenario':<32} {'msgs':>6} {'wall s':>8} {'msg/s':>9} "
        f"{'p50 ms':>8} {'p95 ms':>8} {'p99 ms':>8}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.scenario:<32} {r.messages:>6} {r.wall_seconds:>8.2f} "
            f"{r.throughput:>9.1f} {r.percentile_ms(50):>8.2f} "
            f"{r.percentile_ms(95):>8.2f} {r.percentile_ms(99):>8.2f}"
        )
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--endpoint", default=DEFAULT_ENDPOINT, help="SQS endpoint URL"
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=300,
        help="messages per send/drain scenario (default: 300)",
    )
    parser.add_argument(
        "--round-trips",
        type=int,
        default=100,
        help="iterations for the round-trip scenario (default: 100)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="threads for the concurrent send scenario (default: 8)",
    )
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=1024,
        help="message body size in bytes (default: 1024)",
    )
    args = parser.parse_args()

    body = ("0123456789abcdef" * (args.payload_bytes // 16 + 1))[: args.payload_bytes]

    access, secret, cleanup = bootstrap_credentials(args.endpoint)
    sqs = make_client(args.endpoint, access, secret, pool_size=args.concurrency)

    queue_name = f"bench{uuid.uuid4().hex[:12]}"
    queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    print(f"benchmarking against queue {queue_url}")

    results = []
    try:
        # Warm up connections and the SQLite write path.
        for _ in range(5):
            sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        sqs.purge_queue(QueueUrl=queue_url)

        scenarios = [
            lambda: bench_sequential_send(sqs, queue_url, args.messages, body),
            lambda: bench_batch_send(sqs, queue_url, args.messages, body),
            lambda: bench_concurrent_send(
                sqs, queue_url, args.messages, body, args.concurrency
            ),
            lambda: bench_drain(sqs, queue_url, args.messages, body),
            lambda: bench_round_trip(sqs, queue_url, args.round_trips, body),
        ]
        for scenario in scenarios:
            result = scenario()
            print(f"  {result.scenario}: done")
            results.append(result)
            sqs.purge_queue(QueueUrl=queue_url)
    finally:
        try:
            sqs.delete_queue(QueueUrl=queue_url)
        finally:
            cleanup()

    print_report(args.endpoint, args.payload_bytes, results)


if __name__ == "__main__":
    main()
