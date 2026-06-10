# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "boto3>=1.35",
#     "pytest>=8",
#     "requests>=2.32",
# ]
# ///
"""Integration tests for the NerveMQ SQS-compatible API, using boto3 — the
official AWS SDK for Python — exactly as a real SQS client would.

Run against a local server with uv (https://docs.astral.sh/uv/):

    uv run test_sqs.py

Credentials resolve in two ways:

1. If AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are set, they are used as a
   NerveMQ API key and all queues are created in that key's namespace.
2. Otherwise the suite bootstraps itself through the admin API: it logs in
   with NERVEMQ_ADMIN_EMAIL / NERVEMQ_ADMIN_PASSWORD (defaults match the dev
   server: admin@example.com / password), mints a throwaway namespace and API
   key, and deletes both when the session ends.

Every test runs against its own uniquely named queue, deleted on teardown.

Where NerveMQ intentionally diverges from AWS SQS (see the table in
README.md), tests assert the NerveMQ behaviour.
"""

import hashlib
import os
import sys
import threading
import time
import uuid

import boto3
import pytest
import requests
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

ENDPOINT_URL = os.environ.get("NERVEMQ_ENDPOINT", "http://localhost:8080/api/sqs")
ADMIN_EMAIL = os.environ.get("NERVEMQ_ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.environ.get("NERVEMQ_ADMIN_PASSWORD", "password")

# Server defaults, from src/config.rs (`defaults::VISIBILITY_TIMEOUT` and
# `defaults::MAX_RETRIES`). The retry limit is per-queue config that the SQS
# API can't change, so tests assume the server runs with the default.
DEFAULT_VISIBILITY_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 10

# AWS policy: maximum individual message size, and maximum total payload of a
# batch (sum of the individual lengths of all batched messages).
MAX_MESSAGE_SIZE = 1_048_576

# How long to wait beyond a visibility timeout before asserting redelivery.
# The server stamps deadlines with unixepoch() (whole seconds), so a 1s
# timeout can take up to ~2s of wall clock to lapse.
TIMING_SLACK = 1.5


def admin_base() -> str:
    """The admin API base, derived from the SQS endpoint (…/api/sqs)."""
    return ENDPOINT_URL.rsplit("/api/", 1)[0] + "/api/admin"


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


def md5_hex(body: str) -> str:
    return hashlib.md5(body.encode("utf-8")).hexdigest()


def http_status(exc_info) -> int:
    return exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def credentials():
    """A NerveMQ API key: from the environment, or minted via the admin API."""
    access = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access and secret:
        yield access, secret
        return

    base = admin_base()
    session = requests.Session()
    try:
        res = admin_request(
            session,
            "POST",
            f"{base}/auth/login",
            json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
        )
    except requests.ConnectionError:
        pytest.skip(f"no NerveMQ server reachable at {ENDPOINT_URL}")
    if res.status_code != 200:
        pytest.skip(
            "admin login failed; either set AWS_ACCESS_KEY_ID/"
            "AWS_SECRET_ACCESS_KEY to a NerveMQ API key or set "
            "NERVEMQ_ADMIN_EMAIL/NERVEMQ_ADMIN_PASSWORD"
        )

    suffix = uuid.uuid4().hex[:10]
    namespace = f"sqstest{suffix}"
    token_name = f"sqstest{suffix}"

    admin_request(session, "POST", f"{base}/ns/{namespace}").raise_for_status()
    res = admin_request(
        session,
        "POST",
        f"{base}/tokens",
        json={"name": token_name, "namespace": namespace},
    )
    res.raise_for_status()
    key = res.json()

    yield key["access_key"], key["secret_key"]

    # Best-effort cleanup; the namespace delete cascades to its queues.
    admin_request(session, "DELETE", f"{base}/tokens", json={"name": token_name})
    admin_request(session, "DELETE", f"{base}/ns/{namespace}")


def make_client(access: str, secret: str):
    return boto3.client(
        "sqs",
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name=os.environ.get("AWS_REGION", "us-west-1"),
        endpoint_url=ENDPOINT_URL,
        config=BotoConfig(
            retries={"max_attempts": 1},
            connect_timeout=5,
            read_timeout=30,
        ),
    )


@pytest.fixture(scope="session")
def sqs(credentials):
    access, secret = credentials
    return make_client(access, secret)


@pytest.fixture
def queue_url(sqs):
    """A fresh, uniquely named queue per test, deleted on teardown."""
    name = f"q{uuid.uuid4().hex[:12]}"
    url = sqs.create_queue(QueueName=name)["QueueUrl"]
    yield url
    try:
        sqs.delete_queue(QueueUrl=url)
    except ClientError:
        pass  # Already deleted by the test itself.


def receive(sqs, queue_url, **kwargs):
    """receive_message, normalizing the absent-vs-empty Messages key."""
    res = sqs.receive_message(QueueUrl=queue_url, **kwargs)
    return res.get("Messages", [])


# ---------------------------------------------------------------------------
# Queue lifecycle
# ---------------------------------------------------------------------------


class TestQueueLifecycle:
    def test_create_queue_returns_namespaced_url(self, sqs, queue_url):
        assert "/api/sqs/" in queue_url
        # URL shape is <host>/api/sqs/<namespace>/<queue>.
        path = queue_url.split("/api/sqs/", 1)[1]
        assert len(path.split("/")) == 2

    def test_get_queue_url_matches_create_queue(self, sqs, queue_url):
        name = queue_url.rsplit("/", 1)[1]
        res = sqs.get_queue_url(QueueName=name)
        assert res["QueueUrl"] == queue_url

    def test_get_queue_url_unknown_queue_fails(self, sqs):
        with pytest.raises(ClientError) as exc_info:
            sqs.get_queue_url(QueueName=f"missing{uuid.uuid4().hex[:8]}")
        assert http_status(exc_info) == 404

    def test_list_queues_contains_created_queue(self, sqs, queue_url):
        res = sqs.list_queues()
        assert queue_url in res.get("QueueUrls", [])

    def test_list_queues_filters_by_prefix(self, sqs):
        marker = uuid.uuid4().hex[:8]
        names = [f"alpha{marker}", f"beta{marker}"]
        urls = {n: sqs.create_queue(QueueName=n)["QueueUrl"] for n in names}
        try:
            res = sqs.list_queues(QueueNamePrefix=f"alpha{marker}")
            listed = res.get("QueueUrls", [])
            assert urls[f"alpha{marker}"] in listed
            assert urls[f"beta{marker}"] not in listed
        finally:
            for url in urls.values():
                sqs.delete_queue(QueueUrl=url)

    def test_delete_queue_removes_queue(self, sqs):
        name = f"q{uuid.uuid4().hex[:12]}"
        url = sqs.create_queue(QueueName=name)["QueueUrl"]
        sqs.delete_queue(QueueUrl=url)
        with pytest.raises(ClientError) as exc_info:
            sqs.get_queue_url(QueueName=name)
        assert http_status(exc_info) == 404

    def test_numeric_queue_name_round_trips(self, sqs):
        # A digits-only name (with a leading zero, which a numeric coercion
        # would destroy) must be stored and listed verbatim.
        name = f"0{uuid.uuid4().int}"[:12]
        url = sqs.create_queue(QueueName=name)["QueueUrl"]
        try:
            assert url.rsplit("/", 1)[1] == name
            assert sqs.get_queue_url(QueueName=name)["QueueUrl"] == url
            assert url in sqs.list_queues().get("QueueUrls", [])
            sqs.send_message(QueueUrl=url, MessageBody="to a numeric queue")
            (msg,) = receive(sqs, url)
            assert msg["Body"] == "to a numeric queue"
        finally:
            sqs.delete_queue(QueueUrl=url)


# ---------------------------------------------------------------------------
# Send & receive
# ---------------------------------------------------------------------------


class TestSendReceive:
    def test_round_trip(self, sqs, queue_url):
        body = "Hello NerveMQ!"
        sent = sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        assert sent["MessageId"]
        assert sent["MD5OfMessageBody"] == md5_hex(body)

        messages = receive(sqs, queue_url)
        assert len(messages) == 1
        msg = messages[0]
        assert msg["Body"] == body
        assert msg["MessageId"] == sent["MessageId"]
        assert msg["MD5OfBody"] == md5_hex(body)
        assert msg["ReceiptHandle"]

    def test_unicode_body_round_trip(self, sqs, queue_url):
        body = "héllo wörld — 日本語 🚀 \"quoted\" \\backslash\\ \n newline"
        sent = sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        assert sent["MD5OfMessageBody"] == md5_hex(body)
        (msg,) = receive(sqs, queue_url)
        assert msg["Body"] == body
        assert msg["MD5OfBody"] == md5_hex(body)

    def test_maximum_size_message_round_trips(self, sqs, queue_url):
        # AWS policy: the maximum individual message size is 1 MiB
        # (1,048,576 bytes) — a body of exactly that size must round-trip.
        body = "y" * MAX_MESSAGE_SIZE
        sent = sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        assert sent["MD5OfMessageBody"] == md5_hex(body)
        (msg,) = receive(sqs, queue_url)
        assert msg["Body"] == body

    def test_message_over_1mib_is_rejected(self, sqs, queue_url):
        body = "y" * (MAX_MESSAGE_SIZE + 1)
        with pytest.raises(ClientError) as exc_info:
            sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        assert http_status(exc_info) == 400

    def test_attributes_count_toward_message_size(self, sqs, queue_url):
        # Message size is body plus, per attribute, name + data type label
        # + value — so an at-the-limit body with any attribute is over.
        body = "y" * MAX_MESSAGE_SIZE
        with pytest.raises(ClientError) as exc_info:
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=body,
                MessageAttributes={
                    "Tip": {"DataType": "String", "StringValue": "over"}
                },
            )
        assert http_status(exc_info) == 400

    def test_receive_on_empty_queue_returns_no_messages(self, sqs, queue_url):
        assert receive(sqs, queue_url) == []

    def test_messages_delivered_in_fifo_order(self, sqs, queue_url):
        bodies = [f"message-{i}" for i in range(5)]
        for body in bodies:
            sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        messages = receive(sqs, queue_url, MaxNumberOfMessages=10)
        assert [m["Body"] for m in messages] == bodies

    def test_max_number_of_messages_caps_batch(self, sqs, queue_url):
        for i in range(5):
            sqs.send_message(QueueUrl=queue_url, MessageBody=f"m{i}")
        assert len(receive(sqs, queue_url, MaxNumberOfMessages=2)) == 2
        # The remaining three are still available.
        assert len(receive(sqs, queue_url, MaxNumberOfMessages=10)) == 3

    def test_long_poll_waits_out_an_empty_queue(self, sqs, queue_url):
        # With nothing to deliver, a long poll holds the connection for the
        # requested window and then returns empty.
        start = time.monotonic()
        messages = receive(sqs, queue_url, WaitTimeSeconds=2)
        elapsed = time.monotonic() - start
        assert messages == []
        assert 1.5 <= elapsed < 2 + TIMING_SLACK

    def test_long_poll_returns_early_when_a_message_arrives(self, sqs, queue_url):
        timer = threading.Timer(
            1.0,
            lambda: sqs.send_message(QueueUrl=queue_url, MessageBody="worth it"),
        )
        timer.start()
        try:
            start = time.monotonic()
            messages = receive(sqs, queue_url, WaitTimeSeconds=10)
            elapsed = time.monotonic() - start
        finally:
            timer.join()
        assert [m["Body"] for m in messages] == ["worth it"]
        # Returned once the message landed, well before the 10s deadline.
        assert 0.9 <= elapsed < 5

    def test_delay_seconds_defers_delivery(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="late", DelaySeconds=1)
        assert receive(sqs, queue_url) == []
        time.sleep(1 + TIMING_SLACK)
        (msg,) = receive(sqs, queue_url)
        assert msg["Body"] == "late"

    def test_delay_seconds_over_aws_maximum_is_rejected(self, sqs, queue_url):
        with pytest.raises(ClientError) as exc_info:
            sqs.send_message(
                QueueUrl=queue_url, MessageBody="too late", DelaySeconds=901
            )
        assert http_status(exc_info) == 400


# ---------------------------------------------------------------------------
# Message attributes
# ---------------------------------------------------------------------------


class TestMessageAttributes:
    ATTRIBUTES = {
        "Stage": {"DataType": "String", "StringValue": "production"},
        "Retries": {"DataType": "Number", "StringValue": "42"},
    }

    def test_string_and_number_attributes_round_trip(self, sqs, queue_url):
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="with attributes",
            MessageAttributes=self.ATTRIBUTES,
        )
        (msg,) = receive(sqs, queue_url, MessageAttributeNames=["All"])
        attrs = msg["MessageAttributes"]
        assert attrs["Stage"] == self.ATTRIBUTES["Stage"]
        assert attrs["Retries"] == self.ATTRIBUTES["Retries"]

    def test_attribute_filtering_by_name(self, sqs, queue_url):
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="filtered",
            MessageAttributes=self.ATTRIBUTES,
        )
        (msg,) = receive(sqs, queue_url, MessageAttributeNames=["Stage"])
        attrs = msg.get("MessageAttributes", {})
        assert "Stage" in attrs
        assert "Retries" not in attrs

    def test_attributes_omitted_unless_requested(self, sqs, queue_url):
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="unrequested",
            MessageAttributes=self.ATTRIBUTES,
        )
        (msg,) = receive(sqs, queue_url)
        assert msg.get("MessageAttributes", {}) == {}

    def test_numeric_attribute_name_round_trips(self, sqs, queue_url):
        # Attribute names are stored as message kv keys; a digits-only name
        # must come back verbatim, not as a number.
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="numeric attribute name",
            MessageAttributes={
                "123": {"DataType": "String", "StringValue": "value"}
            },
        )
        (msg,) = receive(sqs, queue_url, MessageAttributeNames=["All"])
        assert msg["MessageAttributes"]["123"]["StringValue"] == "value"

    def test_binary_attribute_round_trip(self, sqs, queue_url):
        payload = b"\x00\x01\x02binary\xff"
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="binary attr",
            MessageAttributes={
                "Blob": {"DataType": "Binary", "BinaryValue": payload}
            },
        )
        (msg,) = receive(sqs, queue_url, MessageAttributeNames=["All"])
        assert msg["MessageAttributes"]["Blob"]["BinaryValue"] == payload


# ---------------------------------------------------------------------------
# Visibility timeout
# ---------------------------------------------------------------------------


class TestVisibility:
    def test_received_message_becomes_invisible(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="hide me")
        assert len(receive(sqs, queue_url)) == 1
        # Default visibility timeout (30s) hides it from the next receive.
        assert receive(sqs, queue_url) == []

    def test_visibility_timeout_override_redelivers(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="come back")
        (first,) = receive(sqs, queue_url, VisibilityTimeout=1)
        assert receive(sqs, queue_url) == []
        time.sleep(1 + TIMING_SLACK)
        (second,) = receive(sqs, queue_url)
        assert second["MessageId"] == first["MessageId"]
        # Redelivery mints a fresh receipt handle.
        assert second["ReceiptHandle"] != first["ReceiptHandle"]

    def test_visibility_timeout_zero_redelivers_immediately(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="instant retry")
        (first,) = receive(sqs, queue_url, VisibilityTimeout=0)
        (second,) = receive(sqs, queue_url, VisibilityTimeout=0)
        assert second["MessageId"] == first["MessageId"]

    def test_change_message_visibility_releases_message(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="release me")
        (msg,) = receive(sqs, queue_url)  # Hidden for the default 30s.
        sqs.change_message_visibility(
            QueueUrl=queue_url,
            ReceiptHandle=msg["ReceiptHandle"],
            VisibilityTimeout=0,
        )
        assert len(receive(sqs, queue_url)) == 1

    def test_change_message_visibility_extends_timeout(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="keep hidden")
        (msg,) = receive(sqs, queue_url, VisibilityTimeout=1)
        sqs.change_message_visibility(
            QueueUrl=queue_url,
            ReceiptHandle=msg["ReceiptHandle"],
            VisibilityTimeout=30,
        )
        time.sleep(1 + TIMING_SLACK)
        # Without the extension this would have been redelivered by now.
        assert receive(sqs, queue_url) == []

    def test_change_message_visibility_rejects_oversized_timeout(
        self, sqs, queue_url
    ):
        sqs.send_message(QueueUrl=queue_url, MessageBody="limits")
        (msg,) = receive(sqs, queue_url)
        with pytest.raises(ClientError) as exc_info:
            sqs.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=msg["ReceiptHandle"],
                VisibilityTimeout=43201,  # AWS maximum is 43200 (12 hours).
            )
        assert http_status(exc_info) == 400

    def test_change_message_visibility_rejects_unknown_handle(
        self, sqs, queue_url
    ):
        with pytest.raises(ClientError):
            sqs.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=f"0:{uuid.uuid4().hex}",
                VisibilityTimeout=10,
            )

    def test_queue_visibility_timeout_attribute_is_honored(self, sqs, queue_url):
        sqs.set_queue_attributes(
            QueueUrl=queue_url, Attributes={"VisibilityTimeout": "1"}
        )
        sqs.send_message(QueueUrl=queue_url, MessageBody="queue default")
        assert len(receive(sqs, queue_url)) == 1
        assert receive(sqs, queue_url) == []
        time.sleep(1 + TIMING_SLACK)
        assert len(receive(sqs, queue_url)) == 1

    def test_message_stops_redelivering_after_max_retries(self, sqs, queue_url):
        # Each receive counts as a delivery attempt; once the queue's retry
        # limit (default 10, admin-configurable) is exhausted, the message is
        # marked failed and is no longer claimable.
        sqs.send_message(QueueUrl=queue_url, MessageBody="poison pill")
        deliveries = 0
        for _ in range(DEFAULT_MAX_RETRIES + 2):
            if not receive(sqs, queue_url, VisibilityTimeout=0):
                break
            deliveries += 1
        assert deliveries == DEFAULT_MAX_RETRIES
        assert receive(sqs, queue_url, VisibilityTimeout=0) == []


# ---------------------------------------------------------------------------
# Delete & purge
# ---------------------------------------------------------------------------


class TestDeleteMessage:
    def test_delete_acknowledges_message(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="ack me")
        (msg,) = receive(sqs, queue_url, VisibilityTimeout=0)
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
        # Visibility timeout was 0, so only deletion explains its absence.
        assert receive(sqs, queue_url) == []

    def test_delete_with_unknown_receipt_handle_fails(self, sqs, queue_url):
        with pytest.raises(ClientError) as exc_info:
            sqs.delete_message(
                QueueUrl=queue_url, ReceiptHandle=f"0:{uuid.uuid4().hex}"
            )
        assert http_status(exc_info) == 404

    def test_stale_receipt_handle_is_rejected_after_redelivery(
        self, sqs, queue_url
    ):
        # AWS semantics: a redelivery invalidates earlier receipt handles.
        sqs.send_message(QueueUrl=queue_url, MessageBody="contested")
        (first,) = receive(sqs, queue_url, VisibilityTimeout=0)
        (second,) = receive(sqs, queue_url, VisibilityTimeout=0)
        with pytest.raises(ClientError):
            sqs.delete_message(
                QueueUrl=queue_url, ReceiptHandle=first["ReceiptHandle"]
            )
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=second["ReceiptHandle"])

    def test_delete_message_batch(self, sqs, queue_url):
        for i in range(3):
            sqs.send_message(QueueUrl=queue_url, MessageBody=f"m{i}")
        messages = receive(sqs, queue_url, MaxNumberOfMessages=10)
        res = sqs.delete_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]}
                for i, m in enumerate(messages)
            ],
        )
        assert {e["Id"] for e in res["Successful"]} == {"0", "1", "2"}
        assert res.get("Failed", []) == []
        # All three are gone, even past their visibility window.
        for m in messages:
            with pytest.raises(ClientError):
                sqs.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=m["ReceiptHandle"],
                    VisibilityTimeout=0,
                )

    def test_delete_message_batch_reports_per_entry_failures(
        self, sqs, queue_url
    ):
        sqs.send_message(QueueUrl=queue_url, MessageBody="kept")
        (msg,) = receive(sqs, queue_url)
        res = sqs.delete_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": "ok", "ReceiptHandle": msg["ReceiptHandle"]},
                {"Id": "bad", "ReceiptHandle": f"0:{uuid.uuid4().hex}"},
            ],
        )
        assert [e["Id"] for e in res["Successful"]] == ["ok"]
        (failed,) = res["Failed"]
        assert failed["Id"] == "bad"
        assert failed["Code"] == "ReceiptHandleIsInvalid"
        assert failed["SenderFault"] is True


class TestPurgeQueue:
    def test_purge_removes_all_messages(self, sqs, queue_url):
        for i in range(5):
            sqs.send_message(QueueUrl=queue_url, MessageBody=f"m{i}")
        # Including one that is currently in flight.
        receive(sqs, queue_url)
        sqs.purge_queue(QueueUrl=queue_url)
        assert receive(sqs, queue_url, MaxNumberOfMessages=10) == []


# ---------------------------------------------------------------------------
# Batch send
# ---------------------------------------------------------------------------


class TestSendMessageBatch:
    def test_batch_of_ten_succeeds(self, sqs, queue_url):
        entries = [{"Id": str(i), "MessageBody": f"batch-{i}"} for i in range(10)]
        res = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
        successful = res.get("Successful", [])
        assert len(successful) == 10
        assert res.get("Failed", []) == []
        assert {e["Id"] for e in successful} == {str(i) for i in range(10)}
        by_id = {e["Id"]: e for e in successful}
        for i in range(10):
            assert by_id[str(i)]["MD5OfMessageBody"] == md5_hex(f"batch-{i}")

        messages = receive(sqs, queue_url, MaxNumberOfMessages=10)
        assert sorted(m["Body"] for m in messages) == sorted(
            f"batch-{i}" for i in range(10)
        )

    def test_batch_total_payload_over_1mib_is_rejected(self, sqs, queue_url):
        # Individually legal entries whose combined payload exceeds 1 MiB
        # fail the whole request (AWS: BatchRequestTooLong) — and nothing
        # from the failed batch is enqueued.
        entries = [
            {"Id": str(i), "MessageBody": "y" * (400 * 1024)} for i in range(3)
        ]
        with pytest.raises(ClientError) as exc_info:
            sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
        assert http_status(exc_info) == 400
        assert receive(sqs, queue_url, MaxNumberOfMessages=10) == []

        # Two of the same entries stay under the limit and go through.
        res = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries[:2])
        assert len(res.get("Successful", [])) == 2

    def test_batch_entries_carry_message_attributes(self, sqs, queue_url):
        entries = [
            {
                "Id": "0",
                "MessageBody": "tagged",
                "MessageAttributes": {
                    "Origin": {"DataType": "String", "StringValue": "batch"}
                },
            }
        ]
        res = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
        assert len(res.get("Successful", [])) == 1
        (msg,) = receive(sqs, queue_url, MessageAttributeNames=["All"])
        assert msg["MessageAttributes"]["Origin"]["StringValue"] == "batch"


# ---------------------------------------------------------------------------
# Queue attributes & tags
# ---------------------------------------------------------------------------


class TestQueueAttributes:
    def test_set_and_get_attributes_round_trip(self, sqs, queue_url):
        attributes = {
            "VisibilityTimeout": "120",
            "DelaySeconds": "5",
            "MaximumMessageSize": "2048",
            "MessageRetentionPeriod": "3600",
            "ReceiveMessageWaitTimeSeconds": "2",
        }
        sqs.set_queue_attributes(QueueUrl=queue_url, Attributes=attributes)
        res = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])
        got = res.get("Attributes", {})
        for key, value in attributes.items():
            assert got.get(key) == value, f"{key}: {got.get(key)!r} != {value!r}"

    def test_get_attributes_on_fresh_queue_is_empty(self, sqs, queue_url):
        res = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])
        assert res.get("Attributes", {}) == {}

    def test_get_attributes_on_unknown_queue_fails(self, sqs, queue_url):
        bogus = queue_url.rsplit("/", 1)[0] + f"/missing{uuid.uuid4().hex[:8]}"
        with pytest.raises(ClientError) as exc_info:
            sqs.get_queue_attributes(QueueUrl=bogus, AttributeNames=["All"])
        assert http_status(exc_info) == 404

    def test_queue_maximum_message_size_is_enforced(self, sqs, queue_url):
        sqs.set_queue_attributes(
            QueueUrl=queue_url, Attributes={"MaximumMessageSize": "1024"}
        )
        # At the queue's limit: accepted.
        sqs.send_message(QueueUrl=queue_url, MessageBody="y" * 1024)
        # One byte over: rejected.
        with pytest.raises(ClientError) as exc_info:
            sqs.send_message(QueueUrl=queue_url, MessageBody="y" * 1025)
        assert http_status(exc_info) == 400

    def test_create_time_visibility_timeout_is_honored(self, sqs):
        name = f"q{uuid.uuid4().hex[:12]}"
        url = sqs.create_queue(
            QueueName=name, Attributes={"VisibilityTimeout": "1"}
        )["QueueUrl"]
        try:
            sqs.send_message(QueueUrl=url, MessageBody="created with attrs")
            assert len(receive(sqs, url)) == 1
            time.sleep(1 + TIMING_SLACK)
            # Default timeout (30s) would still hide it; 1s would not.
            assert len(receive(sqs, url)) == 1
        finally:
            sqs.delete_queue(QueueUrl=url)


class TestQueueTags:
    def test_tag_and_untag_queue(self, sqs, queue_url):
        sqs.tag_queue(QueueUrl=queue_url, Tags={"owner": "tests", "tier": "gold"})
        tags = sqs.list_queue_tags(QueueUrl=queue_url).get("Tags", {})
        assert tags == {"owner": "tests", "tier": "gold"}

        sqs.untag_queue(QueueUrl=queue_url, TagKeys=["tier"])
        tags = sqs.list_queue_tags(QueueUrl=queue_url).get("Tags", {})
        assert tags == {"owner": "tests"}

    def test_create_queue_tags_are_listed(self, sqs):
        name = f"q{uuid.uuid4().hex[:12]}"
        url = sqs.create_queue(
            QueueName=name, tags={"team": "platform", "env": "test"}
        )["QueueUrl"]
        try:
            res = sqs.list_queue_tags(QueueUrl=url)
            assert res.get("Tags", {}) == {"team": "platform", "env": "test"}
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_numeric_tag_values_round_trip_verbatim(self, sqs, queue_url):
        # Values that look like numbers must come back as the exact strings
        # they were set to — including spellings a numeric coercion would
        # destroy ("01", "2.50").
        tags = {"tier": "1", "version": "01", "ratio": "2.50"}
        sqs.tag_queue(QueueUrl=queue_url, Tags=tags)
        assert sqs.list_queue_tags(QueueUrl=queue_url).get("Tags", {}) == tags


# ---------------------------------------------------------------------------
# Authentication & errors
# ---------------------------------------------------------------------------


class TestAuth:
    def test_wrong_secret_key_is_rejected(self, credentials):
        access, _ = credentials
        client = make_client(access, "nervemq_invalid_secret_key")
        with pytest.raises(ClientError) as exc_info:
            client.list_queues()
        assert http_status(exc_info) == 401

    def test_unknown_access_key_is_rejected(self):
        # Well-formed (base58, like real keys) but not minted by the server.
        client = make_client("1unknownKey", "irrelevant")
        with pytest.raises(ClientError) as exc_info:
            client.list_queues()
        assert http_status(exc_info) == 401

    def test_send_to_unknown_queue_fails(self, sqs, queue_url):
        bogus = queue_url.rsplit("/", 1)[0] + f"/missing{uuid.uuid4().hex[:8]}"
        with pytest.raises(ClientError) as exc_info:
            sqs.send_message(QueueUrl=bogus, MessageBody="lost")
        assert http_status(exc_info) == 404


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", *sys.argv[1:]]))
