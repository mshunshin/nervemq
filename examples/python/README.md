# Python examples

All scripts use [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
— the official AWS SDK for Python — against NerveMQ's SQS-compatible endpoint,
and carry inline dependency metadata so they run directly with
[uv](https://docs.astral.sh/uv/):

| Script | Purpose |
| --- | --- |
| [`example.py`](example.py) | Minimal send/receive walkthrough |
| [`test_sqs.py`](test_sqs.py) | Integration test suite for the SQS API (pytest) |
| [`benchmark.py`](benchmark.py) | Throughput / latency benchmark |

## Credentials

All three scripts target `NERVEMQ_ENDPOINT` (default
`http://localhost:8080/api/sqs`).

`example.py` requires `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` set to a
NerveMQ API key. The tests and benchmark accept the same variables, but when
they are unset they **bootstrap themselves**: they log in to the admin API
with `NERVEMQ_ADMIN_EMAIL` / `NERVEMQ_ADMIN_PASSWORD` (defaulting to the dev
server's `admin@example.com` / `password`), mint a throwaway namespace and API
key, and delete both afterwards. Against a default dev server no setup is
needed at all.

## Running the tests

```sh
cargo run &          # or however you run your server
uv run test_sqs.py   # the file is also a plain pytest module
```

Extra arguments are passed through to pytest, e.g.
`uv run test_sqs.py -k visibility -x`.

The suite covers queue lifecycle (create / get-url / list / delete), send and
receive round trips (MD5 verification, unicode and 64 KiB bodies, FIFO
ordering, `MaxNumberOfMessages`, `DelaySeconds`, long polling), message
attributes (round trip and filtering, including `Binary`), visibility timeouts
(per-request override, per-queue attribute, `ChangeMessageVisibility`, retry
exhaustion), delete/purge semantics (including stale receipt handles and
`DeleteMessageBatch`), `SendMessageBatch`, queue attributes and tags
(including at create time), and authentication failures.

Tests that wait on visibility timeouts sleep a few seconds; the full suite
takes roughly 30 seconds against a local server.

## Running the benchmark

```sh
uv run benchmark.py
uv run benchmark.py --messages 1000 --concurrency 16 --payload-bytes 4096
```

Scenarios: sequential `SendMessage`, `SendMessageBatch` (10 per request),
concurrent `SendMessage` (thread pool), a receive+delete drain, and a full
send→receive→delete round trip. The report shows per-scenario throughput
(messages per wall-clock second) and per-request latency percentiles.

## Known divergences from AWS SQS

Message size limits follow current AWS policy: an individual message (body
plus attributes) and a batch's total payload are both capped at 1 MiB
(1,048,576 bytes), rejected with 400, and a queue's `MaximumMessageSize`
attribute lowers the per-message limit. The remaining intentional
differences, which the tests assert as-is:

| Behaviour | Status |
| --- | --- |
| Error responses | Correct HTTP status codes, but no AWS error envelope (`__type`), so SDKs report generic `ClientError`s rather than typed exceptions like `QueueDoesNotExist` |
| Request envelope cap | The whole HTTP request body is capped at 8 MiB (413) — unreachable by compliant requests, since message payloads are limited to 1 MiB before JSON escaping |

Also note: message ordering is strictly FIFO (AWS standard queues are
best-effort), and a message stops being redelivered once it exhausts the
queue's retry limit (default 10), which has no AWS equivalent outside of a
redrive policy.
