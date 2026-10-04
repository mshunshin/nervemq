# Message lifecycle and state transitions

A message's lifecycle is **derived state**, not a stored column. The
`messages` table stores a handful of timestamps/counters and the lifecycle
is computed from them on every read:

| Column | Meaning |
| --- | --- |
| `message_id` | The `MessageId` clients see: a random v4 UUID, as AWS issues (migration 0016). See [Message IDs](#message-ids) |
| `invisible_until` | Message is hidden from receives while this is in the future; `NULL` or past means available |
| `tries` | Delivery attempts so far (bumped on every receive) |
| `received_at` | When the queue received (stored) the message, in whole seconds. Retention is measured from it (informational otherwise) |
| `sent_at_ms` | The same moment in milliseconds (migration 0015): AWS `SentTimestamp`, and what telemetry measures message times from. `NULL` for messages stored before the migration, which fall back to `received_at` × 1000 |
| `delivered_at` | When the message was last received (informational; availability is governed only by `invisible_until`) |
| `first_delivered_at` | When the message was *first* received, in whole seconds. Stamped once, never overwritten |
| `first_delivered_at_ms` | `first_delivered_at` in milliseconds (migration 0015): AWS `ApproximateFirstReceiveTimestamp`. `NULL` before the first delivery, or if that was before the migration (then `first_delivered_at` × 1000) |
| `sent_by` | User id of the authenticated sender — surfaced as AWS `SenderId` |
| `receipt_handle` | Handle minted on the most recent receive: `<id>:<128-bit random hex>`. Replaced by every redelivery |

The displayed status ([`src/message.rs`](../../src/message.rs)) is computed as:

```sql
CASE
  WHEN delivered_at IS NOT NULL AND invisible_until > now THEN 'delivered'   -- in flight
  WHEN tries >= max_retries                               THEN 'failed'      -- exhausted
  ELSE                                                         'pending'     -- available
END
```

So `delivered` means **currently in flight** (received, visibility window
still open), not "successfully consumed". The only durable record of
successful consumption is the message being *gone* — `DeleteMessage` removes
the row. This is standard queue semantics, but worth internalizing because
the admin UI surfaces these statuses.

## The state machine

```text
                       SendMessage
                            │
              DelaySeconds? │
            ┌───────────────┴───────────────┐
            │ delay > 0:                    │ no delay:
            │ invisible_until = now+delay   │ invisible_until = NULL
            └───────────────┬───────────────┘
                            ▼
                       ┌─────────┐
        ┌─────────────▶│ pending │ (available: window elapsed, tries < max)
        │              └────┬────┘
        │                   │ ReceiveMessage claims it atomically:
        │                   │   tries += 1
        │                   │   delivered_at = now
        │                   │   invisible_until = now + visibility timeout
        │                   │   receipt_handle = fresh random handle
        │                   ▼
        │             ┌───────────┐    DeleteMessage(receipt_handle)   ┌─────────┐
        │             │ delivered │───────────────────────────────────▶│ deleted │
        │             │(in flight)│    ChangeMessageVisibility(h, 0)   │ (gone)  │
        │             └─────┬─────┘──────────────┐                     └─────────┘
        │                   │                    │
        │ window lapses     │ window lapses      │ released immediately
        │ & tries < max     │ & tries >= max     │
        └───────────────────┤                    │
                            ▼                    │
                       ┌────────┐                │
                       │ failed │◀───────────────┘ (if tries >= max)
                       └────────┘
                  (never claimable again;
                   admin "requeue" resets it)
```

The claim is a single atomic `UPDATE ... WHERE id IN (SELECT ... LIMIT n)`
statement ([`src/service.rs`](../../src/service.rs), `sqs_recv_batch`), so
two concurrent consumers can never receive the same in-flight message.

### Effective visibility timeout

On receive, the window is stamped from the first available of:

1. the request's `VisibilityTimeout` override,
2. the queue's `VisibilityTimeout` attribute,
3. the server default (`VISIBILITY_TIMEOUT`, 30 s).

`ChangeMessageVisibility` re-stamps the deadline **from now** (not from the
original receive), capped at AWS's 43,200 s (12 h). Setting it to `0`
releases the message immediately.

## Message IDs

The `MessageId` that `SendMessage` returns, and that receives and the
admin API report, is a random v4 UUID set when the message is stored. It
is never reused, so a consumer can safely record the ids it has processed
and skip a message it has seen before.

The integer row id `id` stays internal: it gives send order (receives
claim in it, and the admin list sorts by it) and is the key `kv_pairs`
hang off. It is never handed out because SQLite reuses it: without
`AUTOINCREMENT`, a new row takes the highest id plus one, so deleting the
newest message hands its id to the next send, and an empty table starts
again at 1. Messages stored before migration 0016 were given a UUID by it.

## Receipt handles, not message IDs

Acknowledgement is keyed on the **receipt handle**, never the message ID:

- `DeleteMessage`, `DeleteMessageBatch` and `ChangeMessageVisibility` all
  match `WHERE queue = ? AND receipt_handle = ?`. The message ID in the
  request plays no role (it isn't even sent for these operations).
- Every receive **replaces** the handle, so a handle identifies *one
  specific delivery* of a message, not the message itself.
- Handles are scoped to the queue and contain 128 bits of randomness —
  unguessable, and a handle from one queue can never act on another.

The resulting acknowledgement rules:

| Situation | `DeleteMessage` | `ChangeMessageVisibility` |
| --- | --- | --- |
| In flight, current handle | ✅ deletes | ✅ re-stamps window |
| Window lapsed, **not yet redelivered** (handle still the latest) | ✅ deletes — matches AWS: the handle outlives the timeout until the next receive | ❌ 404 — matches AWS `MessageNotInflight` |
| Window lapsed, **redelivered to another consumer** (handle replaced) | ❌ 404 | ❌ 404 |
| Handle never issued / message gone | ❌ 404 | ❌ 404 |

So the answer to "a consumer missed its visibility timeout — can it still
delete the message?" is: **yes, until someone else receives it; afterwards
its handle is dead and the message belongs to the new delivery.**

### Divergence: stale-handle deletes are errors, not silent no-ops

AWS **standard** queues return `200 OK` for a `DeleteMessage` with a stale
receipt handle — the call "succeeds" but deletes nothing (only FIFO queues
report an error). NerveMQ always reports a stale or unknown handle as an
error (404, surfaced as `ReceiptHandleIsInvalid` per entry in
`DeleteMessageBatch`). This is stricter than AWS and arguably more useful —
a consumer learns it lost the race — but SDK code written for AWS standard
queues may not expect `delete_message` to raise.

## Retry exhaustion (`failed`)

Every receive increments `tries`; the claim query only considers messages
with `tries < max_retries` (queue configuration, copied from the server
default `NERVEMQ_DEFAULT_MAX_RETRIES` at queue creation — **default 2** —
and adjustable per queue). A message received `max_retries` times without
being deleted is never claimable again and reports `failed`.

Note that despite the name, `max_retries` caps **total delivery attempts,
including the initial delivery** — the default of 2 means one initial
delivery plus one redelivery. This matches the semantics of AWS's redrive
`maxReceiveCount` (which also counts total receives), only the name
differs.

AWS has no equivalent outside of a redrive policy — a standard queue
redelivers forever, and with a redrive policy the message is *moved to the
DLQ* after `maxReceiveCount` receives. NerveMQ's failed messages instead
**stay in the source queue** until an admin deletes, purges, or requeues
them:

- **Dead-letter routing is not implemented.** `queue_configurations.
  dead_letter_queue` and the `RedrivePolicy` attribute are stored and
  round-trip through the API/UI, but no code path ever moves a message —
  see [dead-letter-queues.md](dead-letter-queues.md) for the full
  implementation status and the differences from AWS.
- **`MessageRetentionPeriod` is enforced by a background sweep.** The
  maintenance task (every 10 minutes) deletes messages older than their
  queue's configured period, measured against `received_at` and regardless
  of lifecycle state — in-flight and `failed` messages expire too, as on
  AWS. A queue with no attribute, or with the explicit value `0`, retains
  messages forever (`0` is a safe sentinel: AWS's minimum is 60 s). Unlike
  AWS there is no default period and no 60 s–14 day bounds validation, and
  expiry can lag the configured period by up to one sweep interval.

## Admin (management-plane) transitions

The admin API can force lifecycle state by **message ID** — it is the
management plane and deliberately does not hold receipt handles
([`src/api/queue.rs`](../../src/api/queue.rs)):

- **Requeue (`status = pending`)**: `invisible_until = NULL, tries = 0` —
  makes the message immediately deliverable, whether it was in flight or
  exhausted.
- **Mark failed (`status = failed`)**: saturates `tries` to `max_retries` —
  no further deliveries.
- **`status = delivered` is rejected** (400): `delivered` only ever results
  from a real receive minting a receipt handle.
- **Delete by ID**: removes the message regardless of in-flight state.

### Sharp edge: requeue does not invalidate the outstanding receipt handle

Forcing a message back to `pending` clears its visibility window and retry
counter but leaves `receipt_handle` untouched. The consumer holding the
pre-requeue handle can therefore still `DeleteMessage` (or
`ChangeMessageVisibility` is blocked only by the not-in-flight guard) until
the next receive replaces the handle. In practice this means an admin
"requeue" does not fence off the old consumer the way a redelivery does.
Pinned by `admin_requeue_leaves_prior_receipt_handle_deletable` in
[`src/service.rs`](../../src/service.rs); a fix would add
`receipt_handle = NULL` to the requeue (and arguably the mark-failed)
`UPDATE`.

## Pausing a queue

An owner or admin can pause a queue from the admin UI or API
(`POST /api/admin/queue/{ns}/{queue}/pause`, and `/resume` to undo it), to
drain its consumers and swap them. `queues.paused_at` records when; it is
`NULL` while the queue runs. While paused:

- **Receives return no messages.** The claim query in `sqs_recv_batch`
  requires `q.paused_at IS NULL`, so the check is part of the atomic claim:
  once the pause has committed, no receive can hand out a message. A long
  poll waits as it would on an empty queue, and picks a message up within
  one poll interval of the queue resuming.
- **Everything else works.** Sends are accepted; `DeleteMessage` and
  `ChangeMessageVisibility` work on messages already in flight. A message
  released (visibility 0) or whose visibility lapses goes back to `pending`
  and waits.
- **Draining is visible.** The queue's `delivered` count (in flight) falls
  as consumers finish. It reaches zero once every consumer has deleted or
  released what it holds, or at the latest once the visibility timeout of
  the last message received has lapsed. The UI shows the count while the
  queue is paused.

Nothing else changes: retention still expires messages, and statistics,
listing and the admin message actions behave as for a running queue.
Pausing is not part of the SQS API, so SQS clients see a paused queue as an
empty one.

## Delayed messages

`DelaySeconds` (request field, capped at 900 s, or the queue's
`DelaySeconds` attribute) stamps `invisible_until` at **send** time without
counting a delivery attempt. The delay reuses the visibility mechanism, so a
delayed message is simply "in the future" until the delay lapses.

### Inconsistency: delayed messages fall through the statistics buckets

The two derived-status code paths disagree about a delayed (or any
never-delivered but currently invisible) message:

- `list_messages` reports it as `pending` (its `CASE` has no
  window-elapsed condition on the `pending` arm), so the UI lists it as
  pending;
- `queue_statistics` counts `pending` as *window elapsed and tries < max*,
  `delivered` as *delivered_at set and window open*, `failed` as *window
  elapsed and tries >= max* — a delayed message satisfies none of these, so
  it is included in `message_count` but in **no** status bucket, and the
  three buckets do not sum to the total.

Pinned by `delayed_message_is_listed_pending_but_counted_in_no_stats_bucket`
in [`src/service.rs`](../../src/service.rs).

## Delivery order

There is exactly one ordering rule: the claim query selects available
messages `ORDER BY m.id ASC` ([`src/service.rs`](../../src/service.rs),
`sqs_recv_batch`). `id` is the auto-incrementing rowid assigned at send
time, and sends are serialized by SQLite's single-writer lock, so the base
order is **strict FIFO by send order** — oldest available message first.

Because a message never changes its `id`, it never moves to the back of the
queue. Coming back from *any* form of requeue means re-entering at the
original position:

| How a message becomes available again | Position on next delivery |
| --- | --- |
| Visibility window lapsed (consumer never acked) | Original — ahead of everything sent after it |
| `ChangeMessageVisibility(handle, 0)` | Original, immediately |
| Admin requeue (`status = pending`, `tries = 0`) | Original |
| `DelaySeconds` elapsed | The position its send-time `id` gave it |

Two consequences:

1. **Head-of-line behavior**: a poison message that keeps timing out is
   redelivered *first* every time it resurfaces, ahead of all newer
   messages, until its retries exhaust and it parks as `failed` (dropping
   out of the claim query). An admin requeue resets `tries`, granting it a
   fresh set of attempts at the front again.
2. **Batch receives** claim the *n* lowest-id available messages
   atomically, so ordering holds across batches and concurrent consumers
   receive disjoint runs of the head of the queue.

### Contrast with AWS SQS standard queues

NerveMQ's ordering is *stronger* than what SQS standard queues promise, and
code written against it should not assume AWS will behave the same way:

| Behaviour | NerveMQ | AWS SQS standard queue |
| --- | --- | --- |
| Base ordering | Strict FIFO by send order | **Best-effort only** — messages are stored across distributed servers and can arrive out of order; no ordering guarantee at all |
| Delivery guarantee | Exactly-once *per visibility window*: the claim is one atomic `UPDATE`, so two consumers can never hold the same message concurrently | **At-least-once**: a message can occasionally be delivered more than once, even concurrently, because a copy on an unreachable server can resurface |
| Position after a visibility lapse | Returns to its original (front-most) position — head-of-line behavior | Undefined — the message simply becomes available again somewhere in the (unordered) pool; no head-of-line effect |
| Redelivery limit | Stops after `max_retries` receives, parks as `failed` in the source queue | Redelivers forever; with a redrive policy, moves to the DLQ after `maxReceiveCount` receives |
| Retention | No default — messages live forever unless `MessageRetentionPeriod` is set (`0` also means forever); expired messages are deleted by a 10-minute background sweep | Always enforced: default 4 days, configurable 60 s–14 days; messages are deleted after the period no matter what |
| Duplicates | Never duplicated by the server | Consumers must be idempotent; duplicates are expected behavior |

The nearest AWS analogue to NerveMQ's ordering is a **FIFO queue**, but the
match is loose there too: AWS FIFO queues order *per message group*
(`MessageGroupId`, which NerveMQ accepts on the wire but ignores), enforce
exactly-once via deduplication IDs (also ignored), and block a message
group while one of its messages is in flight. NerveMQ orders the whole
queue globally and lets delivery continue past in-flight messages.

Practical upshot: a consumer written for NerveMQ that silently relies on
FIFO order or on never seeing duplicates will misbehave when pointed at
real SQS standard queues. The portable assumptions are the ones both make:
ack with the latest receipt handle, and treat order and delivery count as
queue-implementation details.

## System attributes on receive

`ReceiveMessage` returns AWS message *system* attributes in the `Attributes`
map, selected by the request's `MessageSystemAttributeNames` (or the
deprecated `AttributeNames` spelling — both are accepted; `All` and the
legacy `.*` select everything). Timestamps are epoch **milliseconds** and
every value travels as a string, AWS-style:

| Attribute | Source |
| --- | --- |
| `SentTimestamp` | `sent_at_ms`, else `received_at` × 1000 |
| `ApproximateReceiveCount` | `tries` |
| `ApproximateFirstReceiveTimestamp` | `first_delivered_at_ms`, else `first_delivered_at` × 1000 (sticky across redeliveries) |
| `SenderId` | The sending principal's **email** (API-key owner for SQS sends, session user for admin-panel sends). AWS returns the opaque IAM principal id here |
| `AWSTraceHeader` | `aws_trace_header`: the message's trace context in X-Ray format (below) |

When nothing is requested, the `Attributes` map is omitted from the
response entirely, matching AWS. Not supported: the FIFO trio
(`MessageDeduplicationId` / `MessageGroupId` / `SequenceNumber` — accepted
on send, ignored), and `DeadLetterQueueSourceArn` (no DLQ redrive).

### `AWSTraceHeader`

The one system attribute a sender sets, as on AWS. Tracing
instrumentations use it to carry a message's trace context to its
consumers; the Java SDK's does, for each message of a batch. A send stores,
in order of preference:

1. the message's `MessageSystemAttributes.AWSTraceHeader`;
2. the request's `X-Amzn-Trace-Id` header (for every message of a batch),
   as AWS does;
3. with OpenTelemetry traces exported, the context the message was created
   in: its `traceparent` attribute, else the send request's own span. AWS
   stores nothing there. See
   [observability.md](observability.md#message-traces);
4. nothing.

It's returned only when it's asked for, by name or with `All`. Other
system attribute names, a type other than `String`, an empty value and a
value over 4 KiB (`MAX_AWS_TRACE_HEADER_BYTES`) are refused with
`InvalidParameterValue`.

As on AWS, the header doesn't count towards the message size. The 4 KiB
cap is NerveMQ's own, so the header can't carry what the size limit
refuses. An `X-Amzn-Trace-Id` header that isn't usable is ignored rather
than failing the send: the client didn't ask for it to be stored.

## Attribute digests

`MD5OfMessageAttributes` is AWS's digest: each attribute's name, data
type, a transport byte and its value, length-prefixed, in order of
**name**. `MD5OfMessageSystemAttributes` is the same digest over the
system attributes a send set. Each field is left out when there is
nothing to digest, as AWS does:
- on send, when the message has no attributes;
- on receive, when none of its attributes were asked for.

SDKs that check the digest (the Java SDK does) reject a reply that
doesn't match. Until October 2026 the send digest was computed in a hash
map's (random) order. A message with two or more attributes, which is what
a tracing instrumentation's `traceparent` makes, therefore got a digest
those SDKs could reject. The digests are pinned by
`types::attribute_digest_tests`, against values from moto, a widely used
AWS mock.

Custom data types (`Number.int`, `String.json`, …) aren't accepted yet: a
send with one is refused as an unparseable body.

## ReceiveMessage input validation

`ReceiveMessage` checks its inputs against AWS's ranges and rejects an
out-of-range value with `InvalidParameterValue` (HTTP 400):

| Parameter | Range | Pinned by |
| --- | --- | --- |
| `VisibilityTimeout` | 0–43,200 s, as `ChangeMessageVisibility` | `receive_rejects_visibility_override_beyond_aws_maximum` |
| `MaxNumberOfMessages` | 1–10 | `receive_rejects_out_of_range_max_number_of_messages` |
| `WaitTimeSeconds` | 0–20 s | `receive_rejects_wait_time_beyond_aws_maximum` |

## Concurrency notes

Two SQLite-specific rules shape every code path that touches messages:

1. **A write transaction must start with a write.** A deferred transaction
   whose first statement is a read takes a snapshot that fails to upgrade
   (`SQLITE_BUSY_SNAPSHOT`) if any other writer commits before its first
   write — it errors immediately rather than waiting out the busy timeout.
   `delete_message` and `change_message_visibility` are single atomic
   statements for this reason; `sqs_send` folds its size check into the
   INSERT's WHERE clause; and the batch paths (`sqs_send_batch`,
   `delete_message_batch`) resolve their namespace/access/queue checks on
   the pool *before* opening the write transaction. (The batch paths
   originally read inside the transaction — under a dashboard polling
   alongside a bulk send, whole batches failed with 500s; the poll writes a
   session row per request, see [sessions.md](sessions.md); pinned by
   `concurrency_tests`.) The admin write paths (`create_namespace`,
   `delete_namespace`, `create_queue`, `set_queue_attributes`,
   `delete_queue`, `create_token_with`) follow the same rule: they used to
   read inside the transaction, and an API key created on a freshly
   started server failed with "database is locked"; pinned by
   `admin_writes_survive_interleaved_writes`.
2. **Never acquire a second pool connection while holding one.** Concurrent
   callers that each hold a connection and wait for another deadlock the
   pool until `PoolTimedOut` fails them all. `sqs_recv_batch` runs its
   per-message attribute lookups on the claim transaction, and
   `list_messages` fetches everything over its single connection (it
   previously spawned a task-per-message, each acquiring its own
   connection; ten concurrent listers deadlocked the default ten-slot
   pool).

Both are exercised by `service::concurrency_tests`, which interleave batch
sends/deletes with single sends, statistics reads and message listings on
one executor.

## Test coverage map

| Behaviour | Test |
| --- | --- |
| In-flight message is invisible | `visibility_tests::received_message_is_invisible_until_timeout` (Rust), `test_received_message_becomes_invisible` (Python) |
| Lapsed window → redelivered with fresh handle | `visibility_tests::message_becomes_available_again_after_timeout`, `test_visibility_timeout_override_redelivers` |
| Retention sweep deletes by age; 0/unset = forever; trumps visibility | `visibility_tests::retention_sweep_deletes_messages_past_their_period`, `retention_zero_or_unset_keeps_messages_forever`, `retention_trumps_visibility_and_exhaustion` |
| Stale handle cannot delete after redelivery | `visibility_tests::delete_requires_current_receipt_handle`, `test_stale_receipt_handle_is_rejected_after_redelivery` |
| Expired-but-not-redelivered handle still deletes | `visibility_tests::delete_succeeds_with_expired_handle_before_redelivery` |
| ChangeMessageVisibility requires in-flight | `visibility_tests::change_visibility_requires_in_flight_message`, `test_change_message_visibility_rejects_unknown_handle` |
| ChangeMessageVisibility(0) releases; redelivery invalidates the old handle | `visibility_tests::change_visibility_zero_releases_and_redelivery_invalidates_handle`, `test_change_message_visibility_releases_message` |
| Retry exhaustion stops delivery; admin requeue revives | `visibility_tests::exhausted_message_reports_failed_and_admin_requeue_revives_it`, `test_message_stops_redelivering_after_max_retries` |
| Admin status forcing endpoints | `endpoint_tests::queue_panel_message_management_roundtrip` |
| Paused queue accepts sends and acks, delivers nothing until resumed | `sqs::endpoint_tests::paused_queue_accepts_and_acknowledges_but_delivers_nothing` |
| Long poll on a paused queue delivers once it resumes | `sqs::endpoint_tests::long_poll_on_a_paused_queue_delivers_once_it_resumes` |
| Long poll answers at once when the server stops (SIGTERM) | `sqs::endpoint_tests::a_long_poll_answers_at_once_when_the_server_stops`, `sigterm_ends_long_polls_and_stops_promptly` (smoke) |
| Pause/resume endpoints, reporting and access | `api::endpoint_tests::pausing_a_queue_is_reported_until_it_is_resumed`, `owners_pause_and_resume_their_queues`, `members_send_messages_but_cannot_manage_queues_in_the_ui` |
| Requeue keeps old handle usable (sharp edge) | `visibility_tests::admin_requeue_leaves_prior_receipt_handle_deletable` |
| Delayed-message stats inconsistency | `visibility_tests::delayed_message_is_listed_pending_but_counted_in_no_stats_bucket` |
| Receive rejects oversized visibility override (0–43200) | `visibility_tests::receive_rejects_visibility_override_beyond_aws_maximum`, `sdk_tests::sdk_receive_message_rejects_oversized_visibility_timeout` |
