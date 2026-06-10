# Message lifecycle and state transitions

A message's lifecycle is **derived state**, not a stored column. The
`messages` table stores three timestamps/counters and the lifecycle is
computed from them on every read:

| Column | Meaning |
| --- | --- |
| `invisible_until` | Message is hidden from receives while this is in the future; `NULL` or past means available |
| `tries` | Delivery attempts so far (bumped on every receive) |
| `delivered_at` | When the message was last received (informational; availability is governed only by `invisible_until`) |
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
with `tries < max_retries` (queue configuration, default 10). A message
received `max_retries` times without being deleted is never claimable again
and reports `failed`.

AWS has no equivalent outside of a redrive policy — a standard queue
redelivers forever, and with a redrive policy the message is *moved to the
DLQ* after `maxReceiveCount` receives. NerveMQ's failed messages instead
**stay in the source queue** until an admin deletes, purges, or requeues
them:

- **Dead-letter routing is not implemented.** `queue_configurations.
  dead_letter_queue` and the `RedrivePolicy` attribute are stored and
  round-trip through the API/UI, but no code path ever moves a message.
- **`MessageRetentionPeriod` is not enforced.** The attribute is stored and
  round-trips, but nothing ever expires messages by age.

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

## Known validation gaps on ReceiveMessage

`ChangeMessageVisibility` validates its timeout (0–43,200), but
`ReceiveMessage` does not validate the equivalent inputs that AWS rejects:

- `VisibilityTimeout` override above 43,200 s is accepted as-is (AWS:
  `InvalidParameterValue`). Pinned by
  `receive_accepts_visibility_override_beyond_aws_maximum`.
- `MaxNumberOfMessages` above 10 is honored rather than rejected (AWS:
  error; at most 10 messages per receive).
- `WaitTimeSeconds` above 20 is silently clamped to 20 rather than rejected.

## Concurrency note

`delete_message` and `change_message_visibility` are deliberately single
atomic statements — a read-then-write transaction would fail concurrent
acknowledgers with `SQLITE_BUSY_SNAPSHOT` instead of letting them lose the
race cleanly (see the comments at their definitions).
`delete_message_batch` does *not* follow this pattern: it opens a
transaction with several `SELECT`s (namespace/access/queue checks) before
its `DELETE`s, so under heavy concurrent ack load a batch delete can hit
`SQLITE_BUSY_SNAPSHOT` and fail with a 500 where the single delete would
have returned a clean per-entry error.

## Test coverage map

| Behaviour | Test |
| --- | --- |
| In-flight message is invisible | `visibility_tests::received_message_is_invisible_until_timeout` (Rust), `test_received_message_becomes_invisible` (Python) |
| Lapsed window → redelivered with fresh handle | `visibility_tests::message_becomes_available_again_after_timeout`, `test_visibility_timeout_override_redelivers` |
| Stale handle cannot delete after redelivery | `visibility_tests::delete_requires_current_receipt_handle`, `test_stale_receipt_handle_is_rejected_after_redelivery` |
| Expired-but-not-redelivered handle still deletes | `visibility_tests::delete_succeeds_with_expired_handle_before_redelivery` |
| ChangeMessageVisibility requires in-flight | `visibility_tests::change_visibility_requires_in_flight_message`, `test_change_message_visibility_rejects_unknown_handle` |
| ChangeMessageVisibility(0) releases; redelivery invalidates the old handle | `visibility_tests::change_visibility_zero_releases_and_redelivery_invalidates_handle`, `test_change_message_visibility_releases_message` |
| Retry exhaustion stops delivery; admin requeue revives | `visibility_tests::exhausted_message_reports_failed_and_admin_requeue_revives_it`, `test_message_stops_redelivering_after_max_retries` |
| Admin status forcing endpoints | `endpoint_tests::queue_panel_message_management_roundtrip` |
| Requeue keeps old handle usable (sharp edge) | `visibility_tests::admin_requeue_leaves_prior_receipt_handle_deletable` |
| Delayed-message stats inconsistency | `visibility_tests::delayed_message_is_listed_pending_but_counted_in_no_stats_bucket` |
| Receive accepts oversized visibility override (divergence) | `visibility_tests::receive_accepts_visibility_override_beyond_aws_maximum` |
