# Dead-letter queues: implementation status

> **TL;DR — the DLQ exists only as stored configuration. No code path ever
> moves a message.** Messages that exhaust their retries park as `failed`
> in the source queue (see
> [message-lifecycle.md](message-lifecycle.md#retry-exhaustion-failed)).
> Don't rely on dead-letter routing without reviewing and finishing it.

## What is implemented

Plumbing all the way down, with nothing acting on it:

1. **Schema** — `queue_configurations.dead_letter_queue` has existed since
   migration `0001`: a nullable foreign key to `queues(id)` with
   `ON DELETE SET NULL`, so deleting the DLQ silently unsets the pointer
   rather than breaking the source queue.
2. **Admin API** — `GET`/`POST /api/admin/queue/{ns}/{queue}/config`
   round-trips it. The setter accepts the DLQ *by name*, validates it
   exists in the same namespace, and rejects unknown queues with a 404
   (`src/api/queue.rs`).
3. **SQS API** — the `RedrivePolicy` queue attribute (a JSON string with
   `deadLetterTargetArn` + `maxReceiveCount`, the AWS wire shape) is
   parsed, stored and returned by `SetQueueAttributes` /
   `GetQueueAttributes` (`src/service.rs`, `RedrivePolicy`). The "ARN"
   field is repurposed: NerveMQ expects `namespace:queue`, not a real ARN.
4. **UI** — the queue-settings dialog has a DLQ dropdown wired to the
   admin config endpoint.

## What is not implemented

**The mover.** Nothing reads `dead_letter_queue` or
`RedrivePolicy.maxReceiveCount` at delivery time. When a message exhausts
its retries, the claim queries simply stop matching it
(`tries >= max_retries` → computed status `failed`) and it stays in the
source queue until an admin deletes, purges or requeues it.

## How this differs from AWS

| Aspect | AWS SQS | NerveMQ today |
| --- | --- | --- |
| Exhaustion behavior | With a redrive policy, the message is **moved to the DLQ** after `maxReceiveCount` receives; without one, it redelivers forever | Parks as `failed` **in the source queue**; never moves, never redelivers |
| What controls the limit | `RedrivePolicy.maxReceiveCount` | `queue_configurations.max_retries` — a separate, parallel setting; the stored `maxReceiveCount` is ignored |
| Target identification | A real queue ARN | `namespace:queue` string in the ARN field |
| Config sources | One (the redrive policy) | Two that don't talk to each other: the SQS-side `RedrivePolicy` attribute and the admin-side `dead_letter_queue` column are stored in different places and neither is enforced |
| Recovery | DLQ redrive (move back) is a first-class API (`StartMessageMoveTask`) | Admin "requeue" resets `tries = 0` in place — convenient, but not SQS-shaped |

## Known rough edges

- **Asymmetric admin API shape**: the config endpoint accepts the DLQ *by
  name* on write but returns the raw *queue id* (`u64`) on read — and the
  frontend's zod schema (`lib/types.ts`) declares it a `string`, so the
  settings round-trip through the UI is shaky.
- **Silent divergence trap**: because `max_retries` is enforced but
  `maxReceiveCount` is not, an SQS client that sets a redrive policy gets
  neither an error nor the behavior — messages just park as `failed` after
  a limit the client never set (default 2).

## If/when finishing it

The natural seam is the exhaustion moment: instead of letting the claim
queries passively skip exhausted messages, a small step in the claim path
(or the existing 10-minute DB-maintenance task) could, for exhausted
messages whose queue has a DLQ configured:

```sql
UPDATE messages
SET queue = <dlq>, tries = 0, receipt_handle = NULL
WHERE tries >= max_retries AND <queue has a DLQ>
```

Open decisions for that work:

- which of the two config sources wins (`RedrivePolicy` vs
  `dead_letter_queue` — or unify them);
- claim-time vs background-sweep semantics (claim-time moves lazily, only
  when someone polls the source queue; the sweep moves eagerly);
- whether `DeadLetterQueueSourceArn` should then be reported as a message
  system attribute (currently absent, see the system-attributes table in
  [message-lifecycle.md](message-lifecycle.md));
- cycle prevention (AWS forbids a queue being its own DLQ; the current
  admin endpoint only checks existence).
