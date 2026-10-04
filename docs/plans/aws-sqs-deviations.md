# Plan: document NerveMQ's deviations from AWS SQS (docs PR)

## Status

Carried out in PR #124 (2026-10-04): the new
[`aws-compatibility.md`](../architecture/aws-compatibility.md) and the
Appendix B edits. Checking against AWS's documentation corrected a few rows,
so the committed document differs slightly from Appendix A:

- AWS recommends no more than 50 tags per queue; it isn't a hard limit.
- `OverLimit` applies to short polls only.
- Standard queues are effectively never throttled.
- `MessageDeduplicationId` applies only to FIFO queues; AWS doesn't refuse
  it on standard ones.

The bugs under [Follow-up](#follow-up-separate-pr-after-approval) are still
open.

## Context

The user asked for a review of the code and a write-up of where NerveMQ deviates from AWS SQS, as a PR.

Three read-only surveys compared the code (`origin/main` = `e321883`, after #123) with:
- AWS's official Smithy model (`models/sqs/service/2012-11-05/sqs-2012-11-05.json` in `aws/api-models-aws` on GitHub);
- AWS's documentation.

About 80 deviations turned up. About 25 are already documented, but scattered across six files, and some of those claims are stale.

This PR:
- adds one document, `docs/architecture/aws-compatibility.md` (full text below);
- fixes the stale claims;
- links the new document from the README and the Python examples.

It changes docs and comments only. The bugs the review found go in a separate PR afterwards (the user's choice); see "Follow-up".

## Steps

1. **Branch.** Run `git fetch` on its own, unsandboxed. Then `git switch main && git merge --ff-only origin/main`, then `git switch -c docs/aws-sqs-deviations`. Re-check the cited code against `main`, since it may have changed since `e321883`.
2. **Write the new document.** Create `docs/architecture/aws-compatibility.md` with the text in Appendix A.
3. **Fix existing docs.** Make the edits in Appendix B.
4. **Verify the document before committing:**
   - **AWS-side facts.** WebFetch the AWS quotas page and the TagQueue, SendMessage and ChangeMessageVisibility API pages. Confirm: tag limits (50 tags, key 128, value 256); attribute-name rules and the 10-attribute limit; the 12-hour total visibility cap. Adjust the text if any differ.
   - **NerveMQ-side facts.** Start the dev server (`PORT=8090 node .claude/skills/run-nervemq/driver.mjs up`) and probe these claims with boto3:
     - an empty `AttributeNames` returns everything;
     - `PurgeQueue` returns `{"Success": true}`;
     - deleting with a stale handle returns 404;
     - an empty receive returns `"Messages": []`;
     - an unknown attribute is stored and echoed;
     - a message isn't delivered a third time;
     - `ListQueues` ignores `MaxResults`.

     Stop the server with `driver.mjs down`.
   - **Links.** A script resolves every relative link and anchor in the changed Markdown files.
   - **Tests.** `cargo test --lib` (only test comments change).
5. **Commit and open the PR.**
   - Make a signed commit, `docs: where NerveMQ differs from AWS SQS`, ending with the Co-Authored-By line. Run it unsandboxed and retry once if Secretive fails.
   - Push and open the PR with `gh pr create --repo mshunshin/nervemq --base main`.
   - The PR body summarises the sections, lists the stale claims fixed, and notes the bug follow-up.

## Follow-up (separate PR, after approval)

Bugs found, not fixed here:
- Attribute names AWS doesn't know are stored verbatim. Internal keys (`visibility_timeout`, …) skip validation and can make `GetQueueAttributes` return 500 (`QueueAttributesSer.other`, `src/service.rs`).
- `ChangeMessageVisibilityBatch` requires `VisibilityTimeout` on each entry.
- The admin API accepts `max_retries` = 0, which stops a queue delivering.
- A long poll on a queue that is deleted and re-created mid-poll can return the new queue's messages.
- A missing required member gets `InvalidParameterValue`, not `MissingParameter`.

Security notes for the report (not for this document): the SigV4 signature comparison isn't constant-time, and the credential scope's region and service are unchecked.

---

## Appendix A: `docs/architecture/aws-compatibility.md`

```markdown
# AWS SQS compatibility: where NerveMQ differs

NerveMQ implements enough of the Amazon SQS API for the standard AWS SDKs to
work against it unchanged: point the SDK's endpoint at
`http://<host>/api/sqs` and use a NerveMQ API key as the access key. This
page lists where NerveMQ behaves differently from AWS SQS, so that code
written against one doesn't misbehave on the other.

Each difference is one of:

- **Deliberate**: NerveMQ chose different behaviour.
- **Not implemented**: AWS has it and NerveMQ doesn't.
- **Gap**: NerveMQ accepts the request but checks or reports less than AWS.

The AWS behaviour is taken from AWS's API model and the SQS API Reference
(see [Sources](#sources)).

## What matches AWS

- 17 operations (see [Operations](#operations)) over AWS's JSON protocol,
  signed with SigV4.
- AWS's error codes and HTTP statuses, in AWS's JSON error format, so SDKs
  raise their typed errors (`QueueDoesNotExist`, `ReceiptHandleIsInvalid`,
  `TooManyEntriesInBatchRequest`, …).
- AWS's batch rules: 1 to 10 entries, with distinct ids of 1 to 80 letters,
  digits, hyphens and underscores, and at most 1 MiB of messages in a
  `SendMessageBatch`.
- Message size: the body plus each attribute's name, type and value, at most
  1 MiB, or less if the queue's `MaximumMessageSize` is lower.
- `MD5OfMessageBody`, `MD5OfMessageAttributes` and
  `MD5OfMessageSystemAttributes`, computed as AWS computes them.
- UUID `MessageId`s, and `SentTimestamp` and
  `ApproximateFirstReceiveTimestamp` in milliseconds.
- Visibility timeouts:
  - 30 s by default;
  - the request's value wins over the queue's, which wins over the default;
  - `ChangeMessageVisibility` counts from the time of the call;
  - every receive gets a new receipt handle.
- Long polling: `WaitTimeSeconds` 0 to 20, the queue's
  `ReceiveMessageWaitTimeSeconds` when the request has none, and an early
  return once a message is available.
- AWS's ranges for queue attributes and request parameters, enforced by
  rejecting out-of-range values rather than clamping them.

## What will surprise an SQS user

1. **FIFO queues aren't implemented.** A `.fifo` queue behaves as a standard
   queue (see [FIFO queues](#fifo-queues)).
2. **A message stops being delivered after 2 receives** if nobody deletes
   it, and no dead-letter queue ever receives it (see
   [Retries and dead-letter queues](#retries-and-dead-letter-queues)).
3. **Messages are kept forever by default.** AWS deletes them after 4 days
   (see [Delay and retention](#delay-and-retention)).
4. **Delivery is strictly first-in-first-out, with no duplicates.** Code
   that relies on this breaks on AWS (see
   [Delivery and ordering](#delivery-and-ordering)).
5. **Deleting with a stale receipt handle is an error** (404), where AWS
   reports success (see
   [Visibility and acknowledgement](#visibility-and-acknowledgement)).
6. **`ListQueues` isn't paginated, and unknown queue attributes are
   stored** rather than rejected (see [Operations](#operations) and
   [Queue attributes](#queue-attributes)).

## Protocol and transport

| Behaviour | NerveMQ | AWS SQS | Kind |
| --- | --- | --- | --- |
| Protocol | AWS's JSON protocol only: `POST /api/sqs` with `X-Amz-Target: AmazonSQS.<Action>` and a JSON body | JSON, and also the older Query protocol (form-encoded `Action=…`, XML responses) | Not implemented (Query) |
| Success content type | `application/json` | `application/x-amz-json-1.0` | Gap. SDKs accept either |
| Request ids | No `x-amzn-RequestId` header | One on every response | Not implemented |
| A missing required member | `InvalidParameterValue` ("invalid request body: missing field …") | `MissingParameter` | Gap |
| Both `AttributeNames` and `MessageSystemAttributeNames` in one `ReceiveMessage` | Refused as a duplicate field | Both accepted | Gap |
| Request body size | Capped at 8 MiB (`InvalidParameterValue`), well above the largest legal request | Messages capped at 1 MiB | Deliberate |

Older SDK releases that use the Query protocol can't talk to NerveMQ;
current releases of every AWS SDK use JSON.

## Accounts and authentication

A namespace stands in for an AWS account (see
[namespaces.md](namespaces.md#mapping-to-aws-concepts)).

| Behaviour | NerveMQ | AWS SQS | Kind |
| --- | --- | --- | --- |
| Accounts | The queue URL's namespace must be the API key's own; there is no cross-namespace access | Account id in the URL; other accounts' queues reachable by policy | Deliberate |
| Permissions | API keys at member (send and receive), owner (also manage queues) or admin access. `Policy` is stored but not enforced | IAM and queue policies; `AddPermission`, `RemovePermission` | Not implemented |
| Signing | SigV4 in the `Authorization` header | SigV4 in the header or a presigned query string; temporary credentials (`X-Amz-Security-Token`) | Not implemented (presigning, session tokens) |
| Other schemes | Also `Authorization: NerveMqApiV1 nervemq_<key_id>_<secret>`, which sends the secret itself, so use it only over TLS | SigV4 only | Deliberate |
| Clock drift | 2 hours either way ([Clock drift](namespaces.md#clock-drift)) | 15 minutes | Deliberate |
| Credential scope | Region and service not checked | Must match the endpoint | Gap |
| `SenderId` | The sender's email | An IAM user or role id | Deliberate |

## Operations

Implemented: `CreateQueue`, `DeleteQueue`, `GetQueueUrl`,
`GetQueueAttributes`, `SetQueueAttributes`, `ListQueues`, `ListQueueTags`,
`TagQueue`, `UntagQueue`, `PurgeQueue`, `SendMessage`, `SendMessageBatch`,
`ReceiveMessage`, `DeleteMessage`, `DeleteMessageBatch`,
`ChangeMessageVisibility`, `ChangeMessageVisibilityBatch`
([`src/sqs/method.rs`](../../src/sqs/method.rs)).

Not implemented: `AddPermission`, `RemovePermission`,
`ListDeadLetterSourceQueues`, `StartMessageMoveTask`,
`CancelMessageMoveTask`, `ListMessageMoveTasks`. These answer
`InvalidAction` (400).

| Operation | NerveMQ | AWS SQS | Kind |
| --- | --- | --- | --- |
| `ListQueues` | `MaxResults` and `NextToken` are ignored: every queue in the namespace (filtered by `QueueNamePrefix`) in one response | Up to 1,000 per response, paged with `NextToken` | Not implemented |
| `GetQueueUrl` | `QueueOwnerAWSAccountId` is ignored; looks only in the key's namespace | Looks in the named account | Deliberate |
| `DeleteQueue` | Immediate: the next send fails, and the name can be re-created at once | Takes up to 60 s; re-creating the name within 60 s fails with `QueueDeletedRecently` | Deliberate |
| `PurgeQueue` | Immediate, and can be repeated at once; answers `{"Success": true}` | Takes up to 60 s; a second purge within 60 s fails with `PurgeQueueInProgress` (403); the response is empty | Deliberate |
| `ReceiveMessage` | Always returns `Messages`, `[]` when there are none. `ReceiveRequestAttemptId` is ignored. `MessageAttributeNames` takes exact names, `All` or `.*` | Omits `Messages` when there are none. `MessageAttributeNames` also takes prefixes such as `bar.*` | Gap |
| `ChangeMessageVisibilityBatch` | Every entry must have a `VisibilityTimeout` | Optional per entry | Gap |
| `TagQueue`, `CreateQueue` tags | No limits on number or length | At most 50 tags per queue; keys up to 128 characters, values up to 256 | Gap |

## Queue attributes

| Attribute | NerveMQ | Kind |
| --- | --- | --- |
| `DelaySeconds`, `MaximumMessageSize`, `MessageRetentionPeriod`, `ReceiveMessageWaitTimeSeconds`, `VisibilityTimeout` | Range-checked and acted on, with AWS's ranges (`MessageRetentionPeriod` also takes `0`, see [Delay and retention](#delay-and-retention)). Returned only once set | Gap (defaults not reported) |
| `RedrivePolicy` | Stored without checking; no message is ever moved ([Retries and dead-letter queues](#retries-and-dead-letter-queues)) | Not implemented |
| `Policy`, `RedriveAllowPolicy`, `KmsMasterKeyId`, `KmsDataKeyReusePeriodSeconds`, `SqsManagedSseEnabled` | Stored and returned; no effect | Not implemented |
| `FifoQueue`, `ContentBasedDeduplication`, `DeduplicationScope`, `FifoThroughputLimit` | Stored and returned; no effect ([FIFO queues](#fifo-queues)) | Not implemented |
| `QueueArn`, `CreatedTimestamp`, `LastModifiedTimestamp` | Never computed | Not implemented |
| `ApproximateNumberOfMessages`, `…NotVisible`, `…Delayed` | Counted exactly when asked. A message that has used up its receives counts in none of them | Deliberate |
| Any other name | Stored and returned | Gap. AWS refuses it with `InvalidAttributeName` |

Defaults compared:

| Attribute | AWS default | NerveMQ |
| --- | --- | --- |
| `VisibilityTimeout` | 30 s | 30 s |
| `DelaySeconds` | 0 | 0 |
| `MaximumMessageSize` | 1,048,576 bytes | 1,048,576 bytes |
| `ReceiveMessageWaitTimeSeconds` | 0 | 0 |
| `MessageRetentionPeriod` | 345,600 s (4 days) | None: messages are kept forever |

Other differences:

- **`GetQueueAttributes`:**
  - With an empty `AttributeNames` list it returns every attribute; AWS
    returns none.
  - Names it doesn't know are ignored; AWS refuses them.
  - It reports only attributes that have been set, so the defaults above
    never appear.
- **`SetQueueAttributes`:**
  - Changes take effect at once. AWS takes up to 60 seconds, and up to
    15 minutes for `MessageRetentionPeriod`.
  - It accepts values that aren't strings and returns them as given.
  - It accepts `MaxMessageSize` as another name for `MaximumMessageSize`.
- **`CreateQueue` on an existing name:**
  - Like AWS, it returns the existing queue when the request's attributes
    match, and `QueueNameExists` when they don't.
  - Only attributes in the request are compared, and one the queue never
    set compares as NerveMQ's default. So `MessageRetentionPeriod=345600`
    against a queue left at the default doesn't match, nor does
    `FifoQueue=false` against a queue that never set it. On AWS both match.

## Messages

| Check | NerveMQ | AWS SQS | Kind |
| --- | --- | --- | --- |
| Empty body | Accepted | At least one character | Gap |
| Body characters | Not checked | Only `#x9`, `#xA`, `#xD`, `#x20`–`#xD7FF`, `#xE000`–`#xFFFD` and `#x10000`–`#x10FFFF`; others fail with `InvalidMessageContents` | Gap |
| Attributes per message | No limit | At most 10 | Gap |
| Attribute names | Not checked | Up to 256 letters, digits, `_`, `-` and `.`; no `AWS.` or `Amazon.` prefix; no leading, trailing or doubled `.` | Gap |
| `Number` values | Not checked | Must be a valid number | Gap |
| Empty attribute values | Accepted | Refused | Gap |
| Custom data types (`Number.int`, `String.json`, `Binary.png`) | Refused (`InvalidParameterValue`) | Accepted | Not implemented |
| `AWSTraceHeader` | At most 4 KiB. When OpenTelemetry tracing is on, a send without one stores the request's trace context ([observability.md](observability.md#message-traces)) | Stored only as sent | Deliberate |

System attributes on receive:

| Attribute | NerveMQ |
| --- | --- |
| `SentTimestamp` | Milliseconds. Messages stored before migration 0015 have whole-second precision |
| `ApproximateReceiveCount` | As AWS, except that an admin requeue resets it to 0 |
| `ApproximateFirstReceiveTimestamp` | Milliseconds |
| `SenderId` | The sender's email |
| `AWSTraceHeader` | As stored |
| `SequenceNumber`, `MessageDeduplicationId`, `MessageGroupId` | Never returned ([FIFO queues](#fifo-queues)) |
| `DeadLetterQueueSourceArn` | Never returned |

## Delivery and ordering

NerveMQ promises more than an AWS standard queue. The full comparison is in
[message-lifecycle.md](message-lifecycle.md#contrast-with-aws-sqs-standard-queues).

| Behaviour | NerveMQ | AWS SQS standard queue | Kind |
| --- | --- | --- | --- |
| Order | Strictly first-in-first-out across the queue | Best effort | Deliberate |
| Duplicates | Never: a message is held by one consumer at a time | At-least-once: occasional duplicates, even at the same time | Deliberate |
| A message that becomes visible again | Keeps its place at the head of the queue | No defined position | Deliberate |
| Short polls (`WaitTimeSeconds` 0) | Return every available message, up to `MaxNumberOfMessages` | Sample some servers, so they can return fewer messages, or none, even when some are available | Deliberate |
| Long polls | Check for messages every 200 ms, so delivery can lag by up to 200 ms; answer at once when the server shuts down | Return as soon as a message arrives | Deliberate |
| Durability | SQLite in WAL mode with `synchronous=NORMAL`: a crash of the server process loses nothing, but a power cut or OS crash can lose the most recent sends and deletes | Stored redundantly before the send returns | Deliberate |

## Visibility and acknowledgement

AWS's rule is that "you must use the ReceiptHandle from the most recent time
you received the message". NerveMQ follows it: a handle still deletes after
its visibility timeout lapses, until the message is received again. The
rules are in
[message-lifecycle.md](message-lifecycle.md#receipt-handles-not-message-ids).

| Behaviour | NerveMQ | AWS SQS | Kind |
| --- | --- | --- | --- |
| Deleting with an old handle (the message was received again since), or retrying a delete that succeeded | `ReceiptHandleIsInvalid` (404) | Succeeds; the message may not be deleted | Deliberate ([why](message-lifecycle.md#divergence-stale-handle-deletes-are-errors-not-silent-no-ops)) |
| `ChangeMessageVisibility` on a message that isn't in flight | `ReceiptHandleIsInvalid` (404) | `MessageNotInflight` (400) | Gap |
| Total visibility | Each call may set up to 12 hours, with no limit on the total | At most 12 hours from the first receive; a longer value is refused | Gap |
| Resolution | Whole seconds, so a timeout can end up to a second early | Not documented | Deliberate |

Receipt handles have the form `<number>:<32 hex digits>`. Treat them as
opaque, as on AWS.

## Retries and dead-letter queues

The full status is in [dead-letter-queues.md](dead-letter-queues.md).

| Behaviour | NerveMQ | AWS SQS | Kind |
| --- | --- | --- | --- |
| Receive limit | Every queue has `max_retries`. The default is 2 (`NERVEMQ_DEFAULT_MAX_RETRIES`), set per queue through the admin API, and the first delivery counts. A message received that many times without being deleted is never delivered again | No limit unless a redrive policy sets `maxReceiveCount` | Deliberate |
| Exhausted messages | Stay in the queue as `failed`: invisible to SQS clients and in no `Approximate…` count, but shown, requeued or cleared in the admin UI | Moved to the dead-letter queue | Deliberate |
| `RedrivePolicy` | Stored without checking. It names the DLQ as `namespace:queue`, not an ARN; `maxReceiveCount` is ignored, and no message is ever moved | Checked, and messages move to the DLQ after `maxReceiveCount` receives | Not implemented |
| `DeadLetterQueueSourceArn`, `ListDeadLetterSourceQueues`, message-move tasks | Absent | Present | Not implemented |

## Delay and retention

| Behaviour | NerveMQ | AWS SQS | Kind |
| --- | --- | --- | --- |
| Delay | A message's own `DelaySeconds` wins over the queue's, in whole seconds | Same | Matches |
| Per-message delay on a `.fifo` queue | Accepted | Refused: FIFO queues take only a queue-wide delay | Gap |
| Default retention | None: messages are kept forever | 4 days | Deliberate |
| `MessageRetentionPeriod` = 0 | Accepted, meaning forever | Minimum 60 s | Deliberate |
| Expiry | A sweep every 10 minutes (and at start-up) deletes expired messages in every state, so an expired message can still be received for up to 10 minutes | Deleted when the period ends | Deliberate |

Messages stored before migration 0006 have no arrival time and never
expire.

## FIFO queues

FIFO queues are not implemented. NerveMQ accepts the FIFO request fields
and ignores them:

- `MessageGroupId`: no ordering within a group, and no holding back the
  rest of a group while one of its messages is in flight.
- `MessageDeduplicationId` and `ContentBasedDeduplication`: no 5-minute
  deduplication window.
- `ReceiveRequestAttemptId`: no retry deduplication on receive.
- No `SequenceNumber` in send results, and no FIFO system attributes on
  receive.

Also:

- A `.fifo` name doesn't need `FifoQueue=true`, and `FifoQueue=true` doesn't
  need a `.fifo` name; AWS requires both together. `FifoQueue` can also be
  changed after creation, which AWS doesn't allow.
- On standard queues, AWS uses `MessageGroupId` for fair queuing between
  groups, and refuses `MessageDeduplicationId`. NerveMQ ignores both.

Every NerveMQ queue already delivers in strict first-in-first-out order,
which is the nearest it comes to FIFO behaviour.

## Quotas

| Quota | NerveMQ | AWS SQS |
| --- | --- | --- |
| Messages in flight | No limit | About 120,000 per standard queue (`OverLimit` beyond it) |
| Request rate | Not throttled | Throttled at high rates (`RequestThrottled`) |
| Queues | No limit | No practical limit; `ListQueues` returns 1,000 per page |
| Tags per queue | No limit | 50 |

## Errors

AWS errors NerveMQ never returns: `MessageNotInflight`,
`PurgeQueueInProgress`, `QueueDeletedRecently`, `OverLimit`,
`InvalidAttributeName`, `InvalidMessageContents`, `RequestThrottled`,
`InvalidAddress`, `UnsupportedOperation` and the `KMS.*` errors.

Codes only NerveMQ returns:

- `ResourceNotFoundException` (404) when the API key's namespace no longer
  exists.
- `InvalidAction` (400) for an operation it doesn't implement.

Where both return an error for the same case, the codes and statuses match
AWS's, apart from the cases above
([`src/sqs/error.rs`](../../src/sqs/error.rs)).

## NerveMQ-only behaviour SQS clients can see

- **Paused queues.** Receives return nothing (a long poll waits its full
  time) while `ApproximateNumberOfMessages` still counts the queue's
  messages ([Pausing a queue](message-lifecycle.md#pausing-a-queue)).
- **Admin requeue** makes a message deliverable again, even while a
  consumer holds it, so two consumers can then hold it. It resets
  `ApproximateReceiveCount` to 0, and the old receipt handle still deletes
  ([sharp edge](message-lifecycle.md#sharp-edge-requeue-does-not-invalidate-the-outstanding-receipt-handle)).
- **Admin "mark failed"** stops a message being delivered over SQS.
- **Admin deletes** by `MessageId`, and clearing a queue's failed messages,
  remove messages a consumer may be holding.
- **Messages sent from the admin UI** have the signed-in user's email as
  `SenderId`.

## Sources

- [Amazon SQS API Reference](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/Welcome.html),
  including [Common Errors](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/CommonErrors.html).
- [Amazon SQS quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-quotas.html).
- [AWS's SQS API model](https://github.com/aws/api-models-aws/tree/main/models/sqs),
  the source of the operation, error and parameter details above.
```

## Appendix B: edits to existing files

1. **`README.md`**
   - **Features bullet (line 20).** Keep "Drop-in replacement for applications using AWS SQS" and add `(see [where it differs](docs/architecture/aws-compatibility.md))`.
   - **SQS section, ~418.** Change "Requests must be signed with SigV4 using an API key" to "Requests are signed with SigV4 using an API key (or carry it in the `NerveMqApiV1` scheme)". Add a sentence after "Easiest consumed via any standard AWS SQS SDK": `Where NerveMQ behaves differently from AWS is listed in [docs/architecture/aws-compatibility.md](docs/architecture/aws-compatibility.md).`
   - **`DeleteMessage` note, ~459.** Replace "**NerveMQ-specific:** the AWS specification leaves unspecified whether a message whose visibility timeout has lapsed can still be deleted by its original consumer. NerveMQ guarantees that it can…" with "As on AWS, where the most recent receipt handle deletes the message, a message whose visibility timeout has lapsed can still be deleted by its original consumer…". Keep the rest. Add one sentence saying a *stale* handle is an error here and succeeds on AWS, with a link.
   - **Retention note, ~480–486.** Drop "and the AWS 60 s–14 day bounds are not enforced". Say instead that values must be 60–1,209,600 s as on AWS, or `0` for forever.
   - **DLQ note, ~494.** Fix "onlly" → "only".
   - **Changes in this fork, ~517.** Change "the NerveMQ-specific guarantee that a lapsed handle still acknowledges until redelivery" to "the guarantee, as on AWS, that a lapsed handle still acknowledges until redelivery".
   - **Docs list, ~601–604.** Add "AWS SQS compatibility" to the list.
2. **`docs/architecture/message-lifecycle.md`**
   - **~167–168.** Replace "no 60 s–14 day bounds validation" with: "`0` aside, values must be 60 s–14 days, as on AWS".
   - **~228.** Change "capped at 900 s" to "0–900 s; larger values are refused".
   - **Contrast section (~280).** Add a link at the end to `aws-compatibility.md` for every other difference.
3. **`examples/python/README.md`**, "Known divergences". Keep the size and batch paragraph. Replace the table and the "Also note" paragraph with one line: "The tests assert NerveMQ's behaviour where it differs from AWS; every difference is listed in [docs/architecture/aws-compatibility.md](../../docs/architecture/aws-compatibility.md)."
4. **`examples/python/test_sqs.py`**
   - **Docstring (~27).** Change "see the table in README.md" to "see docs/architecture/aws-compatibility.md".
   - **Comment (~728).** Change "NerveMQ-specific guarantee (AWS leaves this unspecified)" to "As on AWS, where the most recent handle deletes".
5. **`src/sqs/sdk_tests.rs` (~2761).** Make the same doc-comment change as in `test_sqs.py`.
