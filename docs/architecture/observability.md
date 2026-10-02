# Observability

What the server records about the requests it handles: one span per
request, and the log events made inside it. Both go to stdout, filtered by
`NERVEMQ_LOG`, pretty in debug builds and JSON in release builds.

The span follows OpenTelemetry's semantic conventions, so it can be
exported as a server span.

## The request span

`TracingLogger::<RootSpan>` ([`src/telemetry/root_span.rs`](../../src/telemetry/root_span.rs))
is the outermost middleware ([`build_app`](../../src/lib.rs)). Every
request runs inside its span, from the first middleware to the last byte of
the response. Three things follow:

- **Refused requests are traced.** Those refused by the authentication,
  host or same-origin checks used to get no span.
- **The signature check is a child of the request span** (`authenticate_sigv4`,
  recording only `key_id`). It used to have no parent, so each signed
  request produced a second, orphan trace.
- **Health checks get no span** (`/api/health`). Probes poll it every few
  seconds.

**Name.** A span can't be renamed once it has started, so the name is set
when the request arrives:
- an SQS request is named after its action, from `X-Amz-Target`:
  `SQS.SendMessage`, as the AWS SDKs name their client spans;
- anything else is named `{method} {route}`, e.g.
  `GET /api/admin/stats/queue`, or just the method when no route matches.

**Fields:**

| Field | Value |
| --- | --- |
| `http.request.method`, `http.route`, `url.path`, `network.protocol.version` | From the request |
| `client.address` | The connection's address; never a forwarded header, which a client can set |
| `user_agent.original` | The `User-Agent` header |
| `http.response.status_code` | Recorded when the response is ready |
| `otel.status_code`, `error.type`, `exception.message` | Only for 5xx responses. A 4xx is the client's mistake, and the conventions leave a server span's status unset for it |
| `rpc.system`, `rpc.service`, `rpc.method` | SQS requests: `aws-api`, `AmazonSQS`, the action |
| `messaging.system`, `messaging.operation.name`, `messaging.operation.type` | SQS requests: `aws_sqs`. The operation fields only for message operations: the action, and `send`, `receive` or `settle` |
| `messaging.destination.name`, `nervemq.namespace` | The queue (`namespace/queue`) and its namespace, recorded by the SQS handlers (`target_queue` in [`src/sqs/mod.rs`](../../src/sqs/mod.rs)) |
| `enduser.id` | The caller's **email**. `Authentication` records it for API keys and SigV4, `Protected` for session cookies |
| `request_id` | A random id per request |

Log events made during a request carry the span's fields. Every line of an
authenticated request therefore includes the caller's email.

**Errors.** tracing-actix-web logs one event for each error response,
with the reason: WARN for a 4xx, ERROR for a 5xx. A refused signature is
one WARN; the underlying cause is logged at DEBUG by `Authentication`.

## What is never recorded

The SQS handlers have no `#[instrument]` of their own: their arguments are
the requests. Recording those would put the following into every log line,
and into any exported trace:

- message bodies (up to 1 MiB, formatted when each span was created);
- message attribute values;
- receipt handles (each one lets its holder delete that delivery);
- queue policies and tags.

The SigV4 span records the access key id, never the signature, and no span
or event carries a secret key. `span_tests` in [`src/sqs`](../../src/sqs)
pins this through the production app: it sends, receives, changes the
visibility of and deletes a message, then checks that none of the body, the
attribute value, the receipt handle, the signature or the secret appears in
any recorded span or event.

## Rules for new code

- **No `#[instrument]` that records arguments.** Use `skip_all` and name
  the fields you want.
- **Record identifiers, not content.** A message id or a queue name is
  fine; a body, an attribute value or a receipt handle is not.
- **Add to the request span instead of opening one.** Record on
  `tracing::Span::current()`, and declare the field in `RootSpan` first: a
  field not declared at creation is silently dropped.
