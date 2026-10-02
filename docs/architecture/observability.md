# Observability

What the server records about the requests it handles: one span per
request, the log events made inside it, and metrics.

- **Stdout:** logs, filtered by `NERVEMQ_LOG`, pretty in debug builds and
  JSON in release builds.
- **OpenTelemetry:** with the standard `OTEL_*` variables, traces, metrics
  and logs are also exported over OTLP. See
  [Exporting with OpenTelemetry](#exporting-with-opentelemetry).

The request span follows OpenTelemetry's semantic conventions and is
exported as the server span.

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
| `trace_id` | The OpenTelemetry trace id, so a stdout line can be found in the tracing backend. Only with the `otel` feature |

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

## Exporting with OpenTelemetry

The `otel` Cargo feature, on by default, exports traces, metrics and logs
over OTLP/HTTP. Nothing is exported until the standard variables ask for it.
The code is in [`src/telemetry/otel/`](../../src/telemetry/otel/).

### Turning it on

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318 nervemq
```

A signal is exported when it has an endpoint (`OTEL_EXPORTER_OTLP_ENDPOINT`,
or `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_ENDPOINT` for one signal), or when
`OTEL_{TRACES,METRICS,LOGS}_EXPORTER=otlp` asks for the default endpoint,
`http://localhost:4318`. NerveMQ reads these variables itself:

| Variable | Effect |
| --- | --- |
| `OTEL_SDK_DISABLED=true` | Nothing is exported |
| `OTEL_{TRACES,METRICS,LOGS}_EXPORTER` | `none` turns that signal off. `otlp` turns it on. Anything else turns it off, with a warning |
| `OTEL_EXPORTER_OTLP_PROTOCOL`, `…_{TRACES,METRICS,LOGS}_PROTOCOL` | `http/protobuf` (the default) or `http/json`. gRPC isn't built in, so `grpc` turns the signal off, with a warning |
| `OTEL_SERVICE_NAME` | Overrides `service.name` (default `nervemq`) |
| `OTEL_RESOURCE_ATTRIBUTES` | Overrides NerveMQ's resource attributes (below) |

The rest are read by the SDK and work as documented upstream:

- the exporter's own: `OTEL_EXPORTER_OTLP_HEADERS`, `…_TIMEOUT`,
  `…_COMPRESSION`, and their per-signal forms;
- sampling: `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG`;
- batching and export intervals: `OTEL_BSP_*`, `OTEL_BLRP_*`,
  `OTEL_METRIC_EXPORT_INTERVAL`.

A misconfigured signal is logged as a warning, and the server starts
without it. HTTPS endpoints use the OpenSSL the server already links,
with the system's certificates.

Every signal carries the same resource:
- `service.name`;
- `service.version` (the crate's);
- `service.instance.id` (random for each process, to tell replicas apart);
- the SDK's `telemetry.sdk.*`;
- anything in `OTEL_RESOURCE_ATTRIBUTES`.

### What is exported

**Traces** carry NerveMQ's own spans, whatever `NERVEMQ_LOG` says:
- the request span above;
- the signature check, as its child;
- warning and error events, as span events.

Other crates' spans aren't exported. Health checks have no span.

**Metrics:**

| Metric | Type | Attributes |
| --- | --- | --- |
| `http.server.request.duration` | Histogram (s): the conventions' buckets, plus 20 s for long polls | `http.request.method`, `http.route`, `http.response.status_code`, `rpc.method` (the SQS action), and `error.type` on a 5xx |
| `http.server.active_requests` | Up-down counter | `http.request.method` |
| `db.client.connection.count` | Up-down counter | `db.client.connection.pool.name` (`main`, `sessions`) and `db.client.connection.state` (`idle`, `used`) |
| `nervemq.db.file.size` | Gauge (bytes) | `nervemq.db.file`: `main`, `main-wal`, `sessions` or `sessions-wal` |

**Logs** carry the events `NERVEMQ_LOG` lets through to stdout. An event
made during a request carries that request's trace and span. Two sources
are left out:
- the exporters' own HTTP clients (`hyper`, `h2`, `reqwest`);
- the SDK (`opentelemetry*`).

Otherwise each export would log, and the log would be exported in turn.
The SDK's own problems, such as an unreachable collector, still reach
stdout.

### What never leaves the server

The same rules as for stdout apply. Exported data never contains message
bodies, attribute values, receipt handles or credentials. It does contain
the caller's email (`enduser.id`), the client's address and user agent, and
namespace and queue names.

The smoke test (`tests/smoke.rs`) exports a send, receive and delete from
the real binary to a stand-in collector, and checks that the body appears
in none of it.

### Continuing a caller's trace

A request carrying trace context continues that trace: the request span
becomes a child of the caller's span. Two formats are read:
- W3C `traceparent`;
- AWS X-Ray `X-Amzn-Trace-Id`, which the Java SDK's tracing sends.

With both, W3C wins.

### Stopping

On SIGTERM the server first finishes its requests: up to 30 s, and a long
poll lasts up to 20 s. It then exports whatever is queued and exits. Each
signal's final export waits up to its export timeout, 10 s by default, so a
collector that's down can delay the exit by that much.

Give the process manager time for all this. Docker kills after 10 s by
default: use `docker stop -t 45`, or `stop_grace_period: 45s` in Compose.
SIGINT (Ctrl-C) skips the 30 s drain.

### Trying it locally

Download a Collector from
[opentelemetry-collector-releases](https://github.com/open-telemetry/opentelemetry-collector-releases/releases)
(`otelcol_<version>_<os>_<arch>.tar.gz`; it has a `.sha256` beside it). Give
it a config that writes everything it receives to a file:

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:14318
exporters:
  file:
    path: ./received.json
service:
  pipelines:
    traces: { receivers: [otlp], exporters: [file] }
    metrics: { receivers: [otlp], exporters: [file] }
    logs: { receivers: [otlp], exporters: [file] }
  telemetry:
    metrics:
      level: none
```

Then run the server against it, exporting every few seconds:

```bash
./otelcol --config config.yaml &
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:14318 OTEL_METRIC_EXPORT_INTERVAL=3000 \
  OTEL_BSP_SCHEDULE_DELAY=500 OTEL_BLRP_SCHEDULE_DELAY=500 just run
```

`received.json` holds one OTLP/JSON document per export.
