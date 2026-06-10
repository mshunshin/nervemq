<div align="center">
  <span>
    <h1>NerveMQ</h1>

[![GitHub License](https://img.shields.io/github/license/fortress-build/nervemq)](https://github.com/fortress-build/nervemq/blob/main/LICENSE)

  </span>

A lightweight, SQLite-backed message queue with AWS SQS-compatible API, written in Rust.

</div>



https://github.com/user-attachments/assets/a9a601ec-2163-4656-91f3-80dd4bf58c2f



> [!NOTE]
> This project is still in development and has not been tested in production scenarios.

## Features

- 🚀 **AWS SQS Compatible API** - Drop-in replacement for applications using AWS SQS
- 💾 **SQLite Backend** - Reliable, embedded storage with ACID guarantees
- 🔒 **Multi-tenant** - Namespace isolation with built-in authentication
- 📊 **Queue Attributes** - Track message counts, timestamps, and queue settings
- 🏃 **Fast & Efficient** - Written in Rust for optimal performance
- 🎯 **Self-contained** - Self-contained binary with minimal requirements
- 📱 Admin Interface - Manage queues and tenants via UI or API

## Installation / Quick Start

NerveMQ is intended to be modular and extensible. As such, it can be consumed in two ways: using
the preconfigured binary in `main.rs`, or including `nervemq` as a library and providing the custom
implementations needed for your use-case. We also plan to add more configuration options to the preconfigured
binary so that common use-cases are covered.

For now, you will have to clone the repo from github.

```bash
git clone https://github.com/fortress-build/nervemq
cd nervemq
cargo run --release
```

The server expects a few configuration parameters to be available via
environment variables:

- `NERVEMQ_DB_PATH` (optional; default: `./nervemq.db`)
  Database file path

- `NERVEMQ_DEFAULT_MAX_RETRIES` (optional; default: `10`)
  Default retry limit

- `NERVEMQ_HOST` (optional; default `http://localhost:8080`)
  Server host URL (for UI access)

- `NERVEMQ_ROOT_EMAIL` (optional; default `admin@example.com`)
  Root admin email

- `NERVEMQ_ROOT_PASSWORD` (optional; default `password`)
  Root admin password

The server doesn't have any subcommands or CLI interface. Just run `nervemq` to start.

### Bundled UI (single binary)

The admin UI is compiled into the server binary by default (the `embed-ui`
feature) and served from the same port as the API. Build the static export
first, then build the server:

```bash
git clone https://github.com/fortress-build/nervemq
cd nervemq

# 1. Build the Next.js static export into ./out
bun install
bun run build

# 2. Build the server with the UI embedded
cargo build --release
```

The resulting binary serves the API and the UI together on
`http://localhost:8080`. The build fails with a clear error if `out/` is
missing; for an API-only server that doesn't require `out/`, build with
`cargo build --release --no-default-features`.

### Developing the UI standalone

To iterate on the UI with hot reload, run the Next.js dev server (it points at a
separately running backend on port 8080 via `NEXT_PUBLIC_SERVER_ENDPOINT`):

```bash
cargo run            # API server on :8080
bun run dev          # UI dev server on :3000
```

## Usage Examples

NerveMQ's queue APIs are compatible with SQS, so you can you any SQS client.

### Using AWS SDK

```rust
use aws_sdk_sqs::{Client, Config};

async fn example() {
    let config = Config::builder()
        .endpoint_url("http://localhost:8080/api/sqs")
        .build();

    let client = Client::from_conf(config);

    // Send a message
    client.send_message()
        .queue_url("http://localhost:8080/api/sqs/namespace/myqueue")
        .message_body("Hello World!")
        .send()
        .await?;
}
```

## HTTP API Reference

NerveMQ exposes two HTTP surfaces on the same port (default `http://localhost:8080`):

- A **management API** under `/api/admin` used by the admin UI, for controlling
  queues, namespaces, users and API keys.
- An **SQS-compatible API** under `/api/sqs` for sending and receiving messages.

### Authentication

| Surface | Mechanism |
| --- | --- |
| Management API (`/api/admin/*`) | Session cookie `nervemq_session`, obtained via `POST /api/admin/auth/login`. |
| SQS API (`/api/sqs`) | AWS Signature V4, signed with an API key's `access_key`/`secret_key` (created via `POST /api/admin/tokens`). |

Access levels per scope:

- `/api/admin/auth` — public.
- `/api/admin/queue`, `/api/admin/stats`, `/api/admin/tokens` — any authenticated user.
- `/api/sqs` — authenticated (SigV4); namespace access is additionally checked per request.
- `/api/admin/ns`, `/api/admin/users` — admin role only.

Sessions expire after 1 hour. The default root account is configured via
`NERVEMQ_ROOT_EMAIL` / `NERVEMQ_ROOT_PASSWORD`.

### Auth — `/api/admin/auth` (public)

| Method | Path | Body | Description |
| --- | --- | --- | --- |
| POST | `/api/admin/auth/login` | `{ "email", "password" }` | Logs in and sets the session cookie. Returns `{ "email", "role" }`. |
| POST | `/api/admin/auth/logout` | — | Clears the session. |
| POST | `/api/admin/auth/verify` | — | Returns the current session's `{ "email", "role" }`, or `401` if not logged in. |

### Queues — `/api/admin/queue` (authenticated)

| Method | Path | Body | Description |
| --- | --- | --- | --- |
| GET | `/api/admin/queue` | — | List all queues the user can access. Returns `{ "queues": [...] }`. |
| GET | `/api/admin/queue/{ns}` | — | List queues in namespace `{ns}`. |
| POST | `/api/admin/queue/{ns}/{queue}` | `{ "attributes": {…}, "tags": {…} }` | Create a queue. |
| DELETE | `/api/admin/queue/{ns}/{queue}` | — | Delete a queue. |
| GET | `/api/admin/queue/{ns}/{queue}` | — | Queue statistics (pending / delivered / failed, sizes, etc.). |
| GET | `/api/admin/queue/{ns}/{queue}/messages` | — | List messages currently in the queue. |
| GET | `/api/admin/queue/{ns}/{queue}/config` | — | Get queue config (`max_retries`, `dead_letter_queue`). |
| POST | `/api/admin/queue/{ns}/{queue}/config` | `{ "max_retries": u64, "dead_letter_queue": "name" \| null }` | Update queue config. |

### Statistics — `/api/admin/stats` (authenticated)

| Method | Path | Description |
| --- | --- | --- |
| GET | `/api/admin/stats/queue` | Per-queue statistics across all accessible queues (map keyed by queue). |
| GET | `/api/admin/stats/ns` | Per-namespace statistics. |

### API keys / tokens — `/api/admin/tokens` (authenticated)

| Method | Path | Body | Description |
| --- | --- | --- | --- |
| GET | `/api/admin/tokens` | — | List the caller's API keys (`name`, `namespace`). |
| POST | `/api/admin/tokens` | `{ "name", "namespace" }` | Create an API key. Returns `{ "name", "namespace", "access_key", "secret_key" }` — the `secret_key` is shown only once. |
| DELETE | `/api/admin/tokens` | `{ "name" }` | Delete one of the caller's API keys by name. |

### Namespaces — `/api/admin/ns` (admin only)

| Method | Path | Body | Description |
| --- | --- | --- | --- |
| GET | `/api/admin/ns` | — | List namespaces. |
| POST | `/api/admin/ns/{ns}` | — | Create namespace `{ns}`. Returns `{ "id" }`. |
| DELETE | `/api/admin/ns/{ns}` | — | Delete namespace `{ns}`. |

### Users & permissions — `/api/admin/users` (admin only)

| Method | Path | Body | Description |
| --- | --- | --- | --- |
| GET | `/api/admin/users` | — | List users (`email`, `role`). |
| POST | `/api/admin/users` | `{ "email", "password", "role", "namespaces": [...] }` | Create a user. |
| DELETE | `/api/admin/users` | `{ "email" }` | Delete a user. |
| GET | `/api/admin/users/{email}/permissions` | — | List namespaces the user has access to. |
| PUT | `/api/admin/users/{email}/permissions` | `["ns", …]` | Grant access to the listed namespaces. |
| POST | `/api/admin/users/{email}/permissions` | `["ns", …]` | Replace the user's namespace permissions with the listed set. |
| DELETE | `/api/admin/users/{email}/permissions` | `["ns", …]` | Revoke access to the listed namespaces. |
| GET | `/api/admin/users/{email}/role` | — | Get the user's role. |
| POST | `/api/admin/users/{email}/role` | `{ "role": "user" \| "admin" }` | Set the user's role. |

### SQS-compatible API — `/api/sqs`

All SQS operations are a single `POST /api/sqs` using the AWS JSON protocol: the
operation is selected by the `X-Amz-Target: AmazonSQS.<Operation>` header and the
request/response bodies match the AWS SQS shapes. Queue URLs have the form
`http://<host>/api/sqs/<namespace>/<queue>`. Requests must be signed with SigV4 using
an API key (see `/api/admin/tokens`). Easiest consumed via any standard AWS SQS SDK (see
[Usage Examples](#usage-examples)).

Implemented operations:

`CreateQueue`, `DeleteQueue`, `GetQueueUrl`, `GetQueueAttributes`,
`SetQueueAttributes`, `ListQueues`, `ListQueueTags`, `TagQueue`, `UntagQueue`,
`PurgeQueue`, `SendMessage`, `SendMessageBatch`, `ReceiveMessage`,
`DeleteMessage`.

> [!NOTE]
> `DeleteMessageBatch` is recognized but not yet implemented. Other SQS
> operations (e.g. `ChangeMessageVisibility`, `AddPermission`) are not yet
> supported.

## Why NerveMQ?

- **Simple Deployment**: Single binary, no external dependencies
- **Familiar API**: AWS SQS compatibility means easy migration
- **Reliable Storage**: SQLite provides robust data persistence
- **Cost Effective**: Self-hosted alternative to cloud services
- **Developer Friendly**: Easy to set up for development and testing

## Architecture

NerveMQ uses SQLite as its storage engine, providing:

- ACID compliance
- Reliable message delivery
- Efficient queue operations
- Data durability
- Low maintenance overhead

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

Copyright 2024 Fetchflow, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

---

<div align="center">
Made with ❤️by the Fortress team
</div>
