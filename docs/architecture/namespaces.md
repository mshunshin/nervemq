# Namespaces

A **namespace** is NerveMQ's top-level container: every queue lives in exactly
one namespace, and every SQS credential is scoped to exactly one namespace.
If you know AWS, the one-line summary is: **a namespace plays the role of an
AWS account ID** in SQS's resource model — it is the unit that queue URLs,
credentials and access grants all hang off.

```text
AWS:     https://sqs.us-east-1.amazonaws.com/<account-id>/<queue-name>
NerveMQ: http://<host>/api/sqs/<namespace>/<queue-name>
```

## The model

Four tables ([`migrations/0001_initialization.up.sql`](../../migrations/0001_initialization.up.sql))
define the whole system:

| Table | Holds | Cascade on namespace delete |
| --- | --- | --- |
| `namespaces` | `id`, unique `name`, `created_by` | — |
| `queues` | one row per queue, `(ns, name)` unique | deleted (and their messages with them) |
| `user_permissions` | which **users** may act on which namespaces (`can_delete_ns` flag) | deleted |
| `api_keys` | SQS credentials, each bound to one `ns` | deleted |

Two consequences worth internalizing:

- **Queue names are only unique per namespace** — `hello/jobs` and
  `prod/jobs` are unrelated queues, exactly like a queue name reused across
  two AWS accounts.
- **Deleting a namespace is account-closure semantics**: its queues,
  messages, API keys and permission grants all go with it.

## Two principals, two credentials

NerveMQ has two kinds of caller, mirroring AWS's console-vs-API split:

1. **Users** (email + password) authenticate with a session cookie and drive
   the **admin API** (`/api/admin/*`) and the bundled UI. A user's reach is
   the set of `user_permissions` rows they hold — one row per namespace,
   all-or-nothing. Users with the `admin` role can additionally create
   namespaces and manage users; creating a namespace grants the creator a
   permission row with `can_delete_ns = true`
   ([`create_namespace`](../../src/service.rs)). Other users get plain rows
   (`can_delete_ns = false`) when an admin grants them access, so only a
   namespace's owner-grade holders can delete it.

2. **API keys** (`access_key` + `secret_key`, base58) authenticate the **SQS
   API** (`/api/sqs`) via AWS Signature v4 — the same signing the real AWS
   SDKs perform, which is why boto3 / aws-sdk-rust work unmodified. A key is
   minted *for one namespace* and carries its owning user with it: resolving
   the access key yields `(User, AuthorizedNamespace)`
   ([`src/auth/protocols/sigv4.rs`](../../src/auth/protocols/sigv4.rs)).

## How a request is scoped

Every SQS handler ([`src/sqs/mod.rs`](../../src/sqs/mod.rs)) enforces the
boundary twice:

1. The namespace is parsed from the queue URL in the request body
   (`/api/sqs/<ns>/<queue>`) and must **equal the key's
   `AuthorizedNamespace`** — a key for `staging` cannot touch `prod`'s
   queues even if its owning user has permissions on both. The token, not
   the user, is the hard boundary.
2. The key's user must still hold a `user_permissions` row for that
   namespace (`check_user_access`) — so revoking a user's grant immediately
   disables their keys for it.

Operations that take a queue *name* rather than a URL (`CreateQueue`,
`GetQueueUrl`, `ListQueues`) implicitly operate in the key's namespace:
`ListQueues` only ever lists that one namespace, and `GetQueueUrl` resolves
names within it. There is no cross-namespace operation in the SQS API at
all — working with two namespaces means holding two keys, just as two AWS
accounts mean two sets of credentials.

## Mapping to AWS concepts

| NerveMQ | Closest AWS concept | Differences |
| --- | --- | --- |
| Namespace | **Account ID** (as it appears in queue URLs) | Lightweight: an admin mints one per team/env/tenant; no billing, no region |
| API key | **IAM access key** | Bound to exactly one namespace; no policies, no STS, no cross-account assume-role |
| `user_permissions` row | **IAM policy attachment** | All-or-nothing per namespace — no per-action or per-queue conditions |
| `can_delete_ns` | Resource **owner** | Granted to the creating admin; gates only namespace deletion |
| `admin` / `user` role | **Management account / root** vs member | `admin` gates namespace creation and user management, not data-plane access |
| — (none) | **Region** | One server is, in effect, one region; the URL has no region segment |

The deliberate simplification: AWS expresses isolation through a general
policy language evaluated per request; NerveMQ expresses it structurally —
*which namespace a key belongs to* — and keeps authorization to two joins.
What you give up is granularity (no read-only keys, no per-queue grants);
what you get is a model you can hold in your head and audit by reading one
table.

## Practical workflows

```sh
# Admin: create a namespace and a key for it (UI: Namespaces / API Keys pages)
nervemq apikey add --name ci-worker --namespace staging

# Client: standard AWS SDK, pointed at the namespace-scoped endpoint
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  aws sqs list-queues --endpoint-url http://localhost:8080/api/sqs
```

Multi-tenancy falls out naturally: one namespace per tenant, one key per
tenant-facing service, and the URL/credential scoping above guarantees a
tenant's workers can never read another tenant's queues — enforced
server-side on every call, not by convention.
