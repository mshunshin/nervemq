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

Four tables define the whole system
([`0001`](../../migrations/0001_initialization.up.sql), reshaped by
[`0011`](../../migrations/0011_namespace_ownership.up.sql)):

| Table | Holds | Cascade on namespace delete |
| --- | --- | --- |
| `namespaces` | `id`, unique `name`, the creating admin (`created_by`, `created_by_email`) | — |
| `queues` | one row per queue, `(ns, name)` unique | deleted (and their messages with them) |
| `user_permissions` | which **users** may act on which namespaces, and whether they own it (`is_owner`) | deleted |
| `api_keys` | SQS credentials, each bound to one user and one `ns` | deleted |

Consequences worth internalizing:

- **Queue names are only unique per namespace** — `hello/jobs` and
  `prod/jobs` are unrelated queues, exactly like a queue name reused across
  two AWS accounts.
- **Deleting a namespace is account-closure semantics**: its queues,
  messages, API keys and permission grants all go with it.
- **Deleting a user keeps the record of what they created.** The creator's
  id becomes NULL and their email stays in `created_by_email`; queues they
  created stay listed, with no creator.

## Admins, owners and members

People log in as **users** (email + password, session cookie) to drive the
admin API (`/api/admin/*`) and the bundled UI. What a user may do in a
namespace depends on one of three levels:

| Level | How you get it | What it allows |
| --- | --- | --- |
| **Admin** | `role = 'admin'` on the user | Everything, in every namespace, with or without a permission row. Only admins create namespaces, manage users, and choose owners. |
| **Owner** | a permission row with `is_owner` | Delete the namespace; create, delete, purge and configure its queues; act on individual messages. |
| **Member** | a permission row without it | Send, receive, acknowledge and inspect messages. Managing queues is refused (`403`, or `AccessDeniedException` over SQS). |

- A namespace has **zero or more owners**. The admin who creates one becomes
  its first owner; any admin can add or remove owners later
  (`PUT`/`DELETE /api/admin/ns/{ns}/owners/{email}`, or
  `nervemq namespace owner add|remove`). A namespace with no owners is still
  fully managed by admins.
- Ownership implies membership: making someone an owner grants access if
  they lacked it, and removing ownership leaves them a member.
- Replacing a user's set of namespaces (what the UI's user editor sends)
  keeps ownership of the namespaces that stay. It used to delete and
  re-insert every row, which silently stripped owners of ownership.

The checks live in the service layer, so the admin API and the SQS API
cannot disagree: `check_user_access` decides membership (admin, or holds a
row) and `require_queue_manager` additionally requires admin or owner
([`src/service.rs`](../../src/service.rs)).

### User accounts

- **Disabling** a user (`POST /api/admin/users/{email}/disable`,
  `nervemq user disable`) keeps their account, permissions and keys, but
  they can no longer log in, their open sessions stop working on the next
  request, and their API keys stop authenticating at once.
- The **last active admin** cannot be demoted, disabled or deleted (`409`).
  The guard is part of the same SQL statement as the change, so two
  concurrent requests cannot both pass it.
- Admins can list and revoke any user's API keys and reset any user's
  password; users change their own password with
  `POST /api/admin/auth/password`.

## API keys

**API keys** (`access_key` + `secret_key`, base58) authenticate the **SQS
API** (`/api/sqs`) via AWS Signature v4 — the same signing the real AWS SDKs
perform, which is why boto3 / aws-sdk-rust work unmodified. A key is minted
*for one namespace* and carries its owning user with it: resolving the access
key yields `(User, AuthorizedNamespace, KeyAccess)`
([`src/auth/protocols/sigv4.rs`](../../src/auth/protocols/sigv4.rs)).

Each key also has an **access level**, chosen when it is created
(`api_keys.access`, migration
[`0012`](../../migrations/0012_api_key_access.up.sql)):

| Access | The key may | Who may create it |
| --- | --- | --- |
| **Member** | Send, receive and inspect messages | Anyone with access to the namespace |
| **Owner** | Also manage the namespace's queues | Admins and the namespace's owners |
| **Admin** | Everything its owner can do, including the admin API | Admins |

Without a level, a key gets its creator's own level in the namespace. The
level is a **cap on its owner, never a grant**: a key does the lesser of its
level and what its owner can do *now*. An owner-level key stops managing
queues the moment its owner loses ownership, and a member's key never gains
more when its owner is promoted; mint a new key for that.

So a key can do at most what its owner may do in its namespace, and nothing
outside it:

1. The namespace is parsed from the queue URL in the request body
   (`/api/sqs/<ns>/<queue>`) and must **equal the key's
   `AuthorizedNamespace`** — a key for `staging` cannot touch `prod`'s
   queues even if its owner has permissions on both.
2. The lesser of the key's access and its owner's level applies: queue
   management needs both at owner or above. The SQS dispatcher checks the
   key's level and the service checks the owner's. Revoking the owner's
   grant, or disabling the owner, stops their keys at once.

Operations that take a queue *name* rather than a URL (`CreateQueue`,
`GetQueueUrl`, `ListQueues`) implicitly operate in the key's namespace.
There is no cross-namespace operation in the SQS API at all — working with
two namespaces means holding two keys, just as two AWS accounts mean two sets
of credentials.

**An admin's admin-level key is also a full admin credential.** The
`Authorization` header is accepted on the admin API too, so such a key can,
for example, create users. Keys at owner or member access are refused there
(`401`), whoever owns them, so an admin can hand a workload SQS access
without admin power by restricting its key.

To restrict what a workload's key can do, give it **member** access to send
and receive only, or **owner** access to also manage queues, and put queues
that need separating into separate namespaces. Keys have no finer scoping
(no per-queue grants, no expiry).

## Mapping to AWS concepts

| NerveMQ | Closest AWS concept | Differences |
| --- | --- | --- |
| Namespace | **Account ID** (as it appears in queue URLs) | Lightweight: an admin mints one per team/env/tenant; no billing, no region |
| User with grants on several namespaces | **IAM Identity Center user** assigned to several accounts | An IAM user lives in one account; a NerveMQ user, like an Identity Center user, can reach many |
| Member / owner level | **Permission set** (e.g. send/receive vs full queue management) | Two fixed levels, not a policy language; no per-queue or per-condition grants |
| API key | **IAM access key** | Long-lived, bound to one (user, namespace) pair; no STS, no expiry, no last-used data |
| Key access level | **Session policy** (it can only narrow what the principal may do) | Three fixed levels, set once at creation |
| Disabled user | **Inactive** access keys plus a blocked console login | One switch for both |
| `admin` role | **Root user / administrator** | Reaches every namespace; its admin-level keys can call the admin API |
| — (none) | **SQS queue policy** | No per-queue grants across namespaces |
| — (none) | **Region** | One server is, in effect, one region; the URL has no region segment |

The deliberate simplification: AWS expresses isolation through a general
policy language evaluated per request; NerveMQ expresses it structurally —
*which namespace a key belongs to* and *its owner's level there* — and keeps
authorization to a couple of joins. What you give up is granularity (no
per-queue grants, no expiring keys); what you get is a model you can hold in
your head and audit by reading one table.

## Practical workflows

```sh
# Admin: create a namespace, a worker user with member access, and its key
nervemq namespace add staging
nervemq user add worker@example.com --namespace staging
nervemq apikey add --name ci-worker --namespace staging --user worker@example.com

# Let a team lead manage the namespace's queues
nervemq namespace owner add staging lead@example.com

# Client: standard AWS SDK, pointed at the namespace-scoped endpoint
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  aws sqs list-queues --endpoint-url http://localhost:8080/api/sqs
```

Multi-tenancy falls out naturally: one namespace per tenant, one member key
per tenant-facing service, and the URL/credential scoping above guarantees a
tenant's workers can never read another tenant's queues — enforced
server-side on every call, not by convention.
