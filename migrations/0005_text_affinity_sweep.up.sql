-- Sweep the remaining `string`-declared columns to TEXT affinity.
--
-- Migration 0004 fixed queue_tags; the same wart — `string` is not a type
-- name SQLite recognizes, so the columns silently got NUMERIC affinity —
-- remains on namespaces.name, queues.name, queue_attributes.k/v, kv_pairs.k
-- and api_keys.name. Anything numeric-looking stored in them ("123" as a
-- queue name, a message-attribute key, a token name) was coerced to a
-- number on insert: reading it back as text then fails (a 500), and
-- spellings like "01" were collapsed to "1" before that.
--
-- SQLite cannot alter a column's type in place, so each table is rebuilt
-- (create → copy → drop → rename). namespaces and queues are foreign-key
-- parents, and dropping one with enforcement active would fail — so FK
-- checks are deferred to the end of the migration transaction (sqlx wraps
-- this file in one; `defer_foreign_keys`, unlike `foreign_keys`, takes
-- effect inside a transaction and resets itself at commit). Copying
-- preserves every id, so the deferred check passes: no child row is ever
-- orphaned.
PRAGMA defer_foreign_keys = ON;

create table namespaces_text (
  id   integer not null,
  name text    not null,
  created_by integer not null,

  primary key (id),
  foreign key (created_by) references users(id)
);
insert into namespaces_text (id, name, created_by)
select id, cast(name as text), created_by from namespaces;
drop table namespaces;
alter table namespaces_text rename to namespaces;
create unique index if not exists namespaces_name_idx on namespaces(name);

create table queues_text (
  id   integer not null,
  ns   integer not null,
  name text    not null,
  created_by integer,

  primary key (id),
  foreign key (ns) references namespaces(id) on delete cascade,
  foreign key (created_by) references users(id) on delete set null
);
insert into queues_text (id, ns, name, created_by)
select id, ns, cast(name as text), created_by from queues;
drop table queues;
alter table queues_text rename to queues;
create unique index if not exists queues_ns_name_idx on queues(ns, name);

create table queue_attributes_text (
  queue integer not null,
  k text not null,
  v text not null,

  primary key (queue, k),
  foreign key (queue) references queues(id) on delete cascade
);
insert into queue_attributes_text (queue, k, v)
select queue, cast(k as text), cast(v as text) from queue_attributes;
drop table queue_attributes;
alter table queue_attributes_text rename to queue_attributes;
create unique index if not exists queue_attrs_queue_key_idx on queue_attributes(queue, k);

create table kv_pairs_text (
  id      integer not null,
  message integer not null,
  k       text    not null,
  v       blob    not null,

  primary key (id),
  foreign key (message) references messages(id) on delete cascade
);
insert into kv_pairs_text (id, message, k, v)
select id, message, cast(k as text), v from kv_pairs;
drop table kv_pairs;
alter table kv_pairs_text rename to kv_pairs;
create unique index if not exists kv_message_idx on kv_pairs(message, k);

create table api_keys_text (
  id integer not null,
  user integer not null,
  ns integer not null,
  name text not null,
  key_id text not null,
  hashed_key text not null,
  encrypted_key blob not null,

  primary key (id),
  foreign key (user) references users(id) on delete cascade,
  foreign key (ns) references namespaces(id) on delete cascade
);
insert into api_keys_text (id, user, ns, name, key_id, hashed_key, encrypted_key)
select id, user, ns, cast(name as text), key_id, hashed_key, encrypted_key from api_keys;
drop table api_keys;
alter table api_keys_text rename to api_keys;
create unique index if not exists api_keys_user_name_idx on api_keys(user, name);
create unique index if not exists api_keys_key_id_idx on api_keys(key_id);
