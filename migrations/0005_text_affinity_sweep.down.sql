-- Restore the original (NUMERIC-affinity) `string` column declarations.
PRAGMA defer_foreign_keys = ON;

create table namespaces_orig (
  id   integer not null,
  name string  not null,
  created_by integer not null,

  primary key (id),
  foreign key (created_by) references users(id)
);
insert into namespaces_orig (id, name, created_by)
select id, name, created_by from namespaces;
drop table namespaces;
alter table namespaces_orig rename to namespaces;
create unique index if not exists namespaces_name_idx on namespaces(name);

create table queues_orig (
  id   integer not null,
  ns   integer not null,
  name string  not null,
  created_by integer,

  primary key (id),
  foreign key (ns) references namespaces(id) on delete cascade,
  foreign key (created_by) references users(id) on delete set null
);
insert into queues_orig (id, ns, name, created_by)
select id, ns, name, created_by from queues;
drop table queues;
alter table queues_orig rename to queues;
create unique index if not exists queues_ns_name_idx on queues(ns, name);

create table queue_attributes_orig (
  queue integer not null,
  k string not null,
  v string not null,

  primary key (queue, k),
  foreign key (queue) references queues(id) on delete cascade
);
insert into queue_attributes_orig (queue, k, v)
select queue, k, v from queue_attributes;
drop table queue_attributes;
alter table queue_attributes_orig rename to queue_attributes;
create unique index if not exists queue_attrs_queue_key_idx on queue_attributes(queue, k);

create table kv_pairs_orig (
  id      integer not null,
  message integer not null,
  k       string  not null,
  v       blob    not null,

  primary key (id),
  foreign key (message) references messages(id) on delete cascade
);
insert into kv_pairs_orig (id, message, k, v)
select id, message, k, v from kv_pairs;
drop table kv_pairs;
alter table kv_pairs_orig rename to kv_pairs;
create unique index if not exists kv_message_idx on kv_pairs(message, k);

create table api_keys_orig (
  id integer not null,
  user integer not null,
  ns integer not null,
  name string not null,
  key_id text not null,
  hashed_key text not null,
  encrypted_key blob not null,

  primary key (id),
  foreign key (user) references users(id) on delete cascade,
  foreign key (ns) references namespaces(id) on delete cascade
);
insert into api_keys_orig (id, user, ns, name, key_id, hashed_key, encrypted_key)
select id, user, ns, name, key_id, hashed_key, encrypted_key from api_keys;
drop table api_keys;
alter table api_keys_orig rename to api_keys;
create unique index if not exists api_keys_user_name_idx on api_keys(user, name);
create unique index if not exists api_keys_key_id_idx on api_keys(key_id);
