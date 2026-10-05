-- Rebuilds `queues` like the up migration, so the same guard applies:
-- refuse to run with foreign-key enforcement on. Dropping the AUTOINCREMENT
-- table also removes its row from `sqlite_sequence`.
create temp table fk_guard (foreign_keys_off integer check (foreign_keys_off));
insert into fk_guard select not foreign_keys from pragma_foreign_keys;
drop table fk_guard;

create table queues_old (
  id   integer not null,
  ns   integer not null,
  name text    not null,
  created_by integer,
  paused_at integer,
  created_at integer,
  attributes_modified_at integer,

  primary key (id),
  foreign key (ns) references namespaces(id) on delete cascade,
  foreign key (created_by) references users(id) on delete set null
);
insert into queues_old (id, ns, name, created_by, paused_at, created_at, attributes_modified_at)
select id, ns, name, created_by, paused_at, created_at, attributes_modified_at from queues;
drop table queues;
alter table queues_old rename to queues;
create unique index if not exists queues_ns_name_idx on queues(ns, name);
