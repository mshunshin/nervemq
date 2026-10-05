-- Queue ids are never reused. Without AUTOINCREMENT, SQLite gives a new row
-- the highest id in use plus one, so deleting the queue with the highest id
-- handed that id to the next queue created, in any namespace. Whatever still
-- held the old id then reached the new queue: a long poll resolves its queue
-- once and keeps reading it by id. AUTOINCREMENT needs the column declared
-- `integer primary key autoincrement`, so the table is rebuilt; every id is
-- kept, and the next queue gets one past the highest.
--
-- This rebuilds `queues`, which messages, queue configurations, attributes
-- and tags reference. Dropping it with foreign-key enforcement on would
-- cascade-delete all of them (see `Service::migrate`), so refuse to run
-- unless enforcement is off: the CHECK fails, and the migration with it,
-- when it is on.
create temp table fk_guard (foreign_keys_off integer check (foreign_keys_off));
insert into fk_guard select not foreign_keys from pragma_foreign_keys;
drop table fk_guard;

create table queues_new (
  id   integer primary key autoincrement,
  ns   integer not null,
  name text    not null,
  created_by integer,
  paused_at integer,
  created_at integer,
  attributes_modified_at integer,

  foreign key (ns) references namespaces(id) on delete cascade,
  foreign key (created_by) references users(id) on delete set null
);
insert into queues_new (id, ns, name, created_by, paused_at, created_at, attributes_modified_at)
select id, ns, name, created_by, paused_at, created_at, attributes_modified_at from queues;
drop table queues;
alter table queues_new rename to queues;
create unique index if not exists queues_ns_name_idx on queues(ns, name);
