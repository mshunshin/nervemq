-- Namespace ownership, a durable creator record, and disabled users.
--
-- This rebuilds `namespaces`, which queues, permissions and API keys
-- reference. Dropping it with foreign-key enforcement on would cascade-delete
-- all of them (see `Service::migrate`), so refuse to run unless enforcement is
-- off: the CHECK fails, and the migration with it, when it is on.
create temp table fk_guard (foreign_keys_off integer check (foreign_keys_off));
insert into fk_guard select not foreign_keys from pragma_foreign_keys;
drop table fk_guard;

-- A permission row with the flag set makes its user an owner of the
-- namespace: they may delete it and manage its queues. Any number of users,
-- including none, can own a namespace. The column had no NOT NULL, so a NULL
-- counts as "not an owner".
alter table user_permissions rename column can_delete_ns to is_owner;
update user_permissions set is_owner = false where is_owner is null;

-- `created_by` was NOT NULL with no ON DELETE action, so a user who had
-- created a namespace could never be deleted. It now becomes NULL when that
-- user is deleted, and the email is kept alongside it so the record of who
-- created the namespace survives.
create table namespaces_new (
  id   integer not null,
  name text    not null,
  created_by integer,
  created_by_email text,

  primary key (id),
  foreign key (created_by) references users(id) on delete set null
);
insert into namespaces_new (id, name, created_by, created_by_email)
select ns.id, ns.name, ns.created_by, u.email
from namespaces ns
left join users u on u.id = ns.created_by;
drop table namespaces;
alter table namespaces_new rename to namespaces;
create unique index if not exists namespaces_name_idx on namespaces(name);

-- When set (unix seconds), the user can neither log in nor authenticate with
-- their API keys. The account, its keys and its permissions are kept.
alter table users add column disabled_at integer;
