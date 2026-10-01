-- Rebuilds `namespaces` like the up migration, so the same guard applies:
-- refuse to run with foreign-key enforcement on.
create temp table fk_guard (foreign_keys_off integer check (foreign_keys_off));
insert into fk_guard select not foreign_keys from pragma_foreign_keys;
drop table fk_guard;

-- `created_by` goes back to NOT NULL, which a namespace whose creator was
-- deleted cannot satisfy. Refuse rather than drop or misattribute it.
create temp table creator_guard (orphaned integer check (orphaned = 0));
insert into creator_guard select count(*) from namespaces where created_by is null;
drop table creator_guard;

alter table users drop column disabled_at;

create table namespaces_old (
  id   integer not null,
  name text    not null,
  created_by integer not null,

  primary key (id),
  foreign key (created_by) references users(id)
);
insert into namespaces_old (id, name, created_by)
select id, name, created_by from namespaces;
drop table namespaces;
alter table namespaces_old rename to namespaces;
create unique index if not exists namespaces_name_idx on namespaces(name);

alter table user_permissions rename column is_owner to can_delete_ns;
