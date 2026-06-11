-- Recreates the session tables in the main database as migration 0009
-- left them.
create table if not exists sessions (
  id integer not null,
  session_key text not null,
  expires_at integer not null default 0,

  primary key (id)
);
create unique index if not exists sessions_key_idx on sessions(session_key);
create index if not exists sessions_expires_at_idx on sessions(expires_at);

create table if not exists session_state (
  session integer not null,
  k text not null,
  v text not null,

  primary key (session, k),
  foreign key (session) references sessions(id) on delete cascade
);
create unique index if not exists sessions_kv_idx on session_state(session, k);
