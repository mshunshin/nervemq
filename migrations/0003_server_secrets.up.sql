-- Server-internal secrets generated on first run, e.g. the session cookie
-- signing key. Persisting these across restarts keeps existing sessions valid.
create table if not exists server_secrets (
  name  text not null,
  value blob not null,

  primary key (name)
);
