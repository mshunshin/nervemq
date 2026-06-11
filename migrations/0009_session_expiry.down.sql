delete from sessions;
drop index if exists sessions_expires_at_idx;
alter table sessions drop column expires_at;
alter table sessions add column ttl integer not null default 0;
