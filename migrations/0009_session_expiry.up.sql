-- Sessions previously stored `ttl` as a bare duration in seconds with no
-- reference point, so a session row could never expire server-side: expiry
-- relied entirely on the cookie's Max-Age, and rows whose cookies were
-- never replayed (or simply discarded) accumulated forever.
--
-- Replace it with an absolute `expires_at` timestamp: loads reject expired
-- rows and a background sweeper deletes them. Existing rows are dropped
-- wholesale rather than backfilled — there is no way to tell a live
-- session from one of the tens of thousands of orphans, so this upgrade
-- logs out all admin sessions once.
delete from sessions;
alter table sessions drop column ttl;
alter table sessions add column expires_at integer not null default 0;
create index if not exists sessions_expires_at_idx on sessions(expires_at);
