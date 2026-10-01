-- The most an API key may do. Its owner's current level still applies, so a
-- key never does more than its owner can:
--
--   member  send, receive and inspect messages in the key's namespace
--   owner   also manage that namespace's queues
--   admin   everything its owner can do, including the admin API if the
--           owner is an admin
--
-- New keys default to the least access. Existing keys get their owner's
-- current level in the key's namespace, which is what they could do until
-- now, so no key changes behaviour.
alter table api_keys add column access text not null default 'member'
  check (access in ('admin', 'owner', 'member'));

update api_keys set access = case
  when (select role from users where users.id = api_keys.user) = 'admin' then 'admin'
  when exists (
    select 1 from user_permissions p
    where p.user = api_keys.user and p.namespace = api_keys.ns and p.is_owner
  ) then 'owner'
  else 'member'
end;
