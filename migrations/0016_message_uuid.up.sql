-- The MessageId clients see: a random (v4) UUID, as AWS issues. The integer
-- `id` stays the internal key (send order, kv_pairs) but is never handed
-- out: without AUTOINCREMENT, SQLite reuses the highest id once that row is
-- deleted, and restarts at 1 when the table empties, so a consumer keeping
-- track of the MessageIds it has processed would drop a new message as a
-- duplicate. Messages stored before this migration get a UUID here; the
-- send path sets it on every new one.
alter table messages add column message_id text;

update messages set message_id = lower(
  hex(randomblob(4)) || '-' ||
  hex(randomblob(2)) || '-' ||
  '4' || substr(hex(randomblob(2)), 2) || '-' ||
  substr('89ab', 1 + (random() & 3), 1) || substr(hex(randomblob(2)), 2) || '-' ||
  hex(randomblob(6))
);

create unique index if not exists messages_message_id_idx on messages(message_id);
