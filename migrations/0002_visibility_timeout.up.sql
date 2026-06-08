-- Visibility timeout support.
--
-- A message is *available* when `invisible_until IS NULL OR invisible_until <= now`.
-- Receiving a message stamps `invisible_until = now + visibility_timeout`, increments
-- `tries`, and assigns a fresh `receipt_handle` token. If the consumer does not delete
-- the message before the window expires, the row becomes available again automatically.
alter table messages add column invisible_until integer;
alter table messages add column receipt_handle text;

create index if not exists messages_receipt_handle_idx on messages(receipt_handle);
