drop index if exists messages_receipt_handle_idx;
create index if not exists messages_receipt_handle_idx on messages(receipt_handle);
