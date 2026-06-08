drop index if exists messages_receipt_handle_idx;
alter table messages drop column receipt_handle;
alter table messages drop column invisible_until;
