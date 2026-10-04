drop index if exists messages_message_id_idx;
alter table messages drop column message_id;
