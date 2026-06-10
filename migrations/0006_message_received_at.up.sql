-- Records when the queue received (stored) each message, the equivalent of
-- AWS SQS's SentTimestamp system attribute. Stamped at insert time by the
-- send path; rows created before this migration stay NULL.
alter table messages add column received_at integer;
