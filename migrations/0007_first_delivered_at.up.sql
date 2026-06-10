-- Records when each message was *first* received by a consumer — the
-- equivalent of AWS SQS's ApproximateFirstReceiveTimestamp system attribute.
-- `delivered_at` is overwritten on every redelivery; this column is stamped
-- once by the first claim and never changes.
alter table messages add column first_delivered_at integer;
