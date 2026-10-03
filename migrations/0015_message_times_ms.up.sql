-- When the queue stored the message and when it was first received, in unix
-- milliseconds. `received_at` and `first_delivered_at` are whole seconds:
-- too coarse for how long a message waits or lives, and for AWS's
-- SentTimestamp and ApproximateFirstReceiveTimestamp, which are
-- milliseconds. NULL for times before this migration; readers fall back to
-- the whole-second column × 1000.
alter table messages add column sent_at_ms integer;
alter table messages add column first_delivered_at_ms integer;
