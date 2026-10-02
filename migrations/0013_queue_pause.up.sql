-- When set (unix seconds), the queue is paused: it still accepts messages,
-- deletes and visibility changes, but receives return no messages. Used to
-- drain a queue's consumers before swapping them.
alter table queues add column paused_at integer;
