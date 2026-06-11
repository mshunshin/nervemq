-- Rebuild the receipt-handle index as a partial index. The original full
-- index mostly contained NULLs (every never-received message), so ANALYZE
-- statistics rated it unselective and the planner answered the ack-path
-- queries (DeleteMessage, DeleteMessageBatch, ChangeMessageVisibility:
-- `WHERE queue = ? AND receipt_handle = ?`) with a scan of the whole
-- queue's messages instead — O(backlog) per acknowledgement. Excluding the
-- NULLs makes the index small and obviously selective, and the plan becomes
-- a point lookup.
drop index if exists messages_receipt_handle_idx;
create index if not exists messages_receipt_handle_idx
  on messages(receipt_handle) where receipt_handle is not null;
