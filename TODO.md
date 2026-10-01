# TODO

Open decisions. Each entry records what is known, so it can be picked up later.

## Database size: retention and reclaiming space

**Status:** undecided. Measured 2026-10-02.

### What happens now

- **Deleted messages do not make the file grow without bound.** Freed pages
  are reused. Measured: 30,000 messages of 4 KB sent and then all deleted left
  `nervemq.db` at 132 MB, almost all of it free pages (33,827 of 33,905). A
  second identical burst reused them, and the file stayed at 132 MB.
- **Free space goes back to the operating system slowly.**
  `Service::spawn_db_maintenance` (`src/service.rs`) runs every 10 minutes:
  1. deletes messages past their queue's `MessageRetentionPeriod`;
  2. runs `PRAGMA incremental_vacuum(1000)`, reclaiming at most 1,000 pages
     (about 4 MB).

  So the 132 MB above takes about 5.5 hours to shrink back, and a 1 GB backlog
  about 43 hours. (`auto_vacuum = INCREMENTAL` applies to every database:
  databases have used `FULL` since the first version, which allows the
  switch.)
- **The WAL file keeps its largest size.** No `journal_size_limit` is set, so
  after a checkpoint `nervemq.db-wal` is reused but not truncated (9.7 MB in
  the run above).
- **Admin sessions** (`sessions.db`) are garbage-collected when they expire.

### What can grow without bound

Messages nobody deletes:

- **There is no default retention.** The sweep only touches queues with an
  explicit `MessageRetentionPeriod` attribute. AWS defaults to 4 days
  (345,600 s).
- **Messages that used up their retries** (`tries >= max_retries`) in a queue
  without a dead-letter queue are never delivered again, and stay until the
  retention sweep removes them (only if the queue has a retention period) or
  someone clears them in the UI.

### Options

1. **Default retention of 4 days**, as on AWS, for queues without one. This is
   the only option that bounds the database. It changes behaviour: unconsumed
   messages in such queues would start expiring.
2. **Faster reclamation:** reclaim more pages per tick when there is a lot of
   free space (e.g. scale with `PRAGMA freelist_count`, or reclaim everything
   above a threshold). The cost is short bursts of write work, holding the
   write lock, during maintenance.
3. **Cap the WAL:** set `journal_size_limit` (e.g. 64 MB) in the connect
   options, so the WAL shrinks after checkpoints.

Options 2 and 3 only affect how much disk the file holds on to after a
backlog. Option 1 is what decides whether it is bounded at all.

### To reproduce the measurement

`just bench` deletes its throwaway database at the end, so use a server you
keep:

```sh
export NERVEMQ_ROOT_PASSWORD=change-me
d=$(mktemp -d)
nervemq --data-dir "$d" namespace add growth
nervemq --data-dir "$d" apikey add --name growth --namespace growth \
    --access-key GROWTHKEY --secret-key growth-secret
nervemq --data-dir "$d" &                      # the server, on :8080

AWS_ACCESS_KEY_ID=GROWTHKEY AWS_SECRET_ACCESS_KEY=growth-secret \
    cargo run --release -p nervemq-example --bin benchmark -- \
    --endpoint http://localhost:8080/api/sqs --messages 30000 --payload-bytes 4096

ls -l "$d"                                     # nervemq.db and nervemq.db-wal
sqlite3 "$d/nervemq.db" 'PRAGMA page_count; PRAGMA freelist_count;'
```
