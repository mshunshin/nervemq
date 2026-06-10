-- Store queue tags as text, verbatim.
--
-- `queue_tags` declared its key/value columns as `string`, which SQLite does
-- not recognize as a type name, so both columns silently got NUMERIC
-- affinity. Numeric-looking tag values ("1", "2.50") were therefore coerced
-- to INTEGER/REAL on insert, and reading them back as text failed — turning
-- ListQueueTags into a 500 for any queue carrying such a tag. Rebuild the
-- table with TEXT affinity (SQLite cannot alter a column's type in place).
create table queue_tags_text (
  queue integer not null,
  k text not null,
  v text not null,

  primary key (queue, k),
  foreign key (queue) references queues(id) on delete cascade
);

-- Carry existing rows over. Values already coerced to numbers can only be
-- recovered as their canonical text rendering ("01" was stored as 1 and
-- comes back as "1") — the original spelling is gone.
insert into queue_tags_text (queue, k, v)
select queue, cast(k as text), cast(v as text) from queue_tags;

drop table queue_tags;
alter table queue_tags_text rename to queue_tags;

create unique index if not exists queue_tags_queue_key_idx on queue_tags(queue, k);
