-- Restore the original (NUMERIC-affinity) queue_tags definition.
create table queue_tags_orig (
  queue integer not null,
  k string not null,
  v string not null,

  primary key (queue, k),
  foreign key (queue) references queues(id) on delete cascade
);

insert into queue_tags_orig (queue, k, v)
select queue, k, v from queue_tags;

drop table queue_tags;
alter table queue_tags_orig rename to queue_tags;

create unique index if not exists queue_tags_queue_key_idx on queue_tags(queue, k);
