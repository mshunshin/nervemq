-- When each queue was created and when its attributes last changed, in unix
-- seconds: AWS's CreatedTimestamp and LastModifiedTimestamp. Queues that
-- existed before this migration get the time of the upgrade.
alter table queues add column created_at integer;
alter table queues add column attributes_modified_at integer;
update queues set created_at = unixepoch(), attributes_modified_at = unixepoch();

-- Attributes stored under their internal keys with a value of the wrong
-- type: an integer attribute that isn't an integer, or a redrive policy
-- that isn't a JSON string. Only a request naming an internal key as if it
-- were an AWS attribute could store one. The receive path read such an
-- integer as 0, and reading the queue's attributes failed. Removing the
-- value restores the default.
delete from queue_attributes
where (
    k in (
        'delay_seconds',
        'max_message_size',
        'message_retention_period',
        'receive_message_wait_time_seconds',
        'visibility_timeout'
    )
    and (v = '' or v glob '*[^0-9]*')
)
or (k = 'redrive_policy' and v not like '"%');
