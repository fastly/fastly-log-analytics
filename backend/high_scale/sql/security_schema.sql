CREATE TABLE IF NOT EXISTS security_minute_dimensions
(
    service_id LowCardinality(String),
    bucket_start DateTime('UTC'),
    dimension LowCardinality(String),
    value String,
    requests UInt64,
    wellknown_bot_name String,
    bot_category String,
    verified_count UInt64,
    impersonator_count UInt64,
    unverified_count UInt64,
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/security_minute_dimensions', '{replica}')
PARTITION BY (service_id, toYYYYMMDD(bucket_start))
ORDER BY (service_id, bucket_start, dimension, value, batch_id);
