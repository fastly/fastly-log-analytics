CREATE TABLE IF NOT EXISTS cmcd_aggregates
(
    service_id LowCardinality(String),
    bucket_start DateTime('UTC'),
    dimension LowCardinality(String),
    value String,
    event_count UInt64,
    session_count UInt64,
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/cmcd_aggregates', '{replica}')
PARTITION BY (service_id, toYYYYMMDD(bucket_start))
ORDER BY (service_id, bucket_start, dimension, value, batch_id);
