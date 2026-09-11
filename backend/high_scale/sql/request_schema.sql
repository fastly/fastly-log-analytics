CREATE TABLE IF NOT EXISTS request_facts
(
    service_id LowCardinality(String),
    event_id UUID,
    event_timestamp DateTime64(3, 'UTC'),
    ingest_timestamp DateTime64(3, 'UTC'),
    source_object_key String,
    source_object_version String,
    line_ordinal UInt64,
    transform_version LowCardinality(String),
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2),
    country LowCardinality(String),
    client_ip String,
    url String,
    custom_fields Map(String, String),
    cmcd Map(String, String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/request_facts', '{replica}')
PARTITION BY (service_id, toYYYYMMDD(event_timestamp))
ORDER BY (service_id, event_timestamp, event_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS request_aggregates
(
    service_id LowCardinality(String),
    bucket_start DateTime('UTC'),
    dimension LowCardinality(String),
    value String,
    request_count UInt64,
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/request_aggregates', '{replica}')
PARTITION BY (service_id, toYYYYMMDD(bucket_start))
ORDER BY (service_id, bucket_start, dimension, value, batch_id);
