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
PARTITION BY toYYYYMMDD(event_timestamp)
ORDER BY (service_id, toStartOfHour(event_timestamp), event_timestamp)
TTL toDateTime(event_timestamp) + INTERVAL 30 DAY
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
PARTITION BY toYYYYMMDD(bucket_start)
ORDER BY (service_id, bucket_start, dimension, value, batch_id)
TTL bucket_start + INTERVAL 30 DAY;

ALTER TABLE request_aggregates ADD PROJECTION IF NOT EXISTS proj_by_dimension
(
    SELECT
        service_id,
        dimension,
        bucket_start,
        value,
        batch_id,
        publication_state,
        sum(request_count)
    GROUP BY
        service_id,
        dimension,
        bucket_start,
        value,
        batch_id,
        publication_state
);

CREATE TABLE IF NOT EXISTS request_aggregates_1h
(
    service_id LowCardinality(String),
    bucket_start DateTime('UTC'),
    dimension LowCardinality(String),
    value String,
    request_count UInt64
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/request_aggregates_1h', '{replica}')
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (service_id, dimension, bucket_start, value)
TTL bucket_start + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS request_aggregates_1h_mv
TO request_aggregates_1h
AS SELECT
    service_id,
    toStartOfHour(bucket_start) AS bucket_start,
    dimension,
    value,
    sum(request_count) AS request_count
FROM request_aggregates
WHERE publication_state = 'visible'
GROUP BY
    service_id,
    bucket_start,
    dimension,
    value;
