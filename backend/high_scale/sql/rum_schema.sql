CREATE TABLE IF NOT EXISTS rum_vitals_facts
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
    client_id String,
    request_event_id Nullable(UUID),
    metric_name LowCardinality(String),
    metric_value Float64,
    metric_rating LowCardinality(String),
    pathname String,
    country LowCardinality(String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/rum_vitals_facts', '{replica}')
PARTITION BY (service_id, toYYYYMMDD(event_timestamp))
ORDER BY (service_id, event_timestamp, event_id);

CREATE TABLE IF NOT EXISTS rum_error_facts
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
    client_id String,
    request_event_id Nullable(UUID),
    error_message String,
    error_file String,
    pathname String,
    country LowCardinality(String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/rum_error_facts', '{replica}')
PARTITION BY (service_id, toYYYYMMDD(event_timestamp))
ORDER BY (service_id, event_timestamp, event_id);
