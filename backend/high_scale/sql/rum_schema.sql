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
    country LowCardinality(String),
    browser LowCardinality(String),
    os LowCardinality(String),
    device LowCardinality(String),
    city String,
    region LowCardinality(String),
    pop LowCardinality(String),
    tls LowCardinality(String),
    ttfb Nullable(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/rum_vitals_facts', '{replica}')
PARTITION BY toYYYYMMDD(event_timestamp)
ORDER BY (service_id, toStartOfHour(event_timestamp), event_timestamp)
TTL toDateTime(event_timestamp) + INTERVAL 30 DAY;

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
    country LowCardinality(String),
    error_line UInt32,
    error_col UInt32,
    browser LowCardinality(String),
    os LowCardinality(String),
    device LowCardinality(String),
    city String,
    region LowCardinality(String),
    pop LowCardinality(String),
    tls LowCardinality(String),
    ttfb Nullable(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/rum_error_facts', '{replica}')
PARTITION BY toYYYYMMDD(event_timestamp)
ORDER BY (service_id, toStartOfHour(event_timestamp), event_timestamp)
TTL toDateTime(event_timestamp) + INTERVAL 30 DAY;

ALTER TABLE rum_vitals_facts ADD COLUMN IF NOT EXISTS browser LowCardinality(String);
ALTER TABLE rum_vitals_facts ADD COLUMN IF NOT EXISTS os LowCardinality(String);
ALTER TABLE rum_vitals_facts ADD COLUMN IF NOT EXISTS device LowCardinality(String);
ALTER TABLE rum_vitals_facts ADD COLUMN IF NOT EXISTS city String;
ALTER TABLE rum_vitals_facts ADD COLUMN IF NOT EXISTS region LowCardinality(String);
ALTER TABLE rum_vitals_facts ADD COLUMN IF NOT EXISTS pop LowCardinality(String);
ALTER TABLE rum_vitals_facts ADD COLUMN IF NOT EXISTS tls LowCardinality(String);
ALTER TABLE rum_vitals_facts ADD COLUMN IF NOT EXISTS ttfb Nullable(Float64);
ALTER TABLE rum_error_facts ADD COLUMN IF NOT EXISTS error_line UInt32;
ALTER TABLE rum_error_facts ADD COLUMN IF NOT EXISTS error_col UInt32;
ALTER TABLE rum_error_facts ADD COLUMN IF NOT EXISTS browser LowCardinality(String);
ALTER TABLE rum_error_facts ADD COLUMN IF NOT EXISTS os LowCardinality(String);
ALTER TABLE rum_error_facts ADD COLUMN IF NOT EXISTS device LowCardinality(String);
ALTER TABLE rum_error_facts ADD COLUMN IF NOT EXISTS city String;
ALTER TABLE rum_error_facts ADD COLUMN IF NOT EXISTS region LowCardinality(String);
ALTER TABLE rum_error_facts ADD COLUMN IF NOT EXISTS pop LowCardinality(String);
ALTER TABLE rum_error_facts ADD COLUMN IF NOT EXISTS tls LowCardinality(String);
ALTER TABLE rum_error_facts ADD COLUMN IF NOT EXISTS ttfb Nullable(Float64);
