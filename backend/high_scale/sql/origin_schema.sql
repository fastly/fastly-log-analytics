CREATE TABLE IF NOT EXISTS origin_minute_summary
(
    service_id LowCardinality(String),
    bucket_start DateTime('UTC'),
    requests UInt64,
    misses UInt64,
    passes UInt64,
    origin_5xx UInt64,
    status_count UInt64,
    origin_bytes UInt64,
    latency_count UInt64,
    ttlb_count UInt64,
    overhead_count UInt64,
    origin_bytes_count UInt64,
    latency_p50_us Nullable(Float64),
    latency_p75_us Nullable(Float64),
    latency_p95_us Nullable(Float64),
    latency_p99_us Nullable(Float64),
    ttlb_p50_us Nullable(Float64),
    ttlb_p95_us Nullable(Float64),
    cdn_overhead_p50_us Nullable(Float64),
    origin_bytes_p50 Nullable(Float64),
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/origin_minute_summary', '{replica}')
PARTITION BY (service_id, toYYYYMMDD(bucket_start))
ORDER BY (service_id, bucket_start, batch_id);

ALTER TABLE origin_minute_summary ADD COLUMN IF NOT EXISTS status_count UInt64 AFTER origin_5xx;
ALTER TABLE origin_minute_summary ADD COLUMN IF NOT EXISTS ttlb_count UInt64 AFTER latency_count;
ALTER TABLE origin_minute_summary ADD COLUMN IF NOT EXISTS overhead_count UInt64 AFTER ttlb_count;
ALTER TABLE origin_minute_summary ADD COLUMN IF NOT EXISTS origin_bytes_count UInt64 AFTER overhead_count;

CREATE TABLE IF NOT EXISTS origin_minute_dimensions
(
    service_id LowCardinality(String),
    bucket_start DateTime('UTC'),
    dimension LowCardinality(String),
    value String,
    requests UInt64,
    origin_5xx UInt64,
    origin_bytes UInt64,
    latency_count UInt64,
    latency_p50_us Nullable(Float64),
    latency_p95_us Nullable(Float64),
    latency_p99_us Nullable(Float64),
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/origin_minute_dimensions', '{replica}')
PARTITION BY (service_id, toYYYYMMDD(bucket_start))
ORDER BY (service_id, bucket_start, dimension, value, batch_id);
