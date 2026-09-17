CREATE TABLE IF NOT EXISTS performance_minute_dimensions
(
    service_id LowCardinality(String),
    bucket_start DateTime('UTC'),
    dimension LowCardinality(String),
    value String,
    requests UInt64,
    latency_count UInt64,
    latency_sum_ms Nullable(Float64),
    latency_p50_ms Nullable(Float64),
    latency_p95_ms Nullable(Float64),
    latency_p99_ms Nullable(Float64),
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/performance_minute_dimensions', '{replica}')
PARTITION BY (service_id, toYYYYMMDD(bucket_start))
ORDER BY (service_id, bucket_start, dimension, value, batch_id);
