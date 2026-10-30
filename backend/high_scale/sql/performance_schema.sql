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
PARTITION BY toYYYYMMDD(bucket_start)
ORDER BY (service_id, bucket_start, dimension, value, batch_id)
TTL bucket_start + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS performance_minute_summary
(
    service_id LowCardinality(String),
    bucket_start DateTime('UTC'),
    component_count UInt64,
    edge_processing_sum_ms Float64,
    origin_wait_sum_ms Float64,
    origin_download_sum_ms Float64,
    client_download_sum_ms Float64,
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/performance_minute_summary', '{replica}')
PARTITION BY toYYYYMMDD(bucket_start)
ORDER BY (service_id, bucket_start, batch_id)
TTL bucket_start + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS performance_scatter_samples
(
    service_id LowCardinality(String),
    bucket_start DateTime('UTC'),
    sample_key UInt64,
    origin_ms Float64,
    edge_ms Float64,
    cache LowCardinality(String),
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/performance_scatter_samples', '{replica}')
PARTITION BY toYYYYMMDD(bucket_start)
ORDER BY (service_id, bucket_start, sample_key, batch_id)
TTL bucket_start + INTERVAL 30 DAY;
