CREATE TABLE IF NOT EXISTS network_minute_dimensions
(
    service_id LowCardinality(String),
    bucket_start DateTime('UTC'),
    dimension LowCardinality(String),
    value String,
    c_speed LowCardinality(String),
    requests UInt64,
    errors UInt64,
    tcp_rtt_count UInt64,
    tcp_rtt_sum Nullable(Float64),
    tcp_rtt_p50_us Nullable(Float64),
    tcp_rtt_p95_us Nullable(Float64),
    tcp_rtt_p99_us Nullable(Float64),
    ploss_sum Nullable(Float64),
    ploss_count UInt64,
    ttfb_p50_us Nullable(Float64),
    ttfb_p95_us Nullable(Float64),
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/network_minute_dimensions', '{replica}')
PARTITION BY (service_id, toYYYYMMDD(bucket_start))
ORDER BY (service_id, bucket_start, dimension, value, c_speed, batch_id);
