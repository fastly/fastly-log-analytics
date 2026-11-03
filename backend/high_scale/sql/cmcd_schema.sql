CREATE TABLE IF NOT EXISTS cmcd_projection_facts
(
    service_id LowCardinality(String),
    projection_key UUID,
    request_event_id UUID,
    event_timestamp DateTime64(3, 'UTC'),
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2),
    cmcd Map(String, String),
    country LowCardinality(String),
    asn Nullable(UInt32)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/cmcd_projection_facts', '{replica}')
PARTITION BY toYYYYMMDD(event_timestamp)
ORDER BY (service_id, toStartOfHour(event_timestamp), event_timestamp, projection_key)
TTL toDateTime(event_timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192,
         map_serialization_version = 'with_buckets',
         map_serialization_version_for_zero_level_parts = 'with_buckets',
         map_buckets_strategy = 'constant',
         max_buckets_in_map = 32,
         map_buckets_min_avg_size = 0;

ALTER TABLE cmcd_projection_facts ADD COLUMN IF NOT EXISTS country LowCardinality(String);
ALTER TABLE cmcd_projection_facts ADD COLUMN IF NOT EXISTS asn Nullable(UInt32);
