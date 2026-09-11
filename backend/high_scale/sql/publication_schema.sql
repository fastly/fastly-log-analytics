CREATE TABLE IF NOT EXISTS high_scale_batch_publications
(
    batch_id String,
    service_id LowCardinality(String),
    domain LowCardinality(String),
    generation String,
    batch_digest String,
    expected_rows UInt64,
    visible_rows UInt64,
    quorum_acked UInt8,
    publication_state Enum8('pending' = 1, 'visible' = 2),
    manifest_version UInt64,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplicatedReplacingMergeTree(manifest_version)
('/clickhouse/tables/{shard}/high_scale_batch_publications', '{replica}')
ORDER BY (service_id, domain, generation, batch_id);

-- Fact and aggregate readers must join or filter against the latest
-- publication row and require publication_state = 'visible'. The external
-- manifest store remains authoritative for retries and replay.
