CREATE TABLE IF NOT EXISTS archive_manifest
(
    manifest_id UUID,
    service_id LowCardinality(String),
    domain LowCardinality(String),
    source_object_key String,
    source_checksum String,
    source_version String,
    source_size_bytes UInt64,
    artifact_uri String,
    artifact_checksum String,
    artifact_size_bytes UInt64,
    row_count UInt64,
    byte_count UInt64,
    canonical_event_digest String,
    schema_version LowCardinality(String),
    transform_version LowCardinality(String),
    coverage_start DateTime64(3, 'UTC'),
    coverage_end DateTime64(3, 'UTC'),
    retention_deadline DateTime64(3, 'UTC'),
    deletion_authorization_deadline DateTime64(3, 'UTC'),
    archive_epoch UInt64,
    state Enum8(
        'artifact_uploading' = 1,
        'artifact_verified' = 2,
        'manifest_prepared' = 3,
        'manifest_committed' = 4,
        'deletion_eligible' = 5,
        'source_deleted' = 6
    ),
    committed_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/archive_manifest', '{replica}')
ORDER BY (service_id, domain, source_object_key, manifest_id);
