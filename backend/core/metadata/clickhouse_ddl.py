"""Additive Postgres control manifest for ADR-20's explicit bounded snapshots."""

CLICKHOUSE_CONTROL_DDL = (
    """
CREATE TABLE IF NOT EXISTS clickhouse_datasets (
    service_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    catalog_identity TEXT NOT NULL,
    source_table TEXT NOT NULL,
    source_snapshot BIGINT NOT NULL CHECK (source_snapshot >= 0),
    coverage_start TIMESTAMPTZ NOT NULL,
    coverage_end TIMESTAMPTZ NOT NULL CHECK (coverage_end >= coverage_start),
    expires_at TIMESTAMPTZ NOT NULL,
    expected_rows BIGINT NOT NULL CHECK (expected_rows BETWEEN 0 AND 1000000),
    canonical_digest TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (service_id, dataset_id)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_artifacts (
    service_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    artifact_uri TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    canonical_digest TEXT NOT NULL,
    byte_size BIGINT NOT NULL CHECK (byte_size BETWEEN 1 AND 16777216),
    ordinal_start BIGINT NOT NULL CHECK (ordinal_start >= 0),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 10000),
    PRIMARY KEY (service_id, dataset_id, batch_id),
    UNIQUE (service_id, dataset_id, ordinal_start),
    FOREIGN KEY (service_id, dataset_id) REFERENCES clickhouse_datasets(service_id, dataset_id)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_generations (
    service_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    target_identity TEXT NOT NULL,
    selection_revision BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'building' CHECK (status IN ('building', 'active', 'retired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    activated_at TIMESTAMPTZ,
    PRIMARY KEY (service_id, generation),
    UNIQUE (service_id, generation, dataset_id),
    FOREIGN KEY (service_id, dataset_id) REFERENCES clickhouse_datasets(service_id, dataset_id)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_publications (
    service_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'claimed', 'published', 'failed')),
    lease_fence BIGINT NOT NULL DEFAULT 0,
    lease_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    first_error TEXT,
    last_error TEXT,
    published_at TIMESTAMPTZ,
    verified_digest TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (service_id, generation, batch_id),
    FOREIGN KEY (service_id, generation, dataset_id)
        REFERENCES clickhouse_generations(service_id, generation, dataset_id),
    FOREIGN KEY (service_id, dataset_id, batch_id)
        REFERENCES clickhouse_artifacts(service_id, dataset_id, batch_id)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_service_selection (
    service_id TEXT PRIMARY KEY,
    generation TEXT,
    revision BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (service_id, generation) REFERENCES clickhouse_generations(service_id, generation)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_schema_state (
    target_identity TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
)
""",
)
