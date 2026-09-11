CREATE TABLE IF NOT EXISTS cmcd_projection_facts
(
    service_id LowCardinality(String),
    projection_key UUID,
    request_event_id UUID,
    event_timestamp DateTime64(3, 'UTC'),
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2),
    cmcd Map(String, String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/cmcd_projection_facts', '{replica}')
PARTITION BY (service_id, toYYYYMMDD(event_timestamp))
ORDER BY (service_id, event_timestamp, projection_key);
