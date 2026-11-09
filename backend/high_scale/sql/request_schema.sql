CREATE TABLE IF NOT EXISTS request_facts
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
    country LowCardinality(String),
    client_ip String,
    url String,
    custom_fields Map(String, String),
    cmcd Map(String, String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/request_facts', '{replica}')
PARTITION BY toYYYYMMDD(event_timestamp)
ORDER BY (service_id, toStartOfHour(event_timestamp), event_timestamp)
TTL toDateTime(event_timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192,
         map_serialization_version = 'with_buckets',
         map_serialization_version_for_zero_level_parts = 'with_buckets',
         map_buckets_strategy = 'constant',
         max_buckets_in_map = 64,
         map_buckets_min_avg_size = 0;

ALTER TABLE request_facts
    ADD COLUMN IF NOT EXISTS `_cf__bot_name` LowCardinality(String) MATERIALIZED custom_fields['_bot_name'],
    ADD COLUMN IF NOT EXISTS `_cf_age` String MATERIALIZED custom_fields['age'],
    ADD COLUMN IF NOT EXISTS `_cf_asn` LowCardinality(String) MATERIALIZED custom_fields['asn'],
    ADD COLUMN IF NOT EXISTS `_cf_backend` LowCardinality(String) MATERIALIZED custom_fields['backend'],
    ADD COLUMN IF NOT EXISTS `_cf_bytes` String MATERIALIZED custom_fields['bytes'],
    ADD COLUMN IF NOT EXISTS `_cf_c_speed` LowCardinality(String) MATERIALIZED custom_fields['c_speed'],
    ADD COLUMN IF NOT EXISTS `_cf_c_type` LowCardinality(String) MATERIALIZED custom_fields['c_type'],
    ADD COLUMN IF NOT EXISTS `_cf_cache` LowCardinality(String) MATERIALIZED custom_fields['cache'],
    ADD COLUMN IF NOT EXISTS `_cf_cid` LowCardinality(String) MATERIALIZED custom_fields['cid'],
    ADD COLUMN IF NOT EXISTS `_cf_city` LowCardinality(String) MATERIALIZED custom_fields['city'],
    ADD COLUMN IF NOT EXISTS `_cf_conn_requests` LowCardinality(String) MATERIALIZED custom_fields['conn_requests'],
    ADD COLUMN IF NOT EXISTS `_cf_cookie_session` LowCardinality(String) MATERIALIZED custom_fields['cookie_session'],
    ADD COLUMN IF NOT EXISTS `_cf_delivery_rate` String MATERIALIZED custom_fields['delivery_rate'],
    ADD COLUMN IF NOT EXISTS `_cf_digest` String MATERIALIZED custom_fields['digest'],
    ADD COLUMN IF NOT EXISTS `_cf_edge` LowCardinality(String) MATERIALIZED custom_fields['edge'],
    ADD COLUMN IF NOT EXISTS `_cf_edge_score_reason` LowCardinality(String) MATERIALIZED custom_fields['edge_score_reason'],
    ADD COLUMN IF NOT EXISTS `_cf_edge_sid` LowCardinality(String) MATERIALIZED custom_fields['edge_sid'],
    ADD COLUMN IF NOT EXISTS `_cf_elapsed` String MATERIALIZED custom_fields['elapsed'],
    ADD COLUMN IF NOT EXISTS `_cf_hits` LowCardinality(String) MATERIALIZED custom_fields['hits'],
    ADD COLUMN IF NOT EXISTS `_cf_host` LowCardinality(String) MATERIALIZED custom_fields['host'],
    ADD COLUMN IF NOT EXISTS `_cf_io_input_format` LowCardinality(String) MATERIALIZED custom_fields['io_input_format'],
    ADD COLUMN IF NOT EXISTS `_cf_io_output_format` LowCardinality(String) MATERIALIZED custom_fields['io_output_format'],
    ADD COLUMN IF NOT EXISTS `_cf_ipv6` LowCardinality(String) MATERIALIZED custom_fields['ipv6'],
    ADD COLUMN IF NOT EXISTS `_cf_is_bot` LowCardinality(String) MATERIALIZED custom_fields['is_bot'],
    ADD COLUMN IF NOT EXISTS `_cf_is_ipv6` LowCardinality(String) MATERIALIZED custom_fields['is_ipv6'],
    ADD COLUMN IF NOT EXISTS `_cf_is_shield` LowCardinality(String) MATERIALIZED custom_fields['is_shield'],
    ADD COLUMN IF NOT EXISTS `_cf_is_ssl` LowCardinality(String) MATERIALIZED custom_fields['is_ssl'],
    ADD COLUMN IF NOT EXISTS `_cf_ja3` LowCardinality(String) MATERIALIZED custom_fields['ja3'],
    ADD COLUMN IF NOT EXISTS `_cf_ja4` LowCardinality(String) MATERIALIZED custom_fields['ja4'],
    ADD COLUMN IF NOT EXISTS `_cf_lat` String MATERIALIZED custom_fields['lat'],
    ADD COLUMN IF NOT EXISTS `_cf_lon` String MATERIALIZED custom_fields['lon'],
    ADD COLUMN IF NOT EXISTS `_cf_method` LowCardinality(String) MATERIALIZED custom_fields['method'],
    ADD COLUMN IF NOT EXISTS `_cf_metro` LowCardinality(String) MATERIALIZED custom_fields['metro'],
    ADD COLUMN IF NOT EXISTS `_cf_ngwaf_bot` LowCardinality(String) MATERIALIZED custom_fields['ngwaf_bot'],
    ADD COLUMN IF NOT EXISTS `_cf_ngwaf_bot_category` LowCardinality(String) MATERIALIZED custom_fields['ngwaf_bot_category'],
    ADD COLUMN IF NOT EXISTS `_cf_ngwaf_wellknown_bot` LowCardinality(String) MATERIALIZED custom_fields['ngwaf_wellknown_bot'],
    ADD COLUMN IF NOT EXISTS `_cf_obytes` String MATERIALIZED custom_fields['obytes'],
    ADD COLUMN IF NOT EXISTS `_cf_oconnect_ms` String MATERIALIZED custom_fields['oconnect_ms'],
    ADD COLUMN IF NOT EXISTS `_cf_oip` LowCardinality(String) MATERIALIZED custom_fields['oip'],
    ADD COLUMN IF NOT EXISTS `_cf_oretries` LowCardinality(String) MATERIALIZED custom_fields['oretries'],
    ADD COLUMN IF NOT EXISTS `_cf_ost` LowCardinality(String) MATERIALIZED custom_fields['ost'],
    ADD COLUMN IF NOT EXISTS `_cf_ottfb` String MATERIALIZED custom_fields['ottfb'],
    ADD COLUMN IF NOT EXISTS `_cf_ottlb` String MATERIALIZED custom_fields['ottlb'],
    ADD COLUMN IF NOT EXISTS `_cf_p_desc` LowCardinality(String) MATERIALIZED custom_fields['p_desc'],
    ADD COLUMN IF NOT EXISTS `_cf_p_type` LowCardinality(String) MATERIALIZED custom_fields['p_type'],
    ADD COLUMN IF NOT EXISTS `_cf_ploss` String MATERIALIZED custom_fields['ploss'],
    ADD COLUMN IF NOT EXISTS `_cf_pop` LowCardinality(String) MATERIALIZED custom_fields['pop'],
    ADD COLUMN IF NOT EXISTS `_cf_prid` LowCardinality(String) MATERIALIZED custom_fields['prid'],
    ADD COLUMN IF NOT EXISTS `_cf_proto` LowCardinality(String) MATERIALIZED custom_fields['proto'],
    ADD COLUMN IF NOT EXISTS `_cf_protocol` LowCardinality(String) MATERIALIZED custom_fields['protocol'],
    ADD COLUMN IF NOT EXISTS `_cf_referer` LowCardinality(String) MATERIALIZED custom_fields['referer'],
    ADD COLUMN IF NOT EXISTS `_cf_region` LowCardinality(String) MATERIALIZED custom_fields['region'],
    ADD COLUMN IF NOT EXISTS `_cf_req_header_bytes` String MATERIALIZED custom_fields['req_header_bytes'],
    ADD COLUMN IF NOT EXISTS `_cf_resp_bytes` String MATERIALIZED custom_fields['resp_bytes'],
    ADD COLUMN IF NOT EXISTS `_cf_resp_header_content_encoding` LowCardinality(String) MATERIALIZED custom_fields['resp_header_content_encoding'],
    ADD COLUMN IF NOT EXISTS `_cf_retrans` String MATERIALIZED custom_fields['retrans'],
    ADD COLUMN IF NOT EXISTS `_cf_rid` String MATERIALIZED custom_fields['rid'],
    ADD COLUMN IF NOT EXISTS `_cf_rtt_min` String MATERIALIZED custom_fields['rtt_min'],
    ADD COLUMN IF NOT EXISTS `_cf_rtt_var` String MATERIALIZED custom_fields['rtt_var'],
    ADD COLUMN IF NOT EXISTS `_cf_server_region` LowCardinality(String) MATERIALIZED custom_fields['server_region'],
    ADD COLUMN IF NOT EXISTS `_cf_status` LowCardinality(String) MATERIALIZED custom_fields['status'],
    ADD COLUMN IF NOT EXISTS `_cf_tcp_rtt` String MATERIALIZED custom_fields['tcp_rtt'],
    ADD COLUMN IF NOT EXISTS `_cf_tls` LowCardinality(String) MATERIALIZED custom_fields['tls'],
    ADD COLUMN IF NOT EXISTS `_cf_tls_ciphers_sha` LowCardinality(String) MATERIALIZED custom_fields['tls_ciphers_sha'],
    ADD COLUMN IF NOT EXISTS `_cf_transport` LowCardinality(String) MATERIALIZED custom_fields['transport'],
    ADD COLUMN IF NOT EXISTS `_cf_ttfb` String MATERIALIZED custom_fields['ttfb'],
    ADD COLUMN IF NOT EXISTS `_cf_ttl` String MATERIALIZED custom_fields['ttl'],
    ADD COLUMN IF NOT EXISTS `_cf_ua` LowCardinality(String) MATERIALIZED custom_fields['ua'],
    ADD COLUMN IF NOT EXISTS `_cf_waf` LowCardinality(String) MATERIALIZED custom_fields['waf'],
    ADD COLUMN IF NOT EXISTS `_cf_waf_blocked_rules` LowCardinality(String) MATERIALIZED custom_fields['waf_blocked_rules'],
    ADD COLUMN IF NOT EXISTS `_cf_waf_logged_rules` LowCardinality(String) MATERIALIZED custom_fields['waf_logged_rules'],
    ADD COLUMN IF NOT EXISTS `_cf_waf_ms` String MATERIALIZED custom_fields['waf_ms'],
    ADD COLUMN IF NOT EXISTS `_cf_waf_resp` LowCardinality(String) MATERIALIZED custom_fields['waf_resp'],
    ADD COLUMN IF NOT EXISTS `_cf_waf_sig` LowCardinality(String) MATERIALIZED custom_fields['waf_sig'];

CREATE TABLE IF NOT EXISTS request_aggregates
(
    service_id LowCardinality(String),
    bucket_start DateTime('UTC'),
    dimension LowCardinality(String),
    value String,
    request_count UInt64,
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/request_aggregates', '{replica}')
PARTITION BY toYYYYMMDD(bucket_start)
ORDER BY (service_id, bucket_start, dimension, value, batch_id)
TTL bucket_start + INTERVAL 30 DAY;

ALTER TABLE request_aggregates ADD PROJECTION IF NOT EXISTS proj_by_dimension
(
    SELECT
        service_id,
        dimension,
        bucket_start,
        value,
        batch_id,
        publication_state,
        sum(request_count)
    GROUP BY
        service_id,
        dimension,
        bucket_start,
        value,
        batch_id,
        publication_state
);

CREATE TABLE IF NOT EXISTS request_aggregates_1h
(
    service_id LowCardinality(String),
    bucket_start DateTime('UTC'),
    dimension LowCardinality(String),
    value String,
    request_count UInt64
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/request_aggregates_1h', '{replica}')
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (service_id, dimension, bucket_start, value)
TTL bucket_start + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS request_aggregates_1h_mv
TO request_aggregates_1h
AS SELECT
    service_id,
    toStartOfHour(bucket_start) AS bucket_start,
    dimension,
    value,
    sum(request_count) AS request_count
FROM request_aggregates
WHERE publication_state = 'visible'
GROUP BY
    service_id,
    bucket_start,
    dimension,
    value;

CREATE TABLE IF NOT EXISTS request_insight_hourly
(
    service_id LowCardinality(String),
    bucket_start DateTime('UTC'),
    shape LowCardinality(String),
    batch_id UUID,
    publication_state Enum8('pending' = 1, 'visible' = 2),
    url String,
    country LowCardinality(String),
    cf_status LowCardinality(String),
    cf_cache LowCardinality(String),
    cf_transport LowCardinality(String),
    cf_ost LowCardinality(String),
    cf_p_type LowCardinality(String),
    cf_method LowCardinality(String),
    cf_c_type LowCardinality(String),
    cf_c_speed LowCardinality(String),
    cf_city LowCardinality(String),
    cf_region LowCardinality(String),
    cf_asn LowCardinality(String),
    cf_ua LowCardinality(String),
    cf_referer LowCardinality(String),
    cf_waf_sig LowCardinality(String),
    request_count UInt64
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/request_insight_hourly', '{replica}')
PARTITION BY toYYYYMMDD(bucket_start)
ORDER BY (service_id, shape, bucket_start, batch_id, publication_state,
          url, country, cf_status, cf_cache, cf_transport, cf_ost, cf_p_type, cf_method, cf_c_type, cf_c_speed, cf_city, cf_region, cf_asn, cf_ua, cf_referer, cf_waf_sig)
TTL bucket_start + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS request_insight_hourly_mv
TO request_insight_hourly
AS SELECT service_id, bucket_start, k.1 AS shape, batch_id, publication_state,
    k.2 AS url, k.3 AS country, k.4 AS cf_status, k.5 AS cf_cache, k.6 AS cf_transport, k.7 AS cf_ost, k.8 AS cf_p_type, k.9 AS cf_method, k.10 AS cf_c_type, k.11 AS cf_c_speed, k.12 AS cf_city, k.13 AS cf_region, k.14 AS cf_asn, k.15 AS cf_ua, k.16 AS cf_referer, k.17 AS cf_waf_sig,
    count() AS request_count
FROM (
    SELECT service_id, toStartOfHour(event_timestamp) AS bucket_start, batch_id, publication_state,
    [('url_status', url, '', custom_fields['status'], '', '', '', '', '', '', '', '', '', '', '', '', ''),
     ('url_cache', url, '', '', custom_fields['cache'], '', '', '', '', '', '', '', '', '', '', '', ''),
     ('misc', '', '', '', custom_fields['cache'], custom_fields['transport'], custom_fields['ost'], custom_fields['p_type'], custom_fields['method'], custom_fields['c_type'], custom_fields['c_speed'], '', '', '', '', '', ''),
     ('geo', '', CAST(country AS String), custom_fields['status'], '', '', '', '', '', '', '', custom_fields['city'], custom_fields['region'], '', '', '', ''),
     ('asn', '', '', '', '', '', '', custom_fields['p_type'], '', '', '', '', '', custom_fields['asn'], '', '', ''),
     ('ua', '', '', '', '', '', '', '', '', '', '', '', '', '', custom_fields['ua'], '', ''),
     ('referer', '', '', '', '', '', '', '', '', '', '', '', '', '', '', custom_fields['referer'], ''),
     ('waf', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', custom_fields['waf_sig'])] AS keys
    FROM request_facts
)
ARRAY JOIN keys AS k
GROUP BY service_id, bucket_start, shape, batch_id, publication_state, url, country, cf_status, cf_cache, cf_transport, cf_ost, cf_p_type, cf_method, cf_c_type, cf_c_speed, cf_city, cf_region, cf_asn, cf_ua, cf_referer, cf_waf_sig;
