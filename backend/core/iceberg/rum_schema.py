import pyarrow as pa

# PyArrow schemas
CLIENT_VITALS_ARROW_SCHEMA = pa.schema(
    [
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("metric_name", pa.string()),
        ("metric_value", pa.float64()),
        ("metric_rating", pa.string()),
        ("pathname", pa.string()),
        ("browser", pa.string()),
        ("os", pa.string()),
        ("device", pa.string()),
        ("cid", pa.string()),
        ("req_id", pa.string()),
        ("city", pa.string()),
        ("region", pa.string()),
        ("country", pa.string()),
        ("pop", pa.string()),
        ("tls", pa.string()),
        ("ttfb", pa.float64()),
    ]
)

CLIENT_ERRORS_ARROW_SCHEMA = pa.schema(
    [
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("error_message", pa.string()),
        ("error_file", pa.string()),
        ("error_line", pa.int32()),
        ("error_col", pa.int32()),
        ("pathname", pa.string()),
        ("browser", pa.string()),
        ("os", pa.string()),
        ("device", pa.string()),
        ("cid", pa.string()),
        ("req_id", pa.string()),
        ("city", pa.string()),
        ("region", pa.string()),
        ("country", pa.string()),
        ("pop", pa.string()),
        ("tls", pa.string()),
        ("ttfb", pa.float64()),
    ]
)

RUM_TABLE_SCHEMAS = {
    "client_vitals": CLIENT_VITALS_ARROW_SCHEMA,
    "client_errors": CLIENT_ERRORS_ARROW_SCHEMA,
}

RUM_NATURAL_KEYS: dict[str, tuple[str, ...]] = {
    "client_vitals": ("timestamp", "metric_name", "pathname", "cid", "req_id"),
    "client_errors": ("timestamp", "error_message", "error_file", "error_line", "pathname", "cid", "req_id"),
}
