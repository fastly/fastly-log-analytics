import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)

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

# PyIceberg schemas
CLIENT_VITALS_ICEBERG_SCHEMA = Schema(
    NestedField(field_id=1, name="timestamp", field_type=TimestampType(), required=True),
    NestedField(field_id=2, name="metric_name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="metric_value", field_type=DoubleType(), required=True),
    NestedField(field_id=4, name="metric_rating", field_type=StringType(), required=False),
    NestedField(field_id=5, name="pathname", field_type=StringType(), required=False),
    NestedField(field_id=6, name="browser", field_type=StringType(), required=False),
    NestedField(field_id=7, name="os", field_type=StringType(), required=False),
    NestedField(field_id=8, name="device", field_type=StringType(), required=False),
    NestedField(field_id=9, name="cid", field_type=StringType(), required=False),
    NestedField(field_id=10, name="req_id", field_type=StringType(), required=False),
    NestedField(field_id=11, name="city", field_type=StringType(), required=False),
    NestedField(field_id=12, name="region", field_type=StringType(), required=False),
    NestedField(field_id=13, name="country", field_type=StringType(), required=False),
    NestedField(field_id=14, name="pop", field_type=StringType(), required=False),
    NestedField(field_id=15, name="tls", field_type=StringType(), required=False),
    NestedField(field_id=16, name="ttfb", field_type=DoubleType(), required=False),
)

CLIENT_ERRORS_ICEBERG_SCHEMA = Schema(
    NestedField(field_id=1, name="timestamp", field_type=TimestampType(), required=True),
    NestedField(field_id=2, name="error_message", field_type=StringType(), required=True),
    NestedField(field_id=3, name="error_file", field_type=StringType(), required=False),
    NestedField(field_id=4, name="error_line", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="error_col", field_type=IntegerType(), required=False),
    NestedField(field_id=6, name="pathname", field_type=StringType(), required=False),
    NestedField(field_id=7, name="browser", field_type=StringType(), required=False),
    NestedField(field_id=8, name="os", field_type=StringType(), required=False),
    NestedField(field_id=9, name="device", field_type=StringType(), required=False),
    NestedField(field_id=10, name="cid", field_type=StringType(), required=False),
    NestedField(field_id=11, name="req_id", field_type=StringType(), required=False),
    NestedField(field_id=12, name="city", field_type=StringType(), required=False),
    NestedField(field_id=13, name="region", field_type=StringType(), required=False),
    NestedField(field_id=14, name="country", field_type=StringType(), required=False),
    NestedField(field_id=15, name="pop", field_type=StringType(), required=False),
    NestedField(field_id=16, name="tls", field_type=StringType(), required=False),
    NestedField(field_id=17, name="ttfb", field_type=DoubleType(), required=False),
)

RUM_ICEBERG_SCHEMAS = {
    "client_vitals": CLIENT_VITALS_ICEBERG_SCHEMA,
    "client_errors": CLIENT_ERRORS_ICEBERG_SCHEMA,
}
