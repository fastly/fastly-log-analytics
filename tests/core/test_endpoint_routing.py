"""Regression tests for FOS endpoint routing.

These guard the contract that boto3 (ListObjectsV2/PutObject/DeleteObject)
and PyIceberg (commits) always hit the native FOS endpoint, while DuckDB's
httpfs path is free to route through the CDN for cached parquet GETs. The
CDN VCL only proxies object-key GET/HEAD requests; using it for lists or
writes returns NoSuchKey/4xx and silently breaks sync, storage stats, and
iceberg metadata reads.
"""

from backend import config as svcconfig


def _cfg(with_cdn: bool) -> dict:
    base = {
        "service_id": "svc1",
        "name": "Test Service",
        "fos_region": "us-east-1",
        "fos_endpoint": "us-east-1.object.fastlystorage.app",
        "fos_access_key_id": "k",
        "fos_secret_access_key": "s",
        "fos_bucket": "test-bucket",
        "fos_prefix": "",
    }
    if with_cdn:
        base["cdn_url"] = "https://cdn.example.com"
        base["cdn_secret"] = "abc123"
        base["cdn_service_id"] = "cdn-svc"
    return base


class TestConfigToSource:
    def test_native_endpoint_always_present(self):
        src = svcconfig.config_to_source(_cfg(with_cdn=False))
        assert src["fos_native_endpoint"] == "us-east-1.object.fastlystorage.app"
        assert src["endpoint"] == "us-east-1.object.fastlystorage.app"

    def test_cdn_routed_endpoint_keeps_native_separate(self):
        """When a CDN is configured, source['endpoint'] points at the CDN
        (used by DuckDB httpfs for cached parquet GETs) but
        source['fos_native_endpoint'] remains the native FOS host so boto3
        and PyIceberg can do lists/writes/deletes."""
        src = svcconfig.config_to_source(_cfg(with_cdn=True))
        assert src["endpoint"] == "cdn.example.com"
        assert src["fos_native_endpoint"] == "us-east-1.object.fastlystorage.app"
        assert src["fos_native_endpoint"] != src["endpoint"]


class TestPyIcebergEndpointSelection:
    """PyIceberg commits new metadata.json files on every snapshot — those
    PUTs/DELETEs must not go through the CDN."""

    def test_catalog_uses_native_endpoint(self, monkeypatch):
        captured: dict = {}

        class _StubCatalog:
            def __init__(self, _name, **props):
                captured.update(props)

        from backend.core import iceberg as _ic

        monkeypatch.setattr(_ic, "_catalog_cache", {})

        from pyiceberg.catalog import sql as _sql_mod

        monkeypatch.setattr(_sql_mod, "SqlCatalog", _StubCatalog)

        src = svcconfig.config_to_source(_cfg(with_cdn=True))
        # _catalog_db_path/_warehouse_uri need a duckdb_path; supply one.
        src["duckdb_path"] = "/tmp/_endpoint_routing_test.duckdb"
        _ic._get_catalog(src)

        assert captured["s3.endpoint"] == "https://us-east-1.object.fastlystorage.app"
