"""Identifier validators must reject a trailing newline.

``re.match(r"^[a-z]+$", "abc\\n")`` succeeds because ``$`` also matches before a final
newline. Every gate below must use ``fullmatch`` (or ``\\Z``) so ``"abc\\n"`` is refused
while ``"abc"`` still passes.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.security_regression

NL = "\n"


def test_config_service_id_rejects_trailing_newline():
    from backend.config import _validate_service_id

    assert _validate_service_id("svc-1") == "svc-1"
    with pytest.raises(ValueError):
        _validate_service_id("svc-1" + NL)


def test_config_paths_refuse_newline_service_id():
    from backend.config import config_path, duckdb_path

    for fn in (config_path, duckdb_path):
        with pytest.raises(ValueError):
            fn("svc" + NL)


def test_metadata_service_id_rejects_trailing_newline():
    from backend.core.metadata.base import InvalidServiceIdError, _validate_service_id_or_raise

    _validate_service_id_or_raise("svc_1")
    with pytest.raises(InvalidServiceIdError):
        _validate_service_id_or_raise("svc_1" + NL)


def test_rollups_safe_ident_rejects_trailing_newline():
    from backend.core.rollups._common import _is_safe_ident

    assert _is_safe_ident("logs_svc")
    assert not _is_safe_ident("logs_svc" + NL)


def test_lake_ident_rejects_trailing_newline():
    from backend.core.iceberg.buffer import _lake_ident

    assert _lake_ident("client_vitals") == "client_vitals"
    with pytest.raises(ValueError):
        _lake_ident("client_vitals" + NL)


@pytest.mark.parametrize(
    "call",
    [
        lambda m: m._apply_target_file_size(None, "lake" + NL),
        lambda m: m.ducklake_current_snapshot_id(None, "lake" + NL),
        lambda m: m._ducklake_add_data_files(None, ["s3://b/k"], "lake" + NL, "logs"),
        lambda m: m._ducklake_add_data_files(None, ["s3://b/k"], "lake", "logs" + NL),
    ],
)
def test_ducklake_idents_reject_trailing_newline(call):
    from backend.core.iceberg import _ducklake

    with pytest.raises(ValueError, match="invalid ducklake"):
        call(_ducklake)


def test_custom_field_name_regex_rejects_trailing_newline():
    from backend.core.field_registry import VALID_NAME_RE
    from backend.core.log_fields import validate_custom_field

    field = {
        "name": "my_field" + NL,
        "label": "L",
        "vcl_log_expression": "req.url",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
    }
    errors = validate_custom_field(field, [])
    assert any("lowercase alphanumeric" in e for e in errors)
    assert VALID_NAME_RE.fullmatch("my_field")


def test_custom_field_model_rejects_trailing_newline():
    from pydantic import ValidationError

    from backend.models.custom_fields import CustomField

    body_cls = CustomField
    base = {
        "label": "L",
        "vcl_log_expression": "req.url",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
    }
    body_cls(name="my_field", **base)
    with pytest.raises(ValidationError):
        body_cls(name="my_field" + NL, **base)


def test_declarative_feature_state_rejects_newline_custom_field_name():
    from backend.provision.declarative.state import FeatureState, LogFieldsConfig

    base = {
        "service_id": "svc",
        "log_period": 60,
        "sample_rate": 100,
        "edge_only": False,
        "custom_condition": "",
        "fos_prefix": "",
        "fos_endpoint": "",
    }
    FeatureState(**base, log_fields=LogFieldsConfig(custom_fields=[{"name": "ok_name"}]))
    with pytest.raises(ValueError, match="Custom field name"):
        FeatureState(**base, log_fields=LogFieldsConfig(custom_fields=[{"name": "ok_name" + NL}]))


def test_provision_check_domain_rejects_trailing_newline_in_prefix():
    from backend.routers.provision import provision_check_domain

    result = provision_check_domain(prefix="my-prefix" + NL, is_custom=False)
    assert result["available"] is False
    assert "alphanumeric" in result["reason"]


def test_provision_router_has_no_bare_re_match():
    import inspect

    from backend.routers import provision

    assert "re.match(" not in inspect.getsource(provision)
