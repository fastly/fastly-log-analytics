"""The shared ``make_source`` factory keeps the exact dict shapes the ten former ``_make_source`` copies built."""

from __future__ import annotations

from functools import partial

import pytest

from tests._helpers.sources import make_source


def test_lake_kind_creates_cache_dirs_and_duckdb_path(tmp_path):
    src = make_source(tmp_path, "svc", kind="lake", buffer_dir=True)
    assert src == {
        "name": "svc",
        "service_id": "svc",
        "fos_local_warehouse": True,
        "_cache_dir_override": str(tmp_path / "cache_svc"),
        "duckdb_path": str(tmp_path / "svc.duckdb"),
    }
    assert (tmp_path / "cache_svc" / "buffer").is_dir()


def test_lake_kind_without_buffer_dir_only_makes_the_cache_dir(tmp_path):
    make_source(tmp_path, "svc", kind="lake")
    assert (tmp_path / "cache_svc").is_dir()
    assert not (tmp_path / "cache_svc" / "buffer").exists()


def test_lake_kind_applies_overrides_last(tmp_path):
    src = make_source(tmp_path, "svc", kind="lake", fos_local_warehouse=False, extra=1)
    assert src["fos_local_warehouse"] is False
    assert src["extra"] == 1


def test_lake_kind_requires_root_and_name():
    with pytest.raises(TypeError):
        make_source(kind="lake")


def test_cache_kind_defaults_match_the_rollup_reader_tests(tmp_path):
    assert make_source(str(tmp_path)) == {
        "name": "test_service",
        "service_id": "test-service-id",
        "_cache_dir_override": str(tmp_path),
    }


def test_cache_kind_custom_key_and_subdirs(tmp_path):
    root = tmp_path / "cache" / "x"
    src = make_source(root, "x", cache_key="_test_cache_root", mkdirs=("data",))
    assert src == {"name": "x", "service_id": "x", "_test_cache_root": str(root)}
    assert (root / "data").is_dir()


def test_cloud_kind_defaults_and_overrides():
    src = make_source(kind="cloud", bucket="b2", access_key_id="k")
    assert src["name"] == src["service_id"] == "test"
    assert src["bucket"] == "b2" and src["access_key_id"] == "k"
    assert src["storage_mode"] == "cloud" and src["access_level"] == "read_write"
    assert make_source(kind="cloud", access_level="read_only")["access_level"] == "read_only"


def test_bound_presets_never_share_mutable_state_between_calls():
    preset = partial(make_source, kind="cloud", provisioning={})
    first = preset()
    first["provisioning"]["mutated"] = True
    assert preset()["provisioning"] == {}


def test_unknown_kind_is_rejected():
    with pytest.raises(ValueError, match="unknown source kind"):
        make_source(kind="nope")
