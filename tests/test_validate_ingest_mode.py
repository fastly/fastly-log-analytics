"""Boot-time gate on incoherent ``DEPLOYMENT_MODE=high_scale`` configuration and rejection of legacy ``high_throughput``.

``config.validate_deployment_mode()`` is the only thing standing between a
half-configured high-scale deployment and a fleet that degrades invisibly. It
runs from BOTH entry points — the backend lifespan (``main.py``) and
``worker_process_init`` in ``celery_app.py`` — so every process refuses to
start rather than one of them silently operating on pod-local state.

The three requirements it enforces, and why each is fatal rather than a
warning:

- ``CELERY_BROKER_URL`` — discovery would dispatch tasks into nothing.
- a Postgres ``DUCKLAKE_CATALOG`` — a DuckDB-file catalog is single-process,
  so concurrent worker writers tear it (ADR-14).
- a Postgres ``METADATA_DSN`` — per-service SQLite is a pod-local file, so
  the cron lease, ingest ledger, and ingested-file manifest would each be
  private to one process and nothing would serialize the fleet (ADR-15).

``METADATA_DSN`` is read from the environment (not a module constant) so it
cannot disagree with ``metadata.pg_connection.is_postgres()``; these tests
monkeypatch it via ``setenv``/``delenv`` accordingly.
"""

from __future__ import annotations

import pytest

from backend import config as svcconfig

_PG = "postgresql://fla:pw@pg:5432/ducklake"


@pytest.fixture
def high_scale_env(monkeypatch):
    """A fully coherent high_scale configuration."""
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_scale")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", _PG)
    monkeypatch.setenv("METADATA_DSN", _PG)
    return monkeypatch


def test_sync_mode_requires_postgres_dsns_but_no_broker(monkeypatch):
    """Standard mode requires Postgres DSNs, but does not require CELERY_BROKER_URL."""
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "standard")
    monkeypatch.setattr(svcconfig, "CELERY_BROKER_URL", "")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", _PG)
    monkeypatch.setenv("METADATA_DSN", _PG)

    assert svcconfig.validate_deployment_mode() is None


def test_coherent_high_scale_config_passes(high_scale_env):
    assert svcconfig.validate_deployment_mode() is None


def test_high_throughput_is_rejected(monkeypatch):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_throughput")
    with pytest.raises(
        RuntimeError, match="DEPLOYMENT_MODE=high_throughput has been removed; use 'standard' or 'high_scale'"
    ):
        svcconfig.validate_deployment_mode()


@pytest.mark.parametrize(
    "catalog",
    ["", "/app/data/services/svc.ducklake", "svc.ducklake"],
    ids=["unset", "absolute-file", "relative-file"],
)
def test_high_scale_rejects_non_postgres_ducklake_catalog(high_scale_env, catalog):
    high_scale_env.setattr(svcconfig, "DUCKLAKE_CATALOG", catalog)

    with pytest.raises(RuntimeError, match="requires DUCKLAKE_CATALOG to be a Postgres DSN"):
        svcconfig.validate_deployment_mode()


@pytest.mark.parametrize(
    "dsn",
    [None, "", "/app/data/services/svc.metadata.db", "sqlite:///app/data/svc.metadata.db"],
    ids=["unset", "empty", "sqlite-path", "sqlite-url"],
)
def test_high_scale_rejects_non_postgres_metadata_dsn(high_scale_env, dsn):
    if dsn is None:
        high_scale_env.delenv("METADATA_DSN", raising=False)
    else:
        high_scale_env.setenv("METADATA_DSN", dsn)

    with pytest.raises(RuntimeError, match="requires METADATA_DSN to be a Postgres DSN"):
        svcconfig.validate_deployment_mode()


@pytest.mark.parametrize("scheme", ["postgres", "postgresql"])
def test_both_postgres_url_schemes_accepted(high_scale_env, scheme):
    """libpq accepts both spellings; the gate must not reject the short one
    and send an operator hunting a phantom misconfiguration."""
    dsn = f"{scheme}://fla:pw@pg:5432/db"
    high_scale_env.setattr(svcconfig, "DUCKLAKE_CATALOG", dsn)
    high_scale_env.setenv("METADATA_DSN", dsn)

    assert svcconfig.validate_deployment_mode() is None


def test_metadata_dsn_error_names_the_shared_state_at_risk(high_scale_env):
    """The message must explain WHY, matching the DuckLake error's style —
    an operator seeing it should not need to read the source to know what
    breaks."""
    high_scale_env.delenv("METADATA_DSN", raising=False)

    with pytest.raises(RuntimeError) as exc:
        svcconfig.validate_deployment_mode()

    msg = str(exc.value)
    assert "job_runs" in msg
    assert "ingest ledger" in msg
    assert "pod-local" in msg


def test_unknown_deployment_mode_is_rejected(monkeypatch):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "unknown")

    with pytest.raises(RuntimeError, match="DEPLOYMENT_MODE"):
        svcconfig.validate_deployment_mode()


@pytest.mark.parametrize("mode", ["standard", "high_scale"])
def test_config_to_source_emits_deployment_mode(monkeypatch, tmp_path, mode):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", mode)
    monkeypatch.setattr(svcconfig, "duckdb_path", lambda _service_id: str(tmp_path / "service.duckdb"))

    source = svcconfig.config_to_source({"service_id": "svc", "raw_layout_version": 3})

    assert source["deployment_mode"] == mode
    assert "ingest_mode" not in source
    assert "serving_mode" not in source
