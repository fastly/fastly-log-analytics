"""Boot-time gate on incoherent ``INGEST_MODE=celery`` configuration.

``config.validate_ingest_mode()`` is the only thing standing between a
half-configured celery deployment and a fleet that degrades invisibly. It
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
def celery_env(monkeypatch):
    """A fully coherent celery-mode configuration. Each test knocks out the
    one piece it is asserting on, so a passing test proves that piece is
    load-bearing rather than that some unrelated field was missing."""
    monkeypatch.setattr(svcconfig, "INGEST_MODE", "celery")
    monkeypatch.setattr(svcconfig, "CELERY_BROKER_URL", "redis://valkey:6379/0")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", _PG)
    monkeypatch.setenv("METADATA_DSN", _PG)
    return monkeypatch


def test_sync_mode_never_validates(monkeypatch):
    """Sync mode is the single-pod default and requires none of the three."""
    monkeypatch.setattr(svcconfig, "INGEST_MODE", "sync")
    monkeypatch.setattr(svcconfig, "CELERY_BROKER_URL", "")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "")
    monkeypatch.delenv("METADATA_DSN", raising=False)

    assert svcconfig.validate_ingest_mode() is None


def test_coherent_celery_config_passes(celery_env):
    assert svcconfig.validate_ingest_mode() is None


def test_celery_requires_broker(celery_env):
    celery_env.setattr(svcconfig, "CELERY_BROKER_URL", "")

    with pytest.raises(RuntimeError, match="requires CELERY_BROKER_URL"):
        svcconfig.validate_ingest_mode()


@pytest.mark.parametrize(
    "catalog",
    ["", "/app/data/services/svc.ducklake", "svc.ducklake"],
    ids=["unset", "absolute-file", "relative-file"],
)
def test_celery_rejects_non_postgres_ducklake_catalog(celery_env, catalog):
    celery_env.setattr(svcconfig, "DUCKLAKE_CATALOG", catalog)

    with pytest.raises(RuntimeError, match="requires DUCKLAKE_CATALOG to be a Postgres DSN"):
        svcconfig.validate_ingest_mode()


@pytest.mark.parametrize(
    "dsn",
    [None, "", "/app/data/services/svc.metadata.db", "sqlite:///app/data/svc.metadata.db"],
    ids=["unset", "empty", "sqlite-path", "sqlite-url"],
)
def test_celery_rejects_non_postgres_metadata_dsn(celery_env, dsn):
    """The gate ADR-15 claimed existed. Per-pod SQLite metadata in celery
    mode is the failure this prevents: it boots clean and then every worker
    re-discovers the same objects because no lease is shared."""
    if dsn is None:
        celery_env.delenv("METADATA_DSN", raising=False)
    else:
        celery_env.setenv("METADATA_DSN", dsn)

    with pytest.raises(RuntimeError, match="requires METADATA_DSN to be a Postgres DSN"):
        svcconfig.validate_ingest_mode()


@pytest.mark.parametrize("scheme", ["postgres", "postgresql"])
def test_both_postgres_url_schemes_accepted(celery_env, scheme):
    """libpq accepts both spellings; the gate must not reject the short one
    and send an operator hunting a phantom misconfiguration."""
    dsn = f"{scheme}://fla:pw@pg:5432/db"
    celery_env.setattr(svcconfig, "DUCKLAKE_CATALOG", dsn)
    celery_env.setenv("METADATA_DSN", dsn)

    assert svcconfig.validate_ingest_mode() is None


def test_metadata_dsn_error_names_the_shared_state_at_risk(celery_env):
    """The message must explain WHY, matching the DuckLake error's style —
    an operator seeing it should not need to read the source to know what
    breaks."""
    celery_env.delenv("METADATA_DSN", raising=False)

    with pytest.raises(RuntimeError) as exc:
        svcconfig.validate_ingest_mode()

    msg = str(exc.value)
    assert "job_runs" in msg
    assert "ingest ledger" in msg
    assert "pod-local" in msg
