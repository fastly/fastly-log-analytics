"""Centralised pydantic-settings module — one place that knows every env var.

Phase 3.5 of the v2.0 cleanup plan. Today's call sites read env vars ad-hoc
via :func:`os.environ.get`; this module is the migration target for those
reads. Adoption is incremental — each phase that touches a call site
swaps it from ``os.environ.get("FOO")`` to ``settings.foo`` and drops the
inline default.

Until full adoption lands, this module is **additive**: importing it has
zero side effects beyond a single ``Settings()`` instantiation cost, and
it does NOT mutate any global state. Code that hasn't migrated yet keeps
reading ``os.environ`` directly — both shapes coexist by design.

The Phase 0 baseline grep (``pending-docs/baseline/<ts>/summary.txt``)
catalogues 27 env vars currently read by ``backend/``. Every one of them
has a typed field here, with the same default and the same semantic
behavior. Per-site migration is tracked in the docstrings below and on
the surprises log.

The trust-topology guards (``TRUSTED_PROXY_IPS``, ``REQUIRE_PROXY_HEADERS``,
``STRICT_DATA_DIR_CHECK``) get a pydantic ``model_validator`` that mirrors
the existing ``_enforce_proxy_headers_configured`` semantics. The two
co-exist through v2.0 — the legacy function is the runtime guard and the
validator is the additional CI-level guard (a misconfigured ``Settings``
instance raises at import time in tests / CI that import this module).
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process-wide configuration loaded from environment variables.

    Use :func:`get_settings` to access a cached singleton instance. Direct
    instantiation (``Settings()``) is fine for tests that want fresh state
    per-call.

    Fields are grouped by concern. Defaults match what the corresponding
    ``os.environ.get("...", default)`` call returns today so a no-op
    migration of any call site is behavior-preserving.
    """

    model_config = SettingsConfigDict(
        env_file=None,  # we read process env, not .env files
        env_prefix="",  # env var names are uppercase exact
        case_sensitive=False,  # OTEL_ENABLED / otel_enabled both work
        extra="ignore",  # don't error on unrelated env vars
        validate_default=False,
        frozen=False,
    )

    # ── Trust topology (security-load-bearing) ───────────────────────────────

    trusted_proxy_ips: str = Field(default="", alias="TRUSTED_PROXY_IPS")
    """Comma-separated IPs uvicorn trusts in ``X-Forwarded-For``. Production
    sets ``127.0.0.1`` in docker-compose.prod.yml. Empty in prod opens the
    leftmost-XFF spoof + admin Host-spoof bypass."""

    uvicorn_forwarded_allow_ips: str = Field(default="", alias="UVICORN_FORWARDED_ALLOW_IPS")
    """uvicorn's own env-equivalent of ``--forwarded-allow-ips``. Defense
    in depth alongside ``trusted_proxy_ips`` — if a future refactor passes
    the CLI flag but drops our env var, this field still surfaces it."""

    require_proxy_headers: bool = Field(default=False, alias="REQUIRE_PROXY_HEADERS")
    """Promote the proxy-headers guard to FATAL (raises on missing
    ``trusted_proxy_ips``). Set in production compose."""

    strict_data_dir_check: bool = Field(default=False, alias="STRICT_DATA_DIR_CHECK")
    """Refuse to start if ``/app/data`` is not a real mount point. Set in
    production compose to catch the broken-fstab failure mode."""

    # ── Telemetry / observability ────────────────────────────────────────────

    otel_enabled: bool = Field(default=True, alias="OTEL_ENABLED")
    """Master OpenTelemetry SDK off-switch. When False, no providers are
    installed regardless of ``OTEL_EXPORTER``. Default ON everywhere
    except under pytest (the test harness sets ``PYTEST_CURRENT_TEST``
    which ``backend.core.request_telemetry`` reads separately)."""

    otel_exporter: str = Field(default="none", alias="OTEL_EXPORTER")
    """Which OpenTelemetry exporter to install. ``"none"`` (the default)
    keeps the SDK uninstalled so spans/metrics record against no-op
    providers and nothing leaves the process. ``"console"`` installs the
    ConsoleSpanExporter + ConsoleMetricExporter — useful for local dev
    debugging, but DON'T turn on in prod: it dumps ~1 MB/min of JSON to
    stdout. Future OTLP wiring would add another value here."""

    structlog_format: str = Field(default="console", alias="STRUCTLOG_FORMAT")
    """``"console"`` (dev) or ``"json"`` (production log aggregation).
    Read by :mod:`backend.utils.structlog_config`."""

    debug_responses: bool = Field(default=False, alias="DEBUG_RESPONSES")
    """Inject ``_debug_queries`` / ``_debug_calls`` / ``_is_cached`` into
    response bodies. Off in prod; on locally for the debug panel."""

    query_monitor_enabled: bool = Field(default=True, alias="QUERY_MONITOR_ENABLED")
    """Live admin query-monitor surface. When False, the in-memory active
    registry still runs (cost is ~5-10us per query, same as the sqlite
    profiler) but the ``/api/admin/queries`` router returns 404 and the
    frontend tab hides itself via ``/api/admin/app-config``. Kill switch
    for ops if the registry ever causes pressure."""

    # ── DuckDB engine tuning ─────────────────────────────────────────────────

    duckdb_memory_limit: str | None = Field(default=None, alias="DUCKDB_MEMORY_LIMIT")
    """Per-session ``SET max_memory`` value (e.g. ``"6GB"``). When unset,
    ``backend/core/duckdb.py`` auto-calculates from container memory."""

    duckdb_threads: int | None = Field(default=None, alias="DUCKDB_THREADS")
    """Per-session ``SET threads`` value. When unset, DuckDB auto-detects
    from CPU count."""

    duckdb_pool_max_size: int = Field(default=8, alias="DUCKDB_POOL_MAX_SIZE")
    """Max concurrent DuckDB connections in the per-service pool."""

    duckdb_pool_conn_memory_limit: str | None = Field(default=None, alias="DUCKDB_POOL_CONN_MEMORY_LIMIT")
    """Per-connection memory cap inside the pool. Overrides
    ``duckdb_memory_limit`` for pool-acquired connections."""

    duckdb_pool_conn_threads: int | None = Field(default=None, alias="DUCKDB_POOL_CONN_THREADS")
    """Per-connection thread cap inside the pool."""

    duckdb_connection_pool: str | None = Field(default=None, alias="DUCKDB_CONNECTION_POOL")
    """Pool mode toggle (legacy; defaults to the new bounded-pool impl)."""

    # ── Ingest / commit / compaction ─────────────────────────────────────────

    ingest_chunk_size: int | None = Field(default=None, alias="INGEST_CHUNK_SIZE")
    """Row chunk size for streaming sync writes."""

    buffer_commit_chunk_size: int | None = Field(default=None, alias="BUFFER_COMMIT_CHUNK_SIZE")
    """Row chunk size for buffer→Iceberg commits."""

    local_compact_max_partition_mb: int | None = Field(default=None, alias="LOCAL_COMPACT_MAX_PARTITION_MB")
    """Max size of an hour-tier compacted partition file."""

    local_compact_daily_tier_days: int | None = Field(default=None, alias="LOCAL_COMPACT_DAILY_TIER_DAYS")
    """How many days back to roll up into daily-tier files."""

    local_compact_weekly_tier_days: int | None = Field(default=None, alias="LOCAL_COMPACT_WEEKLY_TIER_DAYS")
    """How many days back to roll up into weekly-tier files."""

    # ── FOS proxy ────────────────────────────────────────────────────────────

    fos_manifest_cache_mb: int | None = Field(default=None, alias="FOS_MANIFEST_CACHE_MB")
    """Size of the in-process LRU for ``.metadata.json`` / ``.avro`` reads
    in :mod:`backend.utils.telemetry_proxy`."""

    fos_proxy_keepalive_s: float | None = Field(default=None, alias="FOS_PROXY_KEEPALIVE_S")
    """aiohttp keepalive duration on the proxy's upstream client session."""

    fos_proxy_upstream_timeout_s: float | None = Field(default=None, alias="FOS_PROXY_UPSTREAM_TIMEOUT_S")
    """aiohttp request timeout on the proxy's upstream calls."""

    # ── External APIs ────────────────────────────────────────────────────────

    fastly_api_key: str | None = Field(default=None, alias="FASTLY_API_KEY")
    """Default Fastly API key. Service-specific keys live in per-service
    configs; this is the boot/global default."""

    # ── Remote access / share / SSH ──────────────────────────────────────────

    local_hosts: str | None = Field(default=None, alias="LOCAL_HOSTS")
    """Comma-separated hostnames treated as local-admin in
    :mod:`backend.utils.remote_access`."""

    remote_share_db_dir: str | None = Field(default=None, alias="REMOTE_SHARE_DB_DIR")
    """Override for the analyst-share DB directory. Defaults to
    ``data/system/``."""

    # ── Scoring / VCL ────────────────────────────────────────────────────────

    scoring_require_falco: bool = Field(default=False, alias="SCORING_REQUIRE_FALCO")
    """Require the falco VCL linter for scoring matrix changes. CI sets
    this so an unwitting matrix edit can't skip the lint gate."""

    # ── Validators ───────────────────────────────────────────────────────────

    @model_validator(mode="after")
    def _validate_proxy_headers_required_in_strict_mode(self) -> Settings:
        """Mirror of :func:`backend.main._enforce_proxy_headers_configured`:
        if either ``REQUIRE_PROXY_HEADERS=1`` or ``STRICT_DATA_DIR_CHECK=1``
        is set, ``TRUSTED_PROXY_IPS`` (or its uvicorn-companion
        ``UVICORN_FORWARDED_ALLOW_IPS``) MUST also be set.

        The runtime guard in main.py is the authoritative startup check;
        this validator surfaces the same error at any code path that
        instantiates ``Settings()`` (tests, CI, scripts) so a refactor
        that removes the runtime guard still has a tripwire.
        """
        strict = self.require_proxy_headers or self.strict_data_dir_check
        trusted = (self.trusted_proxy_ips or self.uvicorn_forwarded_allow_ips or "").strip()
        if strict and not trusted:
            raise ValueError(
                "TRUSTED_PROXY_IPS is unset under strict mode "
                "(REQUIRE_PROXY_HEADERS=1 or STRICT_DATA_DIR_CHECK=1). "
                "Set TRUSTED_PROXY_IPS=127.0.0.1 in production env."
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide cached settings instance.

    Read from ``os.environ`` at first call. Use :func:`reset_settings`
    in tests that need to re-read the environment after monkeypatching
    env vars.
    """
    return Settings()


def reset_settings() -> None:
    """Drop the cached :class:`Settings` instance so the next
    :func:`get_settings` call re-reads ``os.environ``. Tests only."""
    get_settings.cache_clear()


# ── Public bootstrap helper ──────────────────────────────────────────────────


def is_strict_mode() -> bool:
    """Convenience shortcut for the most common runtime check.

    Equivalent to ``os.environ.get("REQUIRE_PROXY_HEADERS") == "1" or
    os.environ.get("STRICT_DATA_DIR_CHECK") == "1"``, but goes through
    the validated Settings instance."""
    return get_settings().require_proxy_headers or get_settings().strict_data_dir_check


__all__ = ["Settings", "get_settings", "reset_settings", "is_strict_mode"]


# When this module is imported directly, do NOT instantiate Settings()
# eagerly — that would fail under strict mode if TRUSTED_PROXY_IPS happens
# to be unset at import time. Callers explicitly opt in by calling
# get_settings() / Settings() after their boot sequence has set the env.
# (This matters in particular for test_main.py and the lifespan() startup
# which intentionally re-reads env vars after monkeypatching.)

# Document the legacy os.environ.get() call sites here so per-site
# migration in later phases has a checklist:
#
# - backend/main.py — _enforce_proxy_headers_configured() reads
#   TRUSTED_PROXY_IPS, UVICORN_FORWARDED_ALLOW_IPS, REQUIRE_PROXY_HEADERS,
#   STRICT_DATA_DIR_CHECK. Migrate after Phase 3.5 lands and pydantic
#   validator behavior is verified in prod.
# - backend/core/duckdb.py — DUCKDB_* knobs. Per-call-site migration.
# - backend/core/ingest.py — INGEST_CHUNK_SIZE, BUFFER_COMMIT_CHUNK_SIZE.
# - backend/core/iceberg.py — FOS_MANIFEST_CACHE_MB.
# - backend/utils/telemetry_proxy.py — FOS_PROXY_KEEPALIVE_S,
#   FOS_PROXY_UPSTREAM_TIMEOUT_S.
# - backend/utils/remote_access.py — LOCAL_HOSTS.
# - backend/core/share_db.py — REMOTE_SHARE_DB_DIR.
# - backend/core/request_telemetry.py — OTEL_ENABLED (already structured).
# - backend/utils/structlog_config.py — STRUCTLOG_FORMAT (already structured).
# - backend/models/common.py — DEBUG_RESPONSES.
