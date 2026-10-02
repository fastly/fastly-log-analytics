"""Local-stack DuckDB parallelism governance.

The canonical ``deploy_test_all.sh`` runs TWO full analytical stacks
(Local Standard + Local High-Scale) concurrently on one shared Colima
host (6 vCPU). With ``DUCKDB_THREADS`` unset the backend defaults to
``min(cpu_count, 8) = 6`` threads per DuckDB op and the pool defaults to
8 connections, so in-process ingest/rollup/compaction/insights-prewarmer
crons can fan out to ~48 CPU-bound DuckDB threads per stack. Two stacks
then drive host load to ~100 on 6 cores and the single-threaded asyncio
serving loop is starved so hard that even ``/api/health`` times out
(measured: HTTP 000 at 20s+), which fails the dashboard-render verify.

The ``.env`` file already expresses the intent (``DUCKDB_THREADS=4``) but
compose never injects it — the backend services use an explicit
``environment:`` list with no ``env_file``, so the value never reaches
the container. These tests pin the caps directly on the local compose
services so a future edit can't silently drop them and reintroduce the
serving-loop starvation. Production (docker-compose.prod.yml) already
caps the pool to 4; this brings the local stacks in line.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


def _env_int(environment: list[str], key: str) -> int | None:
    prefix = f"{key}="
    for entry in environment:
        if entry.startswith(prefix):
            return int(entry[len(prefix) :])
    return None


def _service_env(compose_rel: str, service: str) -> list[str]:
    compose = yaml.safe_load((REPO_ROOT / compose_rel).read_text(encoding="utf-8"))
    return compose["services"][service]["environment"]


def test_local_standard_backend_bounds_duckdb_parallelism():
    env = _service_env("docker-compose.yml", "backend")
    threads = _env_int(env, "DUCKDB_THREADS")
    pool = _env_int(env, "DUCKDB_POOL_MAX_SIZE")
    assert threads is not None and 1 <= threads <= 4, (
        "docker-compose.yml backend must pin a bounded DUCKDB_THREADS (<=4) "
        "so cron DuckDB ops don't monopolize all cores and starve serving"
    )
    assert pool is not None and 1 <= pool <= 4, (
        "docker-compose.yml backend must pin a bounded DUCKDB_POOL_MAX_SIZE (<=4)"
    )


def test_local_high_scale_backend_bounds_duckdb_parallelism():
    env = _service_env("docker-compose.multipod.yml", "backend")
    threads = _env_int(env, "DUCKDB_THREADS")
    pool = _env_int(env, "DUCKDB_POOL_MAX_SIZE")
    assert threads is not None and 1 <= threads <= 4, (
        "docker-compose.multipod.yml backend must pin a bounded DUCKDB_THREADS (<=4)"
    )
    assert pool is not None and 1 <= pool <= 4, (
        "docker-compose.multipod.yml backend must pin a bounded DUCKDB_POOL_MAX_SIZE (<=4)"
    )


def test_local_high_scale_worker_bounds_duckdb_parallelism():
    env = _service_env("docker-compose.multipod.yml", "worker")
    threads = _env_int(env, "DUCKDB_THREADS")
    assert threads is not None and 1 <= threads <= 2, (
        "docker-compose.multipod.yml worker must pin a bounded DUCKDB_THREADS "
        "(<=2); 6 celery procs x 6 default threads = 36 CPU-bound threads "
        "that starve the backend serving loop on the shared host"
    )
