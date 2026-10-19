"""Fresh-process checks: no conftest path patches can hide launcher escapes."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
LAUNCHER = REPO / "scripts" / "run_contract_backend.py"


def run_probe(code: str, overrides: dict[str, str]) -> subprocess.CompletedProcess:
    env = {key: os.environ[key] for key in ("PATH", "HOME") if key in os.environ}
    env.update(overrides)
    return subprocess.run(
        [sys.executable, "-B", "-c", code],
        cwd=REPO,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )


@pytest.mark.parametrize("relative", [False, True])
def test_real_paths_resolve_inside_sandbox(tmp_path, relative):
    sandbox = tmp_path / "sandbox"
    configs = sandbox / "configs"
    data = sandbox / "data"
    overrides = {
        "CONTRACT_CONFIGS_DIR": str(configs),
        "CONTRACT_DATA_DIR": str(data),
        "REMOTE_SHARE_DB_DIR": str(tmp_path / "wrong-share-dir"),
    }
    if relative:
        overrides = {key: os.path.relpath(value, REPO) for key, value in overrides.items()}
    result = run_probe(
        f"""
import json, os, runpy, sys
from pathlib import Path

# Path-only probes must never open a database, even on a regressed launcher.
def audit(event, args):
    if event in ("sqlite3.connect", "socket.connect"):
        raise AssertionError("Probe must not open databases or network connections")
sys.addaudithook(audit)
launcher = runpy.run_path({str(LAUNCHER)!r})
launcher["_maybe_redirect_config_paths"]()
from backend import config
from backend.core import metric_snapshots
from backend.core.metadata import base, usage_log_db
from backend.core.share_db import connection as share
from backend.utils import rdns_cache, ngwaf_bot_cache
paths = {{
    "cwd": Path.cwd(),
    "configs": config.CONFIGS_DIR,
    "data": config.DATA_DIR,
    "duckdb": config.duckdb_path("sandbox-service"),
    "metadata": base.db_path("sandbox-service"),
    "usage_db": usage_log_db.db_path("sandbox-service"),
    "metrics": Path(metric_snapshots._DATA_DIR) / metric_snapshots._DB_NAME,
    "rdns": rdns_cache._DB_PATH,
    "ngwaf": ngwaf_bot_cache._db_path(),
    "share": share.db_path(),
    "usage_config": config._USAGE_LOGGING_CONFIG_PATH,
    "buffer_cache": Path("cache"),
}}
print(json.dumps({{key: str(Path(value).resolve()) for key, value in paths.items()}}))
""",
        overrides,
    )
    assert result.returncode == 0, result.stderr
    paths = json.loads(result.stdout)
    assert paths == {
        "cwd": str(sandbox),
        "configs": str(configs),
        "data": str(data),
        "duckdb": str(data / "services/sandbox-service.duckdb"),
        "metadata": str(data / "services/sandbox-service.metadata.db"),
        "usage_db": str(data / "services/sandbox-service.usage_log.db"),
        "metrics": str(data / "system/system_metrics.db"),
        "rdns": str(data / "cache/rdns_cache.db"),
        "ngwaf": str(data / "ngwaf_bot_cache.db"),
        "share": str(data / "system/remote_share.db"),
        "usage_config": str(data / "system/usage_logging.json"),
        "buffer_cache": str(sandbox / "cache"),
    }


@pytest.mark.parametrize(
    "overrides",
    [
        {"CONTRACT_CONFIGS_DIR": "sandbox/configs"},
        {"CONTRACT_DATA_DIR": "sandbox/data"},
        {"CONTRACT_CONFIGS_DIR": "", "CONTRACT_DATA_DIR": ""},
        {"CONTRACT_CONFIGS_DIR": "sandbox/configs", "CONTRACT_DATA_DIR": "elsewhere/data"},
        {"CONTRACT_CONFIGS_DIR": "sandbox/configs", "CONTRACT_DATA_DIR": "sandbox/custom-data"},
    ],
)
def test_incomplete_or_inconsistent_overrides_fail_before_import(overrides):
    result = run_probe(
        f"""
import runpy, sys
def audit(event, args):
    if event in ("os.mkdir", "sqlite3.connect", "socket.connect"):
        raise AssertionError("Invalid overrides must not cause I/O")
sys.addaudithook(audit)
launcher = runpy.run_path({str(LAUNCHER)!r})
try:
    launcher["_maybe_redirect_config_paths"]()
except ValueError:
    assert "backend.config" not in sys.modules
else:
    raise AssertionError("Unsafe sandbox overrides accepted")
""",
        overrides,
    )
    assert result.returncode == 0, result.stderr


def test_no_overrides_is_a_noop():
    result = run_probe(
        f"""
import os, runpy, sys
launcher = runpy.run_path({str(LAUNCHER)!r})
before = os.getcwd(), list(sys.path), dict(os.environ)
launcher["_maybe_redirect_config_paths"]()
assert before == (os.getcwd(), sys.path, dict(os.environ))
assert "backend.config" not in sys.modules
""",
        {},
    )
    assert result.returncode == 0, result.stderr


def test_main_retains_repo_importability_after_chdir(tmp_path):
    result = run_probe(
        f"""
import runpy, sys, types
from pathlib import Path
sys.path = [p for p in sys.path if p and Path(p).resolve() != Path({str(REPO)!r})]
def run(app, **kwargs):
    from backend import config
    assert app == "backend.main:app"
    assert Path.cwd() == Path({str(tmp_path)!r})
    assert config.CONFIGS_DIR == Path({str(tmp_path / "configs")!r})
sys.modules["uvicorn"] = types.SimpleNamespace(run=run)
sys.argv = [{str(LAUNCHER)!r}, "--port", "13003"]
runpy.run_path({str(LAUNCHER)!r}, run_name="__main__")
""",
        {"CONTRACT_CONFIGS_DIR": str(tmp_path / "configs"), "CONTRACT_DATA_DIR": str(tmp_path / "data")},
    )
    assert result.returncode == 0, result.stderr
