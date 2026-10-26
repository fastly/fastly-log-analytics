from __future__ import annotations

import ast
import importlib.util
import json
import os
import re
import shutil
import signal
import subprocess
from pathlib import Path

import pytest
import yaml

from observability.docker_stats_exporter import _block_io_bytes
from scripts import local_stack_processes as stack

_REPO_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = str(_REPO_ROOT)
ROUTERS = _REPO_ROOT / "backend" / "routers"
SCRIPTS = _REPO_ROOT / "scripts"
MAKEFILE = os.path.join(REPO_ROOT, "Makefile")
CI_YML = os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")
CANONICAL_DIR = os.path.join(REPO_ROOT, "observability")
CHART_DIR = os.path.join(REPO_ROOT, "deploy", "chart", "fastly-log-analytics", "files", "observability")
DATEPARSE = "check_no_raw_dateparse.sh"
RETRY = "check_no_handrolled_retry.sh"
COALESCE = "check_no_inline_metric_coalesce.sh"

_spec = importlib.util.spec_from_file_location("analyze_web_vitals", SCRIPTS / "analyze_web_vitals.py")
assert _spec and _spec.loader
awv = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(awv)


def test_capacity_and_recovery_contract_is_explicit() -> None:
    text = (_REPO_ROOT / "docs/runbooks/high-scale-capacity-and-recovery.md").read_text()
    for needle in (
        "2M request events/sec",
        "5M/sec",
        "500k RUM events/sec",
        "1M/sec RUM bursts",
        "15 minutes",
        "never silently samples or discards",
        "FOS is the durable source of truth",
        "request, RUM vitals, RUM errors, and CMCD",
    ):
        assert needle in text


_SQL_CALL = re.compile(
    r"\.execute\(|\.executemany\(|\.fetchall\(|\.fetchone\(|track_query\(|\bDESCRIBE\s+\{|"
    r"[\"'`]\s*(SELECT|INSERT INTO|DELETE FROM|UPDATE\s+\w+\s+SET|CREATE\s+(TEMP\s+)?TABLE|WITH\s+\w+\s+AS)\b"
)
_INLINE_SQL_EXEMPT_ROUTERS = {"admin/log_accounting.py", "control_room.py", "high_scale.py"}


def test_no_inline_sql_in_routers() -> None:
    offenders: list[str] = []
    for path in sorted(ROUTERS.rglob("*.py")):
        if str(path.relative_to(ROUTERS)) in _INLINE_SQL_EXEMPT_ROUTERS:
            continue
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            if not line.lstrip().startswith("#") and _SQL_CALL.search(line):
                offenders.append(f"{path.relative_to(ROUTERS.parent.parent)}:{lineno}: {line.strip()[:100]}")
    assert not offenders, "Move SQL into backend/repositories (+ _sql/):\n" + "\n".join(offenders)


def _read(path: str) -> str:
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _makefile_recipe(name: str) -> str:
    out: list[str] = []
    in_target = False
    for line in _read(MAKEFILE).splitlines():
        if not in_target:
            if re.match(rf"^{re.escape(name)}\s*:", line):
                in_target = True
            continue
        if line.startswith("\t"):
            out.append(line.lstrip("\t"))
        elif line.strip() != "":
            break
    if not out:
        raise AssertionError(f"Makefile target not found or empty: {name}")
    return "\n".join(out)


def _ci_step_run(step_name: str) -> str:
    doc = yaml.safe_load(_read(CI_YML))
    for job in doc.get("jobs", {}).values():
        for step in job.get("steps", []):
            if step.get("name") == step_name:
                run = step.get("run")
                if run is None:
                    raise AssertionError(f"Step has no run block: {step_name}")
                return str(run)
    raise AssertionError(f"ci.yml step not found: {step_name}")


def _marker_set(expr: str) -> frozenset[str]:
    return frozenset(re.findall(r"not\s+(\w+)", expr))


def _cov_floors(text: str) -> set[int]:
    return {int(m) for m in re.findall(r"--cov-fail-under=(\d+)", text)}


def _vitest_thresholds(text: str) -> dict[str, int]:
    return {key: int(val) for key, val in re.findall(r"--coverage\.thresholds\.(\w+)=(\d+)", text)}


def _nonterraform_marker(text: str) -> str:
    for m in re.findall(r'-m\s+"([^"]*)"', text):
        if "not terraform_cli" in m:
            return m
    raise AssertionError(f"no 'not terraform_cli' pytest marker found in:\n{text}")


def test_backend_pytest_marker_parity() -> None:
    make_markers = _marker_set(_nonterraform_marker(_makefile_recipe("test-ci")))
    ci_markers = _marker_set(_nonterraform_marker(_ci_step_run("Tests (pytest with coverage)")))
    assert make_markers == ci_markers
    assert "flaky_quarantine" in make_markers and "terraform_cli" in make_markers


def test_backend_coverage_floor_parity() -> None:
    make_floors = _cov_floors(_makefile_recipe("test-ci"))
    ci_floors = _cov_floors(_ci_step_run("Tests (pytest with coverage)"))
    assert make_floors == ci_floors and len(make_floors) == 1 and min(make_floors) >= 85


def test_frontend_vitest_threshold_parity() -> None:
    make = _vitest_thresholds(_makefile_recipe("test-frontend-ci"))
    ci = _vitest_thresholds(_ci_step_run("Tests (vitest with coverage)"))
    assert make and make == ci
    for key, floor in {"lines": 66, "statements": 65, "functions": 54, "branches": 52}.items():
        assert make.get(key, -1) >= floor


_SHARED_COMMAND_GATES = [
    ("lint", "Lint (ruff)", "uv run ruff check ."),
    ("format-check", "Format check (ruff)", "uv run ruff format --check ."),
    ("import-contracts", "Architectural contracts (import-linter, R-9)", "uv run lint-imports"),
    ("osv", "Dependency vulnerability scan (osv-scanner, CRITICAL gate)", "uv run python scripts/check_osv.py"),
    (
        "otel-guard",
        "Observability guard (no OTEL_EXPORTER=console in deploy files)",
        "bash scripts/check_no_console_otel.sh",
    ),
    ("urllib-guard", "HTTP client guard (no raw urllib in backend app modules)", "bash scripts/check_no_raw_urllib.sh"),
    (
        "dateparse-guard",
        "Date-parsing guard (no new raw strptime/fromisoformat in core + repositories)",
        f"bash scripts/{DATEPARSE}",
    ),
    ("retry-guard", "Retry-loop guard (no new hand-rolled retry loops in backend)", f"bash scripts/{RETRY}"),
    (
        "coalesce-guard",
        "Metric COALESCE guard (no new inline COALESCE in repositories + rollups)",
        f"bash scripts/{COALESCE}",
    ),
    ("security-regression", "Security-regression count gate", "bash scripts/check_security_regression_count.sh"),
]


@pytest.mark.parametrize("make_target,ci_step,expected_cmd", _SHARED_COMMAND_GATES)
def test_shared_single_command_gate_parity(make_target: str, ci_step: str, expected_cmd: str) -> None:
    assert expected_cmd in _makefile_recipe(make_target)
    assert expected_cmd in _ci_step_run(ci_step)


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    (tmp_path / "scripts" / "lib").mkdir(parents=True)
    for name in (DATEPARSE, RETRY, COALESCE, "lib/ratchet_common.sh"):
        shutil.copy(SCRIPTS / name, tmp_path / "scripts" / name)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    for rel, body in files.items():
        f = tmp_path / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(body)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def _run(root: Path, script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", str(root / "scripts" / script)], capture_output=True, text=True)


@pytest.mark.parametrize("script", [DATEPARSE, RETRY, COALESCE])
def test_guards_pass_on_this_repo(script: str) -> None:
    res = subprocess.run(["bash", str(SCRIPTS / script)], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr


def test_guard_flags_new_raw_site_in_new_file(tmp_path: Path) -> None:
    res = _run(_repo(tmp_path, {"backend/repositories/new.py": 'x = datetime.strptime(s, "%Y-%m-%d-%H")\n'}), DATEPARSE)
    assert res.returncode == 1 and "backend/repositories/new.py" in res.stderr


def test_guard_flags_count_above_baseline(tmp_path: Path) -> None:
    body = "a = datetime.fromisoformat(x)\nb = datetime.fromisoformat(y)\n"
    assert _run(_repo(tmp_path, {"backend/core/clickhouse_rows.py": body}), DATEPARSE).returncode == 1


def test_guard_ignores_comments_and_tests(tmp_path: Path) -> None:
    files = {
        "backend/core/ok.py": "# datetime.fromisoformat(x) was the bug\n",
        "backend/utils/date_utils.py": "datetime.fromisoformat(s)\n",
        "tests/test_x.py": "datetime.strptime(s, f)\n",
    }
    assert _run(_repo(tmp_path, files), DATEPARSE).returncode == 0


RETRY_LOOP = """
import time

def f(op):
    for attempt in range(3):
        try:
            return op()
        except OSError:
            time.sleep(0.1)
"""


def test_retry_guard_flags_new_loop(tmp_path: Path) -> None:
    res = _run(_repo(tmp_path, {"backend/core/new_thing.py": RETRY_LOOP}), RETRY)
    assert res.returncode == 1 and "backend/core/new_thing.py" in res.stderr


def test_retry_guard_flags_while_loop(tmp_path: Path) -> None:
    body = "import time\n\ndef f(op):\n    while True:\n        try:\n            return op()\n        except Exception:\n            time.sleep(1)\n"
    assert _run(_repo(tmp_path, {"backend/utils/x.py": body}), RETRY).returncode == 1


def test_retry_guard_ignores_sleep_without_handler_and_nested_functions(tmp_path: Path) -> None:
    body = (
        "import time\n\ndef poll():\n    for _ in range(3):\n        time.sleep(1)\n\n"
        "def outer():\n    for _ in range(3):\n"
        "        def inner():\n            try:\n                pass\n            except OSError:\n                time.sleep(1)\n"
    )
    assert _run(_repo(tmp_path, {"backend/utils/ok.py": body}), RETRY).returncode == 0


def test_retry_guard_exempts_retry_module_and_tests(tmp_path: Path) -> None:
    assert (
        _run(_repo(tmp_path, {"backend/core/retry.py": RETRY_LOOP, "tests/test_x.py": RETRY_LOOP}), RETRY).returncode
        == 0
    )


def test_retry_guard_flags_count_above_baseline(tmp_path: Path) -> None:
    body = RETRY_LOOP + RETRY_LOOP.replace("def f", "def g")
    assert _run(_repo(tmp_path, {"backend/utils/telemetry.py": body}), RETRY).returncode == 1


def test_coalesce_guard_flags_new_inline_site(tmp_path: Path) -> None:
    res = _run(_repo(tmp_path, {"backend/repositories/new.py": 'q = "SELECT COALESCE(SUM(x), 0) FROM t"\n'}), COALESCE)
    assert res.returncode == 1 and "backend/repositories/new.py" in res.stderr


def test_coalesce_guard_flags_rollup_writer(tmp_path: Path) -> None:
    assert (
        _run(_repo(tmp_path, {"backend/core/rollups/new_rollup.py": 'q = "COALESCE(a, 0)"\n'}), COALESCE).returncode
        == 1
    )


def test_coalesce_guard_ignores_comments_sql_templates_and_helpers(tmp_path: Path) -> None:
    files = {
        "backend/repositories/ok.py": "# COALESCE(x, 0) was inline here\n",
        "backend/repositories/_sql/tpl.py": 'T = "COALESCE(a, 0)"\n',
        "backend/core/metric_expressions.py": 'f"COALESCE({a}, {b})"\n',
    }
    assert _run(_repo(tmp_path, files), COALESCE).returncode == 0


ALLOWED_STRING_DETAILS: set[tuple[int, str]] = {
    (415, "Unsupported Media Type"),
    (403, "Read-only services cannot run log discovery."),
    (403, "Read-only services cannot run full sweep."),
    (403, "Read-only services cannot run gap heal."),
    (403, "Read-only services cannot run cloud maintenance."),
    (403, "Read-only services cannot run ledger sweep."),
    (400, "Ledger sweep is only supported under high-scale mode."),
    (403, "Read-only services cannot run RUM sync."),
    (400, "RUM sync is only supported in standard deployment mode."),
    (403, "Read-only services cannot run RUM discovery."),
    (400, "RUM discovery is only supported under high-scale mode."),
}
HELPER_FUNCS = frozenset({"bad_request", "not_found", "validation_failed", "make_error"})


def _detail_kw(call: ast.Call) -> ast.expr | None:
    for kw in call.keywords:
        if kw.arg == "detail":
            return kw.value
    return call.args[1] if len(call.args) >= 2 else None


def _status_code_kw(call: ast.Call) -> int | None:
    for kw in call.keywords:
        if kw.arg == "status_code" and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, int):
            return kw.value.value
    if call.args and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, int):
        return call.args[0].value
    return None


def _is_canonical_helper_call(node: ast.expr) -> bool:
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in HELPER_FUNCS


def _is_error_first_dict(node: ast.expr) -> bool:
    return (
        isinstance(node, ast.Dict)
        and bool(node.keys)
        and isinstance(node.keys[0], ast.Constant)
        and node.keys[0].value in {"error", "errors"}
    )


def test_http_exception_envelope_shape() -> None:
    findings: list[str] = []
    for path in sorted(p for p in ROUTERS.rglob("*.py") if p.name != "__init__.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else None
            if name != "HTTPException":
                continue
            detail = _detail_kw(node)
            if detail is None or _is_canonical_helper_call(detail) or _is_error_first_dict(detail):
                continue
            if isinstance(detail, ast.Constant) and isinstance(detail.value, str):
                status = _status_code_kw(node)
                if status is not None and (status, detail.value) in ALLOWED_STRING_DETAILS:
                    continue
                findings.append(
                    f"{path.relative_to(_REPO_ROOT)}:{node.lineno} — HTTPException(detail={detail.value!r})"
                )
                continue
            findings.append(
                f"{path.relative_to(_REPO_ROOT)}:{node.lineno} — HTTPException(detail=<{type(detail).__name__}>)"
            )
    assert not findings, "Non-canonical HTTPException envelopes:\n  " + "\n  ".join(findings)


CHART_OMITTED_PROMETHEUS_JOBS = {"docker": "docker-stats-exporter uses the Docker Engine API, absent in Kubernetes"}
CHART_EXEMPT_FILES = {
    "docker_stats_exporter.py": "docker-compose-only sidecar",
    "prometheus.yml": "compared structurally above",
    "standard/prometheus.yml": "Standard host/shared-loopback receiver; minimal Helm config is rendered and tested in deploy suite",
    "standard/tempo.yml": "Standard host/shared-loopback listeners cannot be copied to namespace-private Helm pods",
}


def _load_yaml(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    assert isinstance(data, dict), f"expected a YAML mapping in {path}"
    return data


def test_grafana_datasources_chart_copy_matches_canonical() -> None:
    chart = _load_yaml(os.path.join(CHART_DIR, "grafana-datasources.yml"))
    assert "datasources" in chart and isinstance(chart["datasources"], list)


def test_prometheus_chart_copy_matches_canonical_minus_omitted_jobs() -> None:
    canonical = _load_yaml(os.path.join(CANONICAL_DIR, "prometheus.yml"))
    chart = _load_yaml(os.path.join(CHART_DIR, "prometheus.yml"))
    assert {k: v for k, v in canonical.items() if k != "scrape_configs"} == {
        k: v for k, v in chart.items() if k != "scrape_configs"
    }
    canonical_jobs = {j["job_name"]: j for j in canonical.get("scrape_configs", [])}
    chart_jobs = {j["job_name"]: j for j in chart.get("scrape_configs", [])}
    assert set(canonical_jobs) - set(chart_jobs) == set(CHART_OMITTED_PROMETHEUS_JOBS)
    assert not (set(chart_jobs) - set(canonical_jobs))
    for name, chart_job in chart_jobs.items():
        assert chart_job == canonical_jobs[name]


def _relative_files(root: str) -> set[str]:
    found: set[str] = set()
    for dirpath, dirs, names in os.walk(root):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for name in names:
            found.add(os.path.relpath(os.path.join(dirpath, name), root))
    return found


def test_every_canonical_observability_file_is_guarded_in_the_chart() -> None:
    canonical = _relative_files(CANONICAL_DIR)
    chart = _relative_files(CHART_DIR)
    assert not ((canonical - set(CHART_EXEMPT_FILES)) - chart)
    assert not ((canonical - set(CHART_EXEMPT_FILES)) & chart)


def test_chart_observability_copies_are_byte_identical() -> None:
    for rel in sorted(_relative_files(CANONICAL_DIR) - set(CHART_EXEMPT_FILES)):
        assert Path(CANONICAL_DIR, rel).read_bytes() == Path(CHART_DIR, rel).read_bytes()


def test_block_io_bytes_sums_read_and_write_operations():
    stats = {
        "blkio_stats": {
            "io_service_bytes_recursive": [
                {"op": "Read", "value": 1024},
                {"op": "Write", "value": 2048},
                {"op": "Read", "value": 512},
                {"op": "Sync", "value": 99},
            ]
        }
    }
    assert _block_io_bytes(stats) == (1536, 2048)


def test_block_io_bytes_handles_missing_or_invalid_values():
    assert _block_io_bytes({}) == (0, 0)
    assert _block_io_bytes(
        {"blkio_stats": {"io_service_bytes_recursive": [{"op": "Read"}, {"op": "Write", "value": "bad"}]}}
    ) == (0, 0)


def _dashboard() -> dict:
    dashboard_path = Path("local-docs/fla-multipod-elevation.json")
    if not dashboard_path.exists():
        pytest.skip("Elevation dashboard export is intentionally local-only")
    return json.loads(dashboard_path.read_text())


def test_elevation_dashboard_includes_high_scale_operational_panels() -> None:
    panels = _dashboard()["panels"]
    panel_text = json.dumps(panels)
    assert len({panel["id"] for panel in panels}) == len(panels)
    for metric in (
        "fla_high_scale_source_objects",
        "fla_high_scale_oldest_active_age_seconds",
        "fla_high_scale_publication_lag_seconds",
        "fla_high_scale_pending_manifests",
        "fla_high_scale_rows_published_total",
    ):
        assert metric in panel_text


def test_elevation_dashboard_keeps_variable_based_promql() -> None:
    panel_text = json.dumps(_dashboard()["panels"])
    assert "$site" in panel_text and "$namespace" in panel_text


def test_prometheus_preserves_requested_bytes_series_via_recording_alias():
    prometheus = yaml.safe_load(Path("observability/prometheus.yml").read_text())
    assert "/etc/prometheus/clickhouse.rules.yml" in prometheus["rule_files"]
    overlay = yaml.safe_load(Path("docker-compose.observability.yml").read_text())
    assert (
        "./deploy/chart/fastly-log-analytics/files/observability/clickhouse.rules.yml:/etc/prometheus/clickhouse.rules.yml:ro"
        in overlay["services"]["prometheus"]["volumes"]
    )
    rules = yaml.safe_load(
        Path("deploy/chart/fastly-log-analytics/files/observability/clickhouse.rules.yml").read_text()
    )
    assert rules["groups"][0]["rules"] == [
        {"record": "app_clickhouse_bytes_read_bytes_total", "expr": "app_clickhouse_bytes_read_total"}
    ]


class _ComposeLoader(yaml.SafeLoader):
    pass


def _passthrough_tag(loader, node):
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    return loader.construct_scalar(node)


for _tag in ("!override", "!reset"):
    _ComposeLoader.add_constructor(_tag, _passthrough_tag)


def _load_compose(compose_rel: str) -> dict:
    return yaml.load((_REPO_ROOT / compose_rel).read_text(encoding="utf-8"), Loader=_ComposeLoader)


def _env_int(environment: list[str], key: str) -> int | None:
    prefix = f"{key}="
    for entry in environment:
        if entry.startswith(prefix):
            return int(entry[len(prefix) :])
    return None


def _service_env(compose_rel: str, service: str) -> list[str]:
    return yaml.safe_load((_REPO_ROOT / compose_rel).read_text(encoding="utf-8"))["services"][service]["environment"]


def _duration_seconds(value: str) -> int:
    value = value.strip()
    for suffix, mult in (("ms", 0.001), ("m", 60), ("h", 3600), ("s", 1)):
        if value.endswith(suffix):
            return int(float(value[: -len(suffix)]) * mult)
    return int(float(value))


def _service_healthcheck(compose_rel: str, service: str) -> dict:
    return yaml.safe_load((_REPO_ROOT / compose_rel).read_text(encoding="utf-8"))["services"][service]["healthcheck"]


@pytest.mark.parametrize(
    ("compose_rel", "service", "max_threads", "max_pool"),
    [
        ("docker-compose.yml", "backend", 4, 4),
        ("docker-compose.multipod.yml", "backend", 4, 4),
        ("docker-compose.multipod.yml", "worker", 2, None),
    ],
)
def test_compose_bounds_duckdb_parallelism(compose_rel: str, service: str, max_threads: int, max_pool: int | None):
    env = _service_env(compose_rel, service)
    threads = _env_int(env, "DUCKDB_THREADS")
    assert threads is not None and 1 <= threads <= max_threads
    if max_pool is not None:
        pool = _env_int(env, "DUCKDB_POOL_MAX_SIZE")
        assert pool is not None and 1 <= pool <= max_pool


def test_local_standard_backend_healthcheck_covers_cold_start():
    hc = _service_healthcheck("docker-compose.yml", "backend")
    assert _duration_seconds(hc["start_period"]) >= 180


_INIT_REQUIRED_SERVICES = {
    "docker-compose.yml": ["backend"],
    "docker-compose.multipod.yml": ["backend", "worker"],
}


@pytest.mark.parametrize(
    ("compose_rel", "service"),
    [(c, s) for c, services in _INIT_REQUIRED_SERVICES.items() for s in services],
)
def test_long_lived_backend_services_run_an_init(compose_rel: str, service: str):
    assert _load_compose(compose_rel)["services"][service].get("init") is True


def test_local_public_https_is_opt_in_and_published_by_namespace_owner():
    base = _load_compose("docker-compose.yml")["services"]
    overlay = _load_compose("docker-compose.public-https.yml")["services"]
    assert base["backend"]["ports"] == ["80:80"]
    assert base["caddy"]["network_mode"] == "service:backend"
    assert overlay["backend"]["ports"] == ["${LOCAL_PUBLIC_LISTEN_IP:-127.0.0.1}:${LOCAL_PUBLIC_PORT:-443}:443"]
    assert "ports" not in overlay["caddy"]
    assert "admin-gateway" in overlay["caddy"]["depends_on"]
    assert "./caddy/local-public-https.caddy:/etc/caddy/public-https/listener.caddy:ro" in overlay["caddy"]["volumes"]
    assert any(volume.endswith(":/certs/public:ro") for volume in overlay["caddy"]["volumes"])
    assert (
        "ADMIN_GATEWAY_REQUIRED=${ADMIN_GATEWAY_REQUIRED:?set 1 for enforced admin mTLS}"
        in overlay["caddy"]["environment"]
    )


def test_local_public_https_compose_merge_preserves_admin_and_http():
    if not shutil.which("docker"):
        pytest.skip("Docker CLI unavailable for static Compose configuration")
    env = {
        **os.environ,
        "ADMIN_GATEWAY_REQUIRED": "1",
        "ADMIN_GATEWAY_SECRET": "test-compose-only-" + "x" * 32,
        "ADMIN_GATEWAY_HOST": "admin.example.test",
        "ADMIN_GATEWAY_CERT_DIR": "/tmp/example-admin-certs",
        "ADMIN_GATEWAY_LISTEN_IP": "127.0.0.1",
        "ADMIN_GATEWAY_PORT": "9443",
        "LOCAL_HOSTS": "localhost,admin.example.test",
        "LOCAL_PUBLIC_HOST": "127.0.0.1",
        "LOCAL_PUBLIC_CERT_DIR": "/tmp/example-public-certs",
        "LOCAL_PUBLIC_LISTEN_IP": "127.0.0.1",
        "LOCAL_PUBLIC_PORT": "443",
    }
    cmd = [
        "docker",
        "compose",
        "--env-file",
        os.devnull,
        "-f",
        "docker-compose.yml",
        "-f",
        "docker-compose.admin-mtls.yml",
        "-f",
        "docker-compose.public-https.yml",
        "config",
        "--format",
        "json",
    ]
    result = subprocess.run(cmd, cwd=_REPO_ROOT, env=env, capture_output=True, text=True, check=True, timeout=30)
    services = json.loads(result.stdout)["services"]
    ports = services["backend"]["ports"]
    assert {(p.get("host_ip"), p["published"], p["target"]) for p in ports} == {
        (None, "80", 80),
        ("127.0.0.1", "443", 443),
        ("127.0.0.1", "9443", 8443),
    }
    for name in ("caddy", "frontend", "admin-gateway"):
        assert services[name]["network_mode"] == "service:backend" and not services[name].get("ports")
    assert services["backend"]["environment"]["ADMIN_GATEWAY_REQUIRED"] == "1"
    assert services["frontend"]["environment"]["ADMIN_GATEWAY_REQUIRED"] == "1"
    mounts = {volume["target"]: volume for volume in services["caddy"]["volumes"]}
    assert mounts["/etc/caddy/Caddyfile"]["source"].endswith("/caddy/Caddyfile.local")
    assert mounts["/etc/caddy/public-https/listener.caddy"]["source"].endswith("/caddy/local-public-https.caddy")
    assert mounts["/certs/public"]["read_only"] is True
    admin_mounts = {volume["target"]: volume for volume in services["admin-gateway"]["volumes"]}
    assert admin_mounts["/certs"]["source"] == env["ADMIN_GATEWAY_CERT_DIR"]


@pytest.mark.parametrize("required", ["1", "0"])
def test_local_public_https_adapts_separately_from_http_admin(tmp_path, required):
    if not shutil.which("caddy"):
        pytest.skip("Caddy binary unavailable for static configuration adaptation")
    local = (_REPO_ROOT / "caddy/Caddyfile.local").read_text()
    snippet = _REPO_ROOT / "caddy/local-public-https.caddy"
    config = tmp_path / "Caddyfile"
    config.write_text(local.replace("/etc/caddy/public-https/*.caddy", str(snippet)))
    result = subprocess.run(
        ["caddy", "adapt", "--config", str(config), "--adapter", "caddyfile"],
        env={**os.environ, "LOCAL_PUBLIC_HOST": "127.0.0.1", "ADMIN_GATEWAY_REQUIRED": required},
        capture_output=True,
        text=True,
        check=True,
    )
    adapted = json.loads(result.stdout)
    servers = adapted["apps"]["http"]["servers"]
    (public,) = [s for s in servers.values() if s["listen"] == [":443"]]
    (http,) = [s for s in servers.values() if s["listen"] == [":80"]]
    (policy,) = public["tls_connection_policies"]
    assert "match" not in policy and policy["certificate_selection"]["any_tag"]
    public_json, http_json = json.dumps(public), json.dumps(http)
    for expected in (
        '"host": ["127.0.0.1"]',
        "127.0.0.1:8000",
        "127.0.0.1:3000",
        '"X-Proxied-By-Caddy": ["true"]',
        '"X-Forwarded-For": ["{http.request.remote.host}"]',
        '"delete": ["X-Admin-Gateway-Token"]',
        '"status_code": 503',
        f'\\"{required}\\" != \\"1\\"',
    ):
        assert expected in public_json
    assert "X-Remote-Analyst" not in public_json and "client_authentication" not in public_json
    assert '"X-Forwarded-For": ["127.0.0.1"]' in http_json and "X-Proxied-By-Caddy" not in http_json
    (certificate,) = adapted["apps"]["tls"]["certificates"]["load_files"]
    assert certificate["certificate"] == "/certs/public/server.crt"
    assert certificate["key"] == "/certs/public/server.key"


ROOT = Path("/work/project [test]+")


def process(pid, ppid=1, executable="python3", command=None, born="Mon Sep 7 12:00:00 2026"):
    return stack.Process(pid, ppid, born, executable, command or f"python3 {ROOT}/.venv/bin/uvicorn backend.main:app")


class FakeSystem:
    def __init__(self, processes, cwd=None, listeners=None, holders=None):
        self.processes = {p.pid: p for p in processes}
        self.cwd, self.listeners, self.holders = cwd or {}, listeners or {}, holders or {}
        self.signals, self.time = [], 0
        self.on_signal = lambda pid, sig: None

    def snapshot(self):
        return dict(self.processes)

    def working_directory(self, pid):
        return self.cwd.get(pid)

    def executable(self, p):
        return p.executable

    def listening_pids(self, port):
        return self.listeners.get(port, set())

    def holding_pids(self, paths):
        return set().union(*(self.holders.get(p, set()) for p in paths))

    def signal(self, pid, sig):
        assert pid > 1
        self.signals.append((pid, sig))
        self.on_signal(pid, sig)

    def sleep(self, seconds):
        self.time += seconds

    def monotonic(self):
        return self.time


def test_parse_ps_preserves_space_paths_and_birth_identity():
    rows = stack.parse_ps(" 42  1 Mon Sep  7 12:00:00 2026 /work/a b/python3\n")
    assert rows[42] == (1, "Mon Sep 7 12:00:00 2026", "/work/a b/python3")
    with pytest.raises(stack.CleanupError):
        stack.parse_ps("broken output")


@pytest.mark.parametrize(
    "executable,command,owned",
    [
        ("python3", f"python3 {ROOT}/.venv/bin/uvicorn backend.main:app --reload", True),
        ("python3", f'python3 "{ROOT}/.venv/bin/uvicorn" backend.main:app', True),
        ("python3", f"{ROOT}/.venv/bin/python3 -m uvicorn backend.main:app", True),
        ("node", f"node {ROOT}/frontend/node_modules/next/dist/bin/next dev", True),
        ("node", f"node {ROOT}/frontend/node_modules/.bin/next dev", True),
        ("node", f"node {ROOT}/frontend/.next/standalone/server.js", True),
        ("python3", "python3 /work/project\\ [test]+/.venv/bin/uvicorn backend.main:app", True),
        ("bash", f"bash -c 'python3 {ROOT}/.venv/bin/uvicorn backend.main:app'", False),
        ("python3", f"python3 -c 'print(\"uvicorn {ROOT}\")'", False),
        ("node", f"node -e 'console.log(\"{ROOT}/next\")'", False),
        ("python3", f"python3 {ROOT}-other/.venv/bin/uvicorn backend.main:app", False),
        ("python3", "python3 /another/repo/.venv/bin/uvicorn backend.main:app", False),
        ("node", f"node {ROOT}/frontend/custom-next-script.js", False),
    ],
)
def test_root_selection_is_executable_aware_and_literal(executable, command, owned):
    system = FakeSystem([process(42, executable=executable, command=command)])
    assert stack.select_roots(system.snapshot(), ROOT, {}, set(), system, set()) == ({42} if owned else set())


def test_listener_association_uses_cwd_and_captures_reloader():
    parent = process(41, command="python3 -m uvicorn backend.main:app --reload")
    child = process(42, 41, command="python3 -c 'spawn_main()'")
    system = FakeSystem([parent, child], cwd={41: ROOT, 42: ROOT})
    assert stack.select_roots(system.snapshot(), ROOT, {18002: {42}}, set(), system, set()) == {41}


def test_node_next_launcher_is_selected_before_renamed_listener():
    parent = process(41, executable="node", command=f"node {ROOT}/frontend/node_modules/.bin/next dev -H 127.0.0.1")
    child = process(42, 41, executable="node", command="next-server (v16.3.1)")
    system = FakeSystem([parent, child], cwd={42: ROOT / "frontend"})
    assert stack.select_roots(system.snapshot(), ROOT, {13002: {42}}, set(), system, set()) == {41}


def test_unrelated_listener_aborts_before_any_signal():
    system = FakeSystem(
        [process(42), process(43, executable="ssh", command="ssh -L 18002:localhost:8000 host")],
        cwd={42: ROOT.parent / "other"},
    )
    with pytest.raises(stack.CleanupError, match="listener"):
        stack.select_roots(system.snapshot(), ROOT, {18002: {43}}, set(), system, set())
    assert system.signals == []


@pytest.mark.parametrize(
    "executable,command",
    [("python3", "python3 -m http.server 18002"), ("node", "node unrelated-app.js")],
)
def test_unrelated_runtime_listener_in_same_checkout_is_not_killed(executable, command):
    system = FakeSystem([process(42, executable=executable, command=command)], cwd={42: ROOT})
    with pytest.raises(stack.CleanupError, match="recognized stack command"):
        stack.select_roots(system.snapshot(), ROOT, {18002: {42}}, set(), system, set())
    assert system.signals == []


def test_duckdb_holder_requires_python_executable():
    system = FakeSystem(
        [
            process(42, command="python3 -c 'spawn_main()'"),
            process(43, executable="bash", command="bash -c 'echo python3'"),
        ]
    )
    assert stack.select_roots(system.snapshot(), ROOT, {}, {42, 43}, system, set()) == {42}


def test_self_ancestors_and_their_trees_are_not_selected():
    system = FakeSystem([process(40), process(41, 40), process(42, 41), process(50)])
    excluded = stack.ancestors(system.snapshot(), 42)
    assert excluded == {1, 40, 41, 42}
    assert stack.select_roots(system.snapshot(), ROOT, {}, set(), system, excluded) == {50}
    assert stack.descendants(system.snapshot(), {40}, excluded) == set()


def test_descendants_survive_reparenting_and_escalation():
    system = FakeSystem([process(42), process(43, 42), process(44, 43)])

    def terminate(pid, sig):
        if pid == 42:
            system.processes.pop(42, None)
            system.processes[43] = process(43)
        if sig == signal.SIGKILL:
            system.processes.pop(pid, None)

    system.on_signal = terminate
    stack.terminate(system, {42}, system.snapshot(), set(), grace=0.2)
    assert {pid for pid, sig in system.signals if sig == signal.SIGTERM} == {42, 43, 44}
    assert {pid for pid, sig in system.signals if sig == signal.SIGKILL} == {43, 44}


def test_graceful_shutdown_does_not_escalate():
    system = FakeSystem([process(42)])
    system.on_signal = lambda pid, sig: system.processes.pop(pid)
    stack.terminate(system, {42}, system.snapshot(), set(), grace=0.2)
    assert system.signals == [(42, signal.SIGTERM)]


def test_term_visits_reloader_before_child_even_after_pid_wrap():
    system = FakeSystem([process(99), process(42, 99)])
    system.on_signal = lambda pid, sig: system.processes.pop(pid)
    stack.terminate(system, {99}, system.snapshot(), set(), grace=0.2)
    assert system.signals == [(99, signal.SIGTERM), (42, signal.SIGTERM)]


def test_reused_pid_is_never_killed():
    system = FakeSystem([process(42)])
    system.on_signal = lambda pid, sig: system.processes.update({pid: process(pid, born="new birth")})
    stack.terminate(system, {42}, system.snapshot(), set(), grace=0.2)
    assert system.signals == [(42, signal.SIGTERM)]


def test_incomplete_shutdown_is_an_error():
    system = FakeSystem([process(42)])
    with pytest.raises(stack.CleanupError, match="surviv"):
        stack.terminate(system, {42}, system.snapshot(), set(), grace=0.2)


def test_captured_root_identity_reuse_does_not_select_new_descendants():
    system = FakeSystem([process(42, born="new birth"), process(43, 42)])
    stack.terminate(system, {42}, {42: process(42)}, set(), grace=0.2)
    assert system.signals == []


def test_pid_reuse_between_snapshot_and_term_is_not_signalled():
    system = FakeSystem([process(42)])
    captured = system.snapshot()
    system.processes[42] = process(42, born="new birth")
    stack.terminate(system, {42}, captured, set(), grace=0.2)
    assert system.signals == []


def test_permission_denied_is_not_silently_ignored():
    system = FakeSystem([process(42)])
    system.on_signal = lambda pid, sig: (_ for _ in ()).throw(PermissionError("denied"))
    with pytest.raises(stack.CleanupError, match="Permission denied"):
        stack.terminate(system, {42}, system.snapshot(), set(), grace=0.2)


def test_system_uses_argument_arrays_for_ps_lsof_and_preserves_literal_paths(monkeypatch):
    commands = []
    path = ROOT / "data/services/file [a]+.duckdb"

    def fake_run(args, **kwargs):
        assert isinstance(args, list) and not kwargs.get("shell")
        commands.append(args)
        if args[0] == "ps":
            tail = "python3" if "comm=" in args[-1] else f"python3 {ROOT}/.venv/bin/uvicorn backend.main:app"
            return subprocess.CompletedProcess(args, 0, f"42 1 Mon Sep 7 12:00:00 2026 {tail}\n", "")
        if "-d" in args:
            return subprocess.CompletedProcess(args, 0, f"p42\nfcwd\nn{ROOT}\n", "")
        return subprocess.CompletedProcess(args, 0, "42\n", "")

    monkeypatch.setattr(stack.subprocess, "run", fake_run)
    system = stack.System()
    assert system.snapshot() == {42: process(42)}
    assert system.working_directory(42) == ROOT
    assert system.listening_pids(18002) == {42}
    assert system.holding_pids([path]) == {42}
    assert ["lsof", "-t", "--", str(path)] in commands
    assert ["lsof", "-nP", "-iTCP:18002", "-sTCP:LISTEN", "-t"] in commands


def test_next_title_requires_actual_node_text_executable(monkeypatch):
    system = stack.System()
    monkeypatch.setattr(system, "run", lambda *args, **kwargs: "p42\nftxt\nn/usr/local/bin/node\n")
    p = process(42, executable="next-server (v16.3.1)", command="next-server (v16.3.1)")
    assert system.executable(p) == "/usr/local/bin/node"
    monkeypatch.setattr(system, "run", lambda *args, **kwargs: "p42\nftxt\nn/usr/local/bin/bash\n")
    assert stack.runtime(system.executable(p)) is None


@pytest.mark.parametrize("returncode,stderr", [(2, ""), (1, "permission denied")])
def test_lsof_failures_are_not_treated_as_an_empty_stack(monkeypatch, returncode, stderr):
    monkeypatch.setattr(
        stack.subprocess, "run", lambda args, **kwargs: subprocess.CompletedProcess(args, returncode, "", stderr)
    )
    with pytest.raises(stack.CleanupError):
        stack.System().listening_pids(18002)


def test_stop_only_inspects_exact_existing_service_databases(monkeypatch, tmp_path):
    checkout = tmp_path / "repo"
    db_dir = checkout / "data/services"
    db_dir.mkdir(parents=True)
    expected = db_dir / "example.duckdb"
    expected.touch()
    (db_dir / "example.duckdb.wal").touch()
    (db_dir / "nested").mkdir()
    (db_dir / "nested/other.duckdb").touch()
    outside = tmp_path / "outside.duckdb"
    outside.touch()
    (db_dir / "linked.duckdb").symlink_to(outside)
    system = FakeSystem([])
    calls = []
    system.holding_pids = lambda paths: (calls.append(paths), set())[1]
    monkeypatch.setattr(stack, "System", lambda: system)
    assert stack.main(["stop", "--checkout", str(checkout), "--port", "18002"]) == 0
    assert calls == [[expected], [expected]] and system.signals == []


def test_stop_detects_respawn_after_termination(monkeypatch, tmp_path):
    system = FakeSystem([])
    monkeypatch.setattr(stack, "System", lambda: system)
    monkeypatch.setattr(
        stack,
        "terminate",
        lambda *args: system.processes.update(
            {42: process(42, command=f"python3 {tmp_path}/.venv/bin/uvicorn backend.main:app")}
        ),
    )
    assert stack.main(["stop", "--checkout", str(tmp_path), "--port", "18002"]) == 1


def test_capture_and_children_preserve_orphan_identity(monkeypatch, tmp_path, capsys):
    launcher = process(40, executable="bash", command="bash run.sh")
    helper = process(41, 40, command="python3 scripts/local_stack_processes.py capture")
    root = process(42, 40, executable="uv", command="uv run uvicorn backend.main:app")
    worker = process(43, 42)
    system = FakeSystem([launcher, helper, root, worker])
    monkeypatch.setattr(stack, "System", lambda: system)
    monkeypatch.setattr(stack.os, "getpid", lambda: 41)
    assert stack.main(["capture", "--checkout", str(tmp_path), "--pid", "42"]) == 0
    captured = capsys.readouterr().out.strip()
    assert {record["pid"] for record in json.loads(captured)} == {42, 43}
    system.processes.pop(42)
    system.processes[43] = process(43, 1)
    system.on_signal = lambda pid, sig: system.processes.pop(pid)
    assert stack.main(["children", "--checkout", str(tmp_path), "--captured", captured]) == 0
    assert system.signals == [(43, signal.SIGTERM)]


def test_capture_rejects_unrelated_process(monkeypatch, tmp_path):
    system = FakeSystem([process(42)])
    monkeypatch.setattr(stack, "System", lambda: system)
    assert stack.main(["capture", "--checkout", str(tmp_path), "--pid", "42"]) == 1
    assert system.signals == []


def test_launcher_contains_no_name_or_group_termination():
    source = Path("run.sh").read_text()
    for banned in ("pkill", "killall", "PGID", "xargs kill"):
        assert banned not in source


@pytest.mark.parametrize("status", [0, 7])
def test_run_stop_uses_helper_and_propagates_failure_without_startup(tmp_path, status):
    checkout = tmp_path / "checkout"
    (checkout / "scripts").mkdir(parents=True)
    (checkout / "run.sh").write_text(Path("run.sh").read_text())
    (checkout / ".env").write_text("FRONTEND_PORT=13002\nBACKEND_PORT=18002\n")
    (checkout / "scripts/local_stack_processes.py").write_text(
        f"import json, pathlib, sys\npathlib.Path('called.json').write_text(json.dumps(sys.argv[1:]))\nsys.exit({status})\n"
    )
    result = subprocess.run(["bash", "run.sh", "--stop"], cwd=checkout, capture_output=True, text=True, env=os.environ)
    assert result.returncode == status
    args = json.loads((checkout / "called.json").read_text())
    assert "stop" in args and str(checkout) in args and "18002" in args and "13002" in args
    assert "Syncing backend dependencies" not in result.stdout
    assert ("Stack stopped." in result.stdout) == (status == 0)


def test_percentile_interpolates():
    assert awv._percentile([], 0.5) == 0.0
    assert awv._percentile([42.0], 0.95) == 42.0
    assert awv._percentile([0.0, 10.0], 0.5) == 5.0
    vals = [float(x) for x in range(0, 101, 10)]
    assert awv._percentile(vals, 0.5) == 50.0 and awv._percentile(vals, 0.75) == 75.0


def test_verdict_buckets_against_thresholds():
    assert awv._verdict("LCP", 2000.0) == "good"
    assert awv._verdict("LCP", 3000.0) == "needs-improvement"
    assert awv._verdict("LCP", 5000.0) == "poor"
    assert awv._verdict("CLS", 0.05) == "good"
    assert awv._verdict("UNKNOWN", 1.0) == "unknown"


def test_aggregate_groups_by_route_and_metric():
    samples = [
        {"name": "LCP", "value": 1000.0, "rating": "good", "pathname": "/a"},
        {"name": "LCP", "value": 3000.0, "rating": "needs-improvement", "pathname": "/a"},
        {"name": "LCP", "value": 500.0, "rating": "good", "pathname": "/b"},
        {"name": "BOGUS", "value": 1.0, "rating": "good", "pathname": "/a"},
    ]
    stats = awv.aggregate(samples)
    assert ("/a", "LCP") in stats and ("/b", "LCP") in stats and ("/a", "BOGUS") not in stats
    assert stats[("/a", "LCP")]["count"] == 2
    assert stats[("/a", "LCP")]["ratings"] == {"good": 1, "needs-improvement": 1, "poor": 0}


def test_build_report_data_focus_excludes_good_and_honors_min_samples():
    stats = {
        ("/slow", "LCP"): {
            "count": 10,
            "p50": 4000.0,
            "p75": 5000.0,
            "p95": 6000.0,
            "ratings": {"good": 0, "needs-improvement": 0, "poor": 10},
            "verdict": "poor",
        },
        ("/fast", "LCP"): {
            "count": 10,
            "p50": 800.0,
            "p75": 1000.0,
            "p95": 1200.0,
            "ratings": {"good": 10, "needs-improvement": 0, "poor": 0},
            "verdict": "good",
        },
        ("/rare", "LCP"): {
            "count": 1,
            "p50": 9000.0,
            "p75": 9000.0,
            "p95": 9000.0,
            "ratings": {"good": 0, "needs-improvement": 0, "poor": 1},
            "verdict": "poor",
        },
    }
    report = awv.build_report_data(stats, min_samples=3)
    routes_in_focus = {r["route"] for r in report["focus"]}
    assert routes_in_focus == {"/slow"}
    assert any(r["route"] == "/fast" for r in report["rows"])


def test_load_samples_filters_cohort_and_tolerates_torn_lines(tmp_path):
    p = tmp_path / "wv.jsonl"
    p.write_text(
        '{"name":"LCP","value":1,"rating":"good","cohort":"admin","ts":"2026-06-19T00:00:00Z"}\n'
        '{"name":"LCP","value":2,"rating":"good","cohort":"analyst","ts":"2026-06-19T00:00:00Z"}\n'
        '{"name":"LCP","value":3,  <-- torn line\n',
        encoding="utf-8",
    )
    admin_only = awv.load_samples(p, since=None, cohort="admin")
    assert len(admin_only) == 1 and admin_only[0]["cohort"] == "admin"
    assert len(awv.load_samples(p, since=None, cohort="all")) == 2


def test_main_no_file_is_a_clean_noop(tmp_path, capsys):
    assert awv.main(["--input", str(tmp_path / "missing.jsonl")]) == 0
    assert "No web-vitals data" in capsys.readouterr().err


def _rec(value: float, route: str = "/x") -> str:
    return json.dumps(
        {
            "name": "LCP",
            "value": value,
            "rating": "poor",
            "cohort": "admin",
            "pathname": route,
            "ts": "2026-06-19T00:00:00Z",
        }
    )


def test_main_purge_deletes_file_after_report(tmp_path, capsys):
    p = tmp_path / "wv.jsonl"
    p.write_text("\n".join(_rec(v) for v in (5000, 5100, 5200, 5300)) + "\n", encoding="utf-8")
    assert awv.main(["--input", str(p), "--min-samples", "1", "--purge"]) == 0
    out = capsys.readouterr()
    assert "Web Vitals Analysis" in out.out and "/x" in out.out and not p.exists() and "Purged" in out.err


def test_main_purge_off_keeps_file(tmp_path):
    p = tmp_path / "wv.jsonl"
    p.write_text(_rec(5000) + "\n", encoding="utf-8")
    awv.main(["--input", str(p), "--min-samples", "1"])
    assert p.exists()


@pytest.mark.parametrize("fmt", ["md", "json"])
def test_main_writes_to_out_file(tmp_path, fmt):
    p = tmp_path / "wv.jsonl"
    p.write_text(_rec(5000) + "\n", encoding="utf-8")
    out = tmp_path / "report.out"
    assert awv.main(["--input", str(p), "--min-samples", "1", "--format", fmt, "--out", str(out)]) == 0
    assert out.exists() and out.read_text(encoding="utf-8").strip()


def test_main_reads_rotated_segment_and_purge_removes_both(tmp_path, capsys):
    active, rotated = tmp_path / "wv.jsonl", tmp_path / "wv.jsonl.1"
    rotated.write_text("\n".join(_rec(v, "/old") for v in (5000, 5100, 5200)) + "\n", encoding="utf-8")
    active.write_text("\n".join(_rec(v, "/new") for v in (4800, 4900, 5300)) + "\n", encoding="utf-8")
    assert awv.main(["--input", str(active), "--min-samples", "1", "--purge"]) == 0
    out = capsys.readouterr()
    assert "/old" in out.out and "/new" in out.out and not active.exists() and not rotated.exists()
