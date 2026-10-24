"""Consolidated tests for test_repo_and_ci_ratchets."""

from __future__ import annotations

import os
import re

import pytest
import yaml

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MAKEFILE = os.path.join(REPO_ROOT, "Makefile")
CI_YML = os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")

# Intentional Make<->CI divergences this test deliberately does NOT flag, with
# rationale (AGENTS.md: gate-for-gate parity is intentional; these are the few
# documented exceptions):
#   - `make test-ci` prefixes `nice -n 15` -- local dev ergonomics only; a
#     no-op on CI's dedicated runner, and `-n auto` worker density is identical.
#   - `make scorer-test` uses `--manifest-path compute/scorer/Cargo.toml` while
#     the Scorer CI job sets working-directory and runs a bare `cargo test`.
# These gates are therefore excluded from the byte-for-byte parity list below.


def _read(path: str) -> str:
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _makefile_recipe(name: str) -> str:
    """Return the tab-indented recipe body of a Makefile target as one string."""
    text = _read(MAKEFILE)
    lines = text.splitlines()
    out: list[str] = []
    in_target = False
    for line in lines:
        if not in_target:
            # Target header, e.g. "test-ci:" or "openapi-drift: gen-types"
            if re.match(rf"^{re.escape(name)}\s*:", line):
                in_target = True
            continue
        # Recipe lines are tab-indented; anything at column 0 ends the recipe.
        if line.startswith("\t"):
            out.append(line.lstrip("\t"))
        elif line.strip() == "":
            continue
        else:
            break
    if not out:
        raise AssertionError(f"Makefile target not found or empty: {name}")
    return "\n".join(out)


def _ci_step_run(step_name: str) -> str:
    """Return the ``run`` block of the ci.yml step whose name matches."""
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
    """Normalise a pytest ``-m`` expression into its set of excluded markers.

    Tolerant of ordering/spacing: ``not a and not b`` == ``not b and not a``.
    Only the ``not X [and not Y ...]`` shape the CI gates use is handled.
    """
    return frozenset(re.findall(r"not\s+(\w+)", expr))


def _cov_floors(text: str) -> set[int]:
    return {int(m) for m in re.findall(r"--cov-fail-under=(\d+)", text)}


def _vitest_thresholds(text: str) -> dict[str, int]:
    return {key: int(val) for key, val in re.findall(r"--coverage\.thresholds\.(\w+)=(\d+)", text)}


def _nonterraform_marker(text: str) -> str:
    """Pull the ``-m`` expression from the pytest run that EXCLUDES terraform."""
    for m in re.findall(r'-m\s+"([^"]*)"', text):
        if "not terraform_cli" in m:
            return m
    raise AssertionError(f"no 'not terraform_cli' pytest marker found in:\n{text}")


# ── Backend pytest: marker-expression parity ───────────────────────────────────


def test_backend_pytest_marker_parity() -> None:
    make = _makefile_recipe("test-ci")
    ci = _ci_step_run("Tests (pytest with coverage)")

    make_markers = _marker_set(_nonterraform_marker(make))
    ci_markers = _marker_set(_nonterraform_marker(ci))

    assert make_markers == ci_markers, (
        "Backend pytest marker exclusions drifted between `make test-ci` and "
        f"ci.yml.\n  make: {sorted(make_markers)}\n  ci:   {sorted(ci_markers)}\n"
        "Reconcile both to the same `-m` expression."
    )
    # flaky_quarantine is registered as 'skipped in standard CI runs'; it MUST
    # be excluded on both sides (the original S5 drift).
    assert "flaky_quarantine" in make_markers
    assert "terraform_cli" in make_markers


# ── Backend coverage floor parity ──────────────────────────────────────────────


def test_backend_coverage_floor_parity() -> None:
    make_floors = _cov_floors(_makefile_recipe("test-ci"))
    ci_floors = _cov_floors(_ci_step_run("Tests (pytest with coverage)"))

    assert make_floors == ci_floors, (
        f"Backend --cov-fail-under drifted.\n  make: {sorted(make_floors)}\n  ci:   {sorted(ci_floors)}"
    )
    assert len(make_floors) == 1, f"expected exactly one backend coverage floor, got {make_floors}"
    # Guard against anyone weakening the floor via this reconciliation.
    assert min(make_floors) >= 85, f"backend coverage floor must not drop below 85, got {min(make_floors)}"


# ── Frontend vitest threshold parity ───────────────────────────────────────────


def test_frontend_vitest_threshold_parity() -> None:
    make = _vitest_thresholds(_makefile_recipe("test-frontend-ci"))
    ci = _vitest_thresholds(_ci_step_run("Tests (vitest with coverage)"))

    assert make, "no vitest thresholds parsed from `make test-frontend-ci`"
    assert make == ci, f"Frontend vitest coverage thresholds drifted.\n  make: {make}\n  ci:   {ci}"
    # Known floors must not be silently weakened.
    expected = {"lines": 66, "statements": 65, "functions": 54, "branches": 52}
    for key, floor in expected.items():
        assert make.get(key, -1) >= floor, f"vitest {key} threshold weakened below {floor}: {make.get(key)}"


# ── Shared single-command gates: byte-for-byte parity ──────────────────────────

# (make target, ci.yml step name) pairs whose gate command is a single shell
# line expected to be identical modulo surrounding whitespace.
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
    (
        "urllib-guard",
        "HTTP client guard (no raw urllib in backend app modules)",
        "bash scripts/check_no_raw_urllib.sh",
    ),
    (
        "dateparse-guard",
        "Date-parsing guard (no new raw strptime/fromisoformat in core + repositories)",
        "bash scripts/check_no_raw_dateparse.sh",
    ),
    (
        "retry-guard",
        "Retry-loop guard (no new hand-rolled retry loops in backend)",
        "bash scripts/check_no_handrolled_retry.sh",
    ),
    (
        "coalesce-guard",
        "Metric COALESCE guard (no new inline COALESCE in repositories + rollups)",
        "bash scripts/check_no_inline_metric_coalesce.sh",
    ),
    ("security-regression", "Security-regression count gate", "bash scripts/check_security_regression_count.sh"),
]


@pytest.mark.parametrize("make_target,ci_step,expected_cmd", _SHARED_COMMAND_GATES)
def test_shared_single_command_gate_parity(make_target: str, ci_step: str, expected_cmd: str) -> None:
    make_body = _makefile_recipe(make_target)
    ci_body = _ci_step_run(ci_step)

    # The command must appear verbatim in both the Makefile recipe and the
    # ci.yml run block (both may carry extra comments/wrappers around it).
    assert expected_cmd in make_body, f"`make {make_target}` no longer runs `{expected_cmd}`:\n{make_body}"
    assert expected_cmd in ci_body, f"ci.yml step '{ci_step}' no longer runs `{expected_cmd}`:\n{ci_body}"


# ── Merged from test_ratchet_guards.py ──────────────────────────────────────────────

import shutil
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "check_no_raw_dateparse.sh"


def _run_dateparse(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", str(root / "scripts" / "check_no_raw_dateparse.sh")], capture_output=True, text=True)


def _repo_dateparse(tmp_path: Path, files: dict[str, str]) -> Path:
    (tmp_path / "scripts" / "lib").mkdir(parents=True)
    shutil.copy(SCRIPT, tmp_path / "scripts" / SCRIPT.name)
    shutil.copy(SCRIPT.parent / "lib" / "ratchet_common.sh", tmp_path / "scripts" / "lib" / "ratchet_common.sh")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    for rel, body in files.items():
        f = tmp_path / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(body)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def test_guard_passes_on_this_repo() -> None:
    res = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr


def test_guard_flags_new_raw_site_in_new_file(tmp_path: Path) -> None:
    root = _repo_dateparse(tmp_path, {"backend/repositories/new.py": 'x = datetime.strptime(s, "%Y-%m-%d-%H")\n'})
    res = _run_dateparse(root)
    assert res.returncode == 1
    assert "backend/repositories/new.py" in res.stderr


def test_guard_flags_count_above_baseline(tmp_path: Path) -> None:
    body = "a = datetime.fromisoformat(x)\nb = datetime.fromisoformat(y)\n"
    root = _repo_dateparse(tmp_path, {"backend/core/clickhouse_rows.py": body})
    assert _run_dateparse(root).returncode == 1


def test_guard_ignores_comments_and_tests(tmp_path: Path) -> None:
    root = _repo_dateparse(
        tmp_path,
        {
            "backend/core/ok.py": "# datetime.fromisoformat(x) was the bug\n",
            "backend/utils/date_utils.py": "datetime.fromisoformat(s)\n",
            "tests/test_x.py": "datetime.strptime(s, f)\n",
        },
    )
    assert _run_dateparse(root).returncode == 0


# ── Merged from test_retry_and_coalesce_guards.py ──

from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
RETRY = "check_no_handrolled_retry.sh"
COALESCE = "check_no_inline_metric_coalesce.sh"


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    (tmp_path / "scripts" / "lib").mkdir(parents=True)
    for name in (RETRY, COALESCE, "check_no_handrolled_retry.py", "lib/ratchet_common.sh"):
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


@pytest.mark.parametrize("script", [RETRY, COALESCE])
def test_guards_pass_on_this_repo(script: str) -> None:
    res = subprocess.run(["bash", str(SCRIPTS / script)], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr


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
    root = _repo(tmp_path, {"backend/core/new_thing.py": RETRY_LOOP})
    res = _run(root, RETRY)
    assert res.returncode == 1
    assert "backend/core/new_thing.py" in res.stderr


def test_retry_guard_flags_while_loop(tmp_path: Path) -> None:
    body = "import time\n\ndef f(op):\n    while True:\n        try:\n            return op()\n        except Exception:\n            time.sleep(1)\n"
    assert _run(_repo(tmp_path, {"backend/utils/x.py": body}), RETRY).returncode == 1


def test_retry_guard_ignores_sleep_without_handler_and_nested_functions(tmp_path: Path) -> None:
    body = (
        "import time\n\n"
        "def poll():\n    for _ in range(3):\n        time.sleep(1)\n\n"
        "def outer():\n    for _ in range(3):\n"
        "        def inner():\n            try:\n                pass\n            except OSError:\n                time.sleep(1)\n"
    )
    assert _run(_repo(tmp_path, {"backend/utils/ok.py": body}), RETRY).returncode == 0


def test_retry_guard_exempts_retry_module_and_tests(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/core/retry.py": RETRY_LOOP, "tests/test_x.py": RETRY_LOOP})
    assert _run(root, RETRY).returncode == 0


def test_retry_guard_flags_count_above_baseline(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/utils/valkey_publisher.py": RETRY_LOOP + RETRY_LOOP.replace("def f", "def g")})
    assert _run(root, RETRY).returncode == 1


def test_coalesce_guard_flags_new_inline_site(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/repositories/new.py": 'q = "SELECT COALESCE(SUM(x), 0) FROM t"\n'})
    res = _run(root, COALESCE)
    assert res.returncode == 1
    assert "backend/repositories/new.py" in res.stderr


def test_coalesce_guard_flags_rollup_writer(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/core/rollups/new_rollup.py": 'q = "COALESCE(a, 0)"\n'})
    assert _run(root, COALESCE).returncode == 1


def test_coalesce_guard_ignores_comments_sql_templates_and_helpers(tmp_path: Path) -> None:
    root = _repo(
        tmp_path,
        {
            "backend/repositories/ok.py": "# COALESCE(x, 0) was inline here\n",
            "backend/repositories/_sql/tpl.py": 'T = "COALESCE(a, 0)"\n',
            "backend/core/metric_expressions.py": 'f"COALESCE({a}, {b})"\n',
        },
    )
    assert _run(root, COALESCE).returncode == 0


# ── Merged from test_http_exception_envelope_shape.py ──

import ast
from pathlib import Path

ROUTERS = Path(__file__).resolve().parent.parent / "backend" / "routers"

# Explicit allowlist for HTTP-standard reason-phrase strings that callers may
# want to return verbatim. Add sparingly — prefer the dict envelope where
# possible so the frontend can pattern-match on a machine-readable code.
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

# All four envelope helpers return the canonical `{"error": code, ...}`
# shape; the test treats `HTTPException(detail=<helper>(...))` as the
# preferred form. `make_error` was missed in the initial sweep
# (commit f0f206b) — adding it here so the routers that wrap envelope-
# bearing branches like `db_busy` / `db_locked` round-trip cleanly.
HELPER_FUNCS = frozenset({"bad_request", "not_found", "validation_failed", "make_error"})


def _iter_router_files() -> list[Path]:
    return sorted(p for p in ROUTERS.rglob("*.py") if p.name != "__init__.py")


def _detail_kw(call: ast.Call) -> ast.expr | None:
    for kw in call.keywords:
        if kw.arg == "detail":
            return kw.value
    # detail could also be the second positional arg
    if len(call.args) >= 2:
        return call.args[1]
    return None


def _status_code_kw(call: ast.Call) -> int | None:
    for kw in call.keywords:
        if kw.arg == "status_code" and isinstance(kw.value, ast.Constant):
            value = kw.value.value
            if isinstance(value, int):
                return value
    if call.args and isinstance(call.args[0], ast.Constant):
        value = call.args[0].value
        if isinstance(value, int):
            return value
    return None


def _is_canonical_helper_call(node: ast.expr) -> bool:
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in HELPER_FUNCS


def _is_error_first_dict(node: ast.expr) -> bool:
    """Accept dict literals that lead with ``"error"`` (canonical shape) OR
    ``"errors"`` (legacy multi-message validation shape that extractApiError
    joins for display). A future cleanup pass will migrate the ``errors``
    sites onto ``validation_failed`` once the frontend's extractApiError is
    taught the canonical ``{error, messages}`` shape from that helper."""
    if not isinstance(node, ast.Dict):
        return False
    if not node.keys:
        return False
    first = node.keys[0]
    return isinstance(first, ast.Constant) and first.value in {"error", "errors"}


def test_http_exception_envelope_shape() -> None:
    findings: list[str] = []

    for path in _iter_router_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else None
            if name != "HTTPException":
                continue

            detail = _detail_kw(node)
            if detail is None:
                continue

            # Helper calls and {"error": ...} dicts are fine.
            if _is_canonical_helper_call(detail) or _is_error_first_dict(detail):
                continue

            # String constant: check the allowlist.
            if isinstance(detail, ast.Constant) and isinstance(detail.value, str):
                status = _status_code_kw(node)
                if status is not None and (status, detail.value) in ALLOWED_STRING_DETAILS:
                    continue
                findings.append(
                    f"{path.relative_to(ROUTERS.parent.parent)}:{node.lineno} — "
                    f"HTTPException(detail={detail.value!r}); "
                    "use bad_request/not_found/validation_failed or raise_internal"
                )
                continue

            # Anything else (f-strings, name references, complex exprs) is
            # also suspect.
            findings.append(
                f"{path.relative_to(ROUTERS.parent.parent)}:{node.lineno} — "
                f"HTTPException(detail=<{type(detail).__name__}>); "
                "use bad_request/not_found/validation_failed or raise_internal"
            )

    assert not findings, "Non-canonical HTTPException envelopes:\n  " + "\n  ".join(findings)


# ── Merged from test_observability_config_drift.py ──────────────────────────────────────────────

import os

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CANONICAL_DIR = os.path.join(REPO_ROOT, "observability")
CHART_DIR = os.path.join(REPO_ROOT, "deploy", "chart", "fastly-log-analytics", "files", "observability")

# Scrape jobs the chart copy is allowed to drop vs. the canonical file, with
# the reason. Anything NOT listed here must be present identically in both.
CHART_OMITTED_PROMETHEUS_JOBS = {
    "docker": ("docker-stats-exporter uses the Docker Engine API, absent in Kubernetes"),
}


def _load_yaml(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    assert isinstance(data, dict), f"expected a YAML mapping in {path}"
    return data


def test_grafana_datasources_chart_copy_matches_canonical() -> None:
    chart = _load_yaml(os.path.join(CHART_DIR, "grafana-datasources.yml"))
    assert "datasources" in chart and isinstance(chart["datasources"], list), (
        "deploy/chart/.../files/observability/grafana-datasources.yml must define datasources."
    )


def test_prometheus_chart_copy_matches_canonical_minus_omitted_jobs() -> None:
    canonical = _load_yaml(os.path.join(CANONICAL_DIR, "prometheus.yml"))
    chart = _load_yaml(os.path.join(CHART_DIR, "prometheus.yml"))

    # Non-scrape config (global, rule_files, ...) must match verbatim.
    canonical_rest = {k: v for k, v in canonical.items() if k != "scrape_configs"}
    chart_rest = {k: v for k, v in chart.items() if k != "scrape_configs"}
    assert canonical_rest == chart_rest, (
        "Prometheus chart copy drifted from canonical outside scrape_configs.\n"
        f"  canonical: {canonical_rest}\n  chart:     {chart_rest}"
    )

    canonical_jobs = {j["job_name"]: j for j in canonical.get("scrape_configs", [])}
    chart_jobs = {j["job_name"]: j for j in chart.get("scrape_configs", [])}

    # The chart may ONLY omit the documented jobs; no silent additions/renames.
    missing = set(canonical_jobs) - set(chart_jobs)
    assert missing == set(CHART_OMITTED_PROMETHEUS_JOBS), (
        "Prometheus chart copy omits an UNDOCUMENTED set of scrape jobs.\n"
        f"  omitted:    {sorted(missing)}\n"
        f"  documented: {sorted(CHART_OMITTED_PROMETHEUS_JOBS)}\n"
        "Reconcile the chart copy to the canonical observability/prometheus.yml "
        "or record the intentional omission in CHART_OMITTED_PROMETHEUS_JOBS."
    )
    extra = set(chart_jobs) - set(canonical_jobs)
    assert not extra, f"Prometheus chart copy has scrape jobs not in canonical: {sorted(extra)}"

    # Every job the chart DOES carry must match the canonical definition.
    for name, chart_job in chart_jobs.items():
        assert chart_job == canonical_jobs[name], (
            f"Prometheus scrape job '{name}' drifted between canonical and chart "
            f"copy.\n  canonical: {canonical_jobs[name]}\n  chart:     {chart_job}"
        )


# Canonical files that are intentionally NOT byte-copied into the chart, with
# the reason. prometheus.yml is compared structurally above.
CHART_EXEMPT_FILES = {
    "docker_stats_exporter.py": "docker-compose-only sidecar; needs the Docker Engine API",
    "prometheus.yml": "compared structurally by the Prometheus test above (documented transform)",
}


def _relative_files(root: str) -> set[str]:
    found: set[str] = set()
    for dirpath, dirs, names in os.walk(root):
        # Bytecode caches appear whenever a test imports a script from these dirs.
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for name in names:
            found.add(os.path.relpath(os.path.join(dirpath, name), root))
    return found


def test_every_canonical_observability_file_is_guarded_in_the_chart() -> None:
    canonical = _relative_files(CANONICAL_DIR)
    chart = _relative_files(CHART_DIR)

    missing = (canonical - set(CHART_EXEMPT_FILES)) - chart
    assert not missing, (
        f"chart copy is missing canonical observability files: {sorted(missing)}. "
        "Copy them into deploy/chart/fastly-log-analytics/files/observability/ "
        "or record the exemption in CHART_EXEMPT_FILES."
    )
    duplicate = (canonical - set(CHART_EXEMPT_FILES)) & chart
    assert not duplicate, (
        f"observability/ still holds duplicate copies of chart files: {sorted(duplicate)}. "
        "Reference deploy/chart/fastly-log-analytics/files/observability/ directly."
    )


def test_chart_observability_copies_are_byte_identical() -> None:
    for rel in sorted(_relative_files(CANONICAL_DIR) - set(CHART_EXEMPT_FILES)):
        with open(os.path.join(CANONICAL_DIR, rel), "rb") as fh:
            canonical_bytes = fh.read()
        with open(os.path.join(CHART_DIR, rel), "rb") as fh:
            chart_bytes = fh.read()
        assert chart_bytes == canonical_bytes, (
            f"{rel} drifted from the canonical observability/{rel}. Re-copy it: "
            f"cp observability/{rel} deploy/chart/fastly-log-analytics/files/observability/{rel}"
        )


# ── Merged from test_docker_stats_exporter.py ──

from observability.docker_stats_exporter import _block_io_bytes


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


# ── Merged from test_elevation_dashboard.py ──

import json
from pathlib import Path

import pytest


def _dashboard() -> dict:
    dashboard_path = Path("local-docs/fla-multipod-elevation.json")
    if not dashboard_path.exists():
        pytest.skip("Elevation dashboard export is intentionally local-only")
    return json.loads(dashboard_path.read_text())


def test_elevation_dashboard_includes_high_scale_operational_panels() -> None:
    dashboard = _dashboard()
    panels = dashboard["panels"]
    panel_text = json.dumps(panels)

    expected_metrics = (
        "fla_high_scale_source_objects",
        "fla_high_scale_oldest_active_age_seconds",
        "fla_high_scale_publication_lag_seconds",
        "fla_high_scale_pending_manifests",
        "fla_high_scale_rows_published_total",
    )

    assert len({panel["id"] for panel in panels}) == len(panels)
    for metric in expected_metrics:
        assert metric in panel_text


def test_elevation_dashboard_keeps_variable_based_promql() -> None:
    dashboard = _dashboard()
    panel_text = json.dumps(dashboard["panels"])

    assert "$site" in panel_text
    assert "$namespace" in panel_text


# ── Merged from test_clickhouse_compose.py ──


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
        {
            "record": "app_clickhouse_bytes_read_bytes_total",
            "expr": "app_clickhouse_bytes_read_total",
        }
    ]


# ── Merged from test_local_resource_governance.py ──────────────────────────────────────────────

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


class _ComposeLoader(yaml.SafeLoader):
    """SafeLoader that tolerates compose merge tags (``!override``/``!reset``)."""


def _passthrough_tag(loader, node):
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    return loader.construct_scalar(node)


for _tag in ("!override", "!reset"):
    _ComposeLoader.add_constructor(_tag, _passthrough_tag)


def _load_compose(compose_rel: str) -> dict:
    return yaml.load((REPO_ROOT / compose_rel).read_text(encoding="utf-8"), Loader=_ComposeLoader)


def _env_int(environment: list[str], key: str) -> int | None:
    prefix = f"{key}="
    for entry in environment:
        if entry.startswith(prefix):
            return int(entry[len(prefix) :])
    return None


def _service_env(compose_rel: str, service: str) -> list[str]:
    compose = yaml.safe_load((REPO_ROOT / compose_rel).read_text(encoding="utf-8"))
    return compose["services"][service]["environment"]


def _duration_seconds(value: str) -> int:
    value = value.strip()
    if value.endswith("ms"):
        return int(float(value[:-2]) / 1000)
    if value.endswith("m"):
        return int(float(value[:-1]) * 60)
    if value.endswith("h"):
        return int(float(value[:-1]) * 3600)
    if value.endswith("s"):
        return int(float(value[:-1]))
    return int(float(value))


def _service_healthcheck(compose_rel: str, service: str) -> dict:
    compose = yaml.safe_load((REPO_ROOT / compose_rel).read_text(encoding="utf-8"))
    return compose["services"][service]["healthcheck"]


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


def test_multipod_backend_bounds_duckdb_parallelism():
    env = _service_env("docker-compose.multipod.yml", "backend")
    threads = _env_int(env, "DUCKDB_THREADS")
    pool = _env_int(env, "DUCKDB_POOL_MAX_SIZE")
    assert threads is not None and 1 <= threads <= 4, (
        "docker-compose.multipod.yml backend must pin a bounded DUCKDB_THREADS (<=4)"
    )
    assert pool is not None and 1 <= pool <= 4, (
        "docker-compose.multipod.yml backend must pin a bounded DUCKDB_POOL_MAX_SIZE (<=4)"
    )


def test_multipod_worker_bounds_duckdb_parallelism():
    env = _service_env("docker-compose.multipod.yml", "worker")
    threads = _env_int(env, "DUCKDB_THREADS")
    assert threads is not None and 1 <= threads <= 2, (
        "docker-compose.multipod.yml worker must pin a bounded DUCKDB_THREADS "
        "(<=2); 6 celery procs x 6 default threads = 36 CPU-bound threads "
        "that starve the backend serving loop on the shared host"
    )


def test_local_standard_backend_healthcheck_covers_cold_start():
    """Base compose backend start_period must exceed the real cold-start time.

    Importing ``backend.main`` pulls in the full iceberg/ducklake stack
    (pyiceberg -> pyarrow -> pandas, s3fs/fsspec, duckdb); measured cold
    start to "Application startup complete" is ~107s on an IDLE Colima host
    and stretches well past that under the deploy's double-stack + build
    load. The original 45s start_period was never realistic: compose marks
    the still-importing backend ``unhealthy`` (start_period + retries x
    interval) and aborts ``docker compose up`` because frontend/caddy
    depend on ``service_healthy`` -- which under ``set -e`` kills the whole
    deploy_test_all.sh run before verification. prod (docker-compose.prod.yml)
    already uses a 25m start_period for the same reason; the base compose
    must likewise cover the cold start with margin.
    """
    hc = _service_healthcheck("docker-compose.yml", "backend")
    start_period = _duration_seconds(hc["start_period"])
    assert start_period >= 180, (
        "docker-compose.yml backend healthcheck start_period must be >= 180s "
        "to cover the measured ~107s cold start plus load margin, so compose "
        f"doesn't falsely mark it unhealthy mid-startup (got {hc['start_period']})"
    )


# Long-lived Python processes that run as container PID 1. uvicorn/celery are
# not init systems: an orphaned grandchild (scheduler watchdog subprocess, a
# crashed cold-start helper, an ad-hoc py-spy/pip during debugging) becomes an
# unreapable zombie under them. Docker then can't stop the container ("PID is
# zombie and can not be killed. Use the --init option ..."), so the next
# `docker compose up --force-recreate` fails non-zero and, under the deploy's
# `set -e`, aborts the entire run. `init: true` inserts tini as PID 1 to
# forward signals and reap zombies — Docker's own prescribed fix.
_INIT_REQUIRED_SERVICES = {
    "docker-compose.yml": ["backend"],
    "docker-compose.multipod.yml": ["backend", "worker"],
}


@pytest.mark.parametrize(
    ("compose_rel", "service"),
    [(compose_rel, service) for compose_rel, services in _INIT_REQUIRED_SERVICES.items() for service in services],
)
def test_long_lived_backend_services_run_an_init(compose_rel: str, service: str):
    compose = _load_compose(compose_rel)
    svc = compose["services"][service]
    assert svc.get("init") is True, (
        f"{compose_rel} service '{service}' runs a long-lived Python process as "
        "PID 1 and must set `init: true` so tini reaps orphaned zombies; "
        "without it a wedged child makes `docker stop`/force-recreate fail and "
        "aborts deploy_test_all.sh under set -e"
    )


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
    result = subprocess.run(
        [
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
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    services = json.loads(result.stdout)["services"]
    ports = services["backend"]["ports"]
    assert {(p.get("host_ip"), p["published"], p["target"]) for p in ports} == {
        (None, "80", 80),
        ("127.0.0.1", "443", 443),
        ("127.0.0.1", "9443", 8443),
    }
    for name in ("caddy", "frontend", "admin-gateway"):
        assert services[name]["network_mode"] == "service:backend"
        assert not services[name].get("ports")
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
    local = (REPO_ROOT / "caddy/Caddyfile.local").read_text()
    snippet = REPO_ROOT / "caddy/local-public-https.caddy"
    # The real container uses an optional glob mount. Expand only that import
    # for static adaptation; no server, certificate issuance, or listener starts.
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
    (public,) = [server for server in servers.values() if server["listen"] == [":443"]]
    (http,) = [server for server in servers.values() if server["listen"] == [":80"]]
    # IP-origin TLS clients send no SNI. Every handshake must select the
    # supplied certificate, not fall through an empty catch-all TLS policy.
    (policy,) = public["tls_connection_policies"]
    assert "match" not in policy
    assert policy["certificate_selection"]["any_tag"]
    assert '"host": ["127.0.0.1"]' in json.dumps(public)
    public_json = json.dumps(public)
    http_json = json.dumps(http)
    assert "127.0.0.1:8000" in public_json and "127.0.0.1:3000" in public_json
    assert '"X-Proxied-By-Caddy": ["true"]' in public_json
    assert '"X-Forwarded-For": ["{http.request.remote.host}"]' in public_json
    assert '"delete": ["X-Admin-Gateway-Token"]' in public_json
    assert "X-Remote-Analyst" not in public_json
    assert '"status_code": 503' in public_json
    assert f'\\"{required}\\" != \\"1\\"' in public_json
    assert "client_authentication" not in public_json
    assert '"X-Forwarded-For": ["127.0.0.1"]' in http_json
    assert "X-Proxied-By-Caddy" not in http_json
    (certificate,) = adapted["apps"]["tls"]["certificates"]["load_files"]
    assert certificate["certificate"] == "/certs/public/server.crt"
    assert certificate["key"] == "/certs/public/server.key"


# ── Merged from test_local_stack_processes.py ──────────────────────────────────────────────

import os
import signal
from pathlib import Path

import pytest

from scripts import local_stack_processes as stack

ROOT = Path("/work/project [test]+")


def process(pid, ppid=1, executable="python3", command=None, born="Mon Sep 7 12:00:00 2026"):
    return stack.Process(pid, ppid, born, executable, command or f"python3 {ROOT}/.venv/bin/uvicorn backend.main:app")


class FakeSystem:
    def __init__(self, processes, cwd=None, listeners=None, holders=None):
        self.processes = {p.pid: p for p in processes}
        self.cwd = cwd or {}
        self.listeners = listeners or {}
        self.holders = holders or {}
        self.signals = []
        self.time = 0
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
    p = process(42, executable=executable, command=command)
    system = FakeSystem([p])
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
    system = FakeSystem([process(42)], cwd={42: ROOT.parent / "other"})
    other = process(43, executable="ssh", command="ssh -L 18002:localhost:8000 host")
    system.processes[43] = other
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
    python = process(42, command="python3 -c 'spawn_main()'")
    shell = process(43, executable="bash", command="bash -c 'echo python3'")
    system = FakeSystem([python, shell])
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

    def denied(pid, sig):
        raise PermissionError("denied")

    system.on_signal = denied
    with pytest.raises(stack.CleanupError, match="Permission denied"):
        stack.terminate(system, {42}, system.snapshot(), set(), grace=0.2)


def test_system_uses_argument_arrays_for_ps_lsof_and_preserves_literal_paths(monkeypatch):
    commands = []
    path = ROOT / "data/services/file [a]+.duckdb"

    def fake_run(args, **kwargs):
        assert isinstance(args, list)
        assert not kwargs.get("shell")
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

    def holders(paths):
        calls.append(paths)
        return set()

    system.holding_pids = holders
    monkeypatch.setattr(stack, "System", lambda: system)
    assert stack.main(["stop", "--checkout", str(checkout), "--port", "18002"]) == 0
    assert calls == [[expected], [expected]]
    assert system.signals == []


def test_stop_detects_respawn_after_termination(monkeypatch, tmp_path):
    system = FakeSystem([])
    monkeypatch.setattr(stack, "System", lambda: system)

    def respawn(*args):
        system.processes[42] = process(42, command=f"python3 {tmp_path}/.venv/bin/uvicorn backend.main:app")

    monkeypatch.setattr(stack, "terminate", respawn)
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
    assert "pkill" not in source
    assert "killall" not in source
    assert "PGID" not in source
    assert "xargs kill" not in source


@pytest.mark.parametrize("status", [0, 7])
def test_run_stop_uses_helper_and_propagates_failure_without_startup(tmp_path, status):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "scripts").mkdir()
    (checkout / "run.sh").write_text(Path("run.sh").read_text())
    (checkout / ".env").write_text("FRONTEND_PORT=13002\nBACKEND_PORT=18002\n")
    (checkout / "scripts/local_stack_processes.py").write_text(
        "import json, pathlib, sys\n"
        "pathlib.Path('called.json').write_text(json.dumps(sys.argv[1:]))\n"
        f"sys.exit({status})\n"
    )
    result = subprocess.run(["bash", "run.sh", "--stop"], cwd=checkout, capture_output=True, text=True, env=os.environ)
    assert result.returncode == status
    args = json.loads((checkout / "called.json").read_text())
    assert "stop" in args and str(checkout) in args and "18002" in args and "13002" in args
    assert "Syncing backend dependencies" not in result.stdout
    assert ("Stack stopped." in result.stdout) == (status == 0)


# ── Merged from test_analyze_web_vitals.py ──────────────────────────────────────────────

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "analyze_web_vitals.py"
_spec = importlib.util.spec_from_file_location("analyze_web_vitals", _SCRIPT)
assert _spec and _spec.loader
awv = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(awv)


def test_percentile_interpolates():
    assert awv._percentile([], 0.5) == 0.0
    assert awv._percentile([42.0], 0.95) == 42.0
    assert awv._percentile([0.0, 10.0], 0.5) == 5.0
    # 0..100 step 10 → p50 == 50, p75 == 75 with linear interpolation.
    vals = [float(x) for x in range(0, 101, 10)]
    assert awv._percentile(vals, 0.5) == 50.0
    assert awv._percentile(vals, 0.75) == 75.0


def test_verdict_buckets_against_thresholds():
    assert awv._verdict("LCP", 2000.0) == "good"  # <= 2500
    assert awv._verdict("LCP", 3000.0) == "needs-improvement"  # 2500..4000
    assert awv._verdict("LCP", 5000.0) == "poor"  # > 4000
    assert awv._verdict("CLS", 0.05) == "good"
    assert awv._verdict("UNKNOWN", 1.0) == "unknown"


def test_aggregate_groups_by_route_and_metric():
    samples = [
        {"name": "LCP", "value": 1000.0, "rating": "good", "pathname": "/a"},
        {"name": "LCP", "value": 3000.0, "rating": "needs-improvement", "pathname": "/a"},
        {"name": "LCP", "value": 500.0, "rating": "good", "pathname": "/b"},
        {"name": "BOGUS", "value": 1.0, "rating": "good", "pathname": "/a"},  # dropped: not a vital
    ]
    stats = awv.aggregate(samples)
    assert ("/a", "LCP") in stats
    assert ("/b", "LCP") in stats
    assert ("/a", "BOGUS") not in stats
    a = stats[("/a", "LCP")]
    assert a["count"] == 2
    assert a["ratings"] == {"good": 1, "needs-improvement": 1, "poor": 0}


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
    assert "/slow" in routes_in_focus  # poor + enough samples
    assert "/fast" not in routes_in_focus  # good verdict, never in focus
    assert "/rare" not in routes_in_focus  # below min_samples
    # /fast is still present in the full rows table.
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
    all_valid = awv.load_samples(p, since=None, cohort="all")
    assert len(all_valid) == 2  # torn line skipped


def test_main_no_file_is_a_clean_noop(tmp_path, capsys):
    rc = awv.main(["--input", str(tmp_path / "missing.jsonl")])
    assert rc == 0
    assert "No web-vitals data" in capsys.readouterr().err


def test_main_purge_deletes_file_after_report(tmp_path, capsys):
    p = tmp_path / "wv.jsonl"
    lines = [
        json.dumps(
            {
                "name": "LCP",
                "value": v,
                "rating": "poor",
                "cohort": "admin",
                "pathname": "/x",
                "ts": "2026-06-19T00:00:00Z",
            }
        )
        for v in (5000, 5100, 5200, 5300)
    ]
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")

    rc = awv.main(["--input", str(p), "--min-samples", "1", "--purge"])
    assert rc == 0
    out = capsys.readouterr()
    assert "Web Vitals Analysis" in out.out
    assert "/x" in out.out
    assert not p.exists()  # purged
    assert "Purged" in out.err


def test_main_purge_off_keeps_file(tmp_path):
    p = tmp_path / "wv.jsonl"
    p.write_text(
        '{"name":"LCP","value":5000,"rating":"poor","cohort":"admin","pathname":"/x","ts":"2026-06-19T00:00:00Z"}\n',
        encoding="utf-8",
    )
    awv.main(["--input", str(p), "--min-samples", "1"])
    assert p.exists()  # not purged without the flag


@pytest.mark.parametrize("fmt", ["md", "json"])
def test_main_writes_to_out_file(tmp_path, fmt, capsys):
    p = tmp_path / "wv.jsonl"
    p.write_text(
        '{"name":"LCP","value":5000,"rating":"poor","cohort":"admin","pathname":"/x","ts":"2026-06-19T00:00:00Z"}\n',
        encoding="utf-8",
    )
    out = tmp_path / "report.out"
    rc = awv.main(["--input", str(p), "--min-samples", "1", "--format", fmt, "--out", str(out)])
    assert rc == 0
    assert out.exists() and out.read_text(encoding="utf-8").strip()


def _rec(value: float, route: str) -> str:
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


def test_main_reads_rotated_segment_and_purge_removes_both(tmp_path, capsys):
    """The analyzer includes the rotated .1 backup in both the report and
    the purge, so analysis spans the full retained window."""
    active = tmp_path / "wv.jsonl"
    rotated = tmp_path / "wv.jsonl.1"
    rotated.write_text("\n".join(_rec(v, "/old") for v in (5000, 5100, 5200)) + "\n", encoding="utf-8")
    active.write_text("\n".join(_rec(v, "/new") for v in (4800, 4900, 5300)) + "\n", encoding="utf-8")

    rc = awv.main(["--input", str(active), "--min-samples", "1", "--purge"])
    assert rc == 0
    out = capsys.readouterr()
    # Both segments contributed to the report.
    assert "/old" in out.out and "/new" in out.out
    # Both segments purged.
    assert not active.exists() and not rotated.exists()
