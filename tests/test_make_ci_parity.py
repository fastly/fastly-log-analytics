"""Make <-> CI gate-parity test (DRY finding S5).

The ``Makefile`` CI-parity targets and ``.github/workflows/ci.yml`` express the
SAME gates twice on purpose -- AGENTS.md documents gate-for-gate parity as
intentional so a green ``make ci`` means a green GitHub run. Expressing them
twice, however, lets them DRIFT: at the time this test was written CI excluded
``flaky_quarantine`` from the backend pytest run while ``make test-ci`` did not,
and the two backend coverage floors disagreed (84 vs 85).

This test parses the drift-prone *selectors and thresholds* out of both sides
and asserts they match. It is tolerant of formatting (whitespace, line
continuations, YAML block scalars) but strict on the actual marker
expressions, coverage floors, and vitest thresholds. Genuinely intentional
differences go in ``INTENTIONAL_DIFFERENCES`` with a reason, not silently.
"""

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
