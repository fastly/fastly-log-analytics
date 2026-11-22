"""Keep the dedicated Cloud job and local gate wired to the same checks."""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]


def cloud_job() -> dict:
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    return next(job for job in workflow["jobs"].values() if job["name"] == "Cloud Compute & Browser Engine")


def test_cloud_ci_mirrors_every_workflow_make_gate() -> None:
    make = (ROOT / "Makefile").read_text()
    recipe = make.split("\ncloud-ci:\n", 1)[1].split("\n\n", 1)[0]
    workflow_commands = "\n".join(step.get("run", "") for step in cloud_job()["steps"])
    assert re.findall(r"\$\(MAKE\) ([\w-]+)", recipe) == re.findall(
        r"^\s*make ([\w-]+)\s*$", workflow_commands, re.MULTILINE
    )
    assert "git diff --exit-code compute/cloud/src/logging/catalog.json" in recipe
    assert "\t$(MAKE) cloud-ci\n" in make.split("\nci:\n", 1)[1].split("\n\n", 1)[0]


def test_cloud_ci_checks_openapi_drift() -> None:
    make = (ROOT / "Makefile").read_text()
    recipe = make.split("\ncloud-ci:\n", 1)[1].split("\n\n", 1)[0]
    workflow_commands = "\n".join(step.get("run", "") for step in cloud_job()["steps"])
    assert "$(MAKE) openapi-drift" in recipe
    assert re.search(r"^\s*make openapi-drift\s*$", workflow_commands, re.MULTILINE)


def test_fast_ci_runs_deterministic_release_confidence_tests() -> None:
    make = (ROOT / "Makefile").read_text()
    fast_ci = make.split("\nfast-ci:\n", 1)[1].split("\n\n", 1)[0]
    recipe = make.split("\nrelease-confidence-tests:\n", 1)[1].split("\n\n", 1)[0]

    assert "$(MAKE) release-confidence-tests" in fast_ci
    for test in (
        "test_canonical_fixture_ingest_and_query_parity",
        "test_admin_attachment_analyst_scope_and_revocation_lifecycle",
        "test_custom_field_lifecycle_validates_changes_rejects_unsupported_values_and_removes",
        "canonical_fixture_has_cross_stack_ingest_parity",
        "storage_tree_replays_the_same_cursor_after_a_failed_list",
        "analyst_attachment_is_read_only_and_revocation_invalidates_the_session",
        "custom_field_lifecycle_rejects_bad_types_and_allows_change_and_removal_before_commit",
    ):
        assert test in recipe


def test_release_confidence_offline_uses_existing_cloud_soak() -> None:
    make = (ROOT / "Makefile").read_text()
    recipe = make.split("\nrelease-confidence-offline:\n", 1)[1].split("\n\n", 1)[0]
    assert "$(MAKE) cloud-soak" in recipe
    assert (ROOT / "CONTRIBUTING.md").read_text().count("make release-confidence-offline") >= 1


@pytest.mark.parametrize("makefile", [ROOT / "Makefile", ROOT / "compute/cloud/Makefile"])
def test_live_load_targets_are_owned_by_fdt(makefile: Path) -> None:
    make = makefile.read_text()
    for target in (
        "loadtest-deploy-check:",
        "loadtest-campaign:",
        "deploy-test:",
        "deploy-check:",
        "require-lt-config:",
    ):
        assert f"\n{target}" not in make


def test_cloud_job_supplies_database_required_by_shared_vcl_tests() -> None:
    job = cloud_job()
    assert job["services"]["postgres"]["image"] == "postgres:16-alpine"
    assert "5432:5432" in job["services"]["postgres"]["ports"]
    assert job["env"]["TEST_METADATA_DSN"] == job["env"]["METADATA_DSN"]


def test_nightly_selects_the_matrix_browser_in_playwright_config() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/e2e-nightly.yml").read_text())
    job = workflow["jobs"]["playwright-nightly"]
    step = next(step for step in job["steps"] if step.get("name", "").startswith("Run Playwright"))
    assert step["env"]["PROJECTS"] == "${{ matrix.browser }}"


def test_cloud_acceptance_builds_current_assets_before_playwright_readiness_wait() -> None:
    make = (ROOT / "Makefile").read_text()
    recipe = make.split("\ncloud-e2e:\n", 1)[1].split("\n\n", 1)[0]
    prepare, run = recipe.strip().splitlines()
    assert "node e2e/cloud/native-servers.mjs --build-only" in prepare
    assert "FLP_E2E_NATIVE=1" in run
    assert "PROJECTS=chromium PLAYWRIGHT_WORKERS=1" in run
    assert "npx playwright test --project=chromium" in run


def test_compute_lint_and_wasm_checks_use_workspace_toolchain_and_strict_warnings() -> None:
    make = (ROOT / "Makefile").read_text()
    for target in ("compute-lint", "compute-wasm-check"):
        recipe = make.split(f"\n{target}:\n", 1)[1].split("\n\n", 1)[0]
        assert all(line.startswith("\tcd compute && ") for line in recipe.splitlines())
        assert "-- -D warnings" in recipe


@pytest.mark.parametrize("fail_build", [False, True])
@pytest.mark.parametrize("cross_device", [False, True])
def test_cloud_export_prunes_only_a_snapshot_and_cleans_it_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fail_build: bool, cross_device: bool
) -> None:
    import errno
    import subprocess
    from types import SimpleNamespace

    from compute.cloud.tools.build_e2e_export import build

    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    ignore = "frontend/public/duckdb/\nfrontend/node_modules/\nfrontend/ignored.fixture\n"
    (tmp_path / ".gitignore").write_text(ignore)
    frontend = tmp_path / "frontend"
    (frontend / "app/api").mkdir(parents=True)
    (frontend / "static-export-policy.json").write_text((ROOT / "frontend/static-export-policy.json").read_text())
    (frontend / "node_modules").mkdir()
    (frontend / "node_modules/fixture.json").write_text("{}")
    (frontend / "app/dashboard").mkdir()
    (frontend / "app/api/route.ts").write_text("export const dynamic = 'force-dynamic'")
    page = frontend / "app/dashboard/page.tsx"
    page.write_text("export const dynamic = 'force-dynamic'")
    (frontend / ".env.local").write_text("LOCAL_ONLY=fixture")
    (frontend / "ignored.fixture").write_text("generated local cache")
    destination = tmp_path / "target/cloud-e2e/web-out"
    destination.mkdir(parents=True)
    (destination / "obsolete.html").write_text("stale export")
    snapshots: list[Path] = []

    if cross_device:

        def link(source: str, destination: str) -> None:
            raise OSError(errno.EXDEV, "Cross-device link")

        monkeypatch.setattr("compute.cloud.tools.build_e2e_export.os.link", link)

    def run(command: list[str], *, cwd: Path, env: dict[str, str], check: bool) -> None:
        snapshots.append(cwd)
        assert check is True
        assert (cwd.parent / ".git").is_dir()
        assert (cwd.parent / ".gitignore").read_text() == ignore
        assert not (cwd / "app/api").exists()
        assert not (cwd / ".env.local").exists()
        assert not (cwd / "ignored.fixture").exists()
        assert (cwd / "node_modules/fixture.json").read_text() == "{}"
        assert (cwd / "app/dashboard/page.tsx").read_text() == "export const dynamic = 'force-static'"
        assert env["STATIC_EXPORT"] == "1"
        assert env["NEXT_PUBLIC_DEPLOYMENT_MODE"] == "cloud"
        if "build" in command:
            if fail_build:
                raise subprocess.CalledProcessError(1, command)
            (cwd / "out").mkdir()
            (cwd / "out/dashboard.html").write_text("current export")

    monkeypatch.setattr(
        "compute.cloud.tools.build_e2e_export.subprocess",
        SimpleNamespace(run=run, check_output=subprocess.check_output),
    )
    if fail_build:
        with pytest.raises(subprocess.CalledProcessError):
            build(tmp_path)
        assert (destination / "obsolete.html").exists()
    else:
        assert build(tmp_path) == destination
        assert (destination / "dashboard.html").read_text() == "current export"
        assert not (destination / "obsolete.html").exists()
    assert snapshots
    assert all(not snapshot.exists() for snapshot in snapshots)
    assert (frontend / "app/api/route.ts").exists()
    assert page.read_text() == "export const dynamic = 'force-dynamic'"
