import hashlib
import io
import json
import subprocess
import sys
import tarfile
from collections.abc import Callable
from pathlib import Path

import pytest
import release
from release import (
    check_static_assets,
    frozen_source,
    prepare_static_export_tree,
    restore_node_modules,
    restore_web_out,
    save_web_caches,
    save_web_out,
    snapshot_sources,
    stabilize_mtimes,
    verify_source,
    web_out_cache_key,
)


def test_static_export_uses_policy_from_its_snapshot(tmp_path: Path) -> None:
    web = tmp_path / "frontend"
    for relative in ("app/alerts/page.tsx", "app/admin/rum/page.tsx", "app/sessions/stream/page.tsx"):
        page = web / relative
        page.parent.mkdir(parents=True, exist_ok=True)
        page.write_text('export const dynamic = "force-dynamic";')
    (web / "static-export-policy.json").write_text(json.dumps({"prunePaths": ["app/alerts"]}))

    prepare_static_export_tree(web)

    assert not (web / "app/alerts").exists()
    assert (web / "app/admin/rum/page.tsx").is_file()
    assert (web / "app/sessions/stream/page.tsx").is_file()

    (web / "static-export-policy.json").write_text(json.dumps({"prunePaths": ["app/admin/rum"]}))
    prepare_static_export_tree(web)

    assert not (web / "app/admin/rum").exists()
    assert (web / "app/sessions/stream/page.tsx").is_file()


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.email", "test@example.invalid")
    git(tmp_path, "config", "user.name", "Test")
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "source.rs").write_text("fn main() {}")
    (tmp_path / "app" / "web").mkdir()
    (tmp_path / "app" / "web" / "package-lock.json").write_text("{}")
    (tmp_path / ".gitignore").write_text("app/pkg/\ntarget/\n")
    git(tmp_path, "add", ".gitignore", "app")
    git(tmp_path, "commit", "-qm", "Initial")
    return tmp_path


def test_dirty_runtime_cannot_be_released(repository: Path) -> None:
    (repository / "app" / "source.rs").write_text("fn main() { panic!() }")
    with pytest.raises(RuntimeError, match="uncommitted"):
        frozen_source(repository)


def test_untracked_runtime_cannot_be_omitted_from_provenance(repository: Path) -> None:
    (repository / "app" / "new.rs").write_text("fn other() {}")
    with pytest.raises(RuntimeError, match="uncommitted"):
        frozen_source(repository)


def test_snapshot_builds_committed_bytes_not_concurrent_edits(repository: Path, tmp_path: Path) -> None:
    built_sha = frozen_source(repository)
    (repository / "app" / "source.rs").write_text("concurrent edit")
    destination = tmp_path / "snapshot"
    destination.mkdir()
    snapshot_sources(repository, built_sha, destination)
    assert (destination / "app" / "source.rs").read_text() == "fn main() {}"


def test_unrelated_commit_does_not_change_built_runtime_identity(
    repository: Path,
) -> None:
    built_sha = frozen_source(repository)
    (repository / "notes.txt").write_text("unrelated")
    git(repository, "add", "notes.txt")
    git(repository, "commit", "-qm", "Documentation")
    verify_source(repository, built_sha)
    (repository / "app" / "source.rs").write_text("fn main() { panic!() }")
    git(repository, "add", "app/source.rs")
    git(repository, "commit", "-qm", "Different runtime")
    with pytest.raises(RuntimeError, match="changed"):
        verify_source(repository, built_sha)


def test_release_web_cache_reuses_and_invalidates_by_lockfile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    web = tmp_path / "web"
    web.mkdir()
    (web / "package-lock.json").write_text('{"lockfileVersion": 3, "v": 1}')
    npm_calls: list[list[str]] = []

    def fake_run(cmd: list[str], cwd: Path, check: bool) -> None:
        npm_calls.append(cmd)
        if cmd[:2] == ["npm", "ci"]:
            (cwd / "node_modules").mkdir(exist_ok=True)
            (cwd / "node_modules" / "marker.txt").write_text(f"installed-{len(npm_calls)}")

    monkeypatch.setattr(subprocess, "run", fake_run)

    restore_node_modules(web, cache_dir)
    assert len(npm_calls) == 1
    assert (web / "node_modules" / "marker.txt").read_text() == "installed-1"
    (web / ".next/cache").mkdir(parents=True)
    (web / ".next/cache/webpack.pack").write_text("cached-webpack")
    (web / "tsconfig.tsbuildinfo").write_text("cached-tsbuildinfo")
    (web / "public/duckdb/1.0").mkdir(parents=True)
    (web / "public/duckdb/1.0/duckdb-eh.wasm").write_text("cached-duckdb")
    save_web_caches(web, cache_dir, npm_ready=True)
    assert (cache_dir / "node_modules" / "marker.txt").is_file()
    assert (cache_dir / "next-cache/webpack.pack").is_file()
    assert (cache_dir / "tsconfig.tsbuildinfo").is_file()
    assert (cache_dir / "duckdb/1.0/duckdb-eh.wasm").is_file()

    # Second run with identical package-lock.json skips npm ci and restores caches
    restore_node_modules(web, cache_dir)
    assert [call[:2] for call in npm_calls] == [["npm", "ci"], ["npm", "run"]]
    assert npm_calls[1][2:] == ["postinstall", "--if-present"]
    assert (web / "node_modules" / "marker.txt").read_text() == "installed-1"
    assert (web / ".next/cache/webpack.pack").read_text() == "cached-webpack"
    assert (web / "tsconfig.tsbuildinfo").read_text() == "cached-tsbuildinfo"
    assert (web / "public/duckdb/1.0/duckdb-eh.wasm").read_text() == "cached-duckdb"
    save_web_caches(web, cache_dir, npm_ready=True)

    # Third run with modified package-lock.json invalidates cache and runs npm ci
    (web / "package-lock.json").write_text('{"lockfileVersion": 3, "v": 2}')
    restore_node_modules(web, cache_dir)
    assert len(npm_calls) == 3
    assert npm_calls[2][:2] == ["npm", "ci"]
    assert (web / "node_modules" / "marker.txt").read_text() == "installed-3"
    assert not (web / ".next/cache").exists()
    assert not (web / "tsconfig.tsbuildinfo").exists()
    assert not (web / "public/duckdb").exists()


def test_stabilize_mtimes_preserves_unchanged_file_timestamps(tmp_path: Path) -> None:
    state_file = tmp_path / "mtimes.json"
    snap1 = tmp_path / "snap1"
    snap1.mkdir()
    (snap1 / "unchanged.rs").write_text("pub fn a() {}")
    (snap1 / "changed.rs").write_text("pub fn b() { 1 }")
    stabilize_mtimes(snap1, state_file)
    unchanged_mtime_1 = (snap1 / "unchanged.rs").stat().st_mtime_ns
    changed_mtime_1 = (snap1 / "changed.rs").stat().st_mtime_ns

    snap2 = tmp_path / "snap2"
    snap2.mkdir()
    (snap2 / "unchanged.rs").write_text("pub fn a() {}")
    (snap2 / "changed.rs").write_text("pub fn b() { 2 }")
    stabilize_mtimes(snap2, state_file)
    assert (snap2 / "unchanged.rs").stat().st_mtime_ns == unchanged_mtime_1
    assert (snap2 / "changed.rs").stat().st_mtime_ns >= changed_mtime_1
    assert json.loads(state_file.read_text())["changed.rs"][0] != (hashlib.sha256(b"pub fn b() { 1 }").hexdigest())


def test_web_out_cache_reuses_and_invalidates_by_web_tree_sha(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    web = tmp_path / "web"
    (web / "out").mkdir(parents=True)
    (web / "out/index.html").write_text("<html>v1</html>")

    assert not restore_web_out(web, cache_dir, "tree-sha-1")
    save_web_out(web, cache_dir, "tree-sha-1", web_out_ready=True)
    assert not (web / "out").exists()
    assert (cache_dir / "web-out/index.html").read_text() == "<html>v1</html>"

    # Matching web_tree_sha restores cached out/
    assert restore_web_out(web, cache_dir, "tree-sha-1")
    assert (web / "out/index.html").read_text() == "<html>v1</html>"
    save_web_out(web, cache_dir, "tree-sha-1", web_out_ready=True)

    # Different web_tree_sha invalidates cached out/
    assert not restore_web_out(web, cache_dir, "tree-sha-2")
    assert not (cache_dir / "web-out").exists()


def test_static_export_without_maplibre_worker_fails_the_build(tmp_path: Path) -> None:
    out = tmp_path / "out"
    out.mkdir()
    (out / "maplibre-gl-shared.mjs").write_text("shared")
    with pytest.raises(RuntimeError, match="maplibre-gl-worker.mjs"):
        check_static_assets(out)
    (out / "maplibre-gl-worker.mjs").write_text("worker")
    check_static_assets(out)


def test_web_out_cache_key_changes_when_build_recipe_changes() -> None:
    assert web_out_cache_key("tree-sha-1", b"recipe-1") == web_out_cache_key("tree-sha-1", b"recipe-1")
    assert web_out_cache_key("tree-sha-1", b"recipe-1") != web_out_cache_key("tree-sha-1", b"recipe-2")
    assert web_out_cache_key("tree-sha-1", b"recipe-1") != web_out_cache_key("tree-sha-2", b"recipe-1")


def write_package(repository: Path, sha: str) -> Path:
    package = repository / "app/pkg/flp.tar.gz"
    package.parent.mkdir(exist_ok=True)
    wasm = b"\x00asm" + sha.encode()
    with tarfile.open(package, "w:gz") as archive:
        member = tarfile.TarInfo("bin/main.wasm")
        member.size = len(wasm)
        archive.addfile(member, io.BytesIO(wasm))
    (package.parent / "provenance.json").write_text(
        json.dumps(
            {
                "build_sha": sha,
                "package_sha256": hashlib.sha256(package.read_bytes()).hexdigest(),
            }
        )
    )
    return package


@pytest.fixture
def release_cli(repository: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Callable[[str], None], list[list[str]]]:
    monkeypatch.setattr(release, "__file__", str(repository / "app/tools/release.py"))
    monkeypatch.setattr(sys, "path", [str(Path(__file__).resolve().parents[1]), *sys.path])
    original_open = Path.open
    original_run = subprocess.run
    commands: list[list[str]] = []

    def open_lock(path: Path, *args, **kwargs):
        if path == Path("/tmp/flp-deploy.lock"):
            path = repository / ".deploy.lock"
        return original_open(path, *args, **kwargs)

    def run(command: list[str], *args, **kwargs):
        if command[0] == "git":
            return original_run(command, *args, **kwargs)
        commands.append(command)
        cwd = Path(kwargs["cwd"])
        if command[:2] == ["npm", "ci"]:
            (cwd / "node_modules").mkdir()
        elif command[:3] == ["fastly", "compute", "build"]:
            write_package(cwd.parent, kwargs["env"]["FLP_BUILD_SHA"])
        return subprocess.CompletedProcess(command, 0)

    def invoke(command: str) -> None:
        monkeypatch.setattr(sys, "argv", ["release.py", command, "--name", "team-a"])
        release.main()

    monkeypatch.setattr(Path, "open", open_lock)
    monkeypatch.setattr(subprocess, "run", run)
    return invoke, commands


@pytest.mark.parametrize("command", ["verify", "deploy"])
def test_existing_release_reuses_original_artifact_after_unrelated_commit(
    repository: Path, release_cli, capsys: pytest.CaptureFixture[str], command: str
) -> None:
    sha = frozen_source(repository)
    package = write_package(repository, sha)
    contents = package.read_bytes()
    (repository / "notes.txt").write_text("unrelated change")
    git(repository, "add", "notes.txt")
    git(repository, "commit", "-qm", "Documentation")
    invoke, commands = release_cli

    invoke(command)

    assert package.read_bytes() == contents
    captured = capsys.readouterr()
    event = json.loads(captured.out)
    assert captured.err == ""
    assert event["event"] == "release.verified"
    assert event["build_sha"] == sha != git(repository, "rev-parse", "HEAD")
    assert commands == (
        [
            [
                "uv",
                "run",
                "infra/provision.py",
                "deploy",
                "--name",
                "team-a",
                "--package",
                "pkg/flp.tar.gz",
            ]
        ]
        if command == "deploy"
        else []
    )


@pytest.mark.parametrize(
    "invalid, error",
    [
        ("missing-package", "Missing"),
        ("missing-provenance", "Missing"),
        ("tampered-package", "Package changed"),
        ("invalid-provenance", "Invalid frozen build provenance"),
        ("corrupt-package", "not a gzip file"),
        ("wrong-embedded-sha", "frozen built SHA"),
        ("dirty-source", "uncommitted"),
        ("untracked-source", "uncommitted"),
        ("changed-source", "source changed"),
    ],
)
def test_deploy_rejects_invalid_existing_artifact_without_building_or_deploying(
    repository: Path,
    release_cli,
    invalid: str,
    error: str,
) -> None:
    sha = frozen_source(repository)
    package = write_package(repository, sha)
    provenance = package.parent / "provenance.json"
    if invalid == "missing-package":
        package.unlink()
    elif invalid == "missing-provenance":
        provenance.unlink()
    elif invalid == "tampered-package":
        package.write_bytes(package.read_bytes() + b"tampered")
    elif invalid == "invalid-provenance":
        provenance.write_text('{"build_sha": "not-a-commit"}')
    elif invalid == "corrupt-package":
        package.write_bytes(b"not a gzip file")
        provenance.write_text(
            json.dumps(
                {
                    "build_sha": sha,
                    "package_sha256": hashlib.sha256(package.read_bytes()).hexdigest(),
                }
            )
        )
    elif invalid == "wrong-embedded-sha":
        write_package(repository, "0" * 40)
        data = json.loads(provenance.read_text())
        data["build_sha"] = sha
        provenance.write_text(json.dumps(data))
    elif invalid == "untracked-source":
        (repository / "app/new.rs").write_text("fn other() {}")
    else:
        (repository / "app/source.rs").write_text("changed source")
        if invalid == "changed-source":
            git(repository, "add", "app/source.rs")
            git(repository, "commit", "-qm", "Change runtime")
    invoke, commands = release_cli

    with pytest.raises((RuntimeError, ValueError, tarfile.ReadError), match=error) as failure:
        invoke("deploy")

    assert commands == []
    diagnostic = str(failure.value) + "\n".join(getattr(failure.value, "__notes__", []))
    assert "make -C app build" in diagnostic


def test_explicit_build_still_runs_frozen_frontend_and_compute_builds(
    repository: Path, release_cli, capsys: pytest.CaptureFixture[str]
) -> None:
    invoke, commands = release_cli

    invoke("build")

    assert [command[:3] for command in commands] == [
        ["npm", "ci", "--no-audit"],
        ["npm", "run", "build"],
        ["fastly", "compute", "build"],
    ]
    assert json.loads(capsys.readouterr().out)["build_sha"] == frozen_source(repository)
    assert (repository / "app/pkg/flp.tar.gz").is_file()
    assert not (repository / "target/release-cache/work").exists()
