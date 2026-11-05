import argparse
import fcntl
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from infra.provision import verify_provenance


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def frozen_source(root: Path) -> str:
    if git(root, "status", "--porcelain", "--untracked-files=all", "--", "app"):
        raise RuntimeError("Runtime has uncommitted inputs; commit app/ before a release")
    return git(root, "rev-parse", "HEAD")


def verify_source(root: Path, built_sha: str) -> None:
    frozen_source(root)
    if git(root, "rev-parse", f"{built_sha}:app") != git(root, "rev-parse", "HEAD:app"):
        raise RuntimeError("Runtime source changed after the build")


def snapshot_sources(root: Path, sha: str, destination: Path) -> None:
    data = subprocess.check_output(["git", "-C", str(root), "archive", sha, "app"])
    with tarfile.open(fileobj=io.BytesIO(data)) as archive:
        archive.extractall(destination, filter="data")


def stabilize_mtimes(source_dir: Path, state_file: Path) -> None:
    """Preserve mtimes for unchanged files and directories so incremental
    TypeScript, Turbopack, and Cargo caches are not invalidated by snapshot extraction."""
    previous: dict[str, list[int | str]] = {}
    if state_file.is_file():
        try:
            loaded = json.loads(state_file.read_text())
            if isinstance(loaded, dict):
                previous = loaded
        except (OSError, ValueError):
            previous = {}
    now_ns = time.time_ns()
    updated: dict[str, list[int | str]] = {}
    dirs: list[Path] = [source_dir]
    for path in sorted(source_dir.rglob("*")):
        if path.is_symlink():
            continue
        if path.is_dir():
            dirs.append(path)
            continue
        if not path.is_file():
            continue
        rel = str(path.relative_to(source_dir))
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        entry = previous.get(rel)
        if isinstance(entry, list) and len(entry) == 2 and entry[0] == digest and isinstance(entry[1], int):
            mtime_ns = entry[1]
        else:
            mtime_ns = now_ns
        os.utime(path, ns=(mtime_ns, mtime_ns))
        updated[rel] = [digest, mtime_ns]
    for directory in sorted(dirs, key=lambda d: len(d.parts), reverse=True):
        child_mtimes = [child.stat().st_mtime_ns for child in directory.iterdir() if not child.is_symlink()]
        dir_mtime_ns = max(child_mtimes, default=1_700_000_000_000_000_000)
        os.utime(directory, ns=(dir_mtime_ns, dir_mtime_ns))
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(updated) + "\n")


def _move_cached_path(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        if dst.is_dir():
            shutil.rmtree(dst)
        else:
            dst.unlink()
    shutil.move(str(src), str(dst))


def restore_web_out(web: Path, cache_dir: Path, web_tree_sha: str) -> bool:
    cached_out = cache_dir / "web-out"
    cached_sha = cache_dir / "web-out.sha256"
    if cached_out.is_dir() and cached_sha.is_file() and cached_sha.read_text().strip() == web_tree_sha:
        _move_cached_path(cached_out, web / "out")
        return True
    if cached_out.is_dir():
        shutil.rmtree(cached_out)
    cached_sha.unlink(missing_ok=True)
    return False


def save_web_out(web: Path, cache_dir: Path, web_tree_sha: str, web_out_ready: bool) -> None:
    if web_out_ready and (web / "out").is_dir():
        _move_cached_path(web / "out", cache_dir / "web-out")
        (cache_dir / "web-out.sha256").write_text(web_tree_sha + "\n")


def restore_node_modules(web: Path, cache_dir: Path) -> None:
    lock_sha = hashlib.sha256((web / "package-lock.json").read_bytes()).hexdigest()
    cached_nm = cache_dir / "node_modules"
    cached_sha = cache_dir / "node_modules.sha256"
    web_nm = web / "node_modules"
    if cached_nm.is_dir() and cached_sha.is_file() and cached_sha.read_text().strip() == lock_sha:
        shutil.move(str(cached_nm), str(web_nm))
        _move_cached_path(cache_dir / "next-cache", web / ".next/cache")
        _move_cached_path(cache_dir / "tsconfig.tsbuildinfo", web / "tsconfig.tsbuildinfo")
        _move_cached_path(cache_dir / "duckdb", web / "public/duckdb")
        return
    for stale in (
        cached_nm,
        cache_dir / "next-cache",
        cache_dir / "tsconfig.tsbuildinfo",
        cache_dir / "duckdb",
    ):
        if stale.is_dir():
            shutil.rmtree(stale)
        elif stale.exists():
            stale.unlink()
    cached_sha.unlink(missing_ok=True)
    subprocess.run(
        ["npm", "ci", "--no-audit", "--no-fund", "--prefer-offline"],
        cwd=web,
        check=True,
    )
    cached_sha.write_text(lock_sha + "\n")


def save_web_caches(web: Path, cache_dir: Path, npm_ready: bool) -> None:
    cached_nm = cache_dir / "node_modules"
    web_nm = web / "node_modules"
    if npm_ready and web_nm.is_dir() and not cached_nm.exists():
        shutil.move(str(web_nm), str(cached_nm))
    if npm_ready:
        _move_cached_path(web / ".next/cache", cache_dir / "next-cache")
        _move_cached_path(web / "tsconfig.tsbuildinfo", cache_dir / "tsconfig.tsbuildinfo")
        _move_cached_path(web / "public/duckdb", cache_dir / "duckdb")


def build(root: Path) -> str:
    sha = frozen_source(root)
    cache_dir = root / "target/release-cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cargo_target = Path(os.environ.get("CARGO_TARGET_DIR", root / "app/target")).resolve()
    asset_cache = (cache_dir / "precompressed").resolve()
    asset_cache.mkdir(parents=True, exist_ok=True)
    environment = dict(
        os.environ,
        STATIC_EXPORT="1",
        FLP_BUILD_SHA=sha,
        CARGO_TARGET_DIR=str(cargo_target),
        FLP_ASSET_CACHE_DIR=str(asset_cache),
    )
    environment.setdefault("CARGO_BUILD_JOBS", str(min(8, os.cpu_count() or 4)))
    package = root / "app/pkg/flp.tar.gz"
    snapshot = cache_dir / "work"
    if snapshot.exists():
        shutil.rmtree(snapshot)
    snapshot.mkdir(parents=True, exist_ok=True)
    (snapshot / ".git").mkdir(exist_ok=True)
    web = snapshot / "app/web"
    web_tree_sha = git(root, "rev-parse", f"{sha}:app/web")
    npm_ready = False
    web_out_ready = False
    try:
        snapshot_sources(root, sha, snapshot)
        stabilize_mtimes(snapshot / "app", cache_dir / "source-mtimes.json")
        if restore_web_out(web, cache_dir, web_tree_sha):
            web_out_ready = True
        else:
            restore_node_modules(web, cache_dir)
            npm_ready = True
            subprocess.run(["npm", "run", "build"], cwd=web, env=environment, check=True)
            if (web / "out").is_dir():
                stabilize_mtimes(web / "out", cache_dir / "web-out-mtimes.json")
                web_out_ready = True
        subprocess.run(
            ["fastly", "compute", "build", "--metadata-disable", "--non-interactive"],
            cwd=snapshot / "app",
            env=environment,
            check=True,
        )
        verify_source(root, sha)
        built = snapshot / "app/pkg/flp.tar.gz"
        if not built.is_file():
            raise RuntimeError("Fastly build did not produce pkg/flp.tar.gz")
        package.parent.mkdir(exist_ok=True)
        shutil.copyfile(built, package)
    finally:
        save_web_caches(web, cache_dir, npm_ready)
        save_web_out(web, cache_dir, web_tree_sha, web_out_ready)
        shutil.rmtree(snapshot, ignore_errors=True)

    provenance = {
        "build_sha": sha,
        "package_sha256": hashlib.sha256(package.read_bytes()).hexdigest(),
    }
    (root / "app/pkg/provenance.json").write_text(json.dumps(provenance) + "\n")
    print(json.dumps({"event": "release.built", **provenance}))
    return sha


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["build", "verify", "deploy"])
    parser.add_argument("--name", default="flp")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    with Path("/tmp/flp-deploy.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.command == "build":
            build(root)
        if args.command in {"verify", "deploy"}:
            package = root / "app/pkg/flp.tar.gz"
            for artifact in (package, package.parent / "provenance.json"):
                if not artifact.is_file():
                    raise RuntimeError(
                        f"Missing release artifact: {artifact}; run make -C app build from clean committed app/ sources"
                    )
            try:
                provenance = verify_provenance(root, package)
            except (
                OSError,
                ValueError,
                tarfile.TarError,
                subprocess.CalledProcessError,
            ) as error:
                error.add_note(
                    "Commit or revert any app/ changes, then run make -C app build to create a fresh release artifact."
                )
                raise
            print(json.dumps({"event": "release.verified", **provenance}))
        if args.command == "deploy":
            subprocess.run(
                [
                    "uv",
                    "run",
                    "infra/provision.py",
                    "deploy",
                    "--name",
                    args.name,
                    "--package",
                    "pkg/flp.tar.gz",
                ],
                cwd=root / "app",
                check=True,
            )


if __name__ == "__main__":
    main()
