"""Build Cloud browser fixtures without changing the Standard frontend tree."""

from __future__ import annotations

import errno
import os
import shutil
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.release_compute import prepare_static_export_tree


def _link_or_copy(source: str, destination: str) -> str:
    try:
        os.link(source, destination)
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
        return shutil.copy2(source, destination)
    return destination


def build(root: Path) -> Path:
    frontend = root / "frontend"
    target = root / "target"
    destination = target / "cloud-e2e/web-out"
    target.mkdir(exist_ok=True)
    environment = {
        **os.environ,
        "STATIC_EXPORT": "1",
        "NEXT_PUBLIC_DEPLOYMENT_MODE": "cloud",
        "NEXT_DIST_DIR": ".next",
    }
    with TemporaryDirectory(prefix=".cloud-e2e-export-") as temporary:
        snapshot = Path(temporary)
        (snapshot / ".git").mkdir()
        shutil.copyfile(root / ".gitignore", snapshot / ".gitignore")
        web = snapshot / "frontend"
        files = subprocess.check_output(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", "frontend"],
            cwd=root,
        )
        for filename in files.split(b"\0"):
            if not filename:
                continue
            relative = Path(os.fsdecode(filename))
            if any(part.startswith(".env") for part in relative.parts):
                continue
            source = root / relative
            if source.is_file():
                copied = snapshot / relative
                copied.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, copied)
        # Keep dependencies inside Turbopack's root without duplicating their bytes.
        shutil.copytree(
            frontend / "node_modules",
            web / "node_modules",
            copy_function=_link_or_copy,
            symlinks=True,
            ignore=shutil.ignore_patterns(".cache", ".vite"),
        )
        prepare_static_export_tree(web)
        subprocess.run(["npm", "run", "postinstall"], cwd=web, env=environment, check=True)
        subprocess.run(["node", "scripts/copy-duckdb-assets.mjs"], cwd=web, env=environment, check=True)
        subprocess.run(
            ["node", str(web / "node_modules/next/dist/bin/next"), "build"],
            cwd=web,
            env=environment,
            check=True,
        )
        exported = web / "out"
        if not (exported / "dashboard.html").is_file():
            raise RuntimeError("Cloud static export did not produce dashboard.html")
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(exported, destination)
    return destination


if __name__ == "__main__":
    print(build(Path(__file__).resolve().parents[3]))
