"""Opt-in Local activation of already-qualified, digest-pinned registry roles."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path

import yaml

_REQUIRED_JOBS = {"Backend (Python)", "Frontend (Node)", "Scorer (Rust)", "Playwright (chromium)"}
_ROLES = ("backend", "frontend", "admin-gateway")


def docker_native_platform() -> str:
    """Inspect the selected daemon, not the client host or DOCKER_DEFAULT_PLATFORM."""
    try:
        result = subprocess.run(
            ["docker", "info", "--format", "{{.OSType}}/{{.Architecture}}"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        raise ValueError("Docker daemon native platform is unavailable") from None
    aliases = {"amd64": "amd64", "x86_64": "amd64", "arm64": "arm64", "aarch64": "arm64"}
    operating_system, _, architecture = result.stdout.strip().lower().partition("/")
    if operating_system != "linux" or architecture not in aliases:
        raise ValueError("Docker daemon native platform is unsupported")
    return "linux/" + aliases[architecture]


def _private_bytes(path: Path) -> bytes:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("Qualification evidence must be an owner-only regular file")
    return path.read_bytes()


def qualified_image_services(receipt_path: Path, commit: str) -> dict[str, dict[str, str]]:
    """Validate existing hash-bound hosted qualification; never infer tests from images."""
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("A full deployment commit is required")
    receipt = json.loads(_private_bytes(receipt_path))
    if (
        not isinstance(receipt, dict)
        or receipt.get("target_sha") != commit
        or receipt.get("exact_required_gates_pass") is not True
        or receipt.get("all_three_exact_image_manifests_available") is not True
    ):
        raise ValueError("Images must be qualified for the exact deployment commit")
    try:
        binding = receipt["gate_receipt"]
        gate_bytes = _private_bytes(Path(binding["path"]))
        gates = json.loads(gate_bytes)
        required = [job for job in gates["jobs"] if job["name"] in _REQUIRED_JOBS]
        if (
            hashlib.sha256(gate_bytes).hexdigest() != binding["sha256"]
            or gates["target"] != commit
            or {job["name"] for job in required} != _REQUIRED_JOBS
            or any(job["status"] != "completed" or job["conclusion"] != "success" for job in required)
            or not any(s["context"] == "jenkins/ci" and s["state"] == "success" for s in gates["jenkins"])
        ):
            raise ValueError("Exact required hosted gates are not positively hash-bound")
        native_platform = docker_native_platform()
        architecture = native_platform.split("/")[1]
        services = {}
        for role in _ROLES:
            image = receipt["images"][role]
            reference = image["image_reference_private"]
            tag = reference.rpartition(":")[2]
            if (
                image["available"] is not True
                or image["exact_commit_tag"] != commit[:12]
                or "@" in reference
                or any(c.isspace() for c in reference)
                or not re.fullmatch(re.escape(commit[:12]) + r"(?:-[A-Za-z0-9_.-]+)?", tag)
            ):
                raise ValueError("An exact commit-tagged registry role is missing")
            descriptors = [
                d
                for d in image["descriptors"]
                if d.get("platform", {}).get("os") == "linux"
                and d["platform"].get("architecture") == architecture
                and d["platform"].get("variant") in ((None, "", "v8") if architecture == "arm64" else (None, ""))
            ]
            if len(descriptors) != 1 or not re.fullmatch(r"sha256:[0-9a-f]{64}", descriptors[0]["digest"]):
                raise ValueError("A unique qualified native-platform image digest is required")
            services[role] = {"image": reference + "@" + descriptors[0]["digest"], "platform": native_platform}
        return services
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError("Incomplete hosted image qualification evidence") from exc


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 4:
        raise SystemExit("usage: deploy_images RECEIPT FULL_COMMIT COMPOSE_OVERRIDE PRIVATE_OUTPUT")
    receipt_path, commit, override_path, output_path = args
    try:
        services = qualified_image_services(Path(receipt_path), commit)
        override = Path(override_path)
        config = yaml.safe_load(override.read_text())
        for role, settings in services.items():
            config["services"].setdefault(role, {}).update(settings)
        override.write_text(yaml.safe_dump(config))
        proof = {
            "target_sha": commit,
            "qualification_sha256": hashlib.sha256(_private_bytes(Path(receipt_path))).hexdigest(),
            "daemon_native_platform": services["backend"]["platform"],
            "services": services,
            "activation_policy": "explicit pull; up --no-build --pull never; no native-build fallback",
        }
        Path(output_path).write_text(json.dumps(proof, indent=2) + "\n")
    except (ValueError, OSError, yaml.YAMLError):
        raise SystemExit("Qualified registry activation refused; no native-build fallback") from None


if __name__ == "__main__":
    main()
