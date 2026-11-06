"""Generalized Compute service Wasm deployment & package provenance verification.

Supports both `compute/cloud` (`flp` / `flp-worker`) and `compute/scorer` (`session-scorer`).
Re-exports session-scoring deployment helpers for backwards compatibility.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import subprocess
import tarfile
import uuid
from pathlib import Path
from typing import Any

from backend.core.fastly.client import fastly_raw
from backend.provision.session_scoring.wasm_deploy import (
    DEFAULT_ASSET_EXT_REGEX,
    DEFAULT_ENFORCE_STATUS_CODE,
    SCORING_BACKEND_API_NAME,
    SCORING_BACKEND_VCL_NAME,
    SCORING_DELIVER_NAME,
    SCORING_DOMAIN_TEMPLATE,
    SCORING_ENFORCE_NAME,
    SCORING_FETCH_NAME,
    SCORING_MISS_NAME,
    SCORING_PASS_NAME,
    SCORING_RECV_NAME,
    SCORING_SERVICE_NAME_PREFIX,
    SCORING_SNIPPET_PRIORITY,
    EntitlementError,
    _assert_vcl_string_safe,
    _deploy_wasm_package,
    _find_scoring_service,
    _live_package_files_hash,
    _package_sha,
    _remove_scoring_backend,
    _remove_scoring_snippets,
    _scoring_domain,
    _scoring_service_name,
    delete_scoring_service,
    deliver_snippet,
    enforce_reason_phrase,
    enforce_snippet,
    ensure_scoring_service,
    fetch_snippet,
    generate_scoring_vcl,
    miss_snippet,
    pass_snippet,
    recv_snippet,
    resolve_enforce_status_code,
    resolve_exclude_url_regex,
    scorer_vcl_fingerprint,
    scoring_snippet_names,
    shipped_scorer_identity,
)


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout.strip()


def verify_package_provenance(
    root: Path,
    package: Path,
    *,
    tree_subdir: str | None = None,
    git_fn=_git,
) -> dict[str, str]:
    """Verify `provenance.json`, SHA-256 package digest, clean git tree, and embedded Wasm build SHA."""
    provenance = json.loads((package.parent / "provenance.json").read_text())
    sha = provenance.get("build_sha", "")
    digest = provenance.get("package_sha256", "")
    if not re.fullmatch(r"[a-f0-9]{40}", sha) or not re.fullmatch(r"[a-f0-9]{64}", digest):
        raise ValueError("Invalid frozen build provenance")
    contents = package.read_bytes()
    if hashlib.sha256(contents).hexdigest() != digest:
        raise ValueError("Package changed after the recorded build")
    subdir = tree_subdir or ("compute/cloud" if package.resolve().parent.parent.name == "cloud" else "app")
    if git_fn(root, "status", "--porcelain", "--untracked-files=all", "--", subdir):
        raise ValueError(f"Runtime has uncommitted inputs; commit {subdir}/ before a release")
    if git_fn(root, "rev-parse", f"{sha}:{subdir}") != git_fn(root, "rev-parse", f"HEAD:{subdir}"):
        raise ValueError("Runtime source changed after the build")
    with tarfile.open(fileobj=io.BytesIO(contents), mode="r:gz") as archive:
        members = [member for member in archive.getmembers() if member.isfile() and member.name.endswith(".wasm")]
        if len(members) != 1:
            raise ValueError("Package must contain exactly one Wasm binary")
        stream = archive.extractfile(members[0])
        if stream is None or sha.encode() not in stream.read():
            raise ValueError("Package Wasm does not contain its frozen built SHA")
    return {"build_sha": sha, "package_sha256": digest}


def upload_wasm_package(
    service_id: str,
    version: int,
    package: Path,
    token: str,
    *,
    filename: str | None = None,
) -> dict[str, Any]:
    """Upload a `.tar.gz` Wasm package to a Fastly Compute service version."""
    pkg_bytes = package.read_bytes()
    boundary = "flp-" + uuid.uuid4().hex
    pkg_name = filename or package.name
    body = (
        (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="package"; filename="{pkg_name}"\r\n'
            "Content-Type: application/gzip\r\n\r\n"
        ).encode()
        + pkg_bytes
        + f"\r\n--{boundary}--\r\n".encode()
    )
    raw = fastly_raw(
        "PUT",
        f"/service/{service_id}/version/{version}/package",
        body,
        content_type=f"multipart/form-data; boundary={boundary}",
        token=token,
    )
    return json.loads(raw) if raw else {}


__all__ = [
    "DEFAULT_ASSET_EXT_REGEX",
    "DEFAULT_ENFORCE_STATUS_CODE",
    "EntitlementError",
    "SCORING_BACKEND_API_NAME",
    "SCORING_BACKEND_VCL_NAME",
    "SCORING_DELIVER_NAME",
    "SCORING_DOMAIN_TEMPLATE",
    "SCORING_ENFORCE_NAME",
    "SCORING_FETCH_NAME",
    "SCORING_MISS_NAME",
    "SCORING_PASS_NAME",
    "SCORING_RECV_NAME",
    "SCORING_SERVICE_NAME_PREFIX",
    "SCORING_SNIPPET_PRIORITY",
    "_assert_vcl_string_safe",
    "_deploy_wasm_package",
    "_find_scoring_service",
    "_live_package_files_hash",
    "_package_sha",
    "_remove_scoring_backend",
    "_remove_scoring_snippets",
    "_scoring_domain",
    "_scoring_service_name",
    "delete_scoring_service",
    "deliver_snippet",
    "enforce_reason_phrase",
    "enforce_snippet",
    "ensure_scoring_service",
    "fetch_snippet",
    "generate_scoring_vcl",
    "miss_snippet",
    "pass_snippet",
    "recv_snippet",
    "resolve_enforce_status_code",
    "resolve_exclude_url_regex",
    "scorer_vcl_fingerprint",
    "scoring_snippet_names",
    "shipped_scorer_identity",
    "upload_wasm_package",
    "verify_package_provenance",
]
