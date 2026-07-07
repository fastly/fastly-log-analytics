"""Public-release guard: the real production Fastly **logging service ID** must
never appear in the tracked (committed) tree.

Context: ``github.com/fastly/fastly-log-analytics`` is a PUBLIC repo. The
operator's specific service ID (and the FOS bucket name ``fos-<id>-logs`` that
is trivially derived from it) is deployment-private — it belongs only in
gitignored ``configs/*.json`` and local-only files, never in source, tests,
docs, or commit messages. See the project's secret-hygiene policy.

This guard is a *canary*: it does not embed the plaintext ID (that would
re-introduce the very leak it guards against, and would defeat the scan of its
own file). The needle is stored base64-encoded and decoded at runtime; the scan
is case-insensitive so it catches both the canonical form and the lowercased
``logs_<id>`` table-name form.

As written this test FAILS until the leaking occurrences are scrubbed — it is
the repro for the pre-release finding. Once the tracked tree is clean it becomes
the permanent regression guard. To extend it to additional deployment-private
tokens, add more base64 needles to ``_NEEDLES_B64``.
"""

from __future__ import annotations

import base64
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.security_regression

_REPO_ROOT = Path(__file__).resolve().parents[2]

# base64 of deployment-private tokens that must not appear in the tracked tree.
# Stored encoded so this guard file itself contains no plaintext leak. Each was
# confirmed real by matching the operator's gitignored production service config
# (the producer), not guessed. Scan is case-insensitive so the lowercased
# ``logs_<id>`` table-name form is caught too.
#   S0xKUFV0SmtDMVpsTFZjalBHVjFqNQ== -> production logging service ID
#   ZnBSZnlrdTQyNThxdGlmaW9UY2dVbw== -> production CDN service ID
#   ZHJld19jb3JwLnRlc3Q=             -> production NGWAF workspace ID
_NEEDLES_B64 = (
    "S0xKUFV0SmtDMVpsTFZjalBHVjFqNQ==",
    "ZnBSZnlrdTQyNThxdGlmaW9UY2dVbw==",
    "ZHJld19jb3JwLnRlc3Q=",
)

# Suffixes that are binary / generated / lock files where a coincidental match
# is noise rather than a human-authored leak.
_SKIP_SUFFIXES = {
    ".lock",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".pdf",
    ".zip",
    ".gz",
    ".duckdb",
    ".db",
    ".wasm",
}


def _tracked_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [_REPO_ROOT / p for p in out.split("\0") if p]


def test_no_production_service_id_in_tracked_tree() -> None:
    needles = [base64.b64decode(b).decode().lower() for b in _NEEDLES_B64]
    self_path = Path(__file__).resolve()

    offenders: list[str] = []
    for path in _tracked_files():
        if path.resolve() == self_path:
            continue  # the guard stores only the base64 form
        if path.suffix.lower() in _SKIP_SUFFIXES:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore").lower()
        except (OSError, UnicodeError):
            continue
        for needle in needles:
            if needle in text:
                rel = path.relative_to(_REPO_ROOT)
                offenders.append(f"{rel} (contains a deployment-private token)")
                break

    assert not offenders, (
        "Deployment-private token(s) found in the tracked, world-readable tree.\n"
        "Scrub these before merging to main (and rotate/retire the pushed branch "
        "ref to un-publish them):\n  " + "\n  ".join(sorted(offenders))
    )


def _configured_oauth_secrets() -> list[str]:
    """Derive OAuth client_id / client_secret / flow-state secret from the
    gitignored provider registry + env (the PRODUCER), so this guard never
    hardcodes a secret (per the ``leak-sweep-producer-derived`` pattern). Returns
    lowercased needles; empty when OAuth is not configured in this environment.
    """
    out: list[str] = []
    try:
        from backend.core.oauth import registry

        registry.reset_cache_for_tests()
        for p in registry.get_all_providers():
            out.extend(v for v in (p.client_id, p.client_secret) if v)
        fss = registry.flow_state_secret()
        if fss:
            out.append(fss)
    except Exception:
        return []
    # Ignore trivially-short values that would produce noisy false positives.
    return [v.lower() for v in out if len(v) >= 8]


def test_no_oauth_client_credentials_in_tracked_tree() -> None:
    """When OAuth is configured (gitignored registry + env), the real client_id /
    client_secret / OAUTH_FLOW_STATE_SECRET must never appear in the tracked
    tree. Dormant (skipped) where OAuth isn't configured, e.g. CI."""
    needles = _configured_oauth_secrets()
    if not needles:
        pytest.skip("OAuth not configured in this environment — no credentials to scan for")

    self_path = Path(__file__).resolve()
    offenders: list[str] = []
    for path in _tracked_files():
        if path.resolve() == self_path:
            continue
        if path.suffix.lower() in _SKIP_SUFFIXES:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore").lower()
        except (OSError, UnicodeError):
            continue
        for needle in needles:
            if needle in text:
                offenders.append(f"{path.relative_to(_REPO_ROOT)} (contains an OAuth credential)")
                break

    assert not offenders, (
        "OAuth client credential(s) found in the tracked, world-readable tree.\n"
        "Move them to the gitignored registry / env and rotate them:\n  " + "\n  ".join(sorted(offenders))
    )
