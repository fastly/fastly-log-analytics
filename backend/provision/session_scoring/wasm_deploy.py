"""Compute service lifecycle, WASM package deployment, and VCL snippet builders for session scoring."""

from __future__ import annotations

import hashlib
import json
import secrets
import urllib.parse
from pathlib import Path
from typing import Any

from backend.core.fastly.client import fastly, fastly_raw, get_active_version, list_vcl_snippets
from backend.core.vcl_generator import VclSafetyError, validate_quoted_literal
from backend.provision.cli import BOLD, _c, draft_version, info, ok, warn
from backend.provision.orchestrator import product_enabled
from backend.provision.session_scoring.stores import (
    _SCORER_PACKAGE,
    COMPUTE_MANAGE_URL,
    CONFIG_RESOURCE_LINK_NAME,
    CONFIG_STORE_MANAGE_URL,
    CONFIG_STORE_NAME_TEMPLATE,
    CURRENT_KEY_HEX,
    DEBUG_LOG_DEFAULT,
    DEBUG_LOG_KEY,
    KEYS_RESOURCE_LINK_NAME,
    KEYS_STORE_NAME_TEMPLATE,
    KV_STORE_MANAGE_URL,
    MATRIX_KEY_NAME,
    MATRIX_RESOURCE_LINK_NAME,
    MATRIX_STORE_NAME_TEMPLATE,
    REQUEST_SECRET_KEY,
    _ensure_matrix_kv_store,
    _ensure_scoring_enabled_at,
    _find_config_store,
    _find_kv_store,
    _store_id,
)


class EntitlementError(Exception):
    """A Fastly product required by session scoring isn't enabled on the
    account. Carries a machine-readable ``code`` and an actionable
    ``link`` (a manage.fastly.com deep link) so the SSE layer can render a
    clickable "enable it" message instead of a raw ``HTTP 4xx`` string.

    Not a RuntimeError subclass on purpose: the create path maps ``fastly``'s
    ``RuntimeError("HTTP 4xx …")`` into this richer type, and keeping it off the
    RuntimeError hierarchy ensures a stray ``except RuntimeError`` upstream
    can't silently swallow an entitlement failure."""

    def __init__(self, code: str, message: str, link: str = ""):
        super().__init__(message)
        self.code = code
        self.link = link


SCORING_SERVICE_NAME_PREFIX = "Session Scoring Service for "
SCORING_DOMAIN_TEMPLATE = "fos-{sid_lower}-session-scorer.edgecompute.app"


def _scoring_service_name(logging_service_id: str) -> str:
    return f"{SCORING_SERVICE_NAME_PREFIX}{logging_service_id}"


def _scoring_domain(logging_service_id: str) -> str:
    return SCORING_DOMAIN_TEMPLATE.format(sid_lower=logging_service_id.lower())


def _find_scoring_service(logging_service_id: str, token: str) -> dict | None:
    """Return the existing scoring service for this logging service, if any.     Idempotency lever — ``ensure_scoring_service`` reuses an existing     service rather than failing on duplicate-name."""
    name = _scoring_service_name(logging_service_id)
    try:
        services = fastly("GET", "/service", token=token) or []
    except RuntimeError:
        return None
    for svc in services:
        if svc.get("name") == name:
            return svc
    return None


def ensure_scoring_service(logging_service_id: str, token: str, *, status_cb=None) -> dict[str, Any]:
    """Create (or reuse) the per-customer session-scoring Compute service,     its two ConfigStores, the AES-256 key, and resource links from v1 of     the service to the stores."""
    name = _scoring_service_name(logging_service_id)
    domain = _scoring_domain(logging_service_id)
    info(f"Ensuring scoring service {_c(BOLD, name)}")
    if status_cb:
        status_cb(f"⏳ Ensuring scoring service '{name}'...")
    existing = _find_scoring_service(logging_service_id, token)
    if existing:
        ok(f"Scoring service already exists ({existing['id']})")
        if status_cb:
            status_cb(f"✅ Scoring service '{name}' already exists.")
        scoring_service_id = existing["id"]
        keys_store = _find_config_store(KEYS_STORE_NAME_TEMPLATE.format(sid=scoring_service_id), token)
        cfg_store = _find_config_store(CONFIG_STORE_NAME_TEMPLATE.format(sid=scoring_service_id), token)
        matrix_store = _find_kv_store(MATRIX_STORE_NAME_TEMPLATE.format(sid=scoring_service_id), token)
        if not matrix_store:
            matrix_store = _ensure_matrix_kv_store(scoring_service_id, token, status_cb=status_cb)
        _ensure_scoring_enabled_at((cfg_store or {}).get("id", ""), token)
        store_request_secret = ""
        if keys_store:
            try:
                item = fastly(
                    "GET", f"/resources/stores/config/{keys_store['id']}/item/{REQUEST_SECRET_KEY}", token=token
                )
                store_request_secret = (item or {}).get("item_value", "") or ""
            except RuntimeError:
                store_request_secret = ""  # missing → caller heals
        return {
            "scoring_service_id": scoring_service_id,
            "scoring_service_name": name,
            "scoring_domain": domain,
            "scoring_keys_store_id": (keys_store or {}).get("id", ""),
            "scoring_config_store_id": (cfg_store or {}).get("id", ""),
            "scoring_matrix_store_id": _store_id(matrix_store or {}),
            "aes_key_hex": "",  # AES key stays write-only by policy.
            "request_secret": store_request_secret,
        }
    # ── Fresh create. The scorer runs on Compute, which needs THREE Fastly
    # botched enable never leaves an orphaned half-built service behind. ───────
    if not product_enabled(token, "kv_store"):
        raise EntitlementError(
            "kv_store_not_enabled",
            "KV Store isn't enabled on this Fastly account, and the scorer needs it to "
            "hold the L2 transition matrix. Enable the KV Store product for your account, "
            "then click Enable again.",
            KV_STORE_MANAGE_URL,
        )
    scoring_service_id = ""
    keys_store_id = ""
    cfg_store_id = ""
    matrix_store_id = ""
    try:
        try:
            svc = fastly("POST", "/service", {"name": name, "type": "wasm"}, token=token)
        except RuntimeError as exc:
            if "HTTP 4" in str(exc):
                raise EntitlementError(
                    "compute_not_enabled",
                    "Compute isn't enabled on this Fastly account, and session scoring runs as a "
                    "Compute (Wasm) service. Enable Compute for your account, then click Enable again.",
                    COMPUTE_MANAGE_URL,
                ) from exc
            raise
        scoring_service_id = svc["id"]
        ok(f"Created scoring service {scoring_service_id}")
        if status_cb:
            status_cb(f"✅ Created scoring service '{name}'.")
        fastly("POST", f"/service/{scoring_service_id}/version/1/domain", {"name": domain}, token=token)
        ok(f"Added domain {domain}")
        if status_cb:
            status_cb(f"✅ Added domain '{domain}'.")
        fastly(
            "POST",
            f"/service/{scoring_service_id}/version/1/backend",
            {"name": "placeholder_origin", "address": "127.0.0.1", "port": 80, "override_host": "example.com"},
            token=token,
        )
        ok("Added placeholder backend")
        keys_store_name = KEYS_STORE_NAME_TEMPLATE.format(sid=scoring_service_id)
        cfg_store_name = CONFIG_STORE_NAME_TEMPLATE.format(sid=scoring_service_id)
        try:
            keys_store = fastly("POST", "/resources/stores/config", {"name": keys_store_name}, token=token)
            keys_store_id = keys_store["id"]
            cfg_store = fastly("POST", "/resources/stores/config", {"name": cfg_store_name}, token=token)
            cfg_store_id = cfg_store["id"]
        except RuntimeError as exc:
            if "HTTP 4" in str(exc):
                raise EntitlementError(
                    "config_store_not_enabled",
                    "Config Store isn't enabled for Compute on this Fastly account. The scorer keeps "
                    "its keys and toggles in two Config Stores. Enable the Config Store product for your "
                    "account, then click Enable again.",
                    CONFIG_STORE_MANAGE_URL,
                ) from exc
            raise
        ok(f"Created config stores {keys_store_name}, {cfg_store_name}")
        if status_cb:
            status_cb("✅ Created config stores.")
        matrix_store = _ensure_matrix_kv_store(scoring_service_id, token, status_cb=status_cb)
        matrix_store_id = _store_id(matrix_store)
        aes_key_hex = secrets.token_hex(32)
        request_secret = secrets.token_hex(32)
        fastly(
            "POST",
            f"/resources/stores/config/{keys_store['id']}/item",
            {"item_key": CURRENT_KEY_HEX, "item_value": aes_key_hex},
            token=token,
        )
        fastly(
            "POST",
            f"/resources/stores/config/{keys_store['id']}/item",
            {"item_key": REQUEST_SECRET_KEY, "item_value": request_secret},
            token=token,
        )
        fastly(
            "POST",
            f"/resources/stores/config/{cfg_store['id']}/item",
            {"item_key": DEBUG_LOG_KEY, "item_value": DEBUG_LOG_DEFAULT},
            token=token,
        )
        _ensure_scoring_enabled_at(cfg_store["id"], token)
        ok("Populated config stores")
        fastly(
            "POST",
            f"/service/{scoring_service_id}/version/1/resource",
            {"name": KEYS_RESOURCE_LINK_NAME, "resource_id": keys_store["id"]},
            token=token,
        )
        fastly(
            "POST",
            f"/service/{scoring_service_id}/version/1/resource",
            {"name": CONFIG_RESOURCE_LINK_NAME, "resource_id": cfg_store["id"]},
            token=token,
        )
        fastly(
            "POST",
            f"/service/{scoring_service_id}/version/1/resource",
            {"name": MATRIX_RESOURCE_LINK_NAME, "resource_id": _store_id(matrix_store)},
            token=token,
        )
        ok("Linked stores to service v1")
        if status_cb:
            status_cb("✅ Linked config + matrix stores to service v1.")
        return {
            "scoring_service_id": scoring_service_id,
            "scoring_service_name": name,
            "scoring_domain": domain,
            "scoring_keys_store_id": keys_store["id"],
            "scoring_config_store_id": cfg_store["id"],
            "scoring_matrix_store_id": _store_id(matrix_store),
            "aes_key_hex": aes_key_hex,
            "request_secret": request_secret,
        }
    except Exception:
        if scoring_service_id:
            warn(f"Enable failed — rolling back partial scoring service {scoring_service_id}")
            if status_cb:
                status_cb("⚠️ Enable failed — rolling back the partially-created scoring service…")
            try:
                delete_scoring_service(
                    scoring_service_id,
                    scoring_keys_store_id=keys_store_id,
                    scoring_config_store_id=cfg_store_id,
                    scoring_matrix_store_id=matrix_store_id,
                    token=token,
                )
            except Exception:
                warn("Rollback of partial scoring service failed — manual cleanup may be needed")
        raise


def delete_scoring_service(
    scoring_service_id: str,
    *,
    scoring_keys_store_id: str = "",
    scoring_config_store_id: str = "",
    scoring_matrix_store_id: str = "",
    token: str,
    status_cb=None,
) -> list[tuple[str, str]]:
    """Tear down the Compute service AND its stores (2 ConfigStores + the     matrix KV Store). Idempotent — deleting an already-deleted resource is a     no-op."""
    failed: list[tuple[str, str]] = []
    if not scoring_service_id:
        warn("delete_scoring_service called with empty service id — nothing to do")
        return failed
    info(f"Tearing down scoring service {_c(BOLD, scoring_service_id)}")
    if status_cb:
        status_cb(f"⏳ Tearing down scoring service '{scoring_service_id}'...")
    try:
        versions = fastly("GET", f"/service/{scoring_service_id}/version", token=token) or []
        for v in versions:
            if v.get("active"):
                if status_cb:
                    status_cb(f"⏳ Deactivating version {v['number']}...")
                fastly("PUT", f"/service/{scoring_service_id}/version/{v['number']}/deactivate", token=token)
    except RuntimeError as exc:
        if "404" in str(exc):
            ok("Scoring service already deleted")
            return failed
        warn(f"Failed to deactivate versions (will try delete anyway): {exc}")
    try:
        fastly("DELETE", f"/service/{scoring_service_id}", token=token, expect_empty=True)
        ok("Scoring service deleted")
    except RuntimeError as exc:
        if "404" in str(exc):
            ok("Scoring service already deleted")
        else:
            raise
    for label, store_id in (("scoring_keys", scoring_keys_store_id), ("scoring_config", scoring_config_store_id)):
        if not store_id:
            continue
        try:
            fastly("DELETE", f"/resources/stores/config/{store_id}", token=token, expect_empty=True)
            ok(f"Deleted {label} store ({store_id})")
        except RuntimeError as exc:
            if "404" in str(exc):
                ok(f"{label} store already deleted")
            else:
                warn(f"Could not delete {label} store {store_id}: {exc}")
                failed.append((label, store_id))
                if status_cb:
                    status_cb(f"⚠️ Could not delete {label} store {store_id} — remove it manually: {exc}")
    if scoring_matrix_store_id:
        for sub in (f"/keys/{MATRIX_KEY_NAME}", ""):
            try:
                fastly("DELETE", f"/resources/stores/kv/{scoring_matrix_store_id}{sub}", token=token, expect_empty=True)
            except RuntimeError as exc:
                if "404" not in str(exc):
                    warn(f"Could not delete matrix KV {sub or 'store'} {scoring_matrix_store_id}: {exc}")
                    if sub == "":
                        failed.append(("scoring_matrix", scoring_matrix_store_id))
                        if status_cb:
                            status_cb(
                                f"⚠️ Could not delete matrix KV store {scoring_matrix_store_id} "
                                f"— remove it manually: {exc}"
                            )
        ok(f"Deleted matrix KV store ({scoring_matrix_store_id})")
    if status_cb:
        status_cb("✅ Scoring service torn down.")
    return failed


_VCL_FINGERPRINT_SECRET = "drift-fingerprint"


def _package_sha(path: Path = _SCORER_PACKAGE) -> str | None:
    """sha256 of the committed Wasm package bytes — the exact bytes the     backend uploads on deploy. Returns None if the file is missing/unreadable     (a fresh checkout without the prebuilt package)."""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def scorer_vcl_fingerprint(logging_service_id: str) -> str:
    """Content hash of the scoring VCL the current generator produces for     this service, using canonical inputs (see _VCL_FINGERPRINT_SECRET). A     change to generate_scoring_vcl shifts this hash; operator config tweaks     do not."""
    snippets = generate_scoring_vcl(logging_service_id, _VCL_FINGERPRINT_SECRET)
    return hashlib.sha256(json.dumps(snippets, sort_keys=True).encode()).hexdigest()


def shipped_scorer_identity(logging_service_id: str) -> dict[str, str | None]:
    """Identity of the scorer build the backend would deploy *right now*:     the committed Wasm package sha + the VCL generator fingerprint. The     status endpoint compares this to the {deployed_package_sha,     deployed_vcl_sha} stamped at the last enable to flag edge drift."""
    return {"package_sha": _package_sha(_SCORER_PACKAGE), "vcl_sha": scorer_vcl_fingerprint(logging_service_id)}


def _live_package_files_hash(scoring_service_id: str, version: int, token: str) -> str:
    """The edge's OWN integrity hash for a Compute version's package     (``files_hash``, falling back to the deprecated ``hashsum``). Empty string     when unavailable. Lets a redeploy detect a package that changed at the edge     out from under us (e.g. a manual deploy) without trusting only the cfg     stamp — we compare the live value against the one we stamped at our last     deploy. Fastly's hash format is undocumented/deprecated and is NOT the     sha256 of the tarball bytes, so we never compute it locally; we only ever     compare edge-stamped value to edge-live value."""
    try:
        pkg = fastly("GET", f"/service/{scoring_service_id}/version/{version}/package", token=token) or {}
    except RuntimeError:
        return ""
    meta = (pkg.get("metadata") if isinstance(pkg, dict) else None) or {}
    return meta.get("files_hash") or meta.get("hashsum") or ""


def _deploy_wasm_package(
    scoring_service_id: str,
    matrix_store_id: str,
    token: str,
    status_cb=None,
    *,
    prior_package_sha: str = "",
    prior_files_hash: str = "",
) -> dict[str, Any]:
    """Deploy the prebuilt, matrix-less Wasm package to the scoring service     via the Fastly API — no Rust/Fastly-CLI toolchain required (the backend     container has neither). Replaces the old ``deploy_wasm.sh`` subprocess."""
    info("Deploying prebuilt Wasm package to the scoring Compute service")
    if not _SCORER_PACKAGE.exists():
        raise RuntimeError(
            f"prebuilt scorer package not found at {_SCORER_PACKAGE} — "
            "the Docker image builds it from source (backend/Dockerfile scorer-builder stage), so "
            "rebuild the image; outside Docker run `make scorer-package`"
        )
    committed_sha = _package_sha(_SCORER_PACKAGE)
    active = get_active_version(scoring_service_id, token)
    if active is not None and committed_sha and committed_sha == prior_package_sha:
        live_files_hash = _live_package_files_hash(scoring_service_id, active, token)
        if live_files_hash and live_files_hash == prior_files_hash:
            ok(f"Scorer package unchanged at the edge (v{active}) — skipping Wasm upload")
            if status_cb:
                status_cb(f"✅ [Compute] Package already current (v{active}) — skipped upload")
            return {"version": active, "sha": committed_sha, "files_hash": live_files_hash, "skipped": True}
    if status_cb:
        status_cb("⏳ [Compute] Uploading scorer Wasm package (no build)…")
    versions = fastly("GET", f"/service/{scoring_service_id}/version", token=token) or []
    base = max(int(v["number"]) for v in versions)
    with draft_version(
        scoring_service_id,
        token,
        base_version=base,
        call=fastly,
        validate=False,
        expect_empty_activate=True,
        rollback=False,
    ) as new_ver:
        if matrix_store_id:
            try:
                fastly(
                    "POST",
                    f"/service/{scoring_service_id}/version/{new_ver}/resource",
                    {"name": MATRIX_RESOURCE_LINK_NAME, "resource_id": matrix_store_id},
                    token=token,
                )
            except RuntimeError as exc:
                if not ("409" in str(exc) or "conflict" in str(exc).lower() or "already" in str(exc).lower()):
                    raise
        pkg_bytes = _SCORER_PACKAGE.read_bytes()
        boundary = "----fos-scorer-package-boundary"
        body = (
            (
                f"--{boundary}\r\n"
                'Content-Disposition: form-data; name="package"; filename="session-scorer.tar.gz"\r\n'
                "Content-Type: application/octet-stream\r\n\r\n"
            ).encode()
            + pkg_bytes
            + f"\r\n--{boundary}--\r\n".encode()
        )
        fastly_raw(
            "PUT",
            f"/service/{scoring_service_id}/version/{new_ver}/package",
            body,
            content_type=f"multipart/form-data; boundary={boundary}",
            token=token,
        )
    ok(f"Deployed scorer package + activated v{new_ver}")
    if status_cb:
        status_cb(f"✅ [Compute] Activated scorer service v{new_ver}")
    return {
        "version": new_ver,
        "sha": hashlib.sha256(pkg_bytes).hexdigest(),
        "files_hash": _live_package_files_hash(scoring_service_id, new_ver, token),
        "skipped": False,
    }


def _remove_scoring_backend(logging_service_id: str, version: int, token: str) -> None:
    """Remove the scoring backend (idempotent — 404 is fine)."""
    encoded = urllib.parse.quote(SCORING_BACKEND_API_NAME, safe="")
    try:
        fastly(
            "DELETE",
            f"/service/{logging_service_id}/version/{version}/backend/{encoded}",
            token=token,
            expect_empty=True,
        )
        ok(f"Removed scoring backend {SCORING_BACKEND_API_NAME}")
    except RuntimeError as exc:
        if "404" in str(exc):
            ok("Scoring backend already absent")
        else:
            raise


def _remove_scoring_snippets(logging_service_id: str, version: int, token: str) -> None:
    """Delete the six scoring snippets by name (idempotent)."""
    present = set(list_vcl_snippets(logging_service_id, version, token))
    for name in scoring_snippet_names():
        if name not in present:
            continue
        encoded = urllib.parse.quote(name, safe="")
        try:
            fastly(
                "DELETE",
                f"/service/{logging_service_id}/version/{version}/snippet/{encoded}",
                token=token,
                expect_empty=True,
            )
            ok(f"Removed snippet {name}")
        except RuntimeError as exc:
            if "404" in str(exc):
                continue
            raise


def _assert_vcl_string_safe(value: str, *, field: str, allow_backslash: bool = False) -> str:
    """Raise ``ValueError`` if ``value`` can't be safely embedded in a VCL
    double-quoted string literal; return it unchanged otherwise. See the module
    note above — this is a defense-in-depth guard at the generation boundary, not
    the primary validation (that lives in the persisted-write + Fastly /validate
    path). ``allow_backslash`` for regex inputs that legitimately contain ``\\``.

    Delegates to the unified :mod:`backend.utils.vcl_safety` char policy so the
    forbidden-char rules can't drift from the other generators/validators.
    ``VclSafetyError`` subclasses ``ValueError`` so existing callers catching
    ``ValueError`` are unaffected."""
    try:
        return validate_quoted_literal(value, allow_backslash=allow_backslash, field=field)
    except VclSafetyError as exc:
        raise ValueError(str(exc)) from exc


SCORING_BACKEND_API_NAME = "session_scorer"
SCORING_BACKEND_VCL_NAME = f"F_{SCORING_BACKEND_API_NAME}"
SCORING_RECV_NAME = "Session Scoring - Recv"
SCORING_PASS_NAME = "Session Scoring - Pass"
SCORING_FETCH_NAME = "Session Scoring - Fetch"
SCORING_DELIVER_NAME = "Session Scoring - Deliver"
SCORING_MISS_NAME = "Session Scoring - Miss"
SCORING_ENFORCE_NAME = "Session Scoring - Enforce"
SCORING_SNIPPET_PRIORITY = 100
DEFAULT_ASSET_EXT_REGEX = (
    r"^[^?#;]*"
    r"\.(aif|aiff|au|avi|bin|bmp|cab|carb|cct|cdf|class|css|dcr|doc|"
    r"dtd|exe|flv|gcf|gff|gif|grv|hdml|hqx|ico|ini|jpeg|jpg|js|mov|"
    r"mp3|mp4|nc|pct|pdf|png|ppc|pws|svg|swa|swf|txt|vbs|w32|wav|"
    r"wbmp|wml|wmlc|wmls|wmlsc|xsd|zip|webp|woff|woff2|ttf|bz2|gz|"
    r"tgz|tar|lzma|rar|war|bz|7z|ts|m3u8)($|\?|#)"
)


def resolve_exclude_url_regex(operator_override: str | None) -> str:
    """Pick between the operator's override and the built-in default."""
    if operator_override is None:
        return DEFAULT_ASSET_EXT_REGEX
    cleaned = operator_override.strip()
    return cleaned or DEFAULT_ASSET_EXT_REGEX


DEFAULT_ENFORCE_STATUS_CODE = 429
_ENFORCE_STATUS_CODE_MIN = 400
_ENFORCE_STATUS_CODE_MAX = 599


def enforce_reason_phrase(status_code: int) -> str:
    """HTTP reason phrase for the enforce snippet's synthetic body."""
    import http

    try:
        return http.HTTPStatus(status_code).phrase
    except ValueError:
        return "Blocked"


def resolve_enforce_status_code(operator_override: int | None) -> int:
    """Pick the effective enforce status code. None / out-of-range → default."""
    if operator_override is None:
        return DEFAULT_ENFORCE_STATUS_CODE
    try:
        code = int(operator_override)
    except (TypeError, ValueError):
        return DEFAULT_ENFORCE_STATUS_CODE
    if not (_ENFORCE_STATUS_CODE_MIN <= code <= _ENFORCE_STATUS_CODE_MAX):
        return DEFAULT_ENFORCE_STATUS_CODE
    return code


def recv_snippet(logging_service_id: str, *, exclude_url_regex: str | None = None) -> str:
    """vcl_recv snippet: at the EDGE on the first pass (no shield hop yet     AND req.restarts == 0 AND scoring-pass marker not set AND URL doesn't     match the exclusion regex), route to the scorer Compute backend with     X-Edge-Scoring-Pass=1, `return(pass)`. After the scoring restart     completes (req.restarts == 1) and the score was captured, re-enable     shielding for the real-origin pass so the cached object can be     served from the shield POP normally."""
    _ = logging_service_id
    effective_regex = resolve_exclude_url_regex(exclude_url_regex)
    _assert_vcl_string_safe(effective_regex, field="exclude_url_regex", allow_backslash=True)
    return f"""# Session Scoring: client-edge header scrub (anti-spoofing).
# Edge-only (fastly.ff.visits_this_service == 0, unforgeable) so any
# client-supplied X-Edge-* gets stripped before it can be forged into a
# clean score.
if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {{
  unset req.http.X-Edge-Scoring-Pass;
  unset req.http.X-Edge-Score;
  unset req.http.X-Edge-Score-Reason;
  unset req.http.X-Edge-Score-Enforce;
  unset req.http.X-Edge-Sid;
  unset req.http.X-Edge-Score-Set-Cookie;
  # Defense-in-depth for the L2 skip-gram anchor. The scorer already forces
  # prev_anchor to None (compute/scorer/src/main.rs), but a client-supplied
  # X-Edge-Prev-Anchor must never reach Compute regardless: a SEEN
  # high-probability anchor would otherwise raise the L2 transition prob and
  # depress the sequence-anomaly score (the audit F009 evasion class, which
  # the codebase already closed for the sibling X-Edge-Prev-Route header).
  unset req.http.X-Edge-Prev-Anchor;
  # NGWAF skip-inspection bypass guard. Only WE may set
  # x-sigsci-skip-inspection-once (below, on the scoring sub-fetch) — strip any
  # client-supplied copy here so a client can never skip the Next-Gen WAF.
  # NGWAF's own ngwaf_config_init recv code also strips it on sampled requests
  # (and runs before this snippet), but unset here too for defense in depth.
  unset req.http.x-sigsci-skip-inspection-once;
}}

# Session Scoring: route the first-pass dynamic request to the scorer.
# Edge-only — fastly.ff.visits_this_service == 0 is true only at the true
# edge; at a shield POP it is > 0, so the shield skips this block and the
# real-origin pass is served from the shield normally. Unforgeable, so a
# client cannot fake having already transited our edge.
#
# DDoS bypass (fastly.ddos_detected): when Fastly's L7 DDoS detection
# flags this request, do NOT route to Compute. Two reasons:
#   1. Cost ceiling — under attack, Compute invocations scale linearly
#      with attack volume. Skipping flagged requests caps the blast
#      radius while NGWAF / Fastly's mitigation handles the actual block.
#   2. Signal quality — the scorer's L2 transition matrix learns from
#      benign traffic shapes; feeding attack traffic in pollutes the
#      matrix even though those scores wouldn't be acted on.
# See: https://www.fastly.com/documentation/reference/vcl/variables/miscellaneous/fastly-ddos-detected/
if (fastly.ff.visits_this_service == 0 && req.restarts == 0 && req.http.X-Edge-Scoring-Pass != "1" && !fastly.ddos_detected && std.tolower(req.url) !~ "{effective_regex}") {{
  set req.backend = {SCORING_BACKEND_VCL_NAME};
  # Skip NGWAF inspection on the scoring sub-fetch ONLY. The scorer is
  # payload-agnostic — it strips the query string and scores the path — so
  # inspecting this preflight hop is pure cost, and worse: NGWAF 406s attack
  # URLs here, turning every blocked request into a scorer fail-open
  # (compute-unavailable-406) even though the scorer never sees the payload.
  # NGWAF's edge_security (vcl_miss/pass, priority 150) reads this on bereq
  # AFTER our Session Scoring - Pass (priority 100), skips inspection, and
  # self-unsets its bereq copy. The real-origin pass after the restart stays
  # fully inspected — the persisted req.http copy is unset on the restart
  # path below.
  set req.http.x-sigsci-skip-inspection-once = "true";
  set req.http.X-Edge-Scoring-Pass = "1";
  # Stamp the round-trip start (µs since request receipt) so pass-1 deliver
  # can compute edge-observed scorer latency (edge_score_rtt_us). Mirrors
  # the x-of-start TTFB/TTLB idiom in fastly_api.generate_capture_vcl —
  # that timer deliberately SKIPS the scoring sub-fetch, so this stamp is
  # the only place the scorer leg is timed.
  set req.http.x-edge-score-t0 = time.elapsed.usec;
  # PASS — skip cache for the scoring sub-fetch. On the post-restart
  # pass the scoring snippet doesn't re-fire because X-Edge-Scoring-Pass
  # got unset in pass-1 deliver and req.restarts is now 1.
  return(pass);
}}

# Post-scoring restart: we captured the score in pass-1 deliver and the
# request flow is now headed for the real origin. Without this block,
# the previous `return(pass)` would have permanently disabled shielding
# for this request — re-enable it so the real-origin fetch can land on
# the shield POP normally. `var.fastly_req_do_shield` is the magic
# variable Fastly's auto-generated main VCL reads to decide whether to
# shield the request.
if (req.restarts == 1 && req.http.x-edge-score) {{
  set var.fastly_req_do_shield = true;
}}
if (req.restarts == 1) {{
  # Real-origin pass — ensure NGWAF inspects it. We set
  # x-sigsci-skip-inspection-once on the scoring sub-fetch (restarts == 0);
  # edge_security unsets only its bereq copy, so the req.http value persists
  # across the restart. Unset it here so attack traffic to the real origin is
  # never skipped — even on scorer fail-open.
  unset req.http.x-sigsci-skip-inspection-once;
}}"""


def pass_snippet(logging_service_id: str, request_secret: str) -> str:
    """vcl_pass snippet: when this is the scoring sub-fetch (backend ==     scorer), rewrite bereq.method to GET so the client request body stays     buffered at the edge for the restarts == 1 origin pass (paired with the     service's preflight body preservation pragmas), and inject the auth +     service-id headers on bereq for the upcoming sub-fetch. Also unset     bereq.http.x-edge-score so any attacker-supplied inbound x-edge-score     doesn't get echoed into the scorer's view of the request."""
    _assert_vcl_string_safe(request_secret, field="request_secret")  # EC-07
    return f"""# Session Scoring: inject auth + service-id on the scorer sub-fetch.
# vcl_pass is the right subroutine for bereq mutations when recv used
# return(pass).
if (req.backend == {SCORING_BACKEND_VCL_NAME}) {{
  # Rewrite the preflight sub-fetch to GET so Fastly does not stream the
  # inbound request body to the Compute scorer, preserving the body for
  # the restarts == 1 origin pass.
  set bereq.method = "GET";
  set bereq.http.X-Edge-Service-Id = "{logging_service_id}";
  # Shared-secret header — the scorer compares this to the
  # request_secret ConfigStore entry and 401s on mismatch. Embedded
  # literally in VCL which is compiled and never sent to clients.
  set bereq.http.X-Edge-Scorer-Auth = "{request_secret}";
  # X-Edge-Scoring-Pass is an internal marker; the scorer doesn't need
  # to see it and we don't want it polluting any downstream telemetry.
  unset bereq.http.X-Edge-Scoring-Pass;
}}
# Strip any inbound x-edge-score header an attacker may have set; the
# real one is built by us in vcl_deliver after the scorer responds.
unset bereq.http.x-edge-score;"""


def fetch_snippet() -> str:
    """vcl_fetch snippet: when the backend is the scorer, return(deliver)     so the response goes straight to deliver without any cache-related     handling. (return(pass) in recv already prevents caching, but     return(deliver) here is the canonical preflight-pattern shape and     avoids any weird interactions with beresp's TTL.)"""
    return f"""# Session Scoring: skip cache handling for the scorer sub-fetch.
if (req.backend == {SCORING_BACKEND_VCL_NAME}) {{
  return(deliver);
}}"""


def deliver_snippet() -> str:
    """vcl_deliver snippet — the heart of the pattern."""
    return """# Session Scoring: pass-1 stash + naked restart; pass-2 emit cookie.

# ── PASS 1: capture scorer response into req.http.x-edge-score subfields ──
if (req.http.X-Edge-Scoring-Pass == "1") {
  unset req.http.X-Edge-Scoring-Pass;
  # Edge-observed scorer round-trip (µs): diff time.elapsed against the
  # x-edge-score-t0 stamp set in recv just before the sub-fetch. Computed
  # for BOTH success and fail-open (outside the status branch) so timed-out
  # rows record ≈the timeout budget — exactly the signal for tuning it.
  # Same std.atoi(time.elapsed.usec) idiom as the TTFB/TTLB capture VCL.
  if (req.http.x-edge-score-t0 != "") {
    declare local var.rtt INTEGER;
    set var.rtt = std.atoi(time.elapsed.usec);
    set var.rtt -= std.atoi(req.http.x-edge-score-t0);
    # Stringify with the `"" +` idiom: assigning an INTEGER var straight to a
    # header SUBFIELD lands empty (unlike a plain header, which coerces). Every
    # other working subfield is set from a string; match that.
    set req.http.x-edge-score:rtt = "" + var.rtt;
  }
  if (resp.status == 200) {
    set req.http.x-edge-score:score = resp.http.x-edge-score;
    set req.http.x-edge-score:l1 = resp.http.x-edge-score-l1;
    set req.http.x-edge-score:l2 = resp.http.x-edge-score-l2;
    set req.http.x-edge-score:compliance = resp.http.X-Edge-Cookie-Compliance;
    set req.http.x-edge-score:reason = resp.http.x-edge-score-reason;
    # Hex-encoded 6-byte session id. Used by the admin labeling UI to
    # target individual sessions; the scorer issues a fresh sid when
    # the inbound cookie is missing/tampered.
    set req.http.x-edge-score:sid = resp.http.x-edge-sid;
    # Enforcement signal — set by the Rust scorer when the operator
    # has committed enforce_threshold to the scoring_config ConfigStore
    # AND the request's score met it. Captured here so the recv-
    # restart-2 Enforce snippet can read it via subfield.
    set req.http.x-edge-score:enforce = resp.http.x-edge-score-enforce;
    # Scorer-reported Wasm execution time (µs). Only present on 200s —
    # fail-open rows leave edge_score_exec_us NULL since the scorer never
    # responded. Lets us split round-trip latency into compute vs network.
    set req.http.x-edge-score:exec = resp.http.x-edge-score-exec-us;
    # Matrix version (YYYY-MM-DD-<suffix>) that scored this request. Captured
    # into the log subfield BEFORE the anti-leak unset below so a logged edge
    # score can be correlated to the matrix version (and rollback) that produced
    # it (EC-03). Empty on cookie-missing requests (L2 skipped → no matrix load).
    set req.http.x-edge-score:matrix = resp.http.X-Edge-Matrix-Version;
  } else {
    # Scorer returned non-200 — fail open. No cookie to rotate.
    set req.http.x-edge-score:score = "0";
    set req.http.x-edge-score:l1 = "0";
    set req.http.x-edge-score:l2 = "0";
    set req.http.x-edge-score:compliance = "unknown";
    set req.http.x-edge-score:reason = "compute-unavailable-" + resp.status;
  }
  # Stash the rotated cookie as a subfield too; pass-2 reads it back
  # and emits via add resp.http.Set-Cookie.
  set req.http.x-edge-score:set-cookie = resp.http.Set-Cookie;
  # Anti-leak: strip the scorer's resp.http.x-edge-* headers so they
  # don't reach the client even if the restart path were to short-
  # circuit somehow.
  unset resp.http.x-edge-score;
  unset resp.http.x-edge-score-l1;
  unset resp.http.x-edge-score-l2;
  unset resp.http.x-edge-score-reason;
  unset resp.http.x-edge-sid;
  unset resp.http.X-Edge-Cookie-Compliance;
  unset resp.http.X-Edge-Matrix-Version;
  unset resp.http.x-edge-score-enforce;
  unset resp.http.x-edge-score-exec-us;
  restart;
}

# ── PASS 2: real origin response — emit the rotated cookie additively ──
# Only emit at the EDGE (fastly.ff.visits_this_service == 0). At a shield
# POP the counter is > 0, so the shield skips this block and the cookie is
# emitted exactly once, at the edge, to the client. Unforgeable, so no
# attacker-induced duplicate Set-Cookie emission.
if (fastly.ff.visits_this_service == 0 && req.http.x-edge-score:set-cookie != "") {
  add resp.http.Set-Cookie = req.http.x-edge-score:set-cookie;
}"""


def enforce_snippet(status_code: int = DEFAULT_ENFORCE_STATUS_CODE) -> str:
    """vcl_recv snippet that errors ``status_code`` when the scorer flagged the     request as over-threshold."""
    code = resolve_enforce_status_code(status_code)
    reason = enforce_reason_phrase(code)
    return (
        f"# Session Scoring: enforce committed threshold by erroring flagged requests.\n"
        f"# Status code ({code} {reason}) is operator-configurable via\n"
        f"# cfg.scoring.enforce_status_code; the update_enforce_status_code\n"
        f"# orchestrator swaps this snippet on change. Default 429.\n"
        f"# Fires only on the post-scoring restart (req.restarts == 1) when the\n"
        f"# deliver pass-1 captured X-Edge-Score-Enforce=1 from the scorer.\n"
        f"# Edge-only (fastly.ff.visits_this_service == 0, unforgeable) so shield hops don't double-enforce.\n"
        f'if (fastly.ff.visits_this_service == 0 && req.restarts == 1 && req.http.x-edge-score:enforce == "1") {{\n'
        f'  error {code} "{reason}";\n'
        f"}}"
    )


def miss_snippet() -> str:
    """vcl_miss snippet: defensive unsets. Strip inbound x-edge-score     (attacker could try to forge it) and X-Edge-Scoring-Pass (don't     leak the internal marker to the real origin on pass-2 fetch)."""
    return """# Session Scoring: strip internal scoring headers before forwarding to
# the real origin. x-edge-score could be attacker-supplied; the
# X-Edge-Scoring-Pass marker is internal-only.
if (req.backend.is_origin) {
  unset bereq.http.x-edge-score;
  unset bereq.http.X-Edge-Scoring-Pass;
}"""


def generate_scoring_vcl(
    logging_service_id: str,
    request_secret: str,
    *,
    exclude_url_regex: str | None = None,
    enforce_status_code: int | None = None,
) -> dict[str, str]:
    """Return a {snippet_name: vcl_body} dict for all six snippets."""
    return {
        SCORING_RECV_NAME: recv_snippet(logging_service_id, exclude_url_regex=exclude_url_regex),
        SCORING_PASS_NAME: pass_snippet(logging_service_id, request_secret),
        SCORING_FETCH_NAME: fetch_snippet(),
        SCORING_DELIVER_NAME: deliver_snippet(),
        SCORING_MISS_NAME: miss_snippet(),
        SCORING_ENFORCE_NAME: enforce_snippet(resolve_enforce_status_code(enforce_status_code)),
    }


def scoring_snippet_names() -> list[str]:
    """Names of the snippets we install. Used by disable_scoring to find     and remove them by name from the cloned VCL version."""
    return [
        SCORING_RECV_NAME,
        SCORING_PASS_NAME,
        SCORING_FETCH_NAME,
        SCORING_DELIVER_NAME,
        SCORING_MISS_NAME,
        SCORING_ENFORCE_NAME,
    ]
