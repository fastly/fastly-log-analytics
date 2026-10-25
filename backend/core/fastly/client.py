import json
import logging
import os
import re
import sys as _sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator
from typing import Annotated, Any, Literal

import tenacity
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)


# ── Canned Fastly API responses for E2E (formerly backend.core.fastly.mock_fixtures) ──


def is_mock_mode() -> bool:
    """Single-source check the gated callers read on every request."""
    return os.environ.get("FASTLY_MOCK_MODE") == "1"


def api_key_or_mock(key: str) -> str:
    """``key`` unchanged, or a placeholder in FASTLY_MOCK_MODE when it is empty."""
    return key or ("mock-api-key" if is_mock_mode() else key)


def _service_id_from_path(path: str) -> str:
    m = re.match(r"^/service/([^/]+)", path)
    return m.group(1) if m else "mock-svc-id"


def _version_from_path(path: str) -> int:
    m = re.search(r"/version/(\d+)", path)
    return int(m.group(1)) if m else 1


def mock_response(method: str, path: str, body: dict | None = None) -> Any:
    """Return a canned response for the given Fastly endpoint."""
    if path == "/tokens/self" and method == "GET":
        return {
            "id": "mock-token-id",
            "user_id": "mock-user",
            "scope": "global",
            "customer_id": "mock-customer",
            "services": [],
        }

    if path == "/service" and method == "POST":
        return {"id": "mock-svc-id", "name": (body or {}).get("name", "mock"), "version": 1}

    if path == "/service" and method == "GET":
        return []

    if re.fullmatch(r"/service/[^/]+", path) and method in ("GET", "PUT", "DELETE"):
        return {
            "id": _service_id_from_path(path),
            "version": 1,
            "customer_id": "mock-customer",
        }

    if method == "GET" and re.search(r"^/service/[^/]+/version$", path):
        return [{"number": 1, "active": True}]

    if method == "GET" and re.search(r"^/service/[^/]+/version/active$", path):
        return {"number": 1, "active": True}

    if method == "GET" and "/version/" in path:
        if path.endswith("/logging/s3"):
            return []
        if path.endswith("/snippet"):
            return []
        if path.endswith("/condition"):
            return []
        if path.endswith("/domain"):
            return []
        if path.endswith("/backend"):
            return []
        if path.endswith("/vcl"):
            return []

    if "/version/" in path and "/activate" in path:
        return {"number": _version_from_path(path), "active": True}

    if "/version/" in path and "/validate" in path:
        return {"status": "ok", "errors": []}

    if "/version/" in path and path.endswith("/clone"):
        return {"number": _version_from_path(path) + 1}

    if "/domain" in path:
        return {"name": (body or {}).get("name", "mock.example"), "service_id": _service_id_from_path(path)}

    if "/backend" in path:
        return {"name": (body or {}).get("name", "fos_origin"), "address": "mock.example"}

    if "/snippet" in path:
        return {"id": "mock-snippet-id", "name": (body or {}).get("name", "mock_snippet")}

    if "/logging/" in path:
        return {"name": (body or {}).get("name", "mock-logger"), "version": _version_from_path(path)}

    if "/dictionary" in path:
        return {"id": "mock-dict-id", "name": (body or {}).get("name", "mock_dict")}

    if "/acl" in path:
        return {"id": "mock-acl-id", "name": (body or {}).get("name", "mock_acl")}

    if "/condition" in path:
        return {"name": (body or {}).get("name", "mock_condition"), "statement": (body or {}).get("statement", "")}

    if "/vcl" in path:
        return {"name": (body or {}).get("name", "main"), "content": (body or {}).get("content", "")}

    if re.search(r"/version(?:/\d+)?$", path) and method == "POST":
        return {"number": _version_from_path(path)}

    if "/object-storage/access-keys" in path:
        if method == "POST":
            return {
                "access_key": "AKIA_MOCK",
                "secret_key": "SECRET_MOCK",  # gitleaks:allow — canned mock secret, not real
                "description": (body or {}).get("description", "fos-log-analysis-mock"),
            }
        if method == "DELETE":
            return {}
        if method == "GET":
            return {"data": []}

    if "/object-storage/buckets" in path:
        if method == "POST":
            return {"data": {"id": "mock-bucket-id", "attributes": {"name": (body or {}).get("name", "mock-bucket")}}}
        if method == "GET":
            return {"data": []}

    return {"ok": True}


API_BASE = "https://api.fastly.com"
RT_BASE = "https://rt.fastly.com"

_RETRYABLE_HTTP_CODES = (429, 500, 502, 503, 504)

# Upper bound on a server-requested ``Retry-After`` pause so a hostile or
# misconfigured header can't park a worker thread.
RETRY_AFTER_CAP_S = 30.0


def _is_retryable_fastly_error(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in _RETRYABLE_HTTP_CODES
    return isinstance(exc, (urllib.error.URLError, ConnectionError, TimeoutError))


def parse_retry_after(value: str | None) -> float | None:
    """Parse a ``Retry-After`` header given as delay-seconds, clamped to ``[0, RETRY_AFTER_CAP_S]``.

    Returns ``None`` when absent or not a number (HTTP-date form is not honored; the
    default backoff applies instead).
    """
    if not value:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        return None
    if seconds != seconds or seconds < 0:  # NaN or negative
        return None
    return min(seconds, RETRY_AFTER_CAP_S)


def _wait_strategy(method: str) -> Callable[[tenacity.RetryCallState], float]:
    """Default jittered backoff, except that GET retries honor a 429/503 ``Retry-After``.

    Mutations keep the default backoff on purpose: they gain no new behavior here.
    """
    default = tenacity.wait_exponential(multiplier=1, min=1, max=8) + tenacity.wait_random(min=0, max=2)

    def _wait(retry_state: tenacity.RetryCallState) -> float:
        if method.upper() == "GET" and retry_state.outcome is not None:
            exc = retry_state.outcome.exception()
            if isinstance(exc, urllib.error.HTTPError) and exc.code in (429, 503):
                hinted = parse_retry_after(exc.headers.get("Retry-After") if exc.headers else None)
                if hinted is not None:
                    return hinted
        return default(retry_state)

    return _wait


def _request(method, path, *, data, headers, expect_empty, max_retries, timeout, base=API_BASE):
    """Shared transport for ``fastly()`` and ``fastly_raw()``.

    Owns the telemetry acquisition + dispatch, the tenacity retry loop, the
    urlopen + body parse, and the except→RuntimeError translation. The two
    public wrappers differ only in mock-mode short-circuit, header dict
    construction (JSON sets Content-Type only when a body is present; raw
    always sets the caller-supplied type), and default timeout — they build
    ``headers`` + ``data`` themselves and delegate the wire call here.

    Retry policy is method-blind by design: POST/PUT/DELETE are retried on
    429/5xx and on timeouts/connection errors just like GET. Known risk: if
    Fastly accepted a non-idempotent POST (create service/version/domain/backend)
    but the response was lost, the retry can create a duplicate or surface a
    spurious 409 after the first attempt actually succeeded. Kept as-is
    deliberately; callers that create resources must tolerate that.
    """
    try:
        from backend.utils.telemetry import tracked_call

        telemetry_context = tracked_call(method, path, service="Fastly API")
    except ImportError:
        telemetry_context = None

    def _do_call():
        url = base + path

        try:
            # wait_exponential + wait_random_exponential composes as
            # ``base_exp + jitter`` — jitter prevents two admin retries
            # hitting Fastly's per-token+minute window in lock-step. Cap
            # at 8s per the prior shape; jitter adds up to 2s.
            for attempt in tenacity.Retrying(
                retry=tenacity.retry_if_exception(_is_retryable_fastly_error),
                stop=tenacity.stop_after_attempt(max_retries + 1),
                wait=_wait_strategy(method),
                reraise=True,
            ):
                with attempt:
                    req = urllib.request.Request(url, data=data, headers=headers, method=method)
                    with urllib.request.urlopen(req, timeout=timeout) as resp:
                        raw = resp.read().decode()
                        if expect_empty or not raw.strip():
                            return {}
                        return json.loads(raw)
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode(errors="replace")
            raise RuntimeError(f"HTTP {exc.code} {method} {path}\n    {body_text}") from exc
        except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
            raise RuntimeError(f"Network error on {method} {path}: {exc}") from exc

    if telemetry_context:
        with telemetry_context:
            return _do_call()
    else:
        return _do_call()


def fastly(method, path, body=None, *, token, expect_empty=False, max_retries=3, timeout=30):
    """Make a Fastly API request and return parsed JSON."""
    if is_mock_mode():
        return mock_response(method, path, body)

    data = json.dumps(body).encode() if body is not None else None
    hdrs = {"Fastly-Key": token, "Accept": "application/json"}
    if data:
        hdrs["Content-Type"] = "application/json"

    return _request(
        method,
        path,
        data=data,
        headers=hdrs,
        expect_empty=expect_empty,
        max_retries=max_retries,
        timeout=timeout,
    )


def fastly_raw(
    method,
    path,
    data: bytes,
    *,
    content_type: str,
    token,
    expect_empty=False,
    max_retries=3,
    timeout=60,
):
    """Make a Fastly API request with a RAW bytes body (not JSON).

    The JSON-only ``fastly()`` can't carry a KV Store value (raw bytes, up to
    25 MiB) or a Compute package upload (multipart form). Same retry +
    telemetry + mock-mode shape as ``fastly()``; the only differences are the
    caller-supplied ``Content-Type`` and that ``data`` is sent verbatim.

    Returns parsed JSON, or ``{}`` when the response is empty (KV item PUTs
    typically 200 with an empty body). ``timeout`` defaults higher than
    ``fastly()`` because a multi-MB upload can outlast the 30s default.
    """
    if is_mock_mode():
        try:
            body = json.loads(data.decode()) if data else None
        except Exception:
            body = None
        return mock_response(method, path, body)

    hdrs = {"Fastly-Key": token, "Accept": "application/json", "Content-Type": content_type}

    return _request(
        method,
        path,
        data=data,
        headers=hdrs,
        expect_empty=expect_empty,
        max_retries=max_retries,
        timeout=timeout,
    )


def fastly_rt(path, *, token, max_retries=0, timeout=10):
    """GET from the real-time analytics host (``rt.fastly.com``) and return parsed JSON.

    Same transport, telemetry and mock-mode shape as ``fastly()``; only the base host differs.
    """
    if is_mock_mode():
        return mock_response("GET", path, None)

    return _request(
        "GET",
        path,
        data=None,
        headers={"Fastly-Key": token, "Accept": "application/json"},
        expect_empty=False,
        max_retries=max_retries,
        timeout=timeout,
        base=RT_BASE,
    )


PageStyle = Literal["cursor", "page", "link"]


def _next_path_from_link(link: str) -> str | None:
    """Reduce a ``links.next`` value to an API path, or ``None`` if it points off-host.

    The token rides on every request, so an absolute URL on any host other than
    ``api.fastly.com`` is refused rather than followed.
    """
    if link.startswith("/"):
        return link
    parsed = urllib.parse.urlsplit(link)
    if f"{parsed.scheme}://{parsed.netloc}" != API_BASE:
        return None
    return parsed.path + (f"?{parsed.query}" if parsed.query else "")


def fastly_paginate(
    path: str,
    *,
    token: str,
    style: PageStyle = "cursor",
    items_key: str = "data",
    params: dict[str, Any] | None = None,
    cursor_param: str = "cursor",
    next_cursor_path: tuple[str, ...] = ("meta", "next_cursor"),
    page_param: str = "page",
    per_page_param: str = "per_page",
    per_page: int = 100,
    max_pages: int = 1000,
    page_delay: float = 0.0,
    timeout: float = 30,
) -> Iterator[dict]:
    """GET every page of a list endpoint, yielding each parsed page body (reads only).

    ``style``:
      - ``"cursor"``: the next cursor is read from ``next_cursor_path`` (falsy ends
        iteration) and sent back as ``cursor_param``.
      - ``"page"``: ``page_param``/``per_page_param`` numbering; ends on an empty or
        short page.
      - ``"link"``: JSON:API ``links.next`` (path or ``api.fastly.com`` URL); ends when
        absent. Off-host links are refused.

    Each page goes through :func:`fastly`, so auth, retries (incl. ``Retry-After`` on
    GET), telemetry and mock mode apply. ``max_pages`` and repeated-cursor detection
    bound a misbehaving server.
    """
    base_params = dict(params or {})
    sep = "&" if "?" in path else "?"
    seen: set[str] = set()
    cursor: str | None = None
    page_no = 1
    next_path: str | None = None

    for _ in range(max_pages):
        if style == "link" and next_path is not None:
            request_path = next_path
        else:
            q = dict(base_params)
            if style == "cursor" and cursor:
                q[cursor_param] = cursor
            elif style == "page":
                q[page_param] = page_no
                q[per_page_param] = per_page
            query = urllib.parse.urlencode(q, quote_via=urllib.parse.quote, safe="")
            request_path = path + (sep + query if query else "")

        page = fastly("GET", request_path, token=token, timeout=timeout)
        yield page

        if style == "cursor":
            node: Any = page
            for key in next_cursor_path:
                node = node.get(key) if isinstance(node, dict) else None
            if not node or not isinstance(node, str) or node in seen:
                return
            seen.add(node)
            cursor = node
        elif style == "page":
            items = page.get(items_key) if isinstance(page, dict) else None
            if not items or len(items) < per_page:
                return
            page_no += 1
        else:
            link = (page.get("links") or {}).get("next") if isinstance(page, dict) else None
            if not link or not isinstance(link, str):
                return
            next_path = _next_path_from_link(link)
            if next_path is None or next_path in seen:
                return
            seen.add(next_path)

        if page_delay:
            time.sleep(page_delay)


def _count(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _opt_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


Count = Annotated[int, BeforeValidator(_count)]
OptInt = Annotated[int | None, BeforeValidator(_opt_int)]


class _Tolerant(BaseModel):
    model_config = ConfigDict(extra="allow")


class StatsRecord(_Tolerant):
    """One bucket of ``/stats/service/{id}`` or ``/stats/aggregate``."""

    start_time: OptInt = None
    requests: Count = 0
    edge_requests: Count = 0
    object_storage_class_a_operations_count: Count = 0
    object_storage_class_b_operations_count: Count = 0
    object_storage: Any = None

    def fos_ops(self) -> tuple[int, int]:
        a = self.object_storage_class_a_operations_count
        b = self.object_storage_class_b_operations_count
        if a or b:
            return a, b
        sub = self.object_storage
        if isinstance(sub, dict):
            return _count(sub.get("class_a_operations_count")), _count(sub.get("class_b_operations_count"))
        return 0, 0


class StatsResponse(_Tolerant):
    data: list[StatsRecord] = Field(default_factory=list)

    @field_validator("data", mode="before")
    @classmethod
    def _null_data_is_empty(cls, v: Any) -> Any:
        return [] if v is None else v


class NgwafWorkspace(_Tolerant):
    id: str | None = None
    name: str | None = None
    attributes: dict[str, Any] | None = None

    def display_name(self) -> str | None:
        return self.name or (self.attributes or {}).get("name") or self.id


class NgwafWorkspacesResponse(_Tolerant):
    """``GET /ngwaf/v1/workspaces``: list under ``data`` (current) or ``workspaces`` (legacy)."""

    data: list[NgwafWorkspace] | None = None
    workspaces: list[NgwafWorkspace] | None = None

    def workspaces_list(self) -> list[dict[str, str | None]]:
        items = self.data if self.data is not None else (self.workspaces or [])
        return [{"id": w.id, "name": w.display_name()} for w in items]


def get_active_version(service_id: str, token: str) -> int | None:
    info = get_active_version_info(service_id, token, timeout=None, max_retries=None)
    return info["number"] if info else None


def get_active_version_info(
    service_id: str, token: str, *, timeout: int | None = 8, max_retries: int | None = 1
) -> dict | None:
    """Return the active version's number + timestamps for a service.

    Like :func:`get_active_version` but keeps the activation timestamp so
    callers can show *when* the live version went up (Fastly has no dedicated
    "activated_at"; ``updated_at`` on the active version is the activation/last
    -change time, ``created_at`` is when the version was cloned). Snappy
    defaults (1 retry / 8s) because this is called from a polled status path.
    Returns ``None`` on any error or if no active version exists.
    """
    extra_kwargs: dict[str, int] = {}
    if timeout is not None:
        extra_kwargs["timeout"] = timeout
    if max_retries is not None:
        extra_kwargs["max_retries"] = max_retries
    try:
        v = fastly("GET", f"/service/{service_id}/version/active", token=token, **extra_kwargs)
        if isinstance(v, dict) and v.get("active"):
            return {
                "number": int(v["number"]),
                "updated_at": v.get("updated_at"),
                "created_at": v.get("created_at"),
            }
    except RuntimeError:
        pass

    # Fallback for backwards-compatibility / mock testing
    try:
        versions = fastly("GET", f"/service/{service_id}/version", token=token, **extra_kwargs)
        if isinstance(versions, list):
            for v in versions:
                if v.get("active"):
                    return {
                        "number": int(v["number"]),
                        "updated_at": v.get("updated_at"),
                        "created_at": v.get("created_at"),
                    }
    except RuntimeError:
        pass
    return None


def get_generated_vcl(service_id: str, version: int, token: str) -> str | None:
    """Return the fully-compiled (generated) VCL for a service version, or None.

    Fastly's ``GET /service/{id}/version/{n}/generated_vcl`` returns JSON
    ``{"content": "<vcl>", ...}`` — the COMPILED VCL, which carries the
    account-level pragmas Fastly injects (e.g.
    ``pragma optional_param ratelimit_opt_in true;``) that never appear in the
    source VCL we upload. Returns ``None`` for Compute/wasm services (which have
    no generated VCL) and on any API error, mirroring the RuntimeError→None
    sentinel style of :func:`get_active_version`.
    """
    try:
        result = fastly("GET", f"/service/{service_id}/version/{version}/generated_vcl", token=token)
    except RuntimeError:
        return None
    content = result.get("content") if isinstance(result, dict) else None
    return content or None


def find_service_by_name(name: str, token: str) -> dict | None:
    try:
        services = fastly("GET", "/service", token=token)
        for s in services:
            if s.get("name") == name:
                return s
    except RuntimeError:
        pass
    return None


def find_condition(name: str, service_id: str, version: int, token: str) -> dict | None:
    try:
        conditions = fastly("GET", f"/service/{service_id}/version/{version}/condition", token=token)
        for c in conditions:
            if c.get("name") == name:
                return c
    except RuntimeError:
        pass
    return None


def ensure_condition(name: str, statement: str, type: str, service_id: str, version: int, token: str) -> dict:
    existing = find_condition(name, service_id, version, token)
    if existing:
        if existing.get("statement") == statement and existing.get("type") == type:
            return existing
        else:
            # Update existing condition
            payload = {"name": name, "statement": statement, "type": type}
            encoded_name = urllib.parse.quote(name, safe="")
            return fastly(
                "PUT", f"/service/{service_id}/version/{version}/condition/{encoded_name}", payload, token=token
            )

    payload = {"name": name, "statement": statement, "type": type}
    return fastly("POST", f"/service/{service_id}/version/{version}/condition", payload, token=token)


def list_s3_endpoints(service_id: str, version: int, token: str) -> list[str]:
    try:
        res = fastly("GET", f"/service/{service_id}/version/{version}/logging/s3", token=token)
        return [r["name"] for r in res]
    except RuntimeError:
        return []


def list_vcl_snippets(service_id: str, version: int, token: str) -> list[str]:
    try:
        res = fastly("GET", f"/service/{service_id}/version/{version}/snippet", token=token)
        return [r["name"] for r in res]
    except RuntimeError:
        return []


def ensure_vcl_snippet(
    name: str, type: str, content: str, priority: int, service_id: str, version: int, token: str
) -> dict:
    try:
        snippets = fastly("GET", f"/service/{service_id}/version/{version}/snippet", token=token)
        for s in snippets:
            if s.get("name") == name:
                if s.get("type") == type and s.get("content") == content and int(s.get("priority", 100)) == priority:
                    return s
                else:
                    # Update existing snippet
                    payload = {"name": name, "type": type, "content": content, "priority": priority, "dynamic": 0}
                    encoded_name = urllib.parse.quote(name, safe="")
                    return fastly(
                        "PUT", f"/service/{service_id}/version/{version}/snippet/{encoded_name}", payload, token=token
                    )
    except RuntimeError:
        pass

    payload = {"name": name, "type": type, "content": content, "priority": priority, "dynamic": 0}
    return fastly("POST", f"/service/{service_id}/version/{version}/snippet", payload, token=token)


_sys.modules.setdefault("backend.core.fastly.mock_fixtures", _sys.modules[__name__])
