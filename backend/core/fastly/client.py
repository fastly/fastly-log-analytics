import json
import logging
import urllib.error
import urllib.request

import tenacity

logger = logging.getLogger(__name__)

API_BASE = "https://api.fastly.com"

_RETRYABLE_HTTP_CODES = (429, 500, 502, 503, 504)


def _is_retryable_fastly_error(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in _RETRYABLE_HTTP_CODES
    return isinstance(exc, (urllib.error.URLError, ConnectionError, TimeoutError))


def _request(method, path, *, data, headers, expect_empty, max_retries, timeout):
    """Shared transport for ``fastly()`` and ``fastly_raw()``.

    Owns the telemetry acquisition + dispatch, the tenacity retry loop, the
    urlopen + body parse, and the except→RuntimeError translation. The two
    public wrappers differ only in mock-mode short-circuit, header dict
    construction (JSON sets Content-Type only when a body is present; raw
    always sets the caller-supplied type), and default timeout — they build
    ``headers`` + ``data`` themselves and delegate the wire call here.
    """
    try:
        from backend.utils.telemetry import tracked_call

        telemetry_context = tracked_call(method, path, service="Fastly API")
    except ImportError:
        telemetry_context = None

    def _do_call():
        url = API_BASE + path

        try:
            # wait_exponential + wait_random_exponential composes as
            # ``base_exp + jitter`` — jitter prevents two admin retries
            # hitting Fastly's per-token+minute window in lock-step. Cap
            # at 8s per the prior shape; jitter adds up to 2s.
            for attempt in tenacity.Retrying(
                retry=tenacity.retry_if_exception(_is_retryable_fastly_error),
                stop=tenacity.stop_after_attempt(max_retries + 1),
                wait=tenacity.wait_exponential(multiplier=1, min=1, max=8) + tenacity.wait_random(min=0, max=2),
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
    from backend.core.fastly.mock_fixtures import is_mock_mode, mock_response

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
    from backend.core.fastly.mock_fixtures import is_mock_mode, mock_response

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
