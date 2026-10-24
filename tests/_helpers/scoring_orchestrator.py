"""Shared Fastly API mock builders for session_scoring_orchestrator tests."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock


def make_scoring_meta(
    log_svc: str = "TestLogSvc123",
    score_svc: str = "ScoringSvcId",
    keys_store: str = "KEYS_STORE",
    cfg_store: str = "CFG_STORE",
    matrix_store: str = "MATRIX_STORE",
    *,
    with_secret: bool = True,
) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "scoring_service_id": score_svc,
        "scoring_service_name": f"Session Scoring Service for {log_svc}",
        "scoring_domain": f"fos-{log_svc.lower()}-session-scorer.edgecompute.app",
        "scoring_keys_store_id": keys_store,
        "scoring_config_store_id": cfg_store,
        "scoring_matrix_store_id": matrix_store,
        "aes_key_hex": "00" * 32,
    }
    if with_secret:
        meta["request_secret"] = "fake_request_secret_for_tests_" + "x" * 32
    return meta


def make_happy_path_fastly_mock(log_svc: str = "TestLogSvc123") -> MagicMock:
    """Return sensible defaults for every Fastly API call ``enable_scoring``
    makes after Stage 1 (``ensure_scoring_service``)."""
    versions = [{"number": 100, "active": True}]
    s3_endpoints = [{"name": "Fastly Object Storage Logs"}]
    snippets: list[dict[str, Any]] = []
    backends: list[dict[str, Any]] = []

    def side_effect(
        method: str, path: str, body: dict[str, Any] | None = None, token: str | None = None, **kwargs: Any
    ) -> Any:
        if (method, path) == ("GET", f"/service/{log_svc}/version"):
            return versions
        if (method, path) == ("PUT", f"/service/{log_svc}/version/100/clone"):
            return {"number": 101}
        if (method, path) == ("PUT", f"/service/{log_svc}/version/101"):
            return {}
        if (method, path) == ("GET", f"/service/{log_svc}/version/101/backend"):
            return backends
        if (method, path) == ("POST", f"/service/{log_svc}/version/101/backend") and body is not None:
            backends.append({"name": body["name"]})
            return {"name": body["name"]}
        if (method, path) == ("GET", f"/service/{log_svc}/version/101/snippet"):
            return snippets
        if (method, path) == ("POST", f"/service/{log_svc}/version/101/snippet") and body is not None:
            snippets.append(
                {
                    "name": body["name"],
                    "type": body.get("type"),
                    "content": body.get("content"),
                    "priority": body.get("priority"),
                }
            )
            return snippets[-1]
        if (method, path) == ("GET", f"/service/{log_svc}/version/101/logging/s3"):
            return s3_endpoints
        if method == "PUT" and "/logging/s3/" in path:
            return {}
        if (method, path) == ("GET", f"/service/{log_svc}/version/101/validate"):
            return {"status": "ok"}
        if (method, path) == ("PUT", f"/service/{log_svc}/version/101/activate"):
            return {}
        return {}

    return MagicMock(side_effect=side_effect)


def make_happy_path_fastly_mock_with_post_secret(
    log_svc: str = "TestLogSvc123",
    keys_store: str = "KEYS_STORE",
    *,
    post_secret_409: bool = False,
) -> MagicMock:
    """Variant that also handles the ``request_secret`` heal POST/PATCH on
    the keys ConfigStore."""
    base_mock = make_happy_path_fastly_mock(log_svc=log_svc)
    base_side_effect = base_mock.side_effect

    def side_effect(
        method: str, path: str, body: dict[str, Any] | None = None, token: str | None = None, **kwargs: Any
    ) -> Any:
        if (method, path) == ("POST", f"/resources/stores/config/{keys_store}/item") and body is not None:
            if post_secret_409:
                raise RuntimeError("409 Conflict — item already exists")
            return {"item_key": body["item_key"]}
        if method == "PATCH" and path == f"/resources/stores/config/{keys_store}/item/request_secret":
            return {"ok": True}
        return base_side_effect(method, path, body, token=token, **kwargs)

    return MagicMock(side_effect=side_effect)
