"""Clone -> mutate -> validate -> activate (-> rollback) for a Fastly service version.

``draft_version`` owns the call sequence that several provisioning orchestrators hand-wrote:

1. ``PUT /service/{id}/version/{base}/clone``
2. optional ``PUT /service/{id}/version/{new}`` to set the comment (right after the clone, or
   between validate and activate with ``comment_before_activate=True``)
3. the caller's mutations (the ``with`` body), then
4. ``GET .../validate`` (anything but ``status == "ok"`` raises ``RuntimeError``; skipped with
   ``validate=False``),
5. ``PUT .../activate`` (with ``expect_empty=True`` when ``expect_empty_activate`` is set),
6. on any failure with ``rollback=True``: ``PUT /service/{id}/version/{base}/activate``
   (a failed rollback is logged, never raised, so the original error surfaces).

The Fastly client is passed in as ``call`` so each orchestrator keeps its own
module-level ``fastly`` name as the seam its tests patch.

Orchestrators that must log their own failure line BEFORE the rollback runs pass ``on_failure``;
ones that own the rollback entirely pass ``rollback=False`` and call :func:`rollback_to_version`.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)


def rollback_to_version(service_id: str, token: str, base_version: int, *, call: Callable[..., Any]) -> None:
    """Re-activate ``base_version``. A failed rollback is logged at WARNING, never raised."""
    try:
        call("PUT", f"/service/{service_id}/version/{base_version}/activate", token=token)
    except RuntimeError:
        logger.warning("rollback activation of version %s failed for %s", base_version, service_id, exc_info=True)


@contextmanager
def draft_version(
    service_id: str,
    token: str,
    *,
    base_version: int,
    call: Callable[..., Any],
    comment: str | None = None,
    comment_before_activate: bool = False,
    status_cb: Callable[[str], None] | None = None,
    validate: bool = True,
    validate_msg: str = "Validating draft version {ver}...",
    activate_msg: str = "Activating version {ver}...",
    expect_empty_activate: bool = False,
    rollback: bool = True,
    on_failure: Callable[[Exception], None] | None = None,
) -> Iterator[int]:
    """Yield the new draft version number; validate + activate it when the body completes."""
    clone = call("PUT", f"/service/{service_id}/version/{base_version}/clone", token=token)
    new_ver = int(clone["number"])
    if comment is not None and not comment_before_activate:
        call("PUT", f"/service/{service_id}/version/{new_ver}", {"comment": comment}, token=token)

    try:
        yield new_ver

        if validate:
            if status_cb:
                status_cb(validate_msg.format(ver=new_ver))
            result = call("GET", f"/service/{service_id}/version/{new_ver}/validate", token=token)
            if result.get("status") != "ok":
                raise RuntimeError(f"Validation failed: {result.get('errors') or result}")

        if comment is not None and comment_before_activate:
            call("PUT", f"/service/{service_id}/version/{new_ver}", {"comment": comment}, token=token)

        if status_cb:
            status_cb(activate_msg.format(ver=new_ver))
        activate_kwargs: dict[str, Any] = {"expect_empty": True} if expect_empty_activate else {}
        call("PUT", f"/service/{service_id}/version/{new_ver}/activate", token=token, **activate_kwargs)
    except Exception as exc:
        if on_failure is not None:
            on_failure(exc)
        if rollback:
            rollback_to_version(service_id, token, base_version, call=call)
        raise
