"""Clone -> mutate -> validate -> activate (-> rollback) for a Fastly service version.

``draft_version`` owns the call sequence that several provisioning orchestrators hand-wrote:

1. ``PUT /service/{id}/version/{base}/clone``
2. optional ``PUT /service/{id}/version/{new}`` to set the comment
3. the caller's mutations (the ``with`` body), then
4. ``GET .../validate`` (anything but ``status == "ok"`` raises ``RuntimeError``),
5. ``PUT .../activate``,
6. on any failure with ``rollback=True``: ``PUT /service/{id}/version/{base}/activate``
   (a failed rollback is logged, never raised, so the original error surfaces).

The Fastly client is passed in as ``call`` so each orchestrator keeps its own
module-level ``fastly`` name as the seam its tests patch.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)


@contextmanager
def draft_version(
    service_id: str,
    token: str,
    *,
    base_version: int,
    call: Callable[..., Any],
    comment: str | None = None,
    status_cb: Callable[[str], None] | None = None,
    rollback: bool = True,
) -> Iterator[int]:
    """Yield the new draft version number; validate + activate it when the body completes."""
    clone = call("PUT", f"/service/{service_id}/version/{base_version}/clone", token=token)
    new_ver = int(clone["number"])
    if comment is not None:
        call("PUT", f"/service/{service_id}/version/{new_ver}", {"comment": comment}, token=token)

    try:
        yield new_ver

        if status_cb:
            status_cb(f"Validating draft version {new_ver}...")
        result = call("GET", f"/service/{service_id}/version/{new_ver}/validate", token=token)
        if result.get("status") != "ok":
            raise RuntimeError(f"Validation failed: {result.get('errors') or result}")

        if status_cb:
            status_cb(f"Activating version {new_ver}...")
        call("PUT", f"/service/{service_id}/version/{new_ver}/activate", token=token)
    except Exception:
        if rollback:
            try:
                call("PUT", f"/service/{service_id}/version/{base_version}/activate", token=token)
            except RuntimeError:
                logger.warning(
                    "rollback activation of version %s failed for %s", base_version, service_id, exc_info=True
                )
        raise
