"""Contract for ``require_owned_row`` — the pre-mutation cross-tenant gate shared by alert toggle/delete and view delete.

Pinned behaviours (each one re-opens audit finding 018 if it regresses):
admin sessions skip the lookup entirely; a scoped analyst is rejected on the ADDRESSED service first, then on the
row's own ``service_id``; the 403 body is exactly ``{"error": "service_not_authorized", "service": <offender>}``.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from backend.utils.auth import require_owned_row

pytestmark = pytest.mark.security_regression


def _req(service_ids: list[str] | None) -> SimpleNamespace:
    session = None if service_ids is None else SimpleNamespace(service_ids=service_ids)
    return SimpleNamespace(state=SimpleNamespace(analyst_session=session))


class _Fetch:
    def __init__(self, row: dict | None):
        self.row = row
        self.calls: list[tuple[str, str]] = []

    def __call__(self, row_id: str, service_id: str) -> dict | None:
        self.calls.append((row_id, service_id))
        return self.row


def _forbidden(exc: pytest.ExceptionInfo[HTTPException], service: str | None) -> None:
    assert exc.value.status_code == 403
    assert exc.value.detail == {"error": "service_not_authorized", "service": service}


def test_admin_passes_without_touching_the_repository() -> None:
    fetch = _Fetch({"service_id": "other"})
    assert require_owned_row(_req(None), "svcA", "row-1", fetch) is None
    assert fetch.calls == []


def test_analyst_owning_the_row_passes_and_row_is_looked_up_in_the_addressed_service() -> None:
    fetch = _Fetch({"id": "row-1", "service_id": "svcA"})
    assert require_owned_row(_req(["svcA"]), "svcA", "row-1", fetch) == {"id": "row-1", "service_id": "svcA"}
    assert fetch.calls == [("row-1", "svcA")]


def test_analyst_addressing_an_out_of_scope_service_is_rejected_before_any_lookup() -> None:
    fetch = _Fetch(None)
    with pytest.raises(HTTPException) as exc:
        require_owned_row(_req(["svcA"]), "svcB", "row-1", fetch)
    _forbidden(exc, "svcB")
    assert fetch.calls == []


def test_analyst_in_scope_service_but_row_owned_elsewhere_is_rejected() -> None:
    fetch = _Fetch({"id": "row-1", "service_id": "svcB"})
    with pytest.raises(HTTPException) as exc:
        require_owned_row(_req(["svcA"]), "svcA", "row-1", fetch)
    _forbidden(exc, "svcB")


def test_row_without_service_id_is_rejected_for_an_analyst() -> None:
    with pytest.raises(HTTPException) as exc:
        require_owned_row(_req(["svcA"]), "svcA", "row-1", _Fetch({"id": "row-1"}))
    _forbidden(exc, None)


def test_missing_row_in_scope_passes_so_the_mutation_can_report_its_own_not_found() -> None:
    assert require_owned_row(_req(["svcA"]), "svcA", "row-1", _Fetch(None)) is None


def test_analyst_with_empty_scope_is_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        require_owned_row(_req([]), "svcA", "row-1", _Fetch(None))
    _forbidden(exc, "svcA")
