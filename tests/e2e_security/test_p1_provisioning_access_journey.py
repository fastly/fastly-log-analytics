"""Executable offline parity contract for the P1 provisioning/access journey."""

from __future__ import annotations

from typing import Any

import pytest

from tests.e2e_security.p1_journey import make_p1_journey_adapter

pytestmark = pytest.mark.no_postgres


@pytest.fixture(params=("high_scale", "cloud"))
def p1_journey_adapter(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> Any:
    adapter_name = str(request.param)
    if adapter_name == "high_scale":
        return make_p1_journey_adapter(
            adapter_name,
            monkeypatch=monkeypatch,
            tmp_path=tmp_path,
            clickhouse=request.getfixturevalue("ch_recorder"),
        )
    return make_p1_journey_adapter(
        adapter_name,
        monkeypatch=monkeypatch,
        tmp_path=tmp_path,
        cloud=request.getfixturevalue("cloud_server"),
    )


def test_p1_provisioning_access_custom_field_journey(p1_journey_adapter: Any) -> None:
    attachment = p1_journey_adapter.setup_and_attach()
    analyst = p1_journey_adapter.invite_and_sign_in()

    masking = p1_journey_adapter.observe_masking(analyst)
    custom_field = p1_journey_adapter.change_and_remove_custom_field()
    revoked = p1_journey_adapter.revoke(analyst)

    assert attachment.initially_absent is True
    assert attachment.persisted_registration_count == 1
    assert attachment.activation_visible is True
    assert attachment.attached is True
    assert analyst.mask_ips is True
    assert masking.policy_enabled is True
    assert masking.raw_ip_absent is True
    if p1_journey_adapter.kind == "cloud":
        assert masking.raw_file_denied is True
        assert masking.raw_file_positive_control is True
    assert custom_field.read_back_expression == "req.http.X-Journey-Two"
    assert custom_field.removed is True
    assert revoked.old_session_rejected is True
