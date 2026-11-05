import json
import os
from pathlib import Path
from typing import Any

import pytest
from record_api import _redact, fetch_contract, install_identifiers

FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "fastly-api"


def test_redaction_removes_credentials_and_identity_before_recording() -> None:
    assert _redact(
        {
            "id": "token-value",
            "customer_id": "customer",
            "services": ["private-service"],
            "secret": "credential",
            "access_key": "credential",
            "_links": {"self": "https://api.fastly.com/customer/private"},
            "scope": "global:read",
            "expires_at": None,
        }
    ) == {
        "id": "REDACTED_ID",
        "customer_id": "REDACTED_ID",
        "services": ["REDACTED_ID"],
        "scope": "global:read",
        "expires_at": None,
    }


def _matches_schema(recorded: Any, current: Any) -> bool:
    if recorded is None:
        return current is None or isinstance(current, (str, int, float, bool))
    if type(current) is not type(recorded):
        return False
    if isinstance(recorded, dict):
        return current.keys() == recorded.keys() and all(
            _matches_schema(recorded[key], current[key]) for key in recorded
        )
    elif isinstance(recorded, list) and recorded:
        return all(any(_matches_schema(example, value) for example in recorded) for value in current)
    return True


def _assert_schema(recorded: Any, current: Any) -> None:
    assert _matches_schema(recorded, current), "Observed API field/type schema drift"


@pytest.mark.skipif(os.environ.get("FLP_LIVE") != "1", reason="Explicit read-only live gate")
def test_live_recorded_get_contracts_have_not_drifted() -> None:
    service_id = os.environ.get("FLP_TEST_SERVICE_ID", "xa20dYfOsL4MhjIo66mVhh")
    for path in sorted(FIXTURES.glob("*.json")):
        fixture = json.loads(path.read_text())
        if fixture.get("recording_group") == "install":
            continue
        projection = {
            "token-self": "token",
            "object-storage": "product",
            "test-service-generated-vcl": "vcl",
        }.get(path.stem, "")
        status, response = fetch_contract(
            fixture["request"]["path"].replace("TEST_SERVICE", service_id),
            projection=projection,
        )
        assert status == fixture["status"], f"Status drift for {path.name}"
        _assert_schema(fixture["response"], response)


@pytest.mark.skipif(
    os.environ.get("FLP_LIVE") != "1" or not os.environ.get("FLP_INVENTORY"),
    reason="Explicit read-only live install gate and inventory",
)
def test_live_created_install_get_contracts_have_not_drifted() -> None:
    inventory = json.loads(Path(os.environ["FLP_INVENTORY"]).read_text())
    identifiers = install_identifiers(inventory)
    for path in sorted(FIXTURES.glob("install-*.json")):
        fixture = json.loads(path.read_text())
        request_path = fixture["request"]["path"]
        for original, replacement in identifiers.items():
            request_path = request_path.replace(replacement, original)
        status, response = fetch_contract(request_path, identifiers=identifiers)
        assert status == fixture["status"], f"Status drift for {path.name}"
        _assert_schema(fixture["response"], response)


def test_recorded_created_services_have_correct_isolated_store_links() -> None:
    for role, expected_service in (
        ("compute", "INSTALL_COMPUTE_ID"),
        ("worker", "INSTALL_WORKER_ID"),
    ):
        fixture = json.loads((FIXTURES / f"install-{role}-resource-links.json").read_text())
        assert fixture["status"] == 200
        assert {entry["name"]: (entry["resource_id"], entry["resource_type"]) for entry in fixture["response"]} == {
            "flp_config": ("STORE_CONFIG_ID", "config"),
            "flp_secrets": ("STORE_SECRET_ID", "secret-store"),
            "flp_state": ("STORE_KV_ID", "kv-store"),
        }
        assert all(entry["service_id"] == expected_service for entry in fixture["response"])


def test_recorded_backend_readbacks_have_matching_tls_and_only_tick_is_shielded() -> None:
    for role in ("compute", "worker", "front"):
        backends = json.loads((FIXTURES / f"install-{role}-backends.json").read_text())["response"]
        assert len(backends) == 3
        for origin in backends:
            assert origin["use_ssl"] is True
            assert origin["prefer_ipv6"] is False
            assert (
                origin["address"]
                == origin["override_host"]
                == origin["ssl_sni_hostname"]
                == origin["ssl_cert_hostname"]
            )
            assert origin["shield"] == ("iad-va-us" if origin["name"] == "flp_tick" else None)


def test_recorded_front_clock_and_write_only_dictionary_configuration() -> None:
    dictionaries = json.loads((FIXTURES / "install-front-dictionaries.json").read_text())["response"]
    assert {item["name"]: item["write_only"] for item in dictionaries} == {
        "flp_front_auth": True,
        "flp_fos_buckets": False,
        "flp_fos_credentials": True,
    }
    clock = json.loads((FIXTURES / "install-front-healthchecks.json").read_text())["response"]
    assert len(clock) == 1
    assert (clock[0]["method"], clock[0]["path"], clock[0]["host"]) == (
        "GET",
        "/_tick",
        "front.example",
    )
    assert clock[0]["expected_response"] == 200
