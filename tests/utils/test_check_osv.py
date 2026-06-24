"""Tests for ``scripts/check_osv.py`` severity derivation.

Regression for the CRITICAL-gate fail-open: an advisory whose severity lived
only in the canonical top-level ``severity`` CVSS array (not in
``database_specific``) used to bucket as UNKNOWN and pass the gate.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_osv.py"


@pytest.fixture(scope="module")
def osv():
    spec = importlib.util.spec_from_file_location("check_osv", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_osv"] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "vector,expected",
    [
        # Log4Shell (CVE-2021-44228) — the canonical 10.0 CRITICAL.
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H", 10.0),
        # A textbook HIGH (7.5): network, no priv, availability impact only.
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:N/A:H", 7.5),
        # A MEDIUM (6.1): reflected XSS shape, scope changed.
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:R/S:C/C:L/I:L/A:N", 6.1),
        # A LOW (3.1).
        ("CVSS:3.1/AV:N/AC:H/PR:L/UI:N/S:U/C:L/I:N/A:N", 3.1),
    ],
)
def test_cvss3_base_score_known_vectors(osv, vector, expected):
    assert osv.cvss3_base_score(vector) == pytest.approx(expected, abs=0.05)


def test_cvss3_base_score_rejects_v4_vector(osv):
    # CVSS_V4 uses a different formula we do not implement → None.
    assert osv.cvss3_base_score("CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N") is None


def test_severity_from_cvss_array_when_database_specific_absent(osv):
    """The fail-open regression: CRITICAL severity carried ONLY in the CVSS
    array (no database_specific) must still resolve to CRITICAL."""
    vuln = {
        "id": "CVE-2021-44228",
        "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H"}],
    }
    assert osv.severity_for_vuln(vuln) == "CRITICAL"


def test_severity_takes_max_across_sources(osv):
    """database_specific says LOW but the CVSS vector says CRITICAL → CRITICAL."""
    vuln = {
        "id": "X",
        "database_specific": {"severity": "LOW"},
        "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H"}],
    }
    assert osv.severity_for_vuln(vuln) == "CRITICAL"


def test_severity_database_specific_still_honored(osv):
    vuln = {"id": "X", "database_specific": {"severity": "critical"}}
    assert osv.severity_for_vuln(vuln) == "CRITICAL"


def test_severity_unknown_when_no_signal(osv):
    assert osv.severity_for_vuln({"id": "X"}) == "UNKNOWN"


def test_unparsed_v4_only_vector_is_unknown_not_crash(osv):
    vuln = {
        "id": "CVE-2099-0001",
        "severity": [{"type": "CVSS_V4", "score": "CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N"}],
    }
    # No qualitative source and an unparseable vector → UNKNOWN (surfaced via stderr).
    assert osv.severity_for_vuln(vuln) == "UNKNOWN"
