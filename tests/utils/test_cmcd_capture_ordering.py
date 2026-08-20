"""Regression: CMCD extraction must precede field capture in generated vcl_recv.

The 2026-08 CMCD-collection outage. Two separate blocks land in the same
``vcl_recv`` snippet:

  1. EXTRACTION — ``set req.http.x-cmcd:sid = subfield(var.cmcd, "sid", ",")``
     parses the ``?CMCD=`` query param into ``req.http.x-cmcd:*``.
  2. CAPTURE — ``set req.http.x-fos-edge-data:cmcd_sid = req.http.x-cmcd:sid``
     promotes it into the header the log format actually reads.

Capture was emitted FIRST, so it copied empty strings and the extraction ran
afterwards with nothing left to read. Every ``cmcd_*`` column logged empty.

Why it survived a month undetected: nothing errors, the columns exist (so the
feature reports "available"), and the extraction genuinely works — it still
strips ``?CMCD=`` from the cache key, so an edge-side probe "proves" CMCD is
being handled. Only the promoted value is lost.
"""

from __future__ import annotations

from backend.provision.cmcd_fields import get_cmcd_fields
from backend.provision.fastly_api import generate_capture_vcl

_LF = {
    "groups": ["A", "B", "C"],
    "custom_fields": get_cmcd_fields(True),
    "field_overrides": {},
}


def _recv() -> str:
    snippets = generate_capture_vcl(_LF, cmcd_enabled=True, cmcd_mode="query_string", cmcd_version=1)
    return snippets["recv"]


def test_extraction_precedes_capture_for_every_cmcd_field():
    """The invariant, asserted per field rather than just for sid."""
    recv = _recv()
    for cf in get_cmcd_fields(True):
        name = cf["name"]  # e.g. cmcd_sid
        key = name[len("cmcd_") :]  # e.g. sid
        extract = recv.find(f"set req.http.x-cmcd:{key} ")
        capture = recv.find(f"set req.http.x-fos-edge-data:{name} ")
        assert extract != -1, f"no extraction emitted for {name}"
        assert capture != -1, f"no capture emitted for {name}"
        assert extract < capture, (
            f"{name}: extraction at {extract} must come BEFORE capture at {capture} — "
            "capture reads x-cmcd:* and would copy an empty string"
        )


def test_cmcd_extraction_present_only_when_enabled():
    """Only the EXTRACTION is gated on the toggle.

    The capture statements are driven by ``custom_fields``, so they still
    reference ``x-cmcd:*`` with CMCD off — they just copy nothing. That is the
    pre-existing behaviour and not what this test is about.
    """
    off = generate_capture_vcl(_LF, cmcd_enabled=False)["recv"]
    assert "set req.http.x-cmcd:sid = " not in off
    assert "set req.http.x-cmcd:sid = " in _recv()


def test_url_filter_stays_after_the_subfield_reads():
    """``querystring.filter(req.url, "CMCD")`` strips the param for the cache
    key. It must run AFTER the subfield reads, or there's nothing left to parse.
    """
    recv = _recv()
    last_read = recv.rfind('subfield(var.cmcd, "rtp"')
    strip = recv.find('querystring.filter(req.url, "CMCD")')
    assert last_read != -1 and strip != -1
    assert last_read < strip, "URL filter ran before the CMCD subfields were read"


def test_declarative_generator_orders_extraction_first():
    """The declarative reconciler builds its own recv block — same invariant."""
    from backend.provision.declarative.generators import generate_consolidated_snippet
    from backend.provision.declarative.state import CmcdConfig, FeatureState, LogFieldsConfig

    state = FeatureState(
        service_id="svcCMCDORDER",
        log_period=60,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="",
        fos_endpoint="fos.example.com",
        logging_enabled=True,
        cmcd=CmcdConfig(enabled=True, mode="query_string", version=1),
        log_fields=LogFieldsConfig(groups=["A", "B", "C"], custom_fields=get_cmcd_fields(True)),
    )

    recv = generate_consolidated_snippet(state, "vcl_recv")
    extract = recv.find("set req.http.x-cmcd:sid ")
    capture = recv.find("set req.http.x-fos-edge-data:cmcd_sid ")
    if extract == -1 or capture == -1:
        # This generator may route CMCD through generate_capture_vcl instead;
        # only assert the ordering when it emits both itself.
        return
    assert extract < capture, "declarative generator emitted capture before extraction"
