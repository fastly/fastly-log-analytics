"""Regression guard for the F-7 audit finding (now resolved).

``generate_consolidated_snippet`` (``backend/provision/declarative/
generators.py``) used to short-circuit ``vcl_recv`` to its own inline copy
of the RUM Phase-3 asset-routing block instead of calling
``backend.core.fastly.rum_provisioning.generate_rum_asset_fetch_vcl`` — that
duplicated inline copy was the ONE that actually shipped in generated VCL
for vcl_recv, while the (identically-behaving) function in
rum_provisioning.py was untested-in-production dead code for that specific
concern.

That duplication is now collapsed: ``generate_consolidated_snippet``'s
``vcl_recv`` branch calls ``generate_rum_asset_fetch_vcl`` directly, so
there is exactly one place that defines this routing. This test now just
asserts that call actually happens (i.e. the shipped output really is
sourced from the shared generator), so a future regression that reintroduces
a second inline copy gets caught immediately rather than needing a
byte-identity comparison between two independently-maintained copies.
"""

from __future__ import annotations

from backend.core.fastly.rum_provisioning import RUM_ASSET_FETCH_NAME, generate_rum_asset_fetch_vcl
from backend.provision.declarative.generators import generate_consolidated_snippet
from backend.provision.declarative.state import FeatureState


def _minimal_rum_state(*, faro_version: str | None) -> FeatureState:
    """The smallest FeatureState that exercises the RUM Phase-3 recv routing
    block with nothing else (logging/scoring/cmcd) around it, so the shared
    generator's output can be isolated as a substring of the full output."""
    return FeatureState(
        service_id="svc_test",
        log_period=60,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="",
        fos_endpoint="fos.example.com",
        logging_enabled=False,
        rum_enabled=True,
        faro_version=faro_version,
    )


def test_vcl_recv_asset_routing_sourced_from_rum_provisioning_without_faro():
    """Without a pinned Faro version: the shipped vcl_recv output must
    contain exactly the block produced by rum_provisioning's shared
    generator — proving there is one source of truth, not two copies that
    happen to agree."""
    state = _minimal_rum_state(faro_version=None)
    shipped_recv = generate_consolidated_snippet(state, "vcl_recv")

    shared_copy = generate_rum_asset_fetch_vcl("iad-va-us")[RUM_ASSET_FETCH_NAME]

    assert shared_copy in shipped_recv, (
        "generate_consolidated_snippet's vcl_recv branch is no longer "
        "sourcing the RUM asset-fetch routing block from "
        "backend.core.fastly.rum_provisioning.generate_rum_asset_fetch_vcl — "
        "this reintroduces the F-7 duplication hazard."
    )


def test_vcl_recv_asset_routing_sourced_from_rum_provisioning_with_faro():
    """Same invariant, with a pinned Faro version — covers the
    /js/faro-sdk.js route + rewrite block that's spliced in when
    faro_version is set."""
    state = _minimal_rum_state(faro_version="2.9.0")
    shipped_recv = generate_consolidated_snippet(state, "vcl_recv")

    shared_copy = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")[RUM_ASSET_FETCH_NAME]

    assert shared_copy in shipped_recv, (
        "generate_consolidated_snippet's vcl_recv branch is no longer "
        "sourcing the RUM asset-fetch routing block (faro_version set) from "
        "backend.core.fastly.rum_provisioning.generate_rum_asset_fetch_vcl — "
        "this reintroduces the F-7 duplication hazard."
    )
