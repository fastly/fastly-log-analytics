"""Regression guard for the F-7 audit finding.

``generate_consolidated_snippet`` (``backend/provision/declarative/
generators.py``) short-circuits ``vcl_recv`` to its own inline copy of the
RUM Phase-3 asset-routing block instead of calling
``backend.core.fastly.rum_provisioning._generate_asset_fetch_vcl`` (or
``generate_rum_asset_fetch_vcl``) — that duplicated inline copy is the ONE
that actually ships in generated VCL for vcl_recv. The function in
rum_provisioning.py carries the explanatory docstrings (the place a future
engineer would naturally go to edit recv routing) but is dead code for that
specific concern: editing it has zero effect on production VCL, and
``tests/core/test_rum_provisioning.py`` tests it in isolation, so an edit
there passes CI and ships nothing.

This test doesn't fix that duplication (a real fix is a bigger refactor,
deliberately deferred) — it only makes sure the two copies can't silently
diverge without a test noticing.
"""

from __future__ import annotations

from backend.core.fastly.rum_provisioning import RUM_ASSET_FETCH_NAME, generate_rum_asset_fetch_vcl
from backend.provision.declarative.generators import generate_consolidated_snippet
from backend.provision.declarative.state import FeatureState


def _minimal_rum_state(*, faro_version: str | None) -> FeatureState:
    """The smallest FeatureState that exercises the RUM Phase-3 recv routing
    block with nothing else (logging/scoring/cmcd) around it, so the inline
    copy in generate_consolidated_snippet's vcl_recv branch can be isolated
    as a substring of the full output."""
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


def test_vcl_recv_inline_faro_routing_matches_rum_provisioning_module_without_faro():
    """Without a pinned Faro version: generators.py's inline recv-routing
    block (what actually ships) must be byte-identical to
    ``_generate_asset_fetch_vcl``'s output (the documented-but-dead copy)."""
    state = _minimal_rum_state(faro_version=None)
    shipped_recv = generate_consolidated_snippet(state, "vcl_recv")

    dead_copy = generate_rum_asset_fetch_vcl("iad-va-us")[RUM_ASSET_FETCH_NAME]

    assert dead_copy in shipped_recv, (
        "generators.py's inline RUM recv-routing block has diverged from "
        "backend.core.fastly.rum_provisioning's copy — update BOTH together "
        "(see the F-7 audit finding; the generators.py copy is the one that "
        "actually ships)."
    )


def test_vcl_recv_inline_faro_routing_matches_rum_provisioning_module_with_faro():
    """Same invariant, with a pinned Faro version — covers the
    /js/faro-sdk.js route + rewrite block that's spliced in when
    faro_version is set."""
    state = _minimal_rum_state(faro_version="2.9.0")
    shipped_recv = generate_consolidated_snippet(state, "vcl_recv")

    dead_copy = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")[RUM_ASSET_FETCH_NAME]

    assert dead_copy in shipped_recv, (
        "generators.py's inline RUM recv-routing block (faro_version set) "
        "has diverged from backend.core.fastly.rum_provisioning's copy — "
        "update BOTH together (F-7 audit finding)."
    )
