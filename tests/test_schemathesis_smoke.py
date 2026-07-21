"""schemathesis property-based smoke against the OpenAPI surface.

Pins the load-bearing contract for the read-only meta endpoints: a fuzz
of headers / query strings never produces a 5xx, every returned status
code is documented, and every response body matches the documented
schema.

Scope is deliberately narrow: this test only fuzzes the three read paths
in ``ALLOWED_PATHS`` (``/api/health``, ``/api/bootstrap``,
``/api/schema``). For those three it runs all three checks —
``not_a_server_error``, ``status_code_conformance``, and
``response_schema_conformance`` — against the canonical
``ErrorEnvelope`` shape for 400/401/403/404/422/429/500/502/503.

Envelope-shape conformance across ALL router operations (not just these
three) is covered separately and DB-free by the static guard in
``tests/test_error_envelope_contract.py``, which asserts every
422-documenting operation references ``ErrorEnvelope`` and every
router-served operation documents at least one canonical error code.

What this test deliberately does NOT enforce:

  * Coverage of POST / DB-backed endpoints — fuzzing analytics POSTs
    without a seeded DB just times out.

Marked ``slow`` so default ``make test`` skips it; run via
``pytest -m slow`` or wire to a nightly. Widen ``ALLOWED_PATHS`` to
cover more read-only routes — each addition runs in ~1-2s.
"""

from __future__ import annotations

import hypothesis
import pytest
import schemathesis
from schemathesis import checks

from backend.main import app

# Hypothesis settings profile: schemathesis fuzzes each endpoint with
# ~100 generated cases by default → ~5 min wall-clock for 3 endpoints.
# Cap at 20 cases per endpoint so the full test runs in <60s and fits
# under the pytest-timeout default. Bump locally with --hypothesis-profile=full
# when chasing a regression.
hypothesis.settings.register_profile(
    "schemathesis_smoke",
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=list(hypothesis.HealthCheck),
)
hypothesis.settings.load_profile("schemathesis_smoke")


# Zero-side-effect read endpoints safe to fuzz without DB / network setup.
# Session-gated endpoints (share/tos, share/heartbeat) will 401 without a
# cookie — schemathesis still validates the 401 conforms to the documented
# ErrorEnvelope schema, which is the load-bearing contract.
ALLOWED_PATHS = frozenset(
    {
        "/api/health",
        "/api/bootstrap",
        "/api/schema",
        "/api/share/auth-config",
        "/api/share/tos",
        "/api/share/heartbeat",
        "/api/admin/app-config/query-monitor",
        "/api/log-fields/catalog",
    }
)

_PATH_REGEX = "|".join(p.replace("/", r"\/") for p in ALLOWED_PATHS)

# Load + filter the schema at module import. Schemathesis' @schema.parametrize
# decorator generates one pytest case per (op, body shape) combination so the
# test surface scales with ALLOWED_PATHS.
schema = schemathesis.openapi.from_asgi("/openapi.json", app)
schema = schema.include(path_regex=rf"^({_PATH_REGEX})$")


@pytest.mark.slow
@pytest.mark.timeout(120)  # schemathesis can take ~60-90s even with the smoke profile
@schema.parametrize()
def test_safe_read_endpoints_conform(case):
    """For every generated (path × header × query × body) case:

    - ``not_a_server_error`` — no input combination produces a 5xx.
    - ``status_code_conformance`` — the returned status code is in the
      route's documented ``responses=`` mapping (router-wide default
      now covers 400/401/403/404/422/429/500/502/503).
    - ``response_schema_conformance`` — the response body validates
      against the documented schema (``ErrorEnvelope`` for 4xx/5xx,
      the per-route ``response_model`` for 2xx).
    """
    response = case.call()
    case.validate_response(
        response,
        checks=(
            checks.not_a_server_error,
            checks.status_code_conformance,
            checks.response_schema_conformance,
        ),
    )
