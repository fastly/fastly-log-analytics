"""Static OpenAPI error-envelope contract guard.

DB-free companion to ``tests/test_schemathesis_smoke.py`` (which only
fuzzes three read paths). This guard inspects the *whole* documented
surface via ``app.openapi()`` and pins two invariants that
``backend/models/errors.py`` promises ("Apply to every router"):

  (A) Every operation that documents a ``422`` does so with the canonical
      ``ErrorEnvelope`` shape — never FastAPI's default
      ``HTTPValidationError``. A router that forgets
      ``responses=DEFAULT_ERROR_RESPONSES`` regresses to the default and
      this check fails.

  (B) Every operation documents at least one of the canonical
      middleware-produced error codes
      (``400/401/403/404/429/500/502/503``) — i.e. it carries the
      ``DEFAULT_ERROR_RESPONSES`` mapping (router-wide or per-route).

Both invariants are satisfied today; a future router (or an ``@app``
route like ``/api/health``) that ships without the canonical mapping
fails here loudly, before the drift reaches the generated TS client.
"""

from __future__ import annotations

from backend.main import app

_HTTP_METHODS = ("get", "post", "put", "patch", "delete")
# The middleware/handler-produced codes that DEFAULT_ERROR_RESPONSES
# documents. 422 is deliberately excluded here — it's covered by its own
# shape check (A); this set is the "did the router get the canonical
# mapping at all" signal (B).
_CANONICAL_CODES = {"400", "401", "403", "404", "429", "500", "502", "503"}

# Operations that intentionally keep FastAPI's auto-generated
# ``HTTPValidationError`` 422 instead of the canonical ``ErrorEnvelope``.
# ``GET /api/health`` is live-fuzzed by tests/test_schemathesis_smoke.py and
# has typed query params, so its documented 422 must match the REAL
# request-validation body. There is no app-wide RequestValidationError
# reshaper, so the only accurate documentation for its 422 is
# HTTPValidationError (reshaping the body would be an out-of-scope
# wire-format change). The carve-out asserts that exact shape below so it
# can't silently mask some other wrong model.
_ACCURATE_VALIDATION_422 = {"GET    /api/health"}


def _operations():
    """Yield ``(label, operation_dict)`` for every documented HTTP op."""
    paths = app.openapi()["paths"]
    for path, item in sorted(paths.items()):
        for method, op in item.items():
            if method in _HTTP_METHODS:
                yield f"{method.upper():6} {path}", op


def _ref_of(op: dict, code: str) -> str | None:
    """The response-schema ``$ref`` for ``code``, or None if absent."""
    try:
        return op["responses"][code]["content"]["application/json"]["schema"]["$ref"]
    except (KeyError, TypeError):
        return None


def test_documented_422_is_error_envelope():
    """(A) Any operation documenting a 422 must reference ErrorEnvelope,
    except the explicitly-carved-out auto-validation endpoints."""
    offenders = [
        (label, ref.split("/")[-1])
        for label, op in _operations()
        if label not in _ACCURATE_VALIDATION_422
        and (ref := _ref_of(op, "422")) is not None
        and not ref.endswith("ErrorEnvelope")
    ]
    assert not offenders, (
        "These operations document 422 with a non-canonical model "
        "(add responses=DEFAULT_ERROR_RESPONSES to the router):\n"
        + "\n".join(f"  {label} -> {model}" for label, model in offenders)
    )


def test_carved_out_422_is_the_accurate_auto_validation_shape():
    """The carve-out is tight: each exempted op must document 422 as
    FastAPI's HTTPValidationError (the real request-validation body), not
    some other accidental model — otherwise the exemption would mask a
    genuine drift."""
    ops = dict(_operations())
    for label in _ACCURATE_VALIDATION_422:
        assert label in ops, f"carve-out names a non-existent operation: {label}"
        ref = _ref_of(ops[label], "422")
        assert ref is not None and ref.endswith("HTTPValidationError"), (
            f"{label} is carved out of the ErrorEnvelope-422 rule but its 422 "
            f"is {ref!r}, not HTTPValidationError — remove the carve-out or fix the shape."
        )


def test_every_operation_documents_a_canonical_error_code():
    """(B) Every operation documents at least one canonical error code."""
    missing = [label for label, op in _operations() if not (_CANONICAL_CODES & set(op.get("responses", {}).keys()))]
    assert not missing, (
        "These operations document none of the canonical error codes "
        f"{sorted(_CANONICAL_CODES)} (missing DEFAULT_ERROR_RESPONSES):\n"
        + "\n".join(f"  {label}" for label in missing)
    )
