"""Forward-looking guard: a path removal can't ship without a CHANGELOG note.

ADR-12 §2.2 classifies endpoint removals as breaking and §5 requires they be
logged. This test makes that mechanical:

  * ``tests/contract/openapi_baseline.json`` is a frozen snapshot of the
    public 2.0.0 OpenAPI surface.
  * Every run diffs the live ``app.openapi()`` paths against that baseline.
    For each path present in the baseline but ABSENT now (a removal), the
    path must appear in the ``### Breaking`` section of ``CHANGELOG.md``.

It starts GREEN: the baseline equals the current surface, so the removed set
is empty. It only fires when someone deletes (or renames) a path without
recording it.

**Bumping the baseline is a deliberate release act.** When you intentionally
remove an endpoint, (1) add a bullet to the CHANGELOG ``### Breaking`` section
naming the path, then (2) regenerate the baseline from the new surface
(``cp frontend/openapi.json tests/contract/openapi_baseline.json``) as part of
cutting the release. Do NOT bump it casually to silence this test.

Scope (v1): path-level removal detection only. Per-path HTTP-method changes
(e.g. a GET→POST migration, or dropping a PATCH alias) are not auto-diffed
here — those are covered by review + the OpenAPI snapshot test. The CHANGELOG
already records the 2.0.0 method changes.
"""

from __future__ import annotations

import json
import os
import re

from backend.main import app

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_BASELINE_PATH = os.path.join(_REPO_ROOT, "tests", "contract", "openapi_baseline.json")
_CHANGELOG_PATH = os.path.join(_REPO_ROOT, "CHANGELOG.md")


def _normalize_params(text: str) -> str:
    """Collapse ``{anything}`` path params to ``{}`` so a path matches the
    CHANGELOG regardless of which param name each side spells (the spec uses
    ``{service_id}``; prose often writes ``{id}``)."""
    return re.sub(r"\{[^}]+\}", "{}", text)


def _changelog_breaking_section() -> str:
    """Return the text of the most-recent ``### Breaking`` section — from the
    first ``### Breaking`` heading to the next top-level ``## `` version
    heading (or EOF)."""
    with open(_CHANGELOG_PATH) as f:
        text = f.read()
    start = text.find("### Breaking")
    if start == -1:
        return ""
    rest = text[start:]
    # Stop at the next version heading (lines beginning with "## ").
    end = re.search(r"^## ", rest[len("### Breaking") :], flags=re.MULTILINE)
    return rest if end is None else rest[: len("### Breaking") + end.start()]


def test_removed_paths_are_noted_in_changelog():
    with open(_BASELINE_PATH) as f:
        baseline = json.load(f)
    live = app.openapi()

    baseline_paths = set(baseline.get("paths", {}))
    live_paths = set(live.get("paths", {}))
    removed = sorted(baseline_paths - live_paths)

    breaking = _normalize_params(_changelog_breaking_section())
    missing = [p for p in removed if _normalize_params(p) not in breaking]

    assert not missing, (
        "These paths were removed from the OpenAPI surface but are not noted in "
        f"the CHANGELOG ### Breaking section: {missing}\n"
        "Add a Breaking bullet naming each path, then regenerate the baseline "
        "(cp frontend/openapi.json tests/contract/openapi_baseline.json) as a "
        "deliberate release step."
    )
