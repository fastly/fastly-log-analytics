"""Bounded, non-mutating response-cache sampling with an injected request transport."""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from copy import deepcopy
from typing import Any

from scripts.lib.section_measurements import describe_response


def sample_cache_pair(
    request: Callable[[dict, float], Any],
    payload: dict,
    *,
    max_seconds: float = 120,
    max_samples: int = 4,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    if not math.isfinite(max_seconds) or not 0 < max_seconds <= 600:
        raise ValueError("max_seconds must be finite and between 0 and 600")
    if isinstance(max_samples, bool) or not 2 <= max_samples <= 10:
        raise ValueError("max_samples must be between 2 and 10")
    started = monotonic()
    deadline = started + max_seconds
    expiry_ready: dict[str, float] = {}
    samples: list[dict] = []
    components: dict[str, dict] = {}
    latest: dict[str, dict] = {}
    gaps: set[str] = set()
    for index in range(1, max_samples + 1):
        remaining = deadline - monotonic()
        if remaining <= 0:
            gaps.add("observation_deadline_exceeded")
            break
        request_started = monotonic()
        try:
            observation = describe_response(request(deepcopy(payload), remaining))
        except Exception as exc:  # noqa: BLE001 — exception text may contain credentials
            samples.append({"sample": index, "error": type(exc).__name__})
            gaps.add("request_failed")
            break
        samples.append({"sample": index, "started_seconds": request_started - started, "response": observation})
        if monotonic() > deadline:
            gaps.add("observation_deadline_exceeded")
            break
        rows = observation["components"]
        if not rows:
            gaps.add("response_components_missing")
            break
        seen = {row["component"] for row in rows}
        if components and seen != set(components):
            gaps.add("component_set_changed")
            break
        latest = {row["component"]: row for row in rows}
        for name, row in latest.items():
            entry = components.setdefault(name, {"status": "incomplete"})
            if entry["status"] == "observed" or not row["lookup_current"]:
                continue
            state = row["cache_state"]
            ttl = row["ttl_seconds"]
            expired = name in expiry_ready and request_started >= expiry_ready[name]
            if name not in expiry_ready and ttl is not None and ttl > 0:
                expiry_ready[name] = monotonic() + ttl
            if state == "bypass":
                entry["status"] = "bypass"
            elif state == "miss" and expired and not row["execution_reused"]:
                entry["status"] = "incomplete"
                entry["miss_sample"] = index
            elif state == "hit" and "miss_sample" in entry:
                entry.update(status="observed", hit_sample=index)
            elif entry["status"] == "bypass":
                entry["status"] = "incomplete"
        if all(row["status"] in {"observed", "bypass"} for row in components.values()):
            break
        pending = [name for name, row in components.items() if row["status"] == "incomplete"]
        if any("miss_sample" in components[name] and latest[name]["cache_state"] == "miss" for name in pending):
            continue
        ttls = [latest[name]["ttl_seconds"] for name in pending]
        if any(ttl is None or ttl <= 0 for ttl in ttls):
            gaps.add("cache_ttl_unknown")
        known_ttls = [ttl for ttl in ttls if ttl is not None and ttl > 0]
        if not known_ttls:
            break
        wait = max(known_ttls) + 0.01
        if wait >= deadline - monotonic():
            gaps.add("expiry_wait_exceeds_deadline")
            break
        if index < max_samples:
            sleep(wait)
    incomplete = [name for name, row in components.items() if row["status"] == "incomplete"]
    if incomplete:
        gaps.add("miss_hit_pair_unobserved")
    complete = bool(components) and not incomplete and not gaps
    applicable = any(row["status"] == "observed" for row in components.values())
    return {
        "status": "observed" if complete and applicable else "not_applicable" if complete else "incomplete",
        "components": components,
        "samples": samples,
        "gaps": sorted(gaps),
        "database_cache_state": "unknown",
    }
