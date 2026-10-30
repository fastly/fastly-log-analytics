"""Preset configurations for Fastly VCL log field groups."""

from __future__ import annotations

from typing import Any


def _p(
    label: str,
    desc: str,
    groups: list[str],
    field_overrides: dict[str, bool] | None = None,
) -> dict[str, Any]:
    return {
        "label": label,
        "description": desc,
        "groups": groups,
        **({"field_overrides": field_overrides} if field_overrides is not None else {}),
    }


# fmt: off
PRESETS: dict[str, dict[str, Any]] = {
    "minimal": _p("Minimal", "Always-on fields only. Error rates, latency, hit rates, throughput.", []),
    "standard": _p('Standard', 'Recommended for most sites. Request details, infrastructure, basic geo, proxy detection.', ['A', 'C', 'D', 'I']),
    "security": _p('Security', 'Standard + TLS fingerprinting and WAF. For security monitoring.', ['A', 'C', 'D', 'H', 'I', 'J'], field_overrides={'tls_ciphers_sha': True}),
    "performance": _p('Performance', 'Standard + cache deep-dive, network quality core, and origin metrics. For delivery optimization.', ['A', 'B', 'C', 'D', 'F', 'L']),
    "streaming": _p('Streaming', 'Standard + precision geo and full network telemetry. For streaming video analysis.', ['A', 'C', 'D', 'E', 'F', 'G']),
    "full": _p('Full', 'All groups enabled. Maximum data collection.', ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M']),
}
# fmt: on
