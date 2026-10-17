"""SQL templates for `backend.repositories.service_stats` (``{table}`` is a pre-validated identifier)."""

from __future__ import annotations

REQUEST_ROW_STATS = "SELECT COUNT(*), MAX(timestamp) FROM {table}"

WAF_FLAG_COUNTS_7D = """
SELECT SUM(CASE WHEN waf = true THEN 1 ELSE 0 END), COUNT(*)
FROM {table}
WHERE timestamp >= now() - INTERVAL '7 days'
"""
