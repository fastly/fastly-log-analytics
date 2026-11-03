---
title: ClickHouse packed UInt64 GroupArrayNumericImpl sessionization and top-ASN heatmap subquery
author: Drew
date: 2026-11-03
branch: v3.0.0-rc2
cost: { minutes: 30, tokens: 151976 }
id: l-a3e2b51b
scope: repo
status: active
src: agent
touches:
  - scripts/load_test/generate_synthetic_traffic.py
  - tests/scripts/test_infra_and_probe_helpers.py
  - backend/utils/error_utils.py
  - tests/utils/test_router_utils.py
  - backend/core/clickhouse/health.py
  - backend/high_scale/insights/evaluator.py
  - backend/high_scale/insights/sql_queries.py
  - backend/high_scale/network.py
  - backend/high_scale/postgres_control/control_plane.py
  - backend/high_scale/security.py
---

In backend/high_scale/sessions.py (_SESSIONS_FAST_CTE), pack event_timestamp + boolean flags into a single UInt64 so ClickHouse uses GroupArrayNumericImpl<UInt64> instead of heap-allocated GroupArrayGeneralImpl<Tuple>, and only run arraySort when intDiv(p_max_ms, 1000) - intDiv(p_min_ms, 1000) > 1800; in backend/high_scale/network.py (network_health), pre-filter heatmap to top_n ASNs via IN (SELECT ... GROUP BY asn ORDER BY count() DESC LIMIT top_n) so median() reservoirs only allocate for the top ASNs.
