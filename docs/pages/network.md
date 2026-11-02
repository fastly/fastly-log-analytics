# Network & ASN Health (`/network`)

Client-network quality for one service: an animated ASN/geo health map, KPI cards, edge transport metrics, PoP health, ASN and metro leaderboards, an ASN health-over-time heatmap, RTT breakdowns and shielding analysis.

Source: [`frontend/app/network/`](../../frontend/app/network/) (`page.tsx` re-exports `_sections/NetworkClient.tsx`, `NetworkLeaderboardsAndQuality.tsx`, `TransportAndShielding.tsx`, `NetworkHelp.tsx`), [`frontend/components/network/PopHealthHeatmap.tsx`](../../frontend/components/network/PopHealthHeatmap.tsx) and the map in [`frontend/components/Map/`](../../frontend/components/Map/). Built on `ReportLayout`; not SSR-seeded.

## Panels

- **Network map** with metric selector, time-bucket animation and ASN selector.
- **KPI grid**: Global Health, Avg RTT, Worst ASN, Worst Region.
- **Transport** (`/api/network/bundle`, section `transport`): TCP RTT (p50 / p95), TCP Retransmissions, Packet Loss, Delivery Rate, Transport Protocol Share. If the log format lacks the needed fields the response lists `missing_fields` and the section says so.
- **PoP health heatmap** (`GET /api/network/pop-health`).
- **ASN Health Score over Time** heatmap, **ASN Performance Leaderboard**, **Metro / Region Leaderboard**.
- **Network Quality**: Avg RTT by Country / ASN / Region (country selectable, default `US`) / POP, and an RTT vs TTFB scatter. Clicking a bar adds the matching filter.
- **Shielding Analysis** and Edge-to-Shield transit map, with a minimum-request threshold; rows with fewer than 30 requests are not graded.
- If the health response has `available: false`, the page shows "Network Metrics Unavailable" with the server's `reason`.

## Roles

| Role | Access |
|---|---|
| Admin | Full. |
| Analyst (remote share) | Read-only. `/api/network-health`, `/api/network-quality` and `/api/network/` are in the analyst POST allowlist; time window clamped to the invite; IP masking per invite. |

## Backend

Routes in [`backend/routers/network.py`](../../backend/routers/network.py), models in [`backend/models/analytics/network.py`](../../backend/models/analytics/network.py).

- `POST /api/network-health` (`NetworkHealthResponse`): request `metric` (default `health_score`), `bucket_seconds`, `top_n`, `map_asn`, `sections`, plus range fields. Sections: `health`, `summary`, `heatmap`, `buckets`, `leaderboard`, `metro_leaderboard`, `cities`, `map_buckets`, `shielding_analysis`, `quality`, `pop_health`, `transport`. The page issues three calls: core (summary, leaderboards, heatmap, buckets, cities, map buckets), map (when an ASN is selected) and shielding.
- `POST /api/network-quality` (`NetworkQualityResponse`): `by_country`, `by_asn`, `by_region`, `by_pop`, `scatter`; request `region_country`.
- `POST` or `GET /api/network/bundle` (`NetworkBundleResponse`): health, quality, `pop_health` and `transport` (`TransportMetrics`: `tcp_rtt_p50_ms` ... `tcp_rtt_p99_ms`, `retrans_rate_pct`, `packet_loss_pct`, `delivery_rate_mbps`, `protocol_distribution`, `missing_fields`). The page uses it only for transport.
- `GET /api/network/pop-health` (`PopHealthListResponse`).

ASN names come from the PostgreSQL `asn_names` table via `enrich_asn_labels`; unresolved ASNs display as `AS<number>`. Standard mode uses the `network_rtt`, `network_speed`, heatmap and geo rollups where eligible (rollup-served sections set `_approx`); a rollup-served PoP health window must start on an hour boundary, otherwise the scoped raw path is used ([AGENTS.md](../../AGENTS.md) trap 49). High-scale mode reads `request_facts` directly ([`backend/high_scale/network.py`](../../backend/high_scale/network.py)).

## Tests

- Backend: [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py), [`tests/repositories/test_rollup_reader_coverage_golden.py`](../../tests/repositories/test_rollup_reader_coverage_golden.py)
- Frontend: [`frontend/__tests__/components/Map/NetworkMap.test.tsx`](../../frontend/__tests__/components/Map/NetworkMap.test.tsx)
- E2E: `Network Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
