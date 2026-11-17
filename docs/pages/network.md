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

- `POST /api/network-health` (`NetworkHealthResponse`): request `metric` (default `health_score`), `bucket_seconds`, `top_n`, `map_asn`, `sections`, plus range fields. Sections: `health`, `summary`, `heatmap`, `buckets`, `leaderboard`, `metro_leaderboard`, `cities`, `map_buckets`, `shielding_analysis`, `quality`, `pop_health`, `transport`. The page issues three calls: core (summary, leaderboards, heatmap, buckets, cities, map buckets), map (when an ASN is selected, and always in unfiltered Cloud) and shielding. In unfiltered Cloud the shielding call waits until its panel is within 600 px of the viewport.
- `POST /api/network-quality` (`NetworkQualityResponse`): `by_country`, `by_asn`, `by_region`, `by_pop`, `scatter`; request `region_country`. Cloud-internal `quality_parts` (`throughput`, `region`, `asn_client`) with `quality_asns` returns only `{ quality_parts }`, for the summary fallback below.
- `POST` or `GET /api/network/bundle` (`NetworkBundleResponse`): health, quality, `pop_health` and `transport` (`TransportMetrics`: `tcp_rtt_p50_ms` ... `tcp_rtt_p99_ms`, `retrans_rate_pct`, `packet_loss_pct`, `delivery_rate_mbps`, `protocol_distribution`, `missing_fields`). The page uses it only for transport.
- `GET /api/network/pop-health` (`PopHealthListResponse`).

ASN names come from the PostgreSQL `asn_names` table via `enrich_asn_labels`; unresolved ASNs display as `AS<number>`. The ClickHouse reader ([`backend/high_scale/network.py`](../../backend/high_scale/network.py)) reads visible `request_facts` directly for the service and window.

## Cloud

Cloud answers unfiltered reads from the v3 summaries when it can, without loading raw Parquet: the core health call (`/api/network-health` with `summary`, `leaderboard`, `metro_leaderboard`, `heatmap` and `buckets`), the all-ASN map (`buckets`, `cities`, `map_buckets` with `map_asn` unset or `all`), `/api/network-quality`, `/api/network/pop-health`, and a `/api/network/bundle` that asks only for the core health sections, `pop_health` and `transport`. Quantiles and health scores from summaries are approximate; PoP cache hit rate is a lower bound. The transport card's requests, bytes, packet loss, delivery rate and protocol shares come exactly from the summaries' `transport` rows and its TCP RTT percentiles from their RTT histogram, so the card shows ≈; if protocols were cut to a top K, their counts are lower bounds. Summaries written before the `transport` rows existed can't answer transport. If the summaries can't answer, the read falls back to the raw scan.

The map comes from the summaries' `series5` `city` rows (the top 200 cities by requests per window and hour bundle, 50 per day bundle): requests, loss and 5xx shares are exact, coordinates are each cell's average, and RTT medians and health scores come from histograms (≈). Cities outside the top K are missing from the map. Quality comes from the summary's country, ASN, protocol and PoP rows plus its `quality` rows (average throughput, the per-country region table and the client-only ASN loss and RTT variance). Its counts, averages and shares are exact. The RTT vs TTFB scatter is a hashed sample of the summaries' rows (≈), and tables cut to a top K are lower bounds (≥). Two figures differ from raw: rows with an empty PoP fall into the `Unknown` PoP bar instead of their own, and an ASN with several AS names is one row under its smallest name.

Summaries written before the map and `quality` rows (for up to a day after an upgrade on the default range) fall back per section and read only what they lack. The map falls back to a map-only raw pass over its own columns, with no ASN, metro or core grouping. Quality keeps its summary figures and reads raw only for the missing parts, over the summaries' own range, through the `quality_parts` columns. On `route-projection.bench.test.ts`'s 24h, 40-file run (60.1 MiB of Parquet), that cuts the fallback from 51.9 MiB at page load (map 12.4, quality 17.1, shielding 22.4) to 23.8 MiB (map 10.5, quality parts 13.3); shielding's 22.4 MiB loads only when its panel is scrolled near.

Everything else reads raw Parquet in local DuckDB: any filtered read, the map for one ASN, and shielding. Shielding pairs each edge request with its shield request, which can sit in another node's summary, so it can't come from summaries. Each raw read loads only the columns its own sections use, plus filtered fields; the transport card reads about a quarter of the bytes the shared /network column list did. The 256-file and 64 MiB compressed scan limits still apply. Over them, the 413 says which scan and how much it needs, for example `Cloud /network transport over this time range needs 80.0 MiB of log data (columns: …) but the browser scan budget is 64.0 MiB; choose a shorter time range`.

## Tests

- Backend: [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py), [`tests/high_scale/test_query_service.py`](../../tests/high_scale/test_query_service.py)
- Frontend: [`frontend/__tests__/components/Map/NetworkMap.test.tsx`](../../frontend/__tests__/components/Map/NetworkMap.test.tsx)
- E2E: `Network Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
