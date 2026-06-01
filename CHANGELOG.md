# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-06-01

Initial public release. Self-hosted dashboard for searching, filtering, and visualizing request-level Fastly logs streamed to Fastly Object Storage.

### Highlights

- **Apache Iceberg data lake** in Fastly Object Storage — ACID-compliant log storage, safe for concurrent readers and writers, with automated compaction and snapshot expiration.
- **Automated provisioning** — guided wizard (and equivalent `backend/provision.py` CLI) creates the FOS bucket, scoped access key, CDN-fronting Fastly Delivery service, and the logging endpoint on your VCL service. Auto-rollback on failure.
- **Crash-safe ingestion** — buffered locally, atomically committed; interrupted imports never corrupt the table.
- **CDN-accelerated reads** — every FOS data read goes through a Fastly Delivery service for free egress and edge caching.
- **Multi-source support** — analyze logs from multiple Fastly services side by side, each with its own DuckDB engine and Iceberg table.
- **Interactive dashboards** — traffic over time, global request map, top-N aggregations across every dimension, paginated raw-log viewer with click-to-filter.
- **Insights** — automated anomaly detection for error spikes, regional traffic surges, new IPs, WAF signal changes, cache efficiency collapses, and latency regressions.
- **Usage & Cost** — live storage breakdown, FOS Class A / B operation counts, period totals, and an interactive cost estimator pre-filled from your traffic stats.
- **Log-line accounting** — reconciles Fastly's authoritative `/stats/service/{id}` counter against locally-ingested rows bucket-by-bucket and surfaces sustained pipeline loss.
- **Configurable log fields** — thirteen built-in field groups (HTTP, network, geo, TLS, NGWAF, QUIC/HTTP3, origin metrics, etc.) plus arbitrary custom VCL fields with auto-generated Edge Data Capture snippets.
- **Alerts** — threshold-based, webhook-delivered, with optional comparison-period evaluation and per-status-code scope.
- **Two collaboration modes** — invite analysts to run an independent copy (durable JSON-config join with read-only FOS credentials), or share your running instance live via three sharing modes: SSH reverse tunnel via localhost.run, your own hostname, or your own public IP. Per-analyst passcode invites, optional IP allowlist, optional expiry, and instant single-invite or sever-all revoke. Per-mode trust-model trade-offs are documented in [SECURITY.md](SECURITY.md#live-dashboard-sharing--trust-model).
- **Field-size guard** — warns when your selected log fields approach Fastly's ~8 KB log-format limit.

See [docs/features.md](docs/features.md) for the full feature reference.

[1.0.0]: https://github.com/fastly/fastly-log-analytics/releases/tag/1.0.0
