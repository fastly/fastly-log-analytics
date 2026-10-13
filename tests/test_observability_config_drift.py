"""Observability config drift check (DRY finding S12).

``observability/prometheus.yml`` and ``observability/grafana-datasources.yml``
are the canonical source of truth. The Helm chart ships its own copies under
``deploy/chart/fastly-log-analytics/files/observability/`` so the chart is
self-contained. Two hand-maintained copies drift; this test pins them to the
canonical source (modulo one documented, deliberate templating transform).

Documented transform -- Prometheus:
  The chart copy intentionally OMITS the ``docker`` scrape job. docker-compose's
  ``docker-stats-exporter`` talks to the Docker Engine API directly, which does
  not exist in Kubernetes, so the chart drops that job (and carries an
  explanatory comment). Every other job, plus ``global`` and ``rule_files``,
  must match the canonical file exactly.

Grafana datasources: no transform -- the chart copy must equal the canonical
file field-for-field.
"""

from __future__ import annotations

import os

import yaml

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CANONICAL_DIR = os.path.join(REPO_ROOT, "observability")
CHART_DIR = os.path.join(REPO_ROOT, "deploy", "chart", "fastly-log-analytics", "files", "observability")

# Scrape jobs the chart copy is allowed to drop vs. the canonical file, with
# the reason. Anything NOT listed here must be present identically in both.
CHART_OMITTED_PROMETHEUS_JOBS = {
    "docker": ("docker-stats-exporter uses the Docker Engine API, absent in Kubernetes"),
}


def _load_yaml(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    assert isinstance(data, dict), f"expected a YAML mapping in {path}"
    return data


def test_grafana_datasources_chart_copy_matches_canonical() -> None:
    canonical = _load_yaml(os.path.join(CANONICAL_DIR, "grafana-datasources.yml"))
    chart = _load_yaml(os.path.join(CHART_DIR, "grafana-datasources.yml"))

    assert chart == canonical, (
        "deploy/chart/.../files/observability/grafana-datasources.yml drifted "
        "from the canonical observability/grafana-datasources.yml. Reconcile "
        "the chart copy to the canonical source (no transform is documented for "
        "this file)."
    )


def test_prometheus_chart_copy_matches_canonical_minus_omitted_jobs() -> None:
    canonical = _load_yaml(os.path.join(CANONICAL_DIR, "prometheus.yml"))
    chart = _load_yaml(os.path.join(CHART_DIR, "prometheus.yml"))

    # Non-scrape config (global, rule_files, ...) must match verbatim.
    canonical_rest = {k: v for k, v in canonical.items() if k != "scrape_configs"}
    chart_rest = {k: v for k, v in chart.items() if k != "scrape_configs"}
    assert canonical_rest == chart_rest, (
        "Prometheus chart copy drifted from canonical outside scrape_configs.\n"
        f"  canonical: {canonical_rest}\n  chart:     {chart_rest}"
    )

    canonical_jobs = {j["job_name"]: j for j in canonical.get("scrape_configs", [])}
    chart_jobs = {j["job_name"]: j for j in chart.get("scrape_configs", [])}

    # The chart may ONLY omit the documented jobs; no silent additions/renames.
    missing = set(canonical_jobs) - set(chart_jobs)
    assert missing == set(CHART_OMITTED_PROMETHEUS_JOBS), (
        "Prometheus chart copy omits an UNDOCUMENTED set of scrape jobs.\n"
        f"  omitted:    {sorted(missing)}\n"
        f"  documented: {sorted(CHART_OMITTED_PROMETHEUS_JOBS)}\n"
        "Reconcile the chart copy to the canonical observability/prometheus.yml "
        "or record the intentional omission in CHART_OMITTED_PROMETHEUS_JOBS."
    )
    extra = set(chart_jobs) - set(canonical_jobs)
    assert not extra, f"Prometheus chart copy has scrape jobs not in canonical: {sorted(extra)}"

    # Every job the chart DOES carry must match the canonical definition.
    for name, chart_job in chart_jobs.items():
        assert chart_job == canonical_jobs[name], (
            f"Prometheus scrape job '{name}' drifted between canonical and chart "
            f"copy.\n  canonical: {canonical_jobs[name]}\n  chart:     {chart_job}"
        )
