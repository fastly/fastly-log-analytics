---
title: Elevation pd-standard storage class deprecation and hd-base-ext4 migration
author: Drew
date: 2026-10-27
branch: release/v3.0.0-beta4
cost: { minutes: 29, tokens: 246968 }
id: l-ef39230d
scope: repo
status: active
src: agent
touches:
  - deploy/chart/fastly-log-analytics/values.yaml
  - deploy/chart/fastly-log-analytics/README.md
  - deploy/chart/clickhouse/values.yaml
  - scripts/load_test/k8s/postgres-pvc.yaml
  - tests/chart/test_helm.py
  - tests/deploy/test_gateway_deployment.py
---

Elevation GCP deprecated pd-standard storage classes (standard, standard-ext4, standard-xfs) and is removing standard as the cluster default. Because omitting storageClassName causes PVCs to inherit the cluster default (pd-standard), deploy/chart/fastly-log-analytics/values.yaml, deploy/chart/clickhouse/values.yaml, and scripts/load_test/k8s/postgres-pvc.yaml now explicitly set hd-base-ext4 (hyperdisk-balanced). Because hd-base-* requires volumes >= 6Gi, global.privateObservability.prometheusStorage and tempoStorage in deploy/chart/fastly-log-analytics/values.yaml were raised from 1Gi/2Gi to 6Gi.
