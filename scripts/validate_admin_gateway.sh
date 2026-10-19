#!/usr/bin/env bash
# Explicit portable gateway validation. Never downloads or installs tools.
set -euo pipefail
for tool in caddy openssl helm uv node docker; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf 'Missing validation tool: %s; install it explicitly and retry.\n' "$tool" >&2
    exit 1
  fi
done
export ADMIN_MTLS_TEST_REQUIRED=1
uv run pytest -n 0 \
  tests/test_admin_certificates.py \
  tests/chart/test_admin_gateway.py \
  tests/deploy/test_gateway_deployment.py \
  tests/deploy/test_admin_tls_client.py \
  tests/deploy/test_readiness.py \
  tests/deploy/test_public_gateway_runtime.py
node --test tests/deploy/admin_gateway_client.test.cjs
helm lint deploy/chart/admin-gateway \
  --set host=admin.example.com --set existingSecret=operator-gateway \
  --set frontendUpstream=frontend:3000 --set backendUpstream=backend:8000 \
  --set image=registry.example.com/fla-admin-gateway:validate
