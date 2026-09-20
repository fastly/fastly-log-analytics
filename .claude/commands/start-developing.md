---
description: Verify environment health, active port-forwards, and matching footer commit signatures to start development cleanly.
---

Review the active environment topology and verify that everything is completely healthy and synchronized:

1. **Verify Active Listeners & Port Forwards:**
   Check what is currently listening on ports 80 (Standard Local), 8081 (High-Scale Local), 3001 (GCE Standard tunnel), and 3002 (Elevation K8s forward):
   !`lsof -nP -iTCP:80,8081,3001,3002 -sTCP:LISTEN || echo "No active port-forwards"`

2. **Run Connectivity & Tenancy Health Verification:**
   Execute standard multi-environment connectivity, health, and RBAC isolation checks:
   !`uv run python scripts/check_environment_health.py`

3. **Validate Active Commit & Architecture Signatures:**
   Check that each running environment correctly renders the active git commit hash and matching deployment architecture in its server-side footer:
   !`export COMMIT_HASH=$(git rev-parse --short=12 HEAD || echo "unknown") && cd frontend && node ../scripts/verify_dashboard.js "http://127.0.0.1/dashboard" "$COMMIT_HASH" "standard" "local" && node ../scripts/verify_dashboard.js "http://127.0.0.1:8081/dashboard" "$COMMIT_HASH" "high-scale" "local" && node ../scripts/verify_dashboard.js "http://127.0.0.1:3001/dashboard" "$COMMIT_HASH" "standard" "gce"`

Please summarize the status of each target. If any listener is inactive or a commit hash is stale, provide clear, step-by-step remediation instructions (such as starting local containers via `docker compose up -d` or starting the GCE tunnel).
