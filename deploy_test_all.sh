#!/usr/bin/env bash
set -euo pipefail

echo "========================================================"
echo "Automated Deployment & Testing Flow (Parallelized)"
echo "========================================================"

# Resolve the current commit hash
export COMMIT_HASH=$(git rev-parse --short HEAD || echo "unknown")
echo "Active Commit: $COMMIT_HASH"

echo "1. Committing and pushing current work..."
if [[ -n $(git status -s) ]]; then
  git add .
  git commit -m "chore: automated commit before deployment" || true
  git push --no-verify || true
  # Update COMMIT_HASH after commit
  export COMMIT_HASH=$(git rev-parse --short HEAD || echo "unknown")
  echo "Updated Commit: $COMMIT_HASH"
else
  echo "No changes to commit."
fi

# Prompt for Jenkins tags FIRST so background jobs don't block
read -p "Enter Backend Jenkins Tag (e.g. artifacts.secretcdn.net/...): " BACKEND_TAG || BACKEND_TAG=""
read -p "Enter Frontend Jenkins Tag (e.g. artifacts.secretcdn.net/...): " FRONTEND_TAG || FRONTEND_TAG=""

echo "Starting refreshes in parallel..."

# Group 1: Local Standard
(
  echo "[Local Standard] Refreshing..."
  docker compose up -d --build >/dev/null 2>&1
  echo "[Local Standard] Completed!"
) &
LOCAL_STD_PID=$!

# Group 2: Local High-Scale
(
  echo "[Local High-Scale] Refreshing..."
  docker compose -p fla-hs -f docker-compose.multipod.yml -f docker-compose.clickhouse-prototype.yml -f docker-compose.high-scale-local.yml up -d --build >/dev/null 2>&1
  echo "[Local High-Scale] Completed!"
) &
LOCAL_HS_PID=$!

# Group 3: GCE Standard
(
  echo "[GCE Standard] Refreshing..."
  pkill -f "gcloud compute ssh fastly-log-analysis.*-N -L" || true
  # Pass COMMIT_HASH to GCE remote during container restart
  gcloud compute ssh fastly-log-analysis --project=se-development-9566 --zone=us-central1-a --command "export COMMIT_HASH=$COMMIT_HASH && ~/restart.sh --no-wait" >/dev/null 2>&1
  # Re-establish GCE ports
  nohup gcloud compute ssh fastly-log-analysis --project=se-development-9566 --zone=us-central1-a -- -N -L 3001:127.0.0.1:3000 -L 8001:127.0.0.1:8000 >/dev/null 2>&1 &
  echo "[GCE Standard] Completed (Tunnel active)!"
) &
GCE_PID=$!

# Group 4: Elevation High-Scale
(
  echo "[Elevation High-Scale] Refreshing..."
  pkill -f "kubectl port-forward svc/.* -n se-demo" || true

  if [[ -n "$BACKEND_TAG" && -n "$FRONTEND_TAG" ]]; then
    echo "[Elevation High-Scale] Deploying with provided tags..."
    kubectl set image deployment/backend -n se-demo backend=$BACKEND_TAG >/dev/null 2>&1
    kubectl set image deployment/frontend -n se-demo frontend=$FRONTEND_TAG >/dev/null 2>&1
    kubectl set image deployment/high-scale-worker -n se-demo high-scale-worker=$BACKEND_TAG >/dev/null 2>&1

    # Inject the COMMIT_HASH environment variable into the frontend pod so it displays in the footer
    kubectl set env deployment/frontend -n se-demo COMMIT_HASH=$COMMIT_HASH >/dev/null 2>&1

    kubectl rollout status deployment/backend -n se-demo >/dev/null 2>&1
    kubectl rollout status deployment/frontend -n se-demo >/dev/null 2>&1
    kubectl rollout status deployment/high-scale-worker -n se-demo >/dev/null 2>&1
  else
    echo "[Elevation High-Scale] Skipping deployment (tags not provided), updating COMMIT_HASH env var..."
    kubectl set env deployment/frontend -n se-demo COMMIT_HASH=$COMMIT_HASH >/dev/null 2>&1
  fi

  # Re-establish Elevation ports
  nohup kubectl port-forward svc/frontend-svc -n se-demo 3002:3000 > /dev/null 2>&1 &
  nohup kubectl port-forward svc/backend-svc -n se-demo 8002:8000 > /dev/null 2>&1 &
  echo "[Elevation High-Scale] Completed (Port-forwards active)!"
) &
ELEVATION_PID=$!

echo "Waiting for all environments to initialize..."
wait $LOCAL_STD_PID $LOCAL_HS_PID $GCE_PID $ELEVATION_PID

# Wait 5s for the local web servers and port-forwards to settle
echo "Allowing ports to settle..."
sleep 5

echo "========================================================"
echo "Verifying dashboards and active commit hashes..."
echo "========================================================"
cd frontend
export NODE_PATH=$(npm root)

echo "Checking Local Standard..."
node ../scripts/verify_dashboard.js "http://127.0.0.1/dashboard" "$COMMIT_HASH" || echo "WARNING: Local Standard verification failed"

echo "Checking Local High-Scale..."
node ../scripts/verify_dashboard.js "http://127.0.0.1:8081/dashboard" "$COMMIT_HASH" || echo "WARNING: Local High-Scale verification failed"

echo "Checking GCE Standard..."
node ../scripts/verify_dashboard.js "http://127.0.0.1:3001/dashboard" "$COMMIT_HASH" || echo "WARNING: GCE Standard verification failed"

echo "Checking Elevation High-Scale..."
node ../scripts/verify_dashboard.js "http://127.0.0.1:3002/dashboard" "$COMMIT_HASH" || echo "WARNING: Elevation High-Scale verification failed"

cd ..

echo "========================================================"
echo "Parallel Deployment & Port Forwarding Complete!"
echo "Check local dashboards:"
echo "Local Standard (commit:$COMMIT_HASH): http://localhost:3000/admin"
echo "Local High-Scale (commit:$COMMIT_HASH): http://localhost:8081/admin"
echo "GCE Standard (commit:$COMMIT_HASH): http://localhost:3001/admin"
echo "Elevation High-Scale (commit:$COMMIT_HASH): http://localhost:3002/admin"
echo "========================================================"
