#!/bin/bash
set -e

echo "========================================"
echo "1) Prompt for Jenkins Image Tags"
echo "========================================"
FRONTEND_TAG=$1
BACKEND_TAG=$2

if [ -z "$FRONTEND_TAG" ]; then
    read -p "Enter the new FRONTEND image tag from Jenkins: " FRONTEND_TAG
fi
if [ -z "$BACKEND_TAG" ]; then
    read -p "Enter the new BACKEND image tag from Jenkins: " BACKEND_TAG
fi

if [ -z "$FRONTEND_TAG" ] || [ -z "$BACKEND_TAG" ]; then
    echo "Tags cannot be empty!"
    exit 1
fi

echo "========================================"
echo "2) Commit and push code iteration"
echo "========================================"
git add .
git commit -m "Automated deployment and testing iteration" || echo "No changes to commit"
git push

echo "========================================"
echo "3) Local: Rebuild Docker, re-establish ports, verify"
echo "========================================"
# Check if Colima is running (macOS specific for docker)
if ! docker info >/dev/null 2>&1; then
    echo "Docker daemon not running. Attempting to start colima..."
    colima start || echo "Failed to start colima, docker commands may fail."
fi

# Run docker rebuild
echo "Running docker compose down and up --build -d"
docker compose down
docker compose up --build -d

# Wait for local docker to start
echo "Waiting 15s for local containers to initialize..."
sleep 15

# Verify local (Note: Local tests against port 80 via Caddy, not 3000)
echo "Verifying local environment on http://localhost..."
cd frontend
export NODE_PATH=$(npm root)
node ../scripts/verify_dashboard.js "http://127.0.0.1/dashboard" || echo "WARNING: Local check failed"
cd ..

echo "========================================"
echo "4) GCE: Kill forwards, restart, re-establish, verify"
echo "========================================"
# Kill forwards for 3001
lsof -ti:3001 | xargs kill -9 2>/dev/null || true

echo "Restarting GCE environment..."
gcloud compute ssh fastly-log-analysis --project=se-development-9566 --zone=us-central1-a --command='~/restart.sh'

echo "Re-establishing SSH tunnel to GCE (3001)..."
gcloud compute ssh fastly-log-analysis --project=se-development-9566 --zone=us-central1-a -- -N -L 3001:127.0.0.1:3000 -L 8001:127.0.0.1:8000 &
GCE_SSH_PID=$!

echo "Waiting 15s for tunnel and GCE containers..."
sleep 15
cd frontend
export NODE_PATH=$(npm root)
node ../scripts/verify_dashboard.js "http://127.0.0.1:3001/dashboard" || echo "WARNING: GCE check failed"
cd ..

echo "========================================"
echo "5) Elevation: Deploy new images, re-establish, verify"
echo "========================================"
# Kill forwards for 3002
lsof -ti:3002 | xargs kill -9 2>/dev/null || true

echo "Fetching current helm values..."
helm get values fastly-log-analytics -n se-demo -o yaml > /tmp/current-values.yaml

echo "Updating image tags to $FRONTEND_TAG and $BACKEND_TAG..."
python3 scripts/update_helm_values.py "$FRONTEND_TAG" "$BACKEND_TAG"

echo "Deploying to Elevation cluster..."
helm upgrade fastly-log-analytics oci://artifacts.secretcdn.net/fastly-helm/standard-chart \
  --namespace se-demo \
  --set metadata.provider=gcp \
  -f /tmp/current-values-updated.yaml

echo "Waiting for rollout to complete..."
kubectl rollout status deployment/backend -n se-demo --timeout=180s
kubectl rollout status deployment/frontend -n se-demo --timeout=180s

echo "Re-establishing port forward to Elevation (3002)..."
kubectl port-forward svc/frontend-svc -n se-demo 3002:3000 &
ELEVATION_FWD_PID=$!

echo "Waiting 10s for port forward..."
sleep 10
cd frontend
export NODE_PATH=$(npm root)
node ../scripts/verify_dashboard.js "http://127.0.0.1:3002/dashboard" || echo "WARNING: Elevation check failed"
cd ..

echo "========================================"
echo "Deployment and verification complete."
echo "Active Tunnels:"
echo "GCE SSH PID: $GCE_SSH_PID"
echo "Elevation Port-Forward PID: $ELEVATION_FWD_PID"
echo "========================================"
