#!/usr/bin/env bash
# scripts/dev/start_dev_environment.sh
# Automates the startup of Colima, local Docker stacks, and remote environment port forwards.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/lib/gce_ssh.sh
source "$REPO_ROOT/scripts/lib/gce_ssh.sh"
init_dev_colors

BOLD="\033[1m"; GREEN="\033[32m"; YELLOW="\033[33m"; RED="\033[31m"; CYAN="\033[36m"; RESET="\033[0m"

echo -e "${BOLD}${CYAN}🚀 STARTING DEV ENVIRONMENT & PORT-FORWARDS${RESET}"
echo "=========================================="

load_repo_dotenv "$REPO_ROOT"
load_persisted_admin_mtls_env || { echo -e "${RED}❌ Failed to load persisted admin-mTLS settings${RESET}" >&2; exit 1; }

LOCAL_STANDARD_ADMIN_MTLS="${LOCAL_STANDARD_ADMIN_MTLS:-0}"
GCE_ADMIN_MTLS="${GCE_ADMIN_MTLS:-0}"
ELEVATION_ADMIN_MTLS="${ELEVATION_ADMIN_MTLS:-0}"
LOCAL_STD_ORIGIN="$(python3 -c 'from scripts.lib.admin_tls import admin_origin; print(admin_origin("local-standard") or "")' 2>/dev/null || true)"
REMOTE_STD_ORIGIN="$(python3 -c 'from scripts.lib.admin_tls import admin_origin; print(admin_origin("remote-standard") or "")' 2>/dev/null || true)"
REMOTE_HS_ORIGIN="$(python3 -c 'from scripts.lib.admin_tls import admin_origin; print(admin_origin("remote-high-scale") or "")' 2>/dev/null || true)"

LOCAL_STD_COMPOSE=(-f docker-compose.yml)
[ "$LOCAL_STANDARD_ADMIN_MTLS" = "1" ] && LOCAL_STD_COMPOSE+=(-f docker-compose.admin-mtls.yml)

REMOTE_STANDARD_FORWARD_CMD="${REMOTE_STANDARD_FORWARD_CMD:-}"
REMOTE_HIGH_SCALE_FORWARD_CMD="${REMOTE_HIGH_SCALE_FORWARD_CMD:-}"

launch_k8s_forwarders() {
    local log
    log="$(mktemp "${TMPDIR:-/tmp}/fla-port-forward.XXXXXX")"
    nohup python3 -m scripts.lib.port_forward --namespace "${ELEVATION_NAMESPACE}" \
        svc/frontend-svc:3002:3000 svc/backend-svc:8002:8000 >"${log}" 2>&1 &
    echo -e "${CYAN}ℹ️ port-forward supervisor pid $! (log: ${log})${RESET}"
}

echo -e "\n${BOLD}1. Checking Colima status...${RESET}"
if ! colima status &>/dev/null; then
    echo -e "${YELLOW}⚠️ Colima is not running. Starting Colima (4 vCPUs, 8GB RAM)...${RESET}"
    colima start --cpu 4 --memory 8
    echo -e "${GREEN}✅ Colima started successfully!${RESET}"
else
    echo -e "${GREEN}✅ Colima is already running.${RESET}"
fi

echo -e "\n${BOLD}2. Checking Local Standard Docker stack...${RESET}"
if ! docker compose "${LOCAL_STD_COMPOSE[@]}" ps --format json | grep -q "running"; then
    echo -e "${YELLOW}⚠️ Local Standard containers are not running. Starting standard stack...${RESET}"
    docker compose "${LOCAL_STD_COMPOSE[@]}" up -d
    echo -e "${GREEN}✅ Local Standard stack started.${RESET}"
elif [ "${LOCAL_STANDARD_ADMIN_MTLS}" = "1" ] && ! docker compose "${LOCAL_STD_COMPOSE[@]}" ps admin-gateway --format json 2>/dev/null | grep -q "running"; then
    echo -e "${YELLOW}⚠️ Local Admin mTLS gateway is not running. Starting admin-gateway...${RESET}"
    docker compose "${LOCAL_STD_COMPOSE[@]}" up -d admin-gateway
    echo -e "${GREEN}✅ Local Admin mTLS gateway started.${RESET}"
else
    echo -e "${GREEN}✅ Local Standard stack is already running.${RESET}"
fi

echo -e "\n${BOLD}3. Checking Remote Standard Connection...${RESET}"
if [ -n "${REMOTE_STANDARD_FORWARD_CMD}" ]; then
    if ! lsof -i :3001 &>/dev/null && ! lsof -i :8001 &>/dev/null; then
        echo -e "${YELLOW}⚠️ Remote Standard ports 3001/8001 are not bound. Launching forwarder...${RESET}"
        launch_forwarders "${REMOTE_STANDARD_FORWARD_CMD}"
        sleep 3
        if lsof -i :3001 &>/dev/null || lsof -i :8001 &>/dev/null; then
            echo -e "${GREEN}✅ Remote Standard connection established.${RESET}"
        else
            echo -e "${RED}❌ Failed to establish Remote Standard connection.${RESET}"
        fi
    else
        echo -e "${GREEN}✅ Remote Standard connection (3001/8001) is already active.${RESET}"
    fi
elif [ "${GCE_ADMIN_MTLS}" = "1" ] && [ -n "${REMOTE_STD_ORIGIN}" ]; then
    echo -e "${GREEN}✅ Remote Standard configured via Admin mTLS gateway (${REMOTE_STD_ORIGIN}).${RESET}"
else
    echo -e "${YELLOW}⚠️ REMOTE_STANDARD_FORWARD_CMD is unset; skipping remote Standard forwarding.${RESET}"
fi

echo -e "\n${BOLD}4. Checking Remote High-Scale Connection...${RESET}"
if [ -n "${REMOTE_HIGH_SCALE_FORWARD_CMD}" ] || [ -n "${ELEVATION_NAMESPACE:-}" ]; then
    if ! lsof -i :3002 &>/dev/null || ! lsof -i :8002 &>/dev/null; then
        echo -e "${YELLOW}⚠️ Remote High-Scale ports 3002/8002 are not bound. Launching forwarder...${RESET}"
        if [ -n "${REMOTE_HIGH_SCALE_FORWARD_CMD}" ]; then
            launch_forwarders "${REMOTE_HIGH_SCALE_FORWARD_CMD}"
        else
            launch_k8s_forwarders
        fi
        if wait_for_ports 30 3002 8002; then
            echo -e "${GREEN}✅ Remote High-Scale connection established.${RESET}"
        else
            echo -e "${RED}❌ Failed to establish Remote High-Scale connection.${RESET}"
        fi
    else
        echo -e "${GREEN}✅ Remote High-Scale connection (3002/8002) is already active.${RESET}"
    fi
elif [ "${ELEVATION_ADMIN_MTLS}" = "1" ] && [ -n "${REMOTE_HS_ORIGIN}" ]; then
    echo -e "${GREEN}✅ Remote High-Scale configured via Admin mTLS gateway (${REMOTE_HS_ORIGIN}).${RESET}"
else
    echo -e "${YELLOW}⚠️ REMOTE_HIGH_SCALE_FORWARD_CMD and ELEVATION_NAMESPACE are unset; skipping remote High-Scale forwarding.${RESET}"
fi

echo -e "\n${BOLD}5. Performing Environment Health Audit...${RESET}"
python3 scripts/dev/audit_environments.py || true

echo -e "\n${BOLD}${GREEN}🎉 DEV ENVIRONMENT SETUP PROCESS COMPLETE!${RESET}"
echo "=========================================="
