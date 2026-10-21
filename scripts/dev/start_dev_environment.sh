#!/usr/bin/env bash
# scripts/dev/start_dev_environment.sh
# Automates the startup of Colima, local Docker stacks, and remote environment port forwards.

set -euo pipefail

# ANSI color codes for clean terminal output
BOLD="\033[1m"
GREEN="\033[32m"
YELLOW="\033[33m"
RED="\033[31m"
CYAN="\033[36m"
RESET="\033[0m"

echo -e "${BOLD}${CYAN}🚀 STARTING DEV ENVIRONMENT & PORT-FORWARDS${RESET}"
echo "=========================================="

# Load local .env variables if the file exists
if [ -f .env ]; then
    echo -e "${CYAN}ℹ️ Sourcing environment variables from local .env file...${RESET}"
    set -a
    source .env
    set +a
fi

# Load persisted admin-mTLS settings from FLA_ADMIN_DEPLOY_ENV (matching deploy_test_all.sh).
FLA_ADMIN_DEPLOY_ENV="${FLA_ADMIN_DEPLOY_ENV:-$HOME/.config/fastly-log-analytics/admin/deploy.env}"
if [ -e "$FLA_ADMIN_DEPLOY_ENV" ] || [ -L "$FLA_ADMIN_DEPLOY_ENV" ]; then
    echo -e "${CYAN}ℹ️ Loading persisted admin-mTLS settings from ${FLA_ADMIN_DEPLOY_ENV}...${RESET}"
    fla_persisted=$(python3 - "$FLA_ADMIN_DEPLOY_ENV" <<'PY' | base64
import sys
from scripts.lib.environments import load_persisted_deploy_env
for key, value in load_persisted_deploy_env(sys.argv[1]).items():
    sys.stdout.write(f"{key}\0{value}\0")
PY
    ) || { echo -e "${RED}❌ Failed to load ${FLA_ADMIN_DEPLOY_ENV}${RESET}" >&2; exit 1; }
    while IFS= read -r -d '' fla_key && IFS= read -r -d '' fla_value; do
        if [ -n "${!fla_key+x}" ]; then
            continue
        fi
        printf -v "$fla_key" '%s' "$fla_value"
        export "${fla_key?}"
    done < <(printf '%s' "$fla_persisted" | base64 -d)
    unset fla_persisted fla_key fla_value
fi

LOCAL_STANDARD_ADMIN_MTLS="${LOCAL_STANDARD_ADMIN_MTLS:-0}"
GCE_ADMIN_MTLS="${GCE_ADMIN_MTLS:-0}"
ELEVATION_ADMIN_MTLS="${ELEVATION_ADMIN_MTLS:-0}"
LOCAL_STD_ORIGIN="$(python3 -c 'from scripts.lib.admin_tls import admin_origin; print(admin_origin("local-standard") or "")' 2>/dev/null || true)"
REMOTE_STD_ORIGIN="$(python3 -c 'from scripts.lib.admin_tls import admin_origin; print(admin_origin("remote-standard") or "")' 2>/dev/null || true)"
REMOTE_HS_ORIGIN="$(python3 -c 'from scripts.lib.admin_tls import admin_origin; print(admin_origin("remote-high-scale") or "")' 2>/dev/null || true)"

LOCAL_STD_COMPOSE=(-f docker-compose.yml)
if [ "$LOCAL_STANDARD_ADMIN_MTLS" = "1" ]; then
    LOCAL_STD_COMPOSE+=(-f docker-compose.admin-mtls.yml)
fi

# Remote connection commands are deployment-specific and belong in .env.
REMOTE_STANDARD_FORWARD_CMD="${REMOTE_STANDARD_FORWARD_CMD:-}"
REMOTE_HIGH_SCALE_FORWARD_CMD="${REMOTE_HIGH_SCALE_FORWARD_CMD:-}"

# Launch each forwarder in the background without eval. The env value is split
# on whitespace (no shell quoting) and a bare `&` token separates multiple
# commands.
launch_forwarders() {
    local -a words cmd=()
    local w
    read -r -a words <<< "$1"
    for w in "${words[@]}"; do
        if [ "$w" = "&" ]; then
            if [ "${#cmd[@]}" -gt 0 ]; then
                "${cmd[@]}" &
            fi
            cmd=()
        else
            cmd+=("$w")
        fi
    done
    if [ "${#cmd[@]}" -gt 0 ]; then
        "${cmd[@]}" &
    fi
}

# Supervised kubectl forwards (readiness wait, signal cleanup, bounded restarts).
# Stays up in the background until killed; the log path is printed for triage.
launch_k8s_forwarders() {
    local log
    log="$(mktemp "${TMPDIR:-/tmp}/fla-port-forward.XXXXXX")"
    nohup python3 -m scripts.lib.port_forward --namespace "${ELEVATION_NAMESPACE}" \
        svc/frontend-svc:3002:3000 svc/backend-svc:8002:8000 >"${log}" 2>&1 &
    echo -e "${CYAN}ℹ️ port-forward supervisor pid $! (log: ${log})${RESET}"
}

wait_for_ports() {
    local deadline=$((SECONDS + $1)) port
    shift
    while [ "${SECONDS}" -lt "${deadline}" ]; do
        for port in "$@"; do
            lsof -i :"${port}" &>/dev/null || continue 2
        done
        return 0
    done
    return 1
}

# 1. Verify and start Colima (Docker Daemon)
echo -e "\n${BOLD}1. Checking Colima status...${RESET}"
if ! colima status &>/dev/null; then
    echo -e "${YELLOW}⚠️ Colima is not running. Starting Colima (4 vCPUs, 8GB RAM)...${RESET}"
    colima start --cpu 4 --memory 8
    echo -e "${GREEN}✅ Colima started successfully!${RESET}"
else
    echo -e "${GREEN}✅ Colima is already running.${RESET}"
fi

# 2. Check and start Local Standard stack
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

# 3. Check and establish Remote Standard Connection
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

# 4. Check and establish Remote High-Scale Connection
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

# 5. Run final environment health audit
echo -e "\n${BOLD}5. Performing Environment Health Audit...${RESET}"
python3 scripts/dev/audit_environments.py || true

echo -e "\n${BOLD}${GREEN}🎉 DEV ENVIRONMENT SETUP PROCESS COMPLETE!${RESET}"
echo "=========================================="
