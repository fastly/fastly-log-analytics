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

# Remote connection commands are deployment-specific and belong in .env.
REMOTE_STANDARD_FORWARD_CMD="${REMOTE_STANDARD_FORWARD_CMD:-}"
REMOTE_HIGH_SCALE_FORWARD_CMD="${REMOTE_HIGH_SCALE_FORWARD_CMD:-}"

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
if ! docker compose ps --format json | grep -q "running"; then
    echo -e "${YELLOW}⚠️ Local Standard containers are not running. Starting standard stack...${RESET}"
    docker compose up -d
    echo -e "${GREEN}✅ Local Standard stack started.${RESET}"
else
    echo -e "${GREEN}✅ Local Standard stack is already running.${RESET}"
fi

# 3. Check and start Local High-Scale stack
echo -e "\n${BOLD}3. Checking Local High-Scale Docker stack...${RESET}"
if ! docker compose -p fla-hs ps --format json | grep -q "running"; then
    echo -e "${YELLOW}⚠️ Local High-Scale containers are not running. Starting high-scale stack...${RESET}"
    docker compose -p fla-hs -f docker-compose.multipod.yml -f docker-compose.clickhouse-prototype.yml -f docker-compose.high-scale-local.yml up -d
    echo -e "${GREEN}✅ Local High-Scale stack started.${RESET}"
else
    echo -e "${GREEN}✅ Local High-Scale stack is already running.${RESET}"
fi

# 4. Check and establish Remote Standard Connection (3001/8001)
echo -e "\n${BOLD}4. Checking Remote Standard Connection (3001/8001)...${RESET}"
if [ -z "${REMOTE_STANDARD_FORWARD_CMD}" ]; then
    echo -e "${YELLOW}⚠️ REMOTE_STANDARD_FORWARD_CMD is unset; skipping remote Standard forwarding.${RESET}"
elif ! lsof -i :3001 &>/dev/null && ! lsof -i :8001 &>/dev/null; then
    echo -e "${YELLOW}⚠️ Remote Standard ports 3001/8001 are not bound. Launching forwarder...${RESET}"
    eval "${REMOTE_STANDARD_FORWARD_CMD}" &
    sleep 3
    if lsof -i :3001 &>/dev/null || lsof -i :8001 &>/dev/null; then
        echo -e "${GREEN}✅ Remote Standard connection established.${RESET}"
    else
        echo -e "${RED}❌ Failed to establish Remote Standard connection.${RESET}"
    fi
else
    echo -e "${GREEN}✅ Remote Standard connection is already active.${RESET}"
fi

# 5. Check and establish Remote High-Scale Connection (3002/8002)
echo -e "\n${BOLD}5. Checking Remote High-Scale Connection (3002/8002)...${RESET}"
if [ -z "${REMOTE_HIGH_SCALE_FORWARD_CMD}" ]; then
    echo -e "${YELLOW}⚠️ REMOTE_HIGH_SCALE_FORWARD_CMD is unset; skipping remote High-Scale forwarding.${RESET}"
elif ! lsof -i :3002 &>/dev/null || ! lsof -i :8002 &>/dev/null; then
    echo -e "${YELLOW}⚠️ Remote High-Scale ports 3002/8002 are not bound. Launching forwarder...${RESET}"

    # Extract commands and run them in background if multiple forwards are combined with '&'
    if [[ "${REMOTE_HIGH_SCALE_FORWARD_CMD}" == *"&"* ]]; then
        eval "${REMOTE_HIGH_SCALE_FORWARD_CMD}" &
    else
        eval "${REMOTE_HIGH_SCALE_FORWARD_CMD}" &
    fi

    sleep 3
    if lsof -i :3002 &>/dev/null && lsof -i :8002 &>/dev/null; then
        echo -e "${GREEN}✅ Remote High-Scale connection established.${RESET}"
    else
        echo -e "${RED}❌ Failed to establish Remote High-Scale connection.${RESET}"
    fi
else
    echo -e "${GREEN}✅ Remote High-Scale connection is already active.${RESET}"
fi

# 6. Run final environment health audit
echo -e "\n${BOLD}6. Performing Environment Health Audit...${RESET}"
python3 scripts/dev/audit_environments.py || true

echo -e "\n${BOLD}${GREEN}🎉 DEV ENVIRONMENT SETUP PROCESS COMPLETE!${RESET}"
echo "=========================================="
