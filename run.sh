#!/usr/bin/env bash

# run.sh - Starts the FastAPI backend and Next.js frontend.
# Usage: ./run.sh [--dev] [--no-reload] [--kill] [--refresh] [--no-refresh]
#                 [--frontend-port PORT] [--backend-port PORT]

# If the user has a virtualenv activated in their shell, it might point to a
# different path (e.g. if the directory was renamed). We unset it here so that
# uv can correctly discover the project-local .venv without warnings.
unset VIRTUAL_ENV

# Load environment variables from .env if it exists, so they can set defaults
if [ -f ".env" ]; then
    set -a
    source .env
    set +a
fi

DEV_MODE=false
NO_RELOAD=false
KILL_EXISTING=false
FORCE_REFRESH=false
SKIP_REFRESH=false
FRONTEND_PORT=${FRONTEND_PORT:-3000}
BACKEND_PORT=${BACKEND_PORT:-8000}
LOCAL_REFRESH_MAX_AGE_HOURS=${LOCAL_REFRESH_MAX_AGE_HOURS:-24}

while [ $# -gt 0 ]; do
    case "$1" in
        --dev) DEV_MODE=true ;;
        --no-reload) NO_RELOAD=true ;;
        --kill) KILL_EXISTING=true ;;
        --refresh) FORCE_REFRESH=true ;;
        --no-refresh) SKIP_REFRESH=true ;;
        --frontend-port) FRONTEND_PORT="$2"; shift ;;
        --backend-port) BACKEND_PORT="$2"; shift ;;
    esac
    shift
done

# Refuse to take ports that are commonly used by an SSH tunnel forwarding
# a remote backend/frontend (8000 backend, 3001 frontend). Trampling these
# silently routes local requests to the wrong backend and looks like a
# perfectly normal-running app.
for RESERVED in 8000 3001; do
    if [ "$BACKEND_PORT" = "$RESERVED" ] || [ "$FRONTEND_PORT" = "$RESERVED" ]; then
        echo "[!] Refusing to bind port $RESERVED — commonly used by an SSH tunnel to a remote backend/frontend."
        echo "    Override --backend-port / --frontend-port, or set BACKEND_PORT / FRONTEND_PORT in .env."
        exit 1
    fi
done

# Compose NODE_OPTIONS: keep --dns-result-order=ipv4first (required by next
# dev to avoid IPv6 ::1 binding) and append whatever the env adds. Allows a
# .env --max-old-space-size cap to actually reach node, instead of being
# clobbered by the hardcoded NODE_OPTIONS in the npm invocations below.
COMPOSED_NODE_OPTIONS="--dns-result-order=ipv4first ${NODE_OPTIONS_EXTRA:-} ${NODE_OPTIONS:-}"

# Function to safely kill existing processes on the specific ports we use
cleanup_existing() {
    echo "Checking for existing processes on ports $FRONTEND_PORT and $BACKEND_PORT..."
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

    # Kill ALL uvicorn/node processes from this project directory first.
    # Port-only kills miss the reloader process (which doesn't listen on the port
    # itself) — killing just the worker causes the reloader to spawn a new one.
    pkill -9 -f "$SCRIPT_DIR.*uvicorn" 2>/dev/null || true
    pkill -9 -f "$SCRIPT_DIR.*node.*next" 2>/dev/null || true

    # Also catch anything still holding the ports (belt-and-suspenders)
    for PORT in $FRONTEND_PORT $BACKEND_PORT; do
        PIDS=$(lsof -Pi :$PORT -sTCP:LISTEN -t 2>/dev/null)
        if [ -n "$PIDS" ]; then
            echo "Killing remaining process on port $PORT..."
            echo "$PIDS" | xargs kill -9 2>/dev/null || true
        fi
    done

    # Brief pause so the OS releases the ports before we try to bind again
    sleep 1
}

# Function to clean up the spawned background processes when the user presses Ctrl+C
cleanup() {
    echo -e "\nStopping all services..."
    
    # 1. Ask nicely (SIGTERM to the process groups)
    if [ -n "$BACKEND_PGID" ]; then kill -TERM -$BACKEND_PGID 2>/dev/null; fi
    if [ -n "$FRONTEND_PGID" ]; then kill -TERM -$FRONTEND_PGID 2>/dev/null; fi
    kill -TERM $BACKEND_PID $FRONTEND_PID 2>/dev/null
    
    # 2. Give them a second to clean up
    sleep 1
    
    # 3. Force kill (SIGKILL) if they are stubborn
    if [ -n "$BACKEND_PGID" ]; then kill -9 -$BACKEND_PGID 2>/dev/null; fi
    if [ -n "$FRONTEND_PGID" ]; then kill -9 -$FRONTEND_PGID 2>/dev/null; fi
    kill -9 $BACKEND_PID $FRONTEND_PID 2>/dev/null
    
    # 4. Nuclear option: Mop up any stray processes spawned in this directory
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    pkill -9 -f "$SCRIPT_DIR.*uvicorn" 2>/dev/null || true
    pkill -9 -f "$SCRIPT_DIR.*node.*next" 2>/dev/null || true
    
    exit 0
}

trap cleanup SIGINT SIGTERM

if [ "$KILL_EXISTING" = true ]; then
    cleanup_existing
fi

echo "=================================================="
echo "Syncing backend dependencies..."
uv sync
echo "=================================================="

echo "=================================================="
if [ "$DEV_MODE" = true ]; then
    echo "Starting Backend (DEV mode with reload on port $BACKEND_PORT)..."
else
    echo "Starting Backend (PRODUCTION mode on port $BACKEND_PORT)..."
fi
echo "=================================================="

set -m
if [ "$DEV_MODE" = true ] && [ "$NO_RELOAD" = false ]; then
    # In dev mode with reload, we explicitly exclude large directories to
    # prevent the watcher from hanging or causing reload loops.
    uv run uvicorn backend.main:app --host 127.0.0.1 --port $BACKEND_PORT --reload \
        --reload-exclude "cache" \
        --reload-exclude "data" \
        --reload-exclude "frontend" \
        --reload-exclude "node_modules" \
        --reload-exclude ".venv" \
        --reload-exclude ".git" \
        --reload-exclude "*.duckdb*" \
        --reload-exclude "*.db-wal" \
        --reload-exclude "*.db-shm" \
        --reload-exclude "*.sqlite*" \
        --reload-exclude "*.log" \
        --reload-exclude ".aider*" \
        --reload-exclude ".mypy_cache" \
        --reload-exclude ".ruff_cache" \
        --reload-exclude ".pytest_cache" \
        --reload-exclude ".hypothesis" \
        --reload-exclude "__pycache__" &
else
    # --no-reload (or non-dev) skips uvicorn's --reload flag entirely. Avoids
    # the watchfiles thrashing seen when sqlite WAL pulses or other
    # frequently-touched files leak past the reload-exclude patterns.
    uv run uvicorn backend.main:app --host 127.0.0.1 --port $BACKEND_PORT &
fi
BACKEND_PID=$!
BACKEND_PGID=$(ps -o pgid= -p $BACKEND_PID | tr -d ' ')
set +m

echo -n "Waiting for Backend to initialize."
MAX_ATTEMPTS=60
ATTEMPT=1
while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    # Check if backend is still running
    if ! kill -0 $BACKEND_PID 2>/dev/null; then
        echo -e "\n[!] Error: Backend process died early. Check logs above."
        exit 1
    fi

    if curl -s -f http://127.0.0.1:$BACKEND_PORT/api/health > /dev/null; then
        echo " Ready!"
        break
    fi
    echo -n "."
    sleep 1
    ATTEMPT=$((ATTEMPT + 1))
done

if [ $ATTEMPT -gt $MAX_ATTEMPTS ]; then
    echo -e "\n[!] Error: Backend failed to become healthy after $MAX_ATTEMPTS seconds. Exiting."
    echo "Check for port conflicts or backend errors above."
    if [ -n "$BACKEND_PGID" ]; then kill -9 -$BACKEND_PGID 2>/dev/null; fi
    exit 1
fi

echo "=================================================="
if [ "$DEV_MODE" = true ]; then
    echo "Starting Frontend (DEV mode on port $FRONTEND_PORT)..."
else
    echo "Starting Frontend (PRODUCTION mode on port $FRONTEND_PORT)..."
fi
echo "=================================================="

cd frontend || { echo "Failed to enter frontend directory!"; exit 1; }

echo "Syncing frontend dependencies..."
npm install

echo "Syncing API types..."
if ! npm run gen:types; then
    echo -e "\n[!] Error: Type generation failed. Exiting run script."
    if [ -n "$BACKEND_PGID" ]; then kill -TERM -$BACKEND_PGID 2>/dev/null; fi
    exit 1
fi

set -m
if [ "$DEV_MODE" = true ]; then
    NEXT_PUBLIC_BACKEND_PORT=$BACKEND_PORT API_PROXY_URL=http://127.0.0.1:$BACKEND_PORT PORT=$FRONTEND_PORT NODE_OPTIONS="$COMPOSED_NODE_OPTIONS" npm run dev &
else
    echo "Building production frontend (this takes a few seconds)..."
    if ! NEXT_PUBLIC_BACKEND_PORT=$BACKEND_PORT API_PROXY_URL=http://127.0.0.1:$BACKEND_PORT NODE_OPTIONS="$COMPOSED_NODE_OPTIONS" npm run build; then
        echo -e "\n[!] Error: Frontend build failed. Exiting run script."
        if [ -n "$BACKEND_PGID" ]; then kill -TERM -$BACKEND_PGID 2>/dev/null; fi
        exit 1
    fi

    # Standalone mode does not copy static assets by default. We must copy them manually.
    # Next.js workspace detection might place the output inside a "frontend" subfolder.
    if [ -f ".next/standalone/frontend/server.js" ]; then
        cp -r public .next/standalone/frontend/
        mkdir -p .next/standalone/frontend/.next
        cp -r .next/static .next/standalone/frontend/.next/
        API_PROXY_URL=http://127.0.0.1:$BACKEND_PORT PORT=$FRONTEND_PORT NODE_OPTIONS="$COMPOSED_NODE_OPTIONS" node .next/standalone/frontend/server.js &
    else
        cp -r public .next/standalone/
        mkdir -p .next/standalone/.next
        cp -r .next/static .next/standalone/.next/
        API_PROXY_URL=http://127.0.0.1:$BACKEND_PORT PORT=$FRONTEND_PORT NODE_OPTIONS="$COMPOSED_NODE_OPTIONS" node .next/standalone/server.js &
    fi
fi
FRONTEND_PID=$!
FRONTEND_PGID=$(ps -o pgid= -p $FRONTEND_PID | tr -d ' ')
set +m
cd ..

# Wait a brief moment to see if frontend crashes immediately
sleep 3
if ! kill -0 $FRONTEND_PID 2>/dev/null; then
    echo -e "\n[!] Error: Frontend process died early. Check logs above."
    if [ -n "$BACKEND_PGID" ]; then kill -TERM -$BACKEND_PGID 2>/dev/null; fi
    exit 1
fi

if [ "$DEV_MODE" = true ]; then
    echo -e "\n🚀 Both DEV services are up and running!"
else
    echo -e "\n🚀 Both PRODUCTION services are up and running!"
fi
echo "   - Frontend Dashboard: http://localhost:$FRONTEND_PORT"
echo "   - Backend API:        http://localhost:$BACKEND_PORT"
echo -e "   Press Ctrl+C to stop both servers.\n"

wait
