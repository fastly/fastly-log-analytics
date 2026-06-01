# --- Build Stage ---
FROM python:3.12-slim-bullseye AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.6.14 /uv /uvx /bin/

WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy dependency files first
COPY pyproject.toml uv.lock ./

# Install dependencies into a virtual environment
# We use --frozen to ensure exact versions from uv.lock
RUN uv sync --no-dev --frozen --no-install-project

# --- Production Stage ---
FROM python:3.12-slim-bullseye AS runner

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH" \
    DUCKDB_MEMORY_LIMIT=4GB \
    ARROW_DEFAULT_MEMORY_POOL=system

WORKDIR /app

# Install runtime dependencies (curl for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the virtual environment from the builder
COPY --from=builder /app/.venv /app/.venv

# Copy only the necessary backend files and root modules
# This avoids pulling in the entire frontend/ directory
COPY backend/ ./backend/
COPY pyproject.toml README.md uv.lock ./
COPY scripts/generate_openapi.py scripts/

# Expose the backend port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
