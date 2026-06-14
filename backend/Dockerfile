# syntax=docker/dockerfile:1.4

# --- Build Stage ---
FROM python:3.12-slim-bookworm AS builder

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
# Cache mount keeps uv's downloaded wheel cache across builds — content-addressable
# by package hash, so a hit returns a hash-verified artifact identical to a fresh
# download. The cache is never copied into the final image.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --frozen --no-install-project

# --- Production Stage ---
FROM python:3.12-slim-bookworm AS runner

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH" \
    DUCKDB_MEMORY_LIMIT=4GB \
    ARROW_DEFAULT_MEMORY_POOL=system

WORKDIR /app

# Install runtime dependencies (curl for healthcheck, tar for falco install).
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    tar \
    && rm -rf /var/lib/apt/lists/*

# Falco — Fastly VCL static analyser (github.com/ysugimoto/falco).
# Used by backend.utils.vcl_validator to lint the scoring recv snippet
# before publishing a custom URL-exclusion regex. Required in
# production: a missing binary triggers the validator's hard-fail
# branch when SCORING_REQUIRE_FALCO=1 is set on the backend service.
#
# Pin to a specific release so a future falco-side change doesn't
# surprise us; bump when you actively want a new version.
ARG FALCO_VERSION=2.3.0
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
        amd64|arm64) falco_arch="$arch" ;; \
        *) echo "unsupported arch: $arch" >&2; exit 1 ;; \
    esac; \
    url="https://github.com/ysugimoto/falco/releases/download/v${FALCO_VERSION}/falco-linux-${falco_arch}.tar.gz"; \
    curl -fsSL -o /tmp/falco.tar.gz "$url"; \
    tar -xzf /tmp/falco.tar.gz -C /usr/local/bin/; \
    chmod +x /usr/local/bin/falco; \
    rm /tmp/falco.tar.gz; \
    falco --version

# Create a non-root runtime user so a foothold in the FastAPI process
# can't write outside its mounts. UID/GID 1000 matches the host owner
# of `/mnt/app-data/{configs,data,cache}` on the GCE VM — bumping
# these requires `chown -R 1000:1000 /mnt/app-data/*` on the host
# OR the container hits "Permission denied" on every write
# (ingest_log, parquet write, scorer matrix sync, etc.). The
# `addgroup --system` flag tags the group to suppress useradd's "no
# matching entries" warnings on Debian.
#
# DuckDB's `INSTALL httpfs;` (called from _load_httpfs on every
# pool-acquire) writes the extension cache into $HOME. Without a home
# dir it crashes at first request with
# `IO Error: Can't find the home directory at '/home/app'` and every
# analytics endpoint 500s — the 2026-06-13 deploy hit this when the
# original useradd used --no-create-home. Keep --create-home (or
# carry an explicit `mkdir -p /home/app && chown app:app /home/app`)
# any time this directive is touched.
RUN groupadd --gid 1000 app \
 && useradd  --uid 1000 --gid 1000 --create-home --shell /sbin/nologin app

# Pre-create the volume mount targets and chown them to the app user.
# Without this, the first request fails with PermissionError because
# docker creates the bind-mount directories owned by root before the
# mount is applied, and our entrypoint can't write under them.
RUN mkdir -p /app/configs /app/data /app/cache /app/compute/scorer \
 && chown -R app:app /app

# Copy the virtual environment from the builder
COPY --from=builder --chown=app:app /app/.venv /app/.venv

# Copy only the necessary backend files and root modules
# This avoids pulling in the entire frontend/ directory
COPY --chown=app:app backend/ ./backend/
COPY --chown=app:app pyproject.toml README.md uv.lock ./
COPY --chown=app:app scripts/generate_openapi.py scripts/

# Include the default empty scoring matrix. The trained matrix.json is
# a build artifact (gitignored, produced by scripts/scoring/train.py)
# so we can't bake it in at image build time. The backend's _load_matrix()
# prefers matrix.json if present (operator dropped it in via a volume
# mount or post-build copy) and falls back to matrix.default.json so
# the /scoring/evaluation endpoint returns a meaningful "no signal"
# response instead of erroring out.
COPY --chown=app:app compute/scorer/matrix.default.json ./compute/scorer/matrix.default.json

USER app

# Expose the backend port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
