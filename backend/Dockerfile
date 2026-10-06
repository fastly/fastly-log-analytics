# syntax=docker/dockerfile:1.4

# --- Build Stage ---
FROM python:3.13-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    curl \
    git \
    pkg-config \
    unzip \
    zip \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.6.14 /uv /uvx /bin/

WORKDIR /app

# UV_LINK_MODE=copy: the cache mount and .venv are on different filesystems
# by BuildKit design, so uv can't hardlink between them. Explicit copy mode
# silences the warning (behavior is already copy-fallback today).
ENV UV_LINK_MODE=copy
ENV UV_HTTP_TIMEOUT=300

ARG DUCKLAKE_BASE_COMMIT=d318a545571d7d46eb751fa2aa5f6f4389285d3c
ARG DUCKDB_154_COMMIT=08e34c447bae34eaee3723cac61f2878b6bdf787
ARG VCPKG_COMMIT=84bab45d415d22042bd0b9081aea57f362da3f35

# Copy dependency files first
COPY pyproject.toml uv.lock ./

# Install dependencies into a virtual environment
# We use --frozen to ensure exact versions from uv.lock
# Cache mount keeps uv's downloaded wheel cache across builds — content-addressable
# by package hash, so a hit returns a hash-verified artifact identical to a fresh
# download. The cache is never copied into the final image.
# Bytecode compilation is intentionally NOT pre-done at build time — saves
# ~5-15s per build. Cost shifts to container startup (imports parse source
# instead of loading prebuilt .pyc), pushing health-ready by a few seconds;
# per-request latency is unchanged.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --frozen --no-install-project

# Build the DuckLake #1495 backport against the DuckDB release used by uv.lock.
COPY backend/ducklake-1495-duckdb-1.5.4.patch /tmp/ducklake-1495-duckdb-1.5.4.patch
COPY scripts/verify_ducklake_issue_1495.py /tmp/verify_ducklake_issue_1495.py
RUN set -eux; \
    git clone --filter=blob:none --no-checkout https://github.com/duckdb/ducklake.git /tmp/ducklake; \
    git -C /tmp/ducklake checkout --detach "$DUCKLAKE_BASE_COMMIT"; \
    git -C /tmp/ducklake submodule update --init duckdb; \
    git -C /tmp/ducklake/duckdb fetch origin "$DUCKDB_154_COMMIT"; \
    git -C /tmp/ducklake/duckdb checkout --detach "$DUCKDB_154_COMMIT"; \
    git -C /tmp/ducklake/duckdb submodule update --init --recursive; \
    git -C /tmp/ducklake apply /tmp/ducklake-1495-duckdb-1.5.4.patch; \
    git clone --filter=blob:none --no-checkout https://github.com/microsoft/vcpkg.git /tmp/vcpkg; \
    git -C /tmp/vcpkg checkout --detach "$VCPKG_COMMIT"; \
    /tmp/vcpkg/bootstrap-vcpkg.sh -disableMetrics; \
    case "$(dpkg --print-architecture)" in \
        amd64) vcpkg_triplet=x64-linux ;; \
        arm64) vcpkg_triplet=arm64-linux ;; \
        *) echo "unsupported architecture: $(dpkg --print-architecture)" >&2; exit 1 ;; \
    esac; \
    cmake -S /tmp/ducklake/duckdb -B /tmp/ducklake/build/release \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_TOOLCHAIN_FILE=/tmp/vcpkg/scripts/buildsystems/vcpkg.cmake \
        -DVCPKG_BUILD=1 \
        -DVCPKG_TARGET_TRIPLET="$vcpkg_triplet" \
        -DVCPKG_HOST_TRIPLET="$vcpkg_triplet" \
        -DVCPKG_MANIFEST_DIR=/tmp/ducklake \
        -DEXTENSION_STATIC_BUILD=1 \
        -DDUCKDB_EXTENSION_CONFIGS=/tmp/ducklake/extension_config.cmake \
        -DENABLE_UNITTEST_CPP_TESTS=FALSE; \
    cmake --build /tmp/ducklake/build/release --target ducklake_loadable_extension --parallel 2; \
    test -s /tmp/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension; \
    DUCKLAKE_EXTENSION_PATH=/tmp/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension \
        /app/.venv/bin/python /tmp/verify_ducklake_issue_1495.py

# Pre-install DuckDB's platform-specific extensions while the build
# environment has network access. Runtime pods may not be able to reach
# extensions.duckdb.org, especially on local ARM64 Kubernetes clusters.
RUN /app/.venv/bin/python -c \
    "import duckdb; con = duckdb.connect(); [con.execute(f'INSTALL {name}') for name in ('iceberg', 'avro', 'httpfs', 'parquet')]; con.close()"

# Falco — Fastly VCL static analyser (github.com/ysugimoto/falco).
# Download and unpack in builder so the runtime runner stage remains minimal
# and doesn't require tar or network curl at runtime assembly.
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

# --- Production Stage ---
FROM python:3.13-slim-bookworm AS runner

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH" \
    DUCKDB_MEMORY_LIMIT=4GB \
    ARROW_DEFAULT_MEMORY_POOL=system

WORKDIR /app

# Install runtime dependencies (curl for healthcheck, libjemalloc2 for memory management).
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libjemalloc2 \
    && rm -rf /var/lib/apt/lists/*

# Verify the jemalloc preload resolves at build time, so a broken setup fails
# the BUILD instead of warning on every process at container start (LD_PRELOAD
# is set at the end of this stage). Use the bare soname (resolved via ldconfig)
# instead of an absolute multiarch path, so the SAME Dockerfile builds on amd64
# (prod) and arm64 (e.g. Apple-Silicon local dev). The absolute
# /usr/lib/x86_64-linux-gnu/... path only exists on amd64, so it produced a
# harmless-but-noisy "cannot be preloaded: ignored" on arm64. bookworm ships jemalloc 5.3.
RUN LD_PRELOAD=libjemalloc.so.2 python3 -c "print('jemalloc preload OK')"

# Falco binary copied from builder stage (no tar or download tools needed at runtime)
COPY --from=builder /usr/local/bin/falco /usr/local/bin/falco

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
RUN mkdir -p /app/configs /app/data /app/cache /app/compute/scorer /opt/ducklake \
 && chown -R app:app /app

# Copy the virtual environment from the builder
COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --from=builder --chown=app:app /root/.duckdb /home/app/.duckdb
COPY --from=builder --chown=app:app /tmp/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension /opt/ducklake/ducklake.duckdb_extension

# Copy static assets, configuration, and helper scripts first.
# Placing these before backend/ preserves Docker layer caching across application code changes.
COPY --chown=app:app pyproject.toml README.md uv.lock ./
COPY --chown=app:app scripts/generate_openapi.py scripts/clickhouse_replay.py scripts/
COPY --chown=app:app compute/scorer/matrix.default.json ./compute/scorer/matrix.default.json
COPY --chown=app:app compute/scorer/pkg/session-scorer.tar.gz ./compute/scorer/pkg/session-scorer.tar.gz

# Copy backend application source
COPY --chown=app:app backend/ ./backend/

# Route allocations through jemalloc so memory freed when a DuckDB instance is
# recycled (backend.core.duckdb_recycle) is actually returned to the OS. Under
# stock glibc malloc the freed object-cache pages stay in per-arena free lists
# and RSS never drops, so the 12g container still OOMed even after the recycle
# reached zero live connections (verified prod 2026-06-21: recycle logged
# live_after=0 yet freed ~0MB and RSS climbed to the cap). jemalloc's background
# purge returns dirty/muzzy pages to the OS on a short decay. See the
# backend-oom-restart-loop runbook. Set LD_PRELOAD here (late) so the build
# steps above don't run under a not-yet-installed preload.
# Bare soname (arch-portable via ldconfig — see the build-time check above).
ENV LD_PRELOAD=libjemalloc.so.2 \
    MALLOC_CONF=background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:5000 \
    DUCKLAKE_EXTENSION_PATH=/opt/ducklake/ducklake.duckdb_extension

USER app

# Default container healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

# Expose the backend port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
