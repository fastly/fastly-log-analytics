# syntax=docker/dockerfile:1.4

FROM caddy:2.11.7-builder@sha256:c4cd20eaa0b69112fd5d4f721ffc9671687e1d5d0b155c8620cf7546dbef94c0 AS falco-builder
ARG FALCO_VERSION=2.3.0
RUN apk add --no-cache --upgrade 'ca-certificates>=20261020-r0' 'ca-certificates-bundle>=20261020-r0'
RUN go mod download "github.com/ysugimoto/falco@v${FALCO_VERSION}" \
    && cd "$(go env GOPATH)/pkg/mod/github.com/ysugimoto/falco@v${FALCO_VERSION}" \
    && CGO_ENABLED=0 go build -mod=readonly \
    -ldflags "-X main.version=${FALCO_VERSION}" -o /usr/local/bin/falco ./cmd/falco \
    && falco --version

# --- Build Stage ---
FROM python:3.13-slim-bookworm@sha256:281d39ff02e3b389401586134ff9e6572954687822a58055ec07a60ded4e0b7f AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' ca-certificates)" ge '20250419~deb12u2' \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir --upgrade pip==26.2.2

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.6.14 /uv /uvx /bin/

WORKDIR /app

# UV_LINK_MODE=copy: the cache mount and .venv are on different filesystems
# by BuildKit design, so uv can't hardlink between them. Explicit copy mode
# silences the warning (behavior is already copy-fallback today).
ENV UV_LINK_MODE=copy
ENV UV_HTTP_TIMEOUT=300

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

# --- Production Stage ---
FROM python:3.13-slim-bookworm@sha256:281d39ff02e3b389401586134ff9e6572954687822a58055ec07a60ded4e0b7f AS runner

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH" \
    DUCKDB_MEMORY_LIMIT=4GB \
    ARROW_DEFAULT_MEMORY_POOL=system

WORKDIR /app

# Install runtime dependencies (curl for healthcheck, tar for falco install).
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tar \
    libjemalloc2 \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' ca-certificates)" ge '20250419~deb12u2' \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir --upgrade pip==26.2.2

# Verify the jemalloc preload resolves at build time, so a broken setup fails
# the BUILD instead of warning on every process at container start (LD_PRELOAD
# is set at the end of this stage). Use the bare soname (resolved via ldconfig)
# instead of an absolute multiarch path, so the SAME Dockerfile builds on amd64
# (prod) and arm64 (e.g. Apple-Silicon local dev). The absolute
# /usr/lib/x86_64-linux-gnu/... path only exists on amd64, so it produced a
# harmless-but-noisy "cannot be preloaded: ignored" on arm64. bookworm ships jemalloc 5.3.
RUN LD_PRELOAD=libjemalloc.so.2 python3 -c "print('jemalloc preload OK')"

COPY --from=falco-builder /usr/local/bin/falco /usr/local/bin/falco

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

# Prebuilt, matrix-less scorer Wasm package. enable_scoring uploads this to
# the customer's Compute service via the Fastly API (no Rust/Fastly-CLI
# toolchain in this image). Committed + rebuilt off-VM via `make scorer-package`.
COPY --chown=app:app compute/scorer/pkg/session-scorer.tar.gz ./compute/scorer/pkg/session-scorer.tar.gz

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
    MALLOC_CONF=background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:5000

USER app

# Expose the backend port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
