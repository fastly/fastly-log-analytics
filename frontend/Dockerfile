# syntax=docker/dockerfile:1.4

# --- OpenAPI Schema Generation Stage ---
# Needs the Python backend to introspect FastAPI routes and emit openapi.json.
FROM python:3.12-slim-bookworm AS api-schema

COPY --from=ghcr.io/astral-sh/uv:0.6.14 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --frozen --no-install-project

COPY backend/ ./backend/
COPY scripts/generate_openapi.py ./scripts/

RUN uv run python3 scripts/generate_openapi.py openapi.json

# --- Build Stage ---
FROM node:24-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    make \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY frontend/package.json frontend/package-lock.json ./
# Cache mount keeps npm's tarball cache across builds — entries are keyed by the
# SHA-512 integrity hash from package-lock.json and verified before extraction,
# so a hit returns the same bytes as a fresh download. node_modules itself is
# still rebuilt from scratch by `npm ci`.
RUN --mount=type=cache,target=/root/.npm npm ci

COPY frontend/ .

# Generate TypeScript types from the schema produced in the previous stage.
# Security: openapi.json is copied into the final image layer and is
# served by Next.js (used by client-side type generation + occasional
# debug fetches). Keep it free of embedded credentials, example tokens,
# or internal-only hostnames. The schema is built from FastAPI route
# definitions only — no values flow in from running services — but if
# you ever add an ``example=`` to a Pydantic model, double-check it
# before merging.
COPY --from=api-schema /app/openapi.json ./openapi.json
RUN npx openapi-typescript openapi.json -o types/api.generated.ts

ENV NEXT_TELEMETRY_DISABLED=1

# Two-pass build for the O6 bootstrap-manifest pattern:
#   1. First ``next build`` — SSG runs with whatever's in
#      ``lib/_preload-chunks.json`` at git HEAD. Chunk hashes get
#      assigned by Webpack/Turbopack during this pass.
#   2. ``build-preload-manifest.mjs`` — scans the just-built
#      .next/static/chunks/ for plotly bundles, rewrites
#      lib/_preload-chunks.json with the CURRENT build's hashes.
#   3. Second ``next build`` — SSG runs again, but the static JSON
#      import now picks up the freshly-written hashes, so the
#      rendered HTML for /dashboard /network /sessions etc. includes
#      ``<link rel="modulepreload" href="/_next/static/chunks/{hash}.js">``
#      pointing at chunks that actually exist in this image.
# Why two-pass: chunk hashes are not stable across build environments
# (local Mac/node-22 vs Docker/node-24 produce different hashes for
# byte-identical source), so the committed JSON's hashes are only
# accurate within their build environment. Running ``next build``
# twice in the same Docker stage means SSG always sees hashes that
# match THIS build's actual chunks. Cost: ~doubles the builder-stage
# time (~60s → ~120s). Worth it — saves ~300-700ms per first
# dashboard load that uses the plotly chart.
# Cache mount keeps Webpack's filesystem cache + SWC's compiled-module cache
# across builds. Entries are keyed by source content hash, compiler config hash,
# Next/Webpack version, and NODE_ENV, so any source or config change invalidates
# the affected entries. Output chunk hashes are derived from module content
# (Webpack contenthash), not cache state — byte-identical source produces
# byte-identical chunks whether the cache was warm or cold. The cache mount is
# stripped from the final image (only .next/standalone and .next/static are
# copied to the runner stage).
#
# Between the two passes below, build-preload-manifest.mjs rewrites
# lib/_preload-chunks.json. Webpack sees the changed content hash and
# invalidates its cached compilation of that module + dependents, so pass 2's
# SSG picks up the new hashes correctly.
#
# If a build ever produces unexpected output, nuking the cache is the first
# diagnostic: `docker buildx prune --filter type=exec.cachemount`.
RUN --mount=type=cache,target=/app/.next/cache \
    npx next build && node scripts/build-preload-manifest.mjs && npx next build

# --- Production Stage ---
FROM node:24-slim AS runner

WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV PORT=3000
ENV HOSTNAME="0.0.0.0"

RUN groupadd --gid 1001 nodejs && \
    useradd --uid 1001 --gid nodejs nextjs

COPY --from=builder /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

RUN if [ -d "app" ]; then \
      cp -r app/. . && \
      rm -rf app && \
      chown -R nextjs:nodejs . ; \
    fi

USER nextjs

EXPOSE 3000

CMD ["node", "server.js"]
