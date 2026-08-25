# syntax=docker/dockerfile:1.4

# --- OpenAPI Schema Generation Stage ---
# Needs the Python backend to introspect FastAPI routes and emit openapi.json.
FROM python:3.13-slim-bookworm AS api-schema

COPY --from=ghcr.io/astral-sh/uv:0.6.14 /uv /uvx /bin/

WORKDIR /app

ENV UV_LINK_MODE=copy
ENV UV_HTTP_TIMEOUT=300

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --frozen --no-install-project

COPY backend/ ./backend/
COPY scripts/generate_openapi.py ./scripts/

RUN uv run python3 scripts/generate_openapi.py openapi.json

# --- Build Stage ---
FROM node:24-slim AS builder

WORKDIR /app

# No apt deps required: every package in package-lock.json with an install
# script ships prebuilt linux binaries (sharp via @img/sharp-libvips-linux-*,
# unrs-resolver via @unrs/resolver-binding-linux-*-gnu) or is darwin-only
# (fsevents). If a future dep needs node-gyp, reinstate `python3 make g++`.

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

# Single-pass build. The previous two-pass pattern existed to inject
# build-time-generated <link rel="modulepreload"> tags into SSG'd HTML
# for plotly chunks. The layout-level consumer was removed in the P-2
# audit (chunk warm-up moved client-side to PlotlyPrewarm/MapPrewarm
# with an IntersectionObserver gate), so the second pass + the
# preload-manifest script produced no observable runtime effect.
# Cache mount keeps Webpack's filesystem cache + SWC's compiled-module
# cache across builds. Entries are keyed by source content hash,
# compiler config hash, Next/Webpack version, and NODE_ENV, so any
# source or config change invalidates the affected entries. The cache
# mount is stripped from the final image (only .next/standalone and
# .next/static are copied to the runner stage). If a build ever
# produces unexpected output, nuking the cache is the first diagnostic:
# `docker buildx prune --filter type=exec.cachemount`.
RUN --mount=type=cache,target=/app/.next/cache npx next build

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
