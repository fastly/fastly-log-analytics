# --- OpenAPI Schema Generation Stage ---
# Needs the Python backend to introspect FastAPI routes and emit openapi.json.
FROM python:3.12-slim-bookworm AS api-schema

COPY --from=ghcr.io/astral-sh/uv:0.6.14 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --frozen --no-install-project

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
RUN npm ci

COPY frontend/ .

# Generate TypeScript types from the schema produced in the previous stage
COPY --from=api-schema /app/openapi.json ./openapi.json
RUN npx openapi-typescript openapi.json -o types/api.generated.ts

ENV NEXT_TELEMETRY_DISABLED=1

RUN npx next build

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
