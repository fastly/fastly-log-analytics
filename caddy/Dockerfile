# Custom Caddy build that includes the caddy-ratelimit plugin.
# Used by docker-compose.prod.yml.

FROM caddy:2-builder AS builder
RUN xcaddy build --with github.com/mholt/caddy-ratelimit

FROM caddy:2-alpine
COPY --from=builder /usr/bin/caddy /usr/bin/caddy
