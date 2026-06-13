# Custom Caddy build that includes the caddy-ratelimit plugin.
# Used by docker-compose.prod.yml.

FROM caddy:2-builder AS builder
RUN xcaddy build --with github.com/mholt/caddy-ratelimit

FROM caddy:2-alpine
COPY --from=builder /usr/bin/caddy /usr/bin/caddy

# Drop privileges: Caddy is the only externally-facing socket, so a
# foothold here is the highest-blast-radius container compromise.
#
# Two things changed in caddy:2-alpine sometime after early 2026:
#   1. The base image no longer ships a `caddy` user/group (USER caddy
#      now errors with "unable to find user caddy: no matching entries
#      in passwd file"). Recreate it explicitly.
#   2. The official binary has CAP_NET_BIND_SERVICE filecap so a non-
#      root caddy can bind :80/:443. Our COPY from the builder stage
#      wipes that filecap. Re-apply it with setcap.
#
# We bind :80 in Caddyfile (not :8080 — comment in the prior version
# was stale), so the filecap is required even with the user fix.
RUN apk add --no-cache libcap \
 && addgroup -S caddy \
 && adduser -S -D -H -h /var/lib/caddy -s /sbin/nologin -G caddy caddy \
 && mkdir -p /data /config /var/lib/caddy \
 && chown -R caddy:caddy /data /config /var/lib/caddy \
 && setcap 'cap_net_bind_service=+ep' /usr/bin/caddy
USER caddy
