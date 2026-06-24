# Custom Caddy build that includes the caddy-ratelimit plugin.
# Used by docker-compose.prod.yml.

FROM caddy:2-builder AS builder
RUN xcaddy build --with github.com/mholt/caddy-ratelimit

FROM caddy:2-alpine
COPY --from=builder /usr/bin/caddy /usr/bin/caddy

# Drop privileges: Caddy is the only externally-facing socket, so a
# foothold here is the highest-blast-radius container compromise.
#
# Three things about caddy:2-alpine as of mid-2026:
#   1. The base image no longer ships a `caddy` user/group (USER caddy
#      errors with "unable to find user caddy: no matching entries in
#      passwd file"). Recreate it explicitly with addgroup/adduser.
#   2. The official binary has CAP_NET_BIND_SERVICE filecap so a non-
#      root caddy can bind :80/:443. Our COPY from the builder stage
#      wipes that filecap. Re-apply it with setcap.
#   3. Every privileged step below (addgroup/adduser/setcap) must run
#      as root. The base image IS root by default — but only as long as
#      `FROM caddy:2-alpine` resolves to the OFFICIAL base. The built
#      image is therefore tagged distinctly (caddy-ratelimit:2-alpine in
#      docker-compose.prod.yml); if it reused the `caddy:2-alpine` tag it
#      would overwrite this FROM with our own image, whose last line is
#      `USER caddy`, so the next rebuild would run these steps as non-root
#      and fail ("permission denied (are you root?)" / apk "Unable to open
#      log"). setcap (libcap) and addgroup/adduser (busybox) already exist
#      in the base, so no `apk add` is needed.
#
# We bind :80 in Caddyfile (not :8080 — comment in the prior version
# was stale), so the filecap is required even with the user fix.
RUN addgroup -S caddy \
 && adduser -S -D -H -h /var/lib/caddy -s /sbin/nologin -G caddy caddy \
 && mkdir -p /data /config /var/lib/caddy \
 && chown -R caddy:caddy /data /config /var/lib/caddy \
 && setcap 'cap_net_bind_service=+ep' /usr/bin/caddy
USER caddy
