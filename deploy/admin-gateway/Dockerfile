# Purpose-built admin mTLS gateway image (compose overlay + companion chart).
#
# The official caddy binary carries the file capability
# cap_net_bind_service=ep. The gateway runs with every capability dropped
# (cap_drop ALL / capabilities.drop ALL) as a non-root user, and the kernel
# refuses to exec a binary whose effective file capability is outside the
# bounding set ("exec /usr/bin/caddy: operation not permitted"). The gateway
# only listens on 8443, so strip the capability instead of granting one.
# Tag the result distinctly; never reuse a caddy:* tag (see caddy/Dockerfile).
ARG CADDY_IMAGE=caddy:2.11.4-alpine@sha256:6aeddd44c3078b0f9a35206472a11420648a79c184603ef95957d0a20044cb2b
FROM ${CADDY_IMAGE}
# The published Jenkins image retained the base layer's file capability
# despite the guarded strip. Replace the file without preserving xattrs so
# the change survives layer snapshots; verify the published digest as well.
RUN cp /usr/bin/caddy /tmp/caddy \
    && chmod 0755 /tmp/caddy \
    && mv -f /tmp/caddy /usr/bin/caddy \
    && test -z "$(getcap /usr/bin/caddy)"
USER 1000:1000
