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
# Idempotent: some builders (e.g. Kaniko) may already drop xattrs on layer
# extraction, and `setcap -r` exits 1 when there is nothing to remove.
RUN if [ -n "$(getcap /usr/bin/caddy)" ]; then setcap -r /usr/bin/caddy; fi \
    && test -z "$(getcap /usr/bin/caddy)"
USER 1000:1000
