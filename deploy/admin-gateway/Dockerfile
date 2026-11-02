# Purpose-built admin mTLS gateway image (compose overlay + companion chart).
#
# The official caddy binary carries the file capability
# cap_net_bind_service=ep. The gateway runs with every capability dropped
# (cap_drop ALL / capabilities.drop ALL) as a non-root user, and the kernel
# refuses to exec a binary whose effective file capability is outside the
# bounding set ("exec /usr/bin/caddy: operation not permitted"). The gateway
# only listens on 8443, so strip the capability instead of granting one.
# Tag the result distinctly; never reuse a caddy:* tag (see caddy/Dockerfile).
ARG CADDY_IMAGE=caddy:2.11.7-alpine@sha256:e70b164da81ef698b3ad19af799ea6086ce6d9e48ed4079eb9fe26f6fbf14ee5
FROM caddy:2.11.7-builder@sha256:c4cd20eaa0b69112fd5d4f721ffc9671687e1d5d0b155c8620cf7546dbef94c0 AS builder
RUN apk add --no-cache --upgrade 'ca-certificates>=20261020-r0' 'ca-certificates-bundle>=20261020-r0'
RUN xcaddy build v2.11.7

FROM ${CADDY_IMAGE}
RUN apk add --no-cache --upgrade 'ca-certificates>=20261020-r0' 'ca-certificates-bundle>=20261020-r0'
COPY --from=builder /usr/bin/caddy /usr/bin/caddy
# The CI-built image retained the base layer's file capability
# despite the guarded strip. Replace the file without preserving xattrs so
# the change survives layer snapshots; verify the published digest as well.
RUN cp /usr/bin/caddy /tmp/caddy \
    && chmod 0755 /tmp/caddy \
    && mv -f /tmp/caddy /usr/bin/caddy \
    && test -z "$(getcap /usr/bin/caddy)"
USER 1000:1000
