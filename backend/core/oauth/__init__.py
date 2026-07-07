"""OIDC relying-party support for analyst OAuth login.

Everything here is default-OFF: with no provider registry file (or no
``OAUTH_FLOW_STATE_SECRET``) there are no providers, and the analyst login
page renders exactly today's passcode form. See
``local-docs/analyst_oauth_design_plan.md`` for the full design.

Submodules:

- ``registry`` — deployment-global provider registry (hybrid config: gitignored
  JSON for non-secret fields + env vars for client_id/client_secret) and the
  feature-gate helpers.
- ``flow_state`` — AES-256-GCM sealed ``oauth_flow_state`` cookie codec.
- ``client`` — Authlib-backed discovery/JWKS/token/id_token handshake with an
  injectable, timeout-bounded httpx transport.
"""

from __future__ import annotations
