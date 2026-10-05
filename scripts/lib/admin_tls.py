"""Exact-origin client mTLS for operator diagnostics (stdlib only).

ADMIN_GATEWAY_ENDPOINTS is optional JSON mapping environment names to objects:
{"remote-high-scale": {"origin": "https://admin.example.com:8443",
 "cert": "/operator/client.crt", "key": "/operator/client.key",
 "ca": "/operator/server-ca.crt"}}
Paths/values belong only in operator-local configuration. The legacy single
ADMIN_GATEWAY_CLIENT_ORIGIN/CERT/KEY and ADMIN_GATEWAY_SERVER_CA are supported
as a default identity, with ADMIN_GATEWAY_ENVIRONMENT defaulting to
remote-high-scale. Never send a gateway token from this client.
"""

import argparse
import ipaddress
import json
import os
import ssl
import sys
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener, urlopen

ALIASES = {
    "remote-hs": "remote-high-scale",
    "remote-std": "remote-standard",
    "local-std": "local-standard",
    "local-hs": "local-high-scale",
}


def _origin(url: str) -> str:
    if any(character.isspace() for character in url) or "\\" in url:
        raise ValueError("Admin URL contains invalid whitespace or backslash")
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password:
        raise ValueError("Admin endpoint must be an absolute HTTPS origin without userinfo")
    host = parts.hostname.lower()
    try:
        if ipaddress.ip_address(host).version == 6:
            host = f"[{host}]"
    except ValueError:
        pass
    port = parts.port or (443 if parts.scheme == "https" else 80)
    return f"{parts.scheme}://{host}:{port}"


def _settings() -> dict[str, dict[str, str]]:
    raw = os.getenv("ADMIN_GATEWAY_ENDPOINTS", "")
    try:
        settings = json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise ValueError("ADMIN_GATEWAY_ENDPOINTS must be valid JSON") from exc
    if not isinstance(settings, dict):
        raise ValueError("ADMIN_GATEWAY_ENDPOINTS must map environments to TLS configuration")
    single = os.getenv("ADMIN_GATEWAY_CLIENT_ORIGIN", "")
    if single:
        environment = os.getenv("ADMIN_GATEWAY_ENVIRONMENT", "remote-high-scale")
        settings.setdefault(
            environment,
            {
                "origin": single,
                "cert": os.getenv("ADMIN_GATEWAY_CLIENT_CERT", ""),
                "key": os.getenv("ADMIN_GATEWAY_CLIENT_KEY", ""),
                "ca": os.getenv("ADMIN_GATEWAY_SERVER_CA", ""),
            },
        )
    elif any(
        os.getenv(name) for name in ("ADMIN_GATEWAY_CLIENT_CERT", "ADMIN_GATEWAY_CLIENT_KEY", "ADMIN_GATEWAY_SERVER_CA")
    ):
        raise ValueError("Client TLS paths require ADMIN_GATEWAY_CLIENT_ORIGIN or ADMIN_GATEWAY_ENDPOINTS")
    normalized = {}
    identities = {}
    for environment, config in settings.items():
        if not isinstance(environment, str) or not isinstance(config, dict):
            raise ValueError("Admin TLS configuration must map environment strings to objects")
        environment = ALIASES.get(environment, environment)
        if environment not in {"local-standard", "local-high-scale", "remote-standard", "remote-high-scale"}:
            raise ValueError("Unknown admin TLS environment")
        if set(config) != {"origin", "cert", "key", "ca"} or not all(
            isinstance(value, str) and value for value in config.values()
        ):
            raise ValueError("Each admin TLS target needs nonempty origin, cert, key and ca")
        parsed = urlsplit(config["origin"])
        if parsed.scheme != "https" or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise ValueError("Admin TLS target must be an HTTPS origin, not a URL path")
        origin = _origin(config["origin"])
        for name in ("cert", "key", "ca"):
            path = Path(config[name])
            if not path.is_file():
                raise ValueError(f"Admin TLS {name} file is missing")
            if name in {"cert", "key"} and path.stat().st_mode & 0o077:
                raise ValueError(f"Admin TLS {name} must be an owner-only file")
        identity = (config["cert"], config["key"], config["ca"])
        if origin in identities and identities[origin] != identity:
            raise ValueError("Conflicting client identities configured for one admin origin")
        if environment in normalized:
            raise ValueError("Duplicate admin environment alias")
        identities[origin] = identity
        normalized[environment] = {**config, "origin": origin}
    return normalized


def validate_admin_tls_config() -> None:
    """Fail invalid configuration before diagnostics catch transport failures."""
    for config in _settings().values():
        _context(config)


def admin_origin(environment: str) -> str | None:
    """Configured direct origin for canonical environment or short alias."""
    config = _settings().get(ALIASES.get(environment, environment))
    return config["origin"] if config else None


def admin_tls_config(environment: str) -> dict[str, str] | None:
    """Copy of configured origin/cert/key/ca for an environment or alias."""
    config = _settings().get(ALIASES.get(environment, environment))
    return dict(config) if config else None


def is_admin_url(url: str) -> bool:
    """Whether this exact origin has a configured client identity."""
    origin = _origin(url)
    return any(config["origin"] == origin for config in _settings().values())


def _context(config: dict[str, str]) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=config["ca"])
    # Avoid an interactive password prompt in canonical automation.
    # Encrypted browser exports are separate from this owner-only CLI key.
    context.load_cert_chain(config["cert"], config["key"], password=lambda: "")
    return context


class _SameOriginRedirect(HTTPRedirectHandler):
    def __init__(self, origin: str):
        self.origin = origin

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if _origin(newurl) != self.origin:
            raise ValueError("Authenticated admin request refused cross-origin redirect")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def admin_urlopen(request_or_url: Request | str, timeout: float = 5.0):
    """Verified TLS; client certificate only to an exact configured origin.

    Unconfigured requests use ordinary urlopen, without this client identity.
    Use ordinary urlopen explicitly for edge probes even when an endpoint is
    accidentally configured as both edge and admin. No global opener installed.
    """
    url = request_or_url.full_url if isinstance(request_or_url, Request) else request_or_url
    origin = _origin(url)
    configs = _settings()
    config = next((item for item in configs.values() if item["origin"] == origin), None)
    if config is None:
        return urlopen(request_or_url, timeout=timeout)
    request = request_or_url if isinstance(request_or_url, Request) else Request(request_or_url)
    if any(name.lower() in {"x-admin-gateway-token", "x-admin-token"} for name, _ in request.header_items()):
        raise ValueError("Admin client mTLS must not send server gateway or bootstrap credentials")
    opener = build_opener(HTTPSHandler(context=_context(config)), _SameOriginRedirect(origin))
    return opener.open(request, timeout=timeout)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verified exact-origin admin client TLS GET")
    parser.add_argument("url")
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--discard", action="store_true")
    args = parser.parse_args()
    try:
        validate_admin_tls_config()
        with admin_urlopen(args.url, timeout=args.timeout) as response:
            if not args.discard:
                sys.stdout.buffer.write(response.read())
    except (OSError, ValueError) as exc:
        print(f"admin TLS request failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
