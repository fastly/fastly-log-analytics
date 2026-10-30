"""Exact-Origin Client mTLS helpers and CLI for FLA admin gateway connections."""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import json
import os
import ssl
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener, urlopen

ALIASES = {
    "remote-hs": "remote-high-scale",
    "remote-std": "remote-standard",
    "local-std": "local-standard",
}

_CANONICAL_ENVIRONMENTS = frozenset({"local-standard", "remote-standard", "remote-high-scale"})
_REQUIRED_TLS_KEYS = frozenset({"origin", "cert", "key", "ca"})
_SINGLE_ENV_TLS_VARS = (
    "ADMIN_GATEWAY_CLIENT_CERT",
    "ADMIN_GATEWAY_CLIENT_KEY",
    "ADMIN_GATEWAY_SERVER_CA",
)


def _env_attr(name: str, default: Any) -> Any:
    env_mod = sys.modules.get("scripts.lib.environments")
    if env_mod is not None and hasattr(env_mod, name):
        return getattr(env_mod, name)
    return default


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
    default_port = 443 if parts.scheme == "https" else 80
    port = parts.port or default_port
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
    elif any(os.getenv(name) for name in _SINGLE_ENV_TLS_VARS):
        raise ValueError("Client TLS paths require ADMIN_GATEWAY_CLIENT_ORIGIN or ADMIN_GATEWAY_ENDPOINTS")

    normalized: dict[str, dict[str, str]] = {}
    identities: dict[str, tuple[str, str, str]] = {}
    origin_fn = _env_attr("_origin", _origin)
    for environment, config in settings.items():
        if not isinstance(environment, str) or not isinstance(config, dict):
            raise ValueError("Admin TLS configuration must map environment strings to objects")
        environment = ALIASES.get(environment, environment)
        if environment not in _CANONICAL_ENVIRONMENTS:
            raise ValueError("Unknown admin TLS environment")
        has_all_nonempty_strings = all(isinstance(value, str) and bool(value) for value in config.values())
        if set(config) != _REQUIRED_TLS_KEYS or not has_all_nonempty_strings:
            raise ValueError("Each admin TLS target needs nonempty origin, cert, key and ca")
        parsed = urlsplit(config["origin"])
        if parsed.scheme != "https" or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise ValueError("Admin TLS target must be an HTTPS origin, not a URL path")
        origin = origin_fn(config["origin"])
        for name in ("cert", "key", "ca"):
            path = Path(config[name])
            if not path.is_file():
                raise ValueError(f"Admin TLS {name} file is missing")
            if name in {"cert", "key"} and (path.stat().st_mode & 0o077):
                raise ValueError(f"Admin TLS {name} must be an owner-only file")
        identity = (config["cert"], config["key"], config["ca"])
        if origin in identities and identities[origin] != identity:
            raise ValueError("Conflicting client identities configured for one admin origin")
        if environment in normalized:
            raise ValueError("Duplicate admin environment alias")
        identities[origin] = identity
        normalized[environment] = {**config, "origin": origin}
    return normalized


def _context(config: dict[str, str]) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=config["ca"])
    context.load_cert_chain(config["cert"], config["key"], password=lambda: "")
    return context


def validate_admin_tls_config() -> None:
    """Fail invalid configuration before diagnostics catch transport failures."""
    settings_fn = _env_attr("_settings", _settings)
    context_fn = _env_attr("_context", _context)
    for config in settings_fn().values():
        context_fn(config)


def admin_origin(environment: str) -> str | None:
    """Configured direct origin for canonical environment or short alias."""
    settings_fn = _env_attr("_settings", _settings)
    canonical = ALIASES.get(environment, environment)
    config = settings_fn().get(canonical)
    return config["origin"] if config else None


def admin_tls_config(environment: str) -> dict[str, str] | None:
    """Copy of configured origin/cert/key/ca for an environment or alias."""
    settings_fn = _env_attr("_settings", _settings)
    canonical = ALIASES.get(environment, environment)
    config = settings_fn().get(canonical)
    return dict(config) if config else None


def is_admin_url(url: str) -> bool:
    """Whether this exact origin has a configured client identity."""
    origin_fn = _env_attr("_origin", _origin)
    settings_fn = _env_attr("_settings", _settings)
    origin = origin_fn(url)
    return any(config["origin"] == origin for config in settings_fn().values())


class _SameOriginRedirect(HTTPRedirectHandler):
    def __init__(self, origin: str):
        self.origin = origin

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        origin_fn = _env_attr("_origin", _origin)
        if origin_fn(newurl) != self.origin:
            raise ValueError("Authenticated admin request refused cross-origin redirect")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _has_forbidden_gateway_header(request: Request) -> bool:
    return any(name.lower() == "x-admin-gateway-token" for name, _ in request.header_items())


def admin_urlopen(request_or_url: Request | str, timeout: float = 5.0):
    """Verified TLS; client certificate only to an exact configured origin."""
    origin_fn = _env_attr("_origin", _origin)
    settings_fn = _env_attr("_settings", _settings)
    urlopen_fn = _env_attr("urlopen", urlopen)
    build_opener_fn = _env_attr("build_opener", build_opener)
    https_handler_cls = _env_attr("HTTPSHandler", HTTPSHandler)
    context_fn = _env_attr("_context", _context)

    url = request_or_url.full_url if isinstance(request_or_url, Request) else request_or_url
    origin = origin_fn(url)
    configs = settings_fn()
    config = next((item for item in configs.values() if item["origin"] == origin), None)
    if config is None:
        return urlopen_fn(request_or_url, timeout=timeout)
    request = request_or_url if isinstance(request_or_url, Request) else Request(request_or_url)
    if _has_forbidden_gateway_header(request):
        raise ValueError("Admin client mTLS must not send server gateway credentials")
    opener = build_opener_fn(https_handler_cls(context=context_fn(config)), _SameOriginRedirect(origin))
    return opener.open(request, timeout=timeout)


def admin_request_context(request: Request) -> ssl.SSLContext:
    """Require an exact-origin identity for a credential-free administrator request."""
    origin_fn = _env_attr("_origin", _origin)
    settings_fn = _env_attr("_settings", _settings)
    context_fn = _env_attr("_context", _context)

    origin = origin_fn(request.full_url)
    config = next((item for item in settings_fn().values() if item["origin"] == origin), None)
    if config is None:
        raise ValueError("No exact-origin administrator TLS identity configured")
    if _has_forbidden_gateway_header(request):
        raise ValueError("Admin client mTLS must not send server gateway credentials")
    return context_fn(config)


async def async_admin_json(request: Request, timeout: float = 5.0) -> object:
    import httpx

    content = request.data
    if content is not None and not isinstance(content, bytes):
        raise ValueError("JSON diagnostic requests require a byte payload")
    request_context_fn = _env_attr("admin_request_context", admin_request_context)
    context = request_context_fn(request)
    async with asyncio.timeout(timeout):
        async with httpx.AsyncClient(
            verify=context,
            trust_env=False,
            follow_redirects=False,
            timeout=timeout,
        ) as client:
            response = await client.request(
                request.get_method(),
                request.full_url,
                content=content,
                headers=dict(request.header_items()),
            )
            response.raise_for_status()
            return response.json()


def admin_tls_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verified exact-origin admin client TLS GET")
    parser.add_argument("url")
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--discard", action="store_true")
    args = parser.parse_args(argv)
    validate_fn = _env_attr("validate_admin_tls_config", validate_admin_tls_config)
    admin_urlopen_fn = _env_attr("admin_urlopen", admin_urlopen)
    try:
        validate_fn()
        with admin_urlopen_fn(args.url, timeout=args.timeout) as response:
            if not args.discard:
                sys.stdout.buffer.write(response.read())
    except (OSError, ValueError) as exc:
        print(f"Admin HTTP request failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    return 0


__all__ = [
    "ALIASES",
    "HTTPRedirectHandler",
    "HTTPSHandler",
    "Request",
    "_SameOriginRedirect",
    "_context",
    "_origin",
    "_settings",
    "admin_origin",
    "admin_request_context",
    "admin_tls_config",
    "admin_tls_main",
    "admin_urlopen",
    "async_admin_json",
    "build_opener",
    "is_admin_url",
    "ssl",
    "urlopen",
    "validate_admin_tls_config",
]
