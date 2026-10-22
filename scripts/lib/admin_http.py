"""Deadline-bound JSON transport using the existing exact-origin administrator identity."""

from __future__ import annotations

import asyncio
from urllib.request import Request

import httpx

from scripts.lib.admin_tls import admin_request_context


async def async_admin_json(request: Request, timeout: float = 5.0) -> object:
    content = request.data
    if content is not None and not isinstance(content, bytes):
        raise ValueError("JSON diagnostic requests require a byte payload")
    context = admin_request_context(request)
    async with asyncio.timeout(timeout):
        async with httpx.AsyncClient(
            verify=context, trust_env=False, follow_redirects=False, timeout=timeout
        ) as client:
            response = await client.request(
                request.get_method(), request.full_url, content=content, headers=dict(request.header_items())
            )
            response.raise_for_status()
            return response.json()
