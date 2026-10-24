"""Shared fixtures for tests/utils/ telemetry proxy tests."""

from __future__ import annotations

import asyncio

import aiohttp
import boto3
import pytest
from botocore.config import Config
from moto.server import ThreadedMotoServer

from backend.utils import telemetry_proxy


@pytest.fixture
async def proxy_server():
    """Start the proxy in a clean state, poll /healthz until ready, yield it,
    then shut down and reset module globals.
    """
    telemetry_proxy._reset_for_tests()
    telemetry_proxy.start_proxy_server()
    deadline = asyncio.get_event_loop().time() + 5.0
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"{telemetry_proxy.proxy_endpoint()}/healthz") as r:
                    if r.status == 200:
                        break
        except (aiohttp.ClientError, RuntimeError):
            pass
        if asyncio.get_event_loop().time() > deadline:
            telemetry_proxy.stop_proxy_server()
            raise RuntimeError("proxy did not become healthy in 5s")
        await asyncio.sleep(0.02)
    try:
        yield telemetry_proxy
    finally:
        telemetry_proxy.stop_proxy_server()
        telemetry_proxy._reset_for_tests()


@pytest.fixture
def moto_s3_server():
    """Run a real HTTP moto S3 server on a free port and seed a 'test-bucket'."""
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint_url = f"http://{host}:{port}"
    seed_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    seed_client.create_bucket(Bucket="test-bucket")
    try:
        yield endpoint_url, f"{host}:{port}"
    finally:
        server.stop()
