"""Shared SigV4 signing for FOS (Fastly Object Storage) requests.

Extracted from telemetry_proxy._sign_request for reuse in provisioning
and other contexts (RUM asset upload, CDN-fronting, etc.).
"""

from __future__ import annotations

from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials


def sign_fos_request(
    method: str,
    url: str,
    headers: dict | None,
    body: bytes,
    access_key_id: str,
    secret_access_key: str,
    region: str = "us-east-1",
) -> dict[str, str]:
    """Sign a FOS request using AWS SigV4.

    Args:
        method: HTTP method (GET, PUT, POST, etc.)
        url: Full URL to sign
        headers: Dict of headers (will be mutated with auth headers)
        body: Request body bytes
        access_key_id: FOS access key ID
        secret_access_key: FOS secret access key
        region: FOS region (default: us-east-1)

    Returns:
        The updated headers dict with Authorization + x-amz-* headers.
    """
    if headers is None:
        headers = {}

    from urllib.parse import urlparse

    # Ensure Host header is present before signing to prevent signature mismatches when httpx adds it
    if "Host" not in headers and "host" not in headers:
        headers["Host"] = urlparse(url).netloc

    credentials = Credentials(access_key_id, secret_access_key)
    aws_req = AWSRequest(method=method, url=url, headers=headers, data=body)
    S3SigV4Auth(credentials, "s3", region).add_auth(aws_req)
    headers.update(dict(aws_req.headers))
    return headers
