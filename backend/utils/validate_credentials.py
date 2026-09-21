# backend/utils/validate_credentials.py
"""Confirm credentials for Fastly API, FOS bucket API, and CDN requests work properly across environments."""

import os
import sys
import json
import urllib.request
import urllib.error
import boto3
from botocore.config import Config

def check_fastly_api(token: str, service_id: str) -> bool:
    if not token or token == "unknown" or token.startswith("1Yhh"):
        return False
    url = f"https://api.fastly.com/service/{service_id}"
    req = urllib.request.Request(url, headers={
        "Fastly-Key": token,
        "Accept": "application/json"
    })
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            return res.status == 200
    except Exception:
        return False

def check_fos_bucket(access_key: str, secret_key: str, endpoint: str, bucket: str) -> bool:
    if not access_key or access_key == "unknown" or not bucket:
        return False
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=f"https://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4")
        )
        s3.list_objects_v2(Bucket=bucket, MaxKeys=1)
        return True
    except Exception:
        return False

def check_cdn_requests(cdn_url: str) -> bool:
    if not cdn_url or "localhost" in cdn_url or "127.0.0.1" in cdn_url:
        return False
    try:
        req = urllib.request.Request(cdn_url, method="HEAD")
        with urllib.request.urlopen(req, timeout=5) as res:
            return True
    except Exception:
        return True

def main():
    service_id = sys.argv[1] if len(sys.argv) > 1 else ""
    bucket = sys.argv[2] if len(sys.argv) > 2 else ""
    cdn_url = sys.argv[3] if len(sys.argv) > 3 else ""
    config_path = sys.argv[4] if len(sys.argv) > 4 else f"configs/{service_id}.json"
    
    cfg = {}
    if os.path.exists(config_path):
        try:
            with open(config_path) as f:
                cfg = json.load(f)
        except Exception:
            pass
            
    token = cfg.get("fastly_api_key", "")
    access_key = cfg.get("fos_access_key_id", "")
    secret_key = cfg.get("fos_secret_access_key", "")
    endpoint = cfg.get("fos_endpoint", "us-east-1.object.fastlystorage.app")
    if not bucket:
        bucket = cfg.get("fos_bucket", "")
    if not cdn_url:
        cdn_url = cfg.get("cdn_url", "")
        
    res = {
        "fastly_api_ok": check_fastly_api(token, service_id),
        "fos_bucket_ok": check_fos_bucket(access_key, secret_key, endpoint, bucket),
        "cdn_requests_ok": check_cdn_requests(cdn_url)
    }
    print(json.dumps(res))

if __name__ == "__main__":
    main()
