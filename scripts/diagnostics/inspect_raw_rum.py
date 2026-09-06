import gzip
import json
import os
import sys

# Add project to python path
sys.path.append(".")

from backend.core.duckdb import _get_fos_client, get_source_for_service


def inspect_raw_rum():
    service_id = os.getenv("SERVICE_ID", "cVnu9mYB3Cvmob3lsqjQU3")
    src = get_source_for_service(service_id)
    s3 = _get_fos_client(src)
    bucket = src["bucket"]
    prefix = src.get("prefix", "").strip("/")
    rum_prefix = f"{prefix}/rum/raw/" if prefix else "rum/raw/"

    print(f"Connecting to bucket: {bucket}, prefix: {rum_prefix}")

    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=rum_prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".gz"):
                continue
            objects.append(obj["Key"])

    if not objects:
        print("No raw RUM .gz files found!")
        return

    print(f"Scanning ALL {len(objects)} raw files for non-zero CLS metrics...")

    from urllib.parse import parse_qs, urlparse

    non_zero_cls_beacons = []
    zero_cls_count = 0
    empty_cls_count = 0

    for idx, key in enumerate(objects):
        if idx % 500 == 0 and idx > 0:
            print(f"  Scanned {idx} files...")
        resp = s3.get_object(Bucket=bucket, Key=key)
        content = resp["Body"].read()
        try:
            decompressed = gzip.decompress(content).decode("utf-8")
            for line in decompressed.splitlines():
                if not line.strip():
                    continue
                if "rum_metric_name=cls" in line:
                    try:
                        log_data = json.loads(line)
                        raw_url = log_data.get("url") or log_data.get("rum_raw_query") or ""
                        parsed = urlparse(raw_url)
                        qparams = parse_qs(parsed.query)

                        val_str = qparams.get("rum_metric_value", [""])[0].strip()
                        if not val_str:
                            empty_cls_count += 1
                        else:
                            try:
                                val_float = float(val_str)
                                if val_float > 0.0:
                                    non_zero_cls_beacons.append((key, raw_url, val_float))
                                else:
                                    zero_cls_count += 1
                            except ValueError:
                                print(f"  Non-numeric CLS value: '{val_str}' in {key}")
                    except Exception:
                        pass
        except Exception as e:
            print(f"Error scanning {key}: {e}")

    print("\nScan complete!")
    print(f"Total CLS beacons with empty value: {empty_cls_count}")
    print(f"Total CLS beacons with zero value: {zero_cls_count}")
    print(f"Total CLS beacons with non-zero value: {len(non_zero_cls_beacons)}")

    if non_zero_cls_beacons:
        print("\nFirst 10 non-zero CLS beacons found:")
        for key, url, val in non_zero_cls_beacons[:10]:
            print(f"  - File: {key}")
            print(f"    URL: {url}")
            print(f"    Parsed value: {val}")


if __name__ == "__main__":
    inspect_raw_rum()
