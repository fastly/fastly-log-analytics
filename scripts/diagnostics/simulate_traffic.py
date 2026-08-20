import json
import random
import sys

# Add project root to python path
sys.path.append(".")

import os

from fastapi.testclient import TestClient

from backend.core.metadata import get_con
from backend.main import app

SERVICE_ID = os.getenv("SERVICE_ID", "rmWzCRA0lkAOs9Gnxvohs4")


def generate_faro_beacon(path, lcp, cls, inp, load_time, browser, os, device):
    return {
        "meta": {
            "page": {"url": f"https://fastly-se-demo.global.ssl.fastly.net{path}", "pathname": path},
            "browser": {"name": browser, "version": "120.0.0"},
            "os": {"name": os, "version": "14.1"},
            "device": {"type": device},
        },
        "events": [{"name": "faro.performance.navigation", "attributes": {"pageLoadTime": str(load_time)}}],
        "measurements": [{"type": "web-vitals", "values": {"lcp": lcp, "cls": cls, "inp": inp}}],
    }


def run_audit():
    # 1. Clear existing beacons so we get a clean slate for the last 24h audit
    db = get_con(SERVICE_ID)
    db.execute("DELETE FROM rum_beacons WHERE service_id = ?", (SERVICE_ID,))
    db.commit()
    print("Cleared existing RUM beacons in local SQLite.")

    # 2. Setup TestClient
    client = TestClient(app)

    browsers = ["Chrome", "Safari", "Firefox", "Edge"]
    os_list = ["macOS", "Windows", "iOS", "Android", "Linux"]
    devices = ["Desktop", "Mobile", "Tablet"]
    paths = ["/caching/static-assets", "/", "/pricing", "/docs", "/blog"]

    beacons_to_send = []

    # Generate 15 diverse and realistic beacons
    random.seed(42)  # For reproducibility
    for i in range(15):
        path = random.choice(paths)
        lcp = round(random.uniform(0.8, 3.8), 2)
        cls = round(random.uniform(0.01, 0.28), 3)
        inp = int(random.uniform(50, 450))
        load_time = round(random.uniform(0.5, 4.5), 2)
        browser = random.choice(browsers)
        os_name = random.choice(os_list)
        device = random.choice(devices)

        beacon = generate_faro_beacon(path, lcp, cls, inp, load_time, browser, os_name, device)
        beacons_to_send.append(beacon)

    print(f"Sending {len(beacons_to_send)} simulated beacons to /api/services/rum-beacon...")
    for idx, beacon in enumerate(beacons_to_send):
        # We send to /api/services/rum-beacon
        resp = client.post(f"/api/services/rum-beacon?service_id={SERVICE_ID}", json=beacon)
        if resp.status_code == 204:
            print(f"  [{idx + 1}/15] Ingested successfully for {beacon['meta']['page']['pathname']}")
        else:
            print(f"  [{idx + 1}/15] Ingest failed: {resp.status_code} - {resp.text}")

    # Now query analytics and show the result!
    print("\nFetching latest RUM analytics to verify ingestion & load times...")
    resp = client.get(f"/api/services/{SERVICE_ID}/rum/analytics")
    if resp.status_code == 200:
        data = resp.json()
        print("\n=== Audit & Verification Results ===")
        print(f"Is Mock: {data.get('is_mock')}")
        print(f"No Data: {data.get('no_data')}")
        print(f"Beacon Count (last 24h): {data.get('beacon_count')}")

        print("\nWeb Vitals p75:")
        print(f"  LCP: {data['vitals']['lcp']['p75']}s")
        print(f"  CLS: {data['vitals']['cls']['p75']}")
        print(f"  INP: {data['vitals']['inp']['p75']}ms")

        print("\nWorst Performing Pages (Pageviews & Avg Load Time):")
        for page in data.get("worst_pages", []):
            print(
                f"  Path: {page['path']:<28} | Views: {page['views']:<3} | Avg Load: {page['avg_load_time']:.2f}s | LCP p75: {page['lcp_p75']:.2f}s"
            )

        print("\nBrowser Distribution:")
        print(json.dumps(data.get("environments", {}).get("browsers"), indent=2))
    else:
        print(f"Failed to fetch analytics: {resp.status_code} - {resp.text}")


if __name__ == "__main__":
    run_audit()
