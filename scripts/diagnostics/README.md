# Developer Diagnostics & Simulation Scripts

This directory houses the formal, tracked developer utility scripts for **Real User Monitoring (RUM)** and **Core Web Vitals** telemetry. These tools are designed to simulate production-like loads, verify browser-level visual stability measurements (CLS), and audit metrics directly within S3-compatible Fastly Object Storage (FOS).

---

## 🛠️ Diagnostics Catalog

### 1. Live Layout Shift (CLS) Injection & Verification
* **Script:** [`verify_cls_injection.js`](./verify_cls_injection.js)
* **What it does:** An end-to-end Playwright automation script. Navigates to the live demo page, simulates a real browser-level layout shift, intercepts the `/rum-beacon` HTTP payload, and verifies that Faro captures and dispatches a non-zero `CLS` visual stability score (exiting non-zero if no non-zero CLS beacon is observed).
* **How to run:**
  ```bash
  TARGET_URL=http://127.0.0.1:80 node scripts/diagnostics/verify_cls_injection.js
  ```
* **When to use:** Run this after making modifications to the edge-snippet injection or Faro configurations to guarantee Faro doesn't lose layout shift resolution in real browser sessions.

### 2. FOS Gzip Beacon Inspection
* **Script:** [`inspect_raw_rum.py`](./inspect_raw_rum.py)
* **What it does:** Lists raw `.gz` logs stored in the Fastly Object Storage (FOS) raw RUM buffer, decompresses them on the fly, parses query-string payloads, and filters out non-zero Core Web Vitals to check real-world collection rates.
* **How to run:**
  ```bash
  SERVICE_ID=<service-id> uv run scripts/diagnostics/inspect_raw_rum.py
  ```
* **When to use:** Run this to audit production storage logs to confirm real user metrics are flowing correctly to S3-compatible storage.
