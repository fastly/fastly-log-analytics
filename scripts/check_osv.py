#!/usr/bin/env python3
"""Run osv-scanner and fail only if CRITICAL vulnerabilities are found."""

import json
import subprocess
import sys


def main():
    print("Running osv-scanner...")

    # 1. Run osv-scanner to output the human-readable table to stdout
    # We do this first so the user gets the normal visual feedback.
    # It will exit with 1 if ANY vulnerability is found.
    table_result = subprocess.run(["osv-scanner", "scan", "-r", "."], check=False)

    # 2. Run osv-scanner again to capture the JSON output for parsing
    json_result = subprocess.run(
        ["osv-scanner", "scan", "-r", ".", "--format", "json"], capture_output=True, text=True, check=False
    )

    try:
        data = json.loads(json_result.stdout)
    except json.JSONDecodeError:
        print("Error: Failed to parse osv-scanner JSON output.", file=sys.stderr)
        print("Stdout:", json_result.stdout, file=sys.stderr)
        print("Stderr:", json_result.stderr, file=sys.stderr)
        sys.exit(1)

    critical_count = 0
    results = data.get("results", [])
    for r in results:
        for pkg in r.get("packages", []):
            for vuln in pkg.get("vulnerabilities", []):
                # Check for "CRITICAL" in database_specific
                db_specific = vuln.get("database_specific", {})
                if db_specific and str(db_specific.get("severity", "")).upper() == "CRITICAL":
                    critical_count += 1

    if critical_count > 0:
        print(f"\n❌ FAILED: Found {critical_count} CRITICAL vulnerabilities.")
        sys.exit(1)

    if table_result.returncode != 0:
        print("\n✅ PASSED: Vulnerabilities found, but none are CRITICAL.")
        sys.exit(0)

    print("\n✅ PASSED: No vulnerabilities found.")
    sys.exit(0)


if __name__ == "__main__":
    main()
