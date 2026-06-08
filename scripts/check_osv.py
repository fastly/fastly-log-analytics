#!/usr/bin/env python3
"""Run osv-scanner and fail only if CRITICAL vulnerabilities are found."""

import json
import subprocess
import sys


def main():
    print("Running osv-scanner once and parsing results...")

    # 1. Run osv-scanner once capturing the JSON output
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

    vulnerabilities = []
    critical_count = 0
    results = data.get("results", [])
    for r in results:
        source_path = r.get("source", {}).get("path", "unknown")
        for pkg_info in r.get("packages", []):
            pkg = pkg_info.get("package", {})
            pkg_name = pkg.get("name", "unknown")
            pkg_version = pkg.get("version", "unknown")
            for vuln in pkg_info.get("vulnerabilities", []):
                vuln_id = vuln.get("id", "unknown")
                summary = vuln.get("summary", "No summary provided")
                db_specific = vuln.get("database_specific", {})
                severity = str(db_specific.get("severity", "unknown")).upper() if db_specific else "UNKNOWN"

                vulnerabilities.append(
                    {
                        "id": vuln_id,
                        "package": f"{pkg_name}@{pkg_version}",
                        "severity": severity,
                        "summary": summary,
                        "source": source_path,
                    }
                )
                if severity == "CRITICAL":
                    critical_count += 1

    # Print a beautiful, clean summary of found vulnerabilities
    if vulnerabilities:
        print("\n⚠️  Vulnerabilities detected by osv-scanner:")
        print(f"{'Severity':<10} | {'Package':<35} | {'ID':<15} | {'Summary'}")
        print("-" * 100)
        for v in vulnerabilities:
            summary_preview = v["summary"] if len(v["summary"]) <= 50 else v["summary"][:47] + "..."
            print(f"{v['severity']:<10} | {v['package']:<35} | {v['id']:<15} | {summary_preview}")
        print("-" * 100)
    else:
        print("\n✅ No vulnerabilities found.")

    if critical_count > 0:
        print(f"\n❌ FAILED: Found {critical_count} CRITICAL vulnerabilities.")
        sys.exit(1)

    if len(vulnerabilities) > 0:
        print("\n✅ PASSED: Vulnerabilities found, but none are CRITICAL.")
        sys.exit(0)

    print("\n✅ PASSED: No vulnerabilities found.")
    sys.exit(0)


if __name__ == "__main__":
    main()
