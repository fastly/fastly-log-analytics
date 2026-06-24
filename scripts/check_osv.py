#!/usr/bin/env python3
"""Run osv-scanner and fail only if CRITICAL vulnerabilities are found.

Severity derivation is deliberately belt-and-suspenders: an advisory may
express its severity in EITHER the ecosystem-specific ``database_specific``
block (a qualitative string like ``CRITICAL``) OR the canonical top-level
``severity`` array (CVSS vector strings), and many records populate only one.
Keying the CRITICAL gate solely on ``database_specific`` (the old behavior)
let a genuinely CRITICAL CVE whose severity lived in a CVSS_V3/V4 vector slip
through bucketed as ``UNKNOWN``. We now take the MAX severity across both
sources, computing a CVSS v3.x base score from the vector when needed.
"""

import json
import math
import subprocess
import sys

# Qualitative ranking so we can take the max across severity sources.
_RANK = {"UNKNOWN": -1, "NONE": 0, "LOW": 1, "MODERATE": 2, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
# Canonical labels we emit (MODERATE normalizes to MEDIUM).
_LABEL = {-1: "UNKNOWN", 0: "NONE", 1: "LOW", 2: "MEDIUM", 3: "HIGH", 4: "CRITICAL"}


def _cvss_roundup(value: float) -> float:
    """CVSS v3.1 roundup: ceil to one decimal place, float-safe."""
    int_input = round(value * 100000)
    if int_input % 10000 == 0:
        return int_input / 100000.0
    return (math.floor(int_input / 10000) + 1) / 10.0


def cvss3_base_score(vector: str) -> float | None:
    """Compute the CVSS v3.0/v3.1 base score from a vector string.

    Returns None if the vector can't be parsed (e.g. a CVSS_V4 vector, which
    uses a different metric set / scoring formula we don't implement here).
    """
    metrics: dict[str, str] = {}
    for part in vector.strip().split("/"):
        if ":" in part:
            k, _, v = part.partition(":")
            metrics[k] = v
    # CVSS_V4 vectors start with "CVSS:4.0" and use AV/AC/AT/... — bail.
    if metrics.get("CVSS", "").startswith("4"):
        return None
    try:
        av = {"N": 0.85, "A": 0.62, "L": 0.55, "P": 0.2}[metrics["AV"]]
        ac = {"L": 0.77, "H": 0.44}[metrics["AC"]]
        ui = {"N": 0.85, "R": 0.62}[metrics["UI"]]
        scope_changed = metrics["S"] == "C"
        pr_table = {"N": 0.85, "L": 0.68 if scope_changed else 0.62, "H": 0.5 if scope_changed else 0.27}
        pr = pr_table[metrics["PR"]]
        imp = {"H": 0.56, "L": 0.22, "N": 0.0}
        c, i, a = imp[metrics["C"]], imp[metrics["I"]], imp[metrics["A"]]
    except KeyError:
        return None

    isc_base = 1 - (1 - c) * (1 - i) * (1 - a)
    if scope_changed:
        impact = 7.52 * (isc_base - 0.029) - 3.25 * (isc_base - 0.02) ** 15
    else:
        impact = 6.42 * isc_base
    exploitability = 8.22 * av * ac * pr * ui

    if impact <= 0:
        return 0.0
    raw = (1.08 * (impact + exploitability)) if scope_changed else (impact + exploitability)
    return _cvss_roundup(min(raw, 10.0))


def _label_from_score(score: float) -> str:
    if score >= 9.0:
        return "CRITICAL"
    if score >= 7.0:
        return "HIGH"
    if score >= 4.0:
        return "MEDIUM"
    if score > 0.0:
        return "LOW"
    return "NONE"


def severity_for_vuln(vuln: dict) -> str:
    """Return the highest severity label across all sources on the record."""
    best = -1
    unscored_vectors: list[str] = []

    db_specific = vuln.get("database_specific") or {}
    raw = str(db_specific.get("severity", "")).upper().strip()
    if raw in _RANK:
        best = max(best, _RANK[raw])

    for entry in vuln.get("severity", []) or []:
        vector = str(entry.get("score", "")).strip()
        if not vector:
            continue
        score = cvss3_base_score(vector)
        if score is None:
            unscored_vectors.append(f"{entry.get('type', '?')}:{vector}")
            continue
        best = max(best, _RANK[_label_from_score(score)])

    # A vector we could not score (e.g. CVSS_V4) is the ONLY signal and there
    # is no qualitative severity: surface it loudly rather than silently
    # bucketing UNKNOWN, so a possible CRITICAL gets human eyes.
    if best < 0 and unscored_vectors:
        print(
            f"  ⚠️  {vuln.get('id', 'unknown')}: severity only as unparsed vector(s) "
            f"{unscored_vectors}; treating as UNKNOWN — review manually.",
            file=sys.stderr,
        )
    return _LABEL[best]


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
                severity = severity_for_vuln(vuln)

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
