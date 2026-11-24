import json
from pathlib import Path

from backend.utils.bot_sources import extract_entry_referral_stems

_ROOT = Path(__file__).resolve().parents[2]
_SEED = _ROOT / "backend" / "utils" / "well_known_bots_seed.json"
_BROWSER_COPY = _ROOT / "frontend" / "lib" / "cloud-engine" / "bots" / "well-known-bots.generated.ts"


def _browser_rows() -> list[list]:
    lines = _BROWSER_COPY.read_text(encoding="utf-8").splitlines()
    return [json.loads(line.strip().rstrip(",")) for line in lines if line.startswith("  [")]


def test_browser_copy_carries_the_backend_referral_stems_and_verification_domains():
    seed = json.loads(_SEED.read_text(encoding="utf-8"))
    rows = _browser_rows()
    assert [row[0] for row in rows] == [entry["id"] for entry in seed]
    for entry, row in zip(seed, rows, strict=True):
        assert row[3] == sorted(extract_entry_referral_stems(entry)), entry["id"]
        assert row[4] == ((entry.get("verification") or {}).get("domains") or []), entry["id"]
        assert row[5] == (entry.get("aliases") or []), entry["id"]
