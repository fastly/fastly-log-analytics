from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_capacity_and_recovery_contract_is_explicit() -> None:
    text = (ROOT / "docs/runbooks/high-scale-capacity-and-recovery.md").read_text()
    assert "2M request events/sec" in text
    assert "5M/sec" in text
    assert "500k RUM events/sec" in text
    assert "1M/sec RUM bursts" in text
    assert "15 minutes" in text
    assert "never silently samples or discards" in text
    assert "FOS is the durable source of truth" in text
    assert "request, RUM vitals, RUM errors, and CMCD" in text
