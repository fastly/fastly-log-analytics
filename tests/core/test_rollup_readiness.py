from backend.core import rollup_readiness as rr


def test_defaults_to_not_ready():
    rr.reset_rollup_coverage_ready()
    assert rr.rollup_coverage_ready("svc-a") is False


def test_mark_ready_flips_only_that_service():
    rr.reset_rollup_coverage_ready()
    rr.mark_rollup_coverage_ready("svc-a")
    assert rr.rollup_coverage_ready("svc-a") is True
    assert rr.rollup_coverage_ready("svc-b") is False


def test_reset_single_service():
    rr.reset_rollup_coverage_ready()
    rr.mark_rollup_coverage_ready("svc-a")
    rr.mark_rollup_coverage_ready("svc-b")
    rr.reset_rollup_coverage_ready("svc-a")
    assert rr.rollup_coverage_ready("svc-a") is False
    assert rr.rollup_coverage_ready("svc-b") is True
