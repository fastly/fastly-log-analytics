"""Guard that partial_hour_merge job ids stay pod-local (never RedBeat-routed).

Light smoke test — the existing scheduler suite already exercises the
_add_job/interval-registration machinery end-to-end via local_compact's
pattern (see test_scheduler_branches.py, test_compaction_jobs.py). This just
confirms the job id we chose doesn't collide with a RedBeat prefix.
"""

import inspect
import re

from backend.cron import scheduler as scheduler_module
from backend.cron.scheduler import Scheduler


def _actual_partial_hour_job_id_prefix() -> str:
    """Extract the literal prefix from the actual
    ``f"partial_hour_merge_{service_id}"`` expression(s) in
    backend/cron/scheduler.py (M5, final whole-branch review) — so this test
    derives the expected job-id format from source instead of re-typing a
    string literal that could silently drift from a future rename."""
    source = inspect.getsource(scheduler_module)
    matches = set(re.findall(r'f"(partial_hour_merge_)\{service_id\}"', source))
    assert matches, "could not find the partial_hour_merge job-id f-string in scheduler.py — did the format change?"
    assert len(matches) == 1, f"scheduler.py builds this job id inconsistently across call sites: {matches}"
    return next(iter(matches))


def test_partial_hour_merge_registered_pod_local_not_redbeat():
    job_id = f"{_actual_partial_hour_job_id_prefix()}svc"
    assert not job_id.startswith(Scheduler._REDBEAT_JOB_PREFIXES)
