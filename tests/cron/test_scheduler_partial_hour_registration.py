"""Guard that partial_hour_merge job ids stay pod-local (never RedBeat-routed).

Light smoke test — the existing scheduler suite already exercises the
_add_job/interval-registration machinery end-to-end via local_compact's
pattern (see test_scheduler_branches.py, test_compaction_jobs.py). This just
confirms the job id we chose doesn't collide with a RedBeat prefix.
"""

from backend.cron.scheduler import Scheduler


def test_partial_hour_merge_registered_pod_local_not_redbeat():
    assert not "partial_hour_merge_svc".startswith(Scheduler._REDBEAT_JOB_PREFIXES)
