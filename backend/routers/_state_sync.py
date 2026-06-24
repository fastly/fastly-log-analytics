"""``sync_admin_state`` — fire-and-forget admin state export after a
router mutation. Lives under ``backend.routers`` rather than
``backend.utils`` because both of its dependencies
(``backend.state_sync`` and ``backend.scheduler``) sit above
``backend.utils`` in the layering.
"""

from __future__ import annotations


def sync_admin_state(service_id: str | None) -> None:
    """Fire-and-forget admin state export after alert/view mutations.

    Also nudges the scheduler so that toggling alert count between 0 and >0
    immediately registers or removes the alerts evaluation cron — otherwise
    a user who just created their first alert would wait until the next
    process restart for evaluation to start.

    Swallows all exceptions so a sync failure never breaks the primary request.
    """
    if not service_id:
        return
    try:
        from backend.state_sync import export_admin_state

        export_admin_state(service_id)
    except Exception:
        pass
    try:
        from backend.scheduler import get_scheduler

        get_scheduler().reload()
    except Exception:
        pass
