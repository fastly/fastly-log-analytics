import json
from unittest.mock import MagicMock, patch


def _mock_redis(queues: dict[str, int], redbeat: dict[str, str]):
    """Build a MagicMock redis client backing scan_iter/type/llen/hget."""
    r = MagicMock()

    def scan_iter(match=None, count=None):
        if match == "q.*":
            return iter([k for k in queues if k.startswith("q.")])
        if match == "redbeat:*":
            return iter(list(redbeat.keys()) + ["redbeat::schedule", "redbeat::lock"])
        return iter([])

    r.scan_iter.side_effect = scan_iter
    r.type.side_effect = lambda k: "list" if k in queues else "none"
    r.llen.side_effect = lambda k: queues.get(k, 0)
    r.hget.side_effect = lambda k, f: json.dumps({"task": redbeat[k]}) if f == "definition" and k in redbeat else None
    return r


def test_celery_status_success(client):
    queues = {"q.ingest": 5, "celery": 2}
    redbeat = {"redbeat:job1": "backend.tasks.job1"}
    with (
        patch("backend.celery_app.app.control.inspect") as mock_inspect,
        patch("backend.celery_status._get_redis", return_value=_mock_redis(queues, redbeat)),
    ):
        mock_i = MagicMock()
        mock_i.active.return_value = {"worker1": ["task1"]}
        mock_i.stats.return_value = {"worker1": {"pool": {}}}
        mock_i.registered.return_value = {"worker1": ["task_a"]}
        mock_i.scheduled.return_value = {"worker1": ["task_b"]}
        mock_inspect.return_value = mock_i

        resp = client.get("/api/admin/celery/status", headers={"X-Service-Id": "svc1"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["broker_reachable"] is True
        assert data["workers"]["count"] == 1
        assert data["workers"]["active_tasks"] == {"worker1": ["task1"]}
        # Known queues are always reported (LLEN on a missing list is 0);
        # discovered q.* queues are included too.
        assert data["queues"]["q.ingest"] == 5
        assert data["queues"]["celery"] == 2
        assert data["queues"]["q.control"] == 0
        # RedBeat statics (redbeat::schedule / redbeat::lock) are excluded.
        assert data["schedules"] == [{"name": "job1", "task": "backend.tasks.job1"}]


def test_celery_status_broker_down_is_distinguishable(client):
    """Broker down must NOT look like an idle system."""
    broken = MagicMock()
    broken.scan_iter.side_effect = ConnectionError("Redis down")
    with (
        patch("backend.celery_app.app.control.inspect") as mock_inspect,
        patch("backend.celery_status._get_redis", return_value=broken),
    ):
        mock_inspect.side_effect = Exception("Celery down")

        resp = client.get("/api/admin/celery/status", headers={"X-Service-Id": "svc1"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["broker_reachable"] is False
        assert data["workers"]["count"] == 0
        assert "error" in data["workers"]
        assert data["queues"] == {}
        assert data["schedules"] == []


def test_celery_status_idle_system(client):
    """Reachable broker with nothing queued reports zeros, not emptiness."""
    with (
        patch("backend.celery_app.app.control.inspect") as mock_inspect,
        patch("backend.celery_status._get_redis", return_value=_mock_redis({}, {})),
    ):
        mock_i = MagicMock()
        mock_i.active.return_value = {}
        mock_i.stats.return_value = {}
        mock_i.registered.return_value = {}
        mock_i.scheduled.return_value = {}
        mock_inspect.return_value = mock_i

        resp = client.get("/api/admin/celery/status", headers={"X-Service-Id": "svc1"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["broker_reachable"] is True
        assert data["workers"]["count"] == 0
        assert data["queues"] == {"celery": 0, "q.control": 0, "q.ingest": 0}


def test_task_routes_match_registered_tasks():
    """Every task_routes key must correspond to a registered task (or a
    prefix pattern that matches at least one). The v3.0.0 branch shipped a
    route for a task name that didn't exist ('convert_batch'), sending all
    ingest tasks to the default queue that no worker consumed."""
    import importlib

    from backend.celery_app import app

    # `include` modules are only imported at worker boot; import them here so
    # their @app.task registrations exist.
    for mod in app.conf.include:
        importlib.import_module(mod)

    registered = set(app.tasks.keys())
    for pattern in app.conf.task_routes:
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            assert any(t.startswith(prefix) for t in registered), (
                f"task_routes pattern {pattern!r} matches no registered task"
            )
        else:
            assert pattern in registered, f"task_routes key {pattern!r} is not a registered task"


def test_worker_queues_cover_all_routed_queues():
    """The queues KNOWN_QUEUES advertises (and the worker -Q flags in
    docker-compose.multipod.yml / the Helm chart) must cover every queue
    task_routes can route to, plus the default queue."""
    from backend.celery_app import app
    from backend.celery_status import KNOWN_QUEUES

    routed = {spec["queue"] for spec in app.conf.task_routes.values()}
    routed.add(app.conf.task_default_queue or "celery")
    assert routed <= set(KNOWN_QUEUES), f"queues {routed - set(KNOWN_QUEUES)} not covered by KNOWN_QUEUES"
