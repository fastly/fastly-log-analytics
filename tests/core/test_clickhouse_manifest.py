"""Transaction/parameter contracts supplement the separate real-Postgres gate."""

from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from backend.core.clickhouse_manifest import PgManifest
from tests.core.test_clickhouse_publication import fixture_manifest


@pytest.fixture
def manifest(monkeypatch):
    monkeypatch.setenv("METADATA_DSN", "postgresql://placeholder")
    pool, con = MagicMock(), MagicMock()
    pool.connection.return_value.__enter__.return_value = con
    monkeypatch.setattr("backend.core.clickhouse_manifest.get_pg_pool", lambda: pool)
    return PgManifest(), con


def result(con, row=None, rows=None, count=1):
    cursor = MagicMock()
    cursor.fetchone.return_value = row
    cursor.fetchall.return_value = rows or []
    cursor.rowcount = count
    con.execute.return_value = cursor


def test_admin_manifest_transactions_bound_pool_and_statements(manifest):
    store, con = manifest
    store.bounded = True
    with store.transaction():
        pass
    store.pool.connection.assert_called_once_with(timeout=2)
    assert [c.args[0] for c in con.execute.call_args_list] == [
        "SET LOCAL statement_timeout = '2s'",
        "SET LOCAL lock_timeout = '2s'",
    ]


def test_admin_status_projects_only_safe_service_scoped_metadata(manifest):
    store, con = manifest
    now = datetime.now(UTC)
    rows = iter(
        [
            {"schema_version": 1},
            {"pending_age": 23, "last_published_at": now},
            {
                "generation": "g",
                "dataset_id": "d",
                "target_identity": "old-target",
                "coverage_start": now,
                "coverage_end": now,
                "expires_at": now,
                "expired": True,
                "coverage_age_seconds": 25,
            },
            {"n": 1},
        ]
    )
    con.execute.return_value.fetchone.side_effect = lambda: next(rows)
    con.execute.return_value.fetchall.return_value = [{"status": "failed", "n": 2}]
    status = store.admin_status("service", "target")
    assert status["schema_version"] == 1
    assert status["publication_counts"] == {"pending": 0, "claimed": 0, "failed": 2, "published": 0}
    assert status["oldest_pending_age_seconds"] == 23
    assert status["last_published_at"] == now.isoformat()
    assert status["active_generation"]["expired"] is True
    assert status["active_generation"]["target_matches"] is False
    assert "target_identity" not in status["active_generation"]
    store.pool.connection.assert_called_once_with(timeout=2)
    selects = [c for c in con.execute.call_args_list if c.args[0].startswith("SELECT")]
    assert selects[0].args[1] == ("target",)
    for call in selects[1:]:
        assert call.args[1] == ("service",)
        assert "artifact_uri" not in call.args[0] and "first_error" not in call.args[0]


@pytest.mark.parametrize("generation", [None, "g"])
def test_replay_preview_only_reads_and_checks_service_references(manifest, generation):
    store, con = manifest
    dataset, _, _ = fixture_manifest()
    rows = []
    if generation:
        rows.append(
            {"dataset_id": "dataset", "status": "building", "target_identity": "target", "current_revision": True}
        )
    rows.extend([{"expires_at": dataset.expires_at, "schema_version": 1}, {"n": 3}])
    if generation:
        rows.append({"n": 2})
    con.execute.return_value.fetchone.side_effect = rows
    assert store.replay_preview(
        "service", dataset_id=None if generation else "dataset", generation=generation, target="target", limit=1
    ) == {"dataset_id": "dataset", "artifact_count": 3, "planned_artifacts": 1}
    store.pool.connection.assert_called_once_with(timeout=2)
    assert con.execute.call_args_list[0].args[0] == "SET TRANSACTION READ ONLY"
    for call in con.execute.call_args_list[2:]:
        assert call.args[0].startswith("SELECT")
        assert call.args[1][0] == "service"


@pytest.mark.parametrize("case", ["missing", "expired", "schema", "oversized", "retired", "superseded", "target"])
def test_replay_preview_rejects_unreplayable_references(manifest, case):
    store, con = manifest
    dataset, _, _ = fixture_manifest()
    generation = "g" if case in ("retired", "superseded", "target") else None
    if generation:
        rows = [
            {
                "dataset_id": "dataset",
                "status": "retired" if case == "retired" else "building",
                "current_revision": case != "superseded",
                "target_identity": "other" if case == "target" else "target",
            }
        ]
    elif case == "missing":
        rows = [None]
    else:
        rows = [
            {
                "expires_at": datetime.now(UTC) - timedelta(seconds=1) if case == "expired" else dataset.expires_at,
                "schema_version": 99 if case == "schema" else 1,
            },
            {"n": 101},
        ]
    con.execute.return_value.fetchone.side_effect = rows
    with pytest.raises(LookupError if case == "missing" else ValueError):
        store.replay_preview(
            "service", dataset_id=None if generation else "dataset", generation=generation, target="target", limit=100
        )


def test_publication_lag_is_bounded_actionable_metadata_only(manifest):
    store, con = manifest
    result(con, {"lag_seconds": 42.5})
    assert store.publication_lag_seconds() == 42.5
    store.pool.connection.assert_called_once_with(timeout=2)
    sql = con.execute.call_args.args[0]
    assert "p.created_at" in sql and "MAX(" in sql
    assert "p.status IN ('pending', 'claimed', 'failed')" in sql
    assert "g.status='building'" in sql
    assert "d.expires_at>clock_timestamp()" in sql
    assert "s.revision=g.selection_revision" in sql
    assert "s.service_id=g.service_id" in sql
    assert "g.service_id=p.service_id" in sql
    assert "g.generation=p.generation" in sql
    assert "d.dataset_id=g.dataset_id" in sql
    assert any("statement_timeout" in call.args[0] for call in con.execute.call_args_list)
    result(con, {"lag_seconds": None})
    assert store.publication_lag_seconds() == 0
    con.execute.side_effect = RuntimeError("offline")
    with pytest.raises(RuntimeError, match="offline"):
        store.publication_lag_seconds()


def test_seal_uses_one_real_transaction_and_parameterized_membership(manifest):
    store, con = manifest
    dataset, artifact, _ = fixture_manifest()
    store.seal(dataset, [artifact])
    con.transaction.assert_called_once_with()
    assert con.execute.call_count == 2
    assert con.execute.call_args_list[0].args[1] == asdict(dataset)
    assert con.execute.call_args_list[1].args[1] == asdict(artifact)
    with pytest.raises(ValueError, match="membership"):
        store.seal(dataset, [replace(artifact, service_id="other")])
    with pytest.raises(ValueError, match="coverage"):
        store.seal(dataset, [])
    with pytest.raises(ValueError, match="bounded dataset"):
        store.seal(replace(dataset, expected_rows=1000001), [artifact])


def test_dataset_expiry_version_and_missing_are_explicit(manifest):
    store, con = manifest
    dataset, _, _ = fixture_manifest()
    result(con, asdict(dataset))
    assert store.dataset(dataset.service_id, dataset.dataset_id) == dataset
    assert con.execute.call_args.args[1] == (dataset.service_id, dataset.dataset_id)
    result(con)
    with pytest.raises(ValueError, match="not found"):
        store.dataset(dataset.service_id, dataset.dataset_id)
    result(con, asdict(replace(dataset, expires_at=datetime.now(UTC) - timedelta(seconds=1))))
    with pytest.raises(ValueError, match="expired"):
        store.dataset(dataset.service_id, dataset.dataset_id)
    result(con, asdict(replace(dataset, schema_version=99)))
    with pytest.raises(ValueError, match="unsupported"):
        store.dataset(dataset.service_id, dataset.dataset_id)


def test_generation_and_artifact_reads_are_service_scoped(manifest, monkeypatch):
    store, con = manifest
    dataset, artifact, _ = fixture_manifest()
    result(con, {"dataset_id": dataset.dataset_id})
    assert store.generation(dataset.service_id, "generation") == {"dataset_id": dataset.dataset_id}
    assert con.execute.call_args.args[1] == (dataset.service_id, "generation")
    result(con)
    with pytest.raises(ValueError, match="generation not found"):
        store.generation(dataset.service_id, "missing")
    monkeypatch.setattr(store, "generation", lambda *args: {"dataset_id": dataset.dataset_id})
    monkeypatch.setattr(store, "dataset", lambda *args: dataset)
    result(con, asdict(artifact))
    assert store.artifact(dataset.service_id, "generation", artifact.batch_id) == artifact
    assert con.execute.call_args.args[1] == (dataset.service_id, dataset.dataset_id, artifact.batch_id)
    result(con)
    with pytest.raises(ValueError, match="not in generation"):
        store.artifact(dataset.service_id, "generation", "missing")
    result(con, rows=[asdict(artifact)])
    assert store.artifacts(dataset.service_id, dataset.dataset_id) == [artifact]
    assert "ORDER BY ordinal_start" in con.execute.call_args.args[0]


def test_generation_uses_locked_selection_revision_and_all_artifacts(manifest, monkeypatch):
    store, con = manifest
    dataset, _, _ = fixture_manifest()
    monkeypatch.setattr(store, "dataset", lambda *args: dataset)
    result(con, {"revision": 4})
    generation = store.begin_generation(dataset.service_id, dataset.dataset_id, "target")
    assert len(generation) == 32
    calls = con.execute.call_args_list
    assert "FOR UPDATE" in calls[1].args[0]
    assert calls[2].args[1] == (dataset.service_id, generation, dataset.dataset_id, "target", 4)
    assert "FROM clickhouse_artifacts" in calls[3].args[0]
    assert "published" not in calls[3].args[0]


def test_claim_is_atomic_bounded_and_finish_checks_live_fence(manifest):
    store, con = manifest
    result(con, {"lease_fence": 3})
    assert store.claim("service", "generation", "batch") == 3
    sql, params = con.execute.call_args.args
    assert "lease_fence=lease_fence+1" in sql
    assert "g.status='building'" in sql and "d.expires_at>clock_timestamp()" in sql
    assert params == (120, "service", "generation", "batch")
    result(con)
    assert store.claim("service", "generation", "batch") is None
    with pytest.raises(ValueError):
        store.claim("service", "generation", "batch", lease_seconds=301)
    assert store.finish("service", "generation", "batch", 3, digest="digest")
    sql, params = con.execute.call_args.args
    assert "p.lease_fence=%s" in sql and "p.lease_until>clock_timestamp()" in sql
    assert params[0] == "published" and params[-1] == 3
    result(con, count=0)
    assert not store.finish("service", "generation", "batch", 2, digest=None, error="TimeoutError")
    assert con.execute.call_args.args[1][0] == "failed"


@pytest.mark.parametrize(
    "scenario,activated", [("valid", True), ("missing", False), ("revision", False), ("pending", False)]
)
def test_activation_requires_complete_membership_locked_revision_and_live_target(manifest, scenario, activated):
    store, con = manifest
    dataset, _, _ = fixture_manifest()
    gen = {
        "status": "building",
        "target_identity": "target",
        "selection_revision": 3,
        "expires_at": dataset.expires_at,
        "dataset_id": dataset.dataset_id,
    }
    if scenario == "revision":
        gen["selection_revision"] = 2
    rows = [None if scenario == "missing" else {"revision": 3}, gen, {"n": int(scenario == "pending")}]

    def execute(sql, params):
        cursor = MagicMock()
        if sql.startswith("SELECT"):
            cursor.fetchone.return_value = rows.pop(0)
        return cursor

    con.execute.side_effect = execute
    assert store.activate(dataset.service_id, "generation", "target") is activated
    assert "FOR UPDATE" in con.execute.call_args_list[0].args[0]
    if activated:
        assert len(con.execute.call_args_list) == 6
        assert "revision=revision+1" in con.execute.call_args.args[0]


def test_pending_and_selection_reads_are_bounded(manifest):
    store, con = manifest
    result(con, rows=[{"batch_id": "b"}])
    assert store.pending("s", "g", 1) == ["b"]
    assert con.execute.call_args.args[1] == ("s", "g", 1)
    with pytest.raises(ValueError):
        store.pending("s", "g", 1001)
    result(con)
    assert store.selected("s") is None
    result(con, {"generation": "g"})
    assert store.selected("s") == {"generation": "g"}
