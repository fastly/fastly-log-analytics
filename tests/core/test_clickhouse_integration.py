"""Real-engine gate: also runnable without pytest inside the backend image.

RUN_CLICKHOUSE_INTEGRATION=1 python -m unittest tests.core.test_clickhouse_integration -v
Only UUID-scoped placeholder services are removed. No ingest or raw-delete state
is touched. The default pytest suite skips this explicit Docker integration gate.
"""

from __future__ import annotations

import os
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from unittest.mock import patch
from uuid import uuid4

import duckdb

from backend.core.clickhouse_client import CLICKHOUSE_FACT_COLUMNS, ClickHouseClient, get_clickhouse_client
from backend.core.clickhouse_export import FosArtifacts, export_reader
from backend.core.clickhouse_manifest import Artifact, Dataset, PgManifest
from backend.core.clickhouse_publication import (
    DurableBatch,
    activate_generation,
    full_rebuild,
    publish_batch,
    readiness,
    replay_unpublished,
)
from backend.core.clickhouse_rows import build_batch_id, digest_rows
from backend.core.clickhouse_schema import create_clickhouse_schema, target_identity
from backend.repositories.clickhouse_dashboard import query_slice
from tests.core.clickhouse_fos_fixture import TinyFos


class AmbiguousClient:
    def __init__(self, client, partial=False):
        self.client = client
        self.partial = partial

    def execute(self, *args, **kwargs):
        return self.client.execute(*args, **kwargs)

    def insert_rows(self, table, columns, rows):
        self.client.insert_rows(table, columns, rows[:1] if self.partial else rows)
        raise RuntimeError("simulated lost acknowledgement")


@unittest.skipUnless(os.getenv("RUN_CLICKHOUSE_INTEGRATION") == "1", "explicit real-engine gate")
class PublicationIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = get_clickhouse_client()
        assert cls.client is not None
        create_clickhouse_schema(cls.client)
        create_clickhouse_schema(cls.client)
        cls.store = PgManifest()

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.store.pool.close()

    def setUp(self):
        self.service = "TestPrototype" + uuid4().hex
        rows = tuple(
            ("raw/placeholder.gz", n, "2026-09-01 00:00:00.123456", None, "192.0.2.1", "/", 2) for n in range(3)
        )
        self.dataset = Dataset(
            self.service,
            uuid4().hex,
            "test-catalog",
            "logs_fixture",
            1,
            datetime(2026, 9, 1, tzinfo=UTC),
            datetime(2026, 9, 2, tzinfo=UTC),
            datetime.now(UTC) + timedelta(hours=1),
            len(rows),
            digest_rows(rows),
        )
        content = sha256(b"fixture immutable bytes").hexdigest()
        self.artifact = Artifact(
            self.service,
            self.dataset.dataset_id,
            build_batch_id(self.service, self.dataset.dataset_id, content),
            "s3://test-bucket/clickhouse-prototype/fixture.parquet",
            content,
            digest_rows(rows),
            100,
            0,
            len(rows),
        )
        self.batch = DurableBatch(self.artifact, rows)
        self.store.seal(self.dataset, [self.artifact])
        self.generation = self.store.begin_generation(
            self.service, self.dataset.dataset_id, target_identity(self.client)
        )

    def tearDown(self):
        self.client.execute(
            "ALTER TABLE log_facts DELETE WHERE service_id={service:String} SETTINGS mutations_sync=1",
            {"service": self.service},
        )
        with self.store.transaction() as con:
            for table in (
                "clickhouse_service_selection",
                "clickhouse_publications",
                "clickhouse_generations",
                "clickhouse_artifacts",
                "clickhouse_datasets",
            ):
                con.execute(f"DELETE FROM {table} WHERE service_id=%s", (self.service,))

    def count(self, generation=None):
        return self.client.execute(
            "SELECT count() AS n FROM log_facts FINAL WHERE service_id={service:String} "
            "AND generation={generation:String}",
            {"service": self.service, "generation": generation or self.generation},
        )[0]["n"]

    def test_admin_status_and_preview_are_service_scoped_read_only(self):
        target = target_identity(self.client)
        store = PgManifest(bounded=True)
        before = store.admin_status(self.service, target)
        self.assertEqual(before["schema_version"], 1)
        self.assertEqual(before["publication_counts"], {"pending": 1, "claimed": 0, "failed": 0, "published": 0})
        self.assertIsNotNone(before["oldest_pending_age_seconds"])
        self.assertIsNone(before["active_generation"])
        empty = store.admin_status("TestMissing" + uuid4().hex, target)
        self.assertEqual(sum(empty["publication_counts"].values()), 0)
        self.assertIsNone(empty["oldest_pending_age_seconds"])
        self.assertIsNone(empty["last_published_at"])
        for generation in (None, self.generation):
            preview = store.replay_preview(
                self.service,
                dataset_id=None if generation else self.dataset.dataset_id,
                generation=generation,
                target=target,
                limit=1,
            )
            self.assertEqual(
                preview, {"dataset_id": self.dataset.dataset_id, "artifact_count": 1, "planned_artifacts": 1}
            )
        with self.assertRaises(LookupError):
            store.replay_preview(
                "TestOther", dataset_id=self.dataset.dataset_id, generation=None, target=target, limit=1
            )
        after = store.admin_status(self.service, target)
        self.assertEqual(before["publication_counts"], after["publication_counts"])
        self.assertEqual(self.count(), 0)

    def test_admin_status_retains_expired_selection_without_claiming_freshness(self):
        result = replay_unpublished(
            self.service,
            generation=self.generation,
            loader=lambda artifact: self.batch,
            store=self.store,
            client=self.client,
        )
        self.assertTrue(result.activated)
        target = target_identity(self.client)
        status = self.store.admin_status(self.service, target)
        self.assertEqual(status["publication_counts"]["published"], 1)
        self.assertIsNotNone(status["last_published_at"])
        self.assertIsNone(status["oldest_pending_age_seconds"])
        self.assertFalse(status["active_generation"]["expired"])
        self.assertTrue(status["active_generation"]["target_matches"])
        with self.store.transaction() as con:
            con.execute(
                "UPDATE clickhouse_datasets SET expires_at=clock_timestamp()-interval '1 second' WHERE service_id=%s",
                (self.service,),
            )
        status = self.store.admin_status(self.service, target)
        self.assertTrue(status["active_generation"]["expired"])
        self.assertEqual(status["expired_dataset_count"], 1)
        with self.assertRaises(ValueError):
            self.store.replay_preview(
                self.service, dataset_id=self.dataset.dataset_id, generation=None, target=target, limit=1
            )

    def test_observable_lag_excludes_terminal_expired_and_superseded_generations(self):
        # Force this fixture to be older than any bounded production backlog;
        # no other tenant's rows are changed.
        with self.store.transaction() as con:
            con.execute(
                "UPDATE clickhouse_publications SET created_at=clock_timestamp()-interval '100 years' "
                "WHERE service_id=%s",
                (self.service,),
            )
        for status in ("pending", "claimed", "failed"):
            with self.store.transaction() as con:
                con.execute("UPDATE clickhouse_publications SET status=%s WHERE service_id=%s", (status, self.service))
            self.assertGreater(self.store.publication_lag_seconds(), 3_000_000_000)
        for exclude, restore in (
            (
                "UPDATE clickhouse_publications SET status='published' WHERE service_id=%s",
                "UPDATE clickhouse_publications SET status='pending' WHERE service_id=%s",
            ),
            (
                "UPDATE clickhouse_generations SET status='retired' WHERE service_id=%s",
                "UPDATE clickhouse_generations SET status='building' WHERE service_id=%s",
            ),
            (
                "UPDATE clickhouse_service_selection SET revision=revision+1 WHERE service_id=%s",
                "UPDATE clickhouse_service_selection SET revision=revision-1 WHERE service_id=%s",
            ),
            (
                "UPDATE clickhouse_datasets SET expires_at=clock_timestamp()-interval '1 second' WHERE service_id=%s",
                "UPDATE clickhouse_datasets SET expires_at=clock_timestamp()+interval '1 hour' WHERE service_id=%s",
            ),
        ):
            with self.store.transaction() as con:
                con.execute(exclude, (self.service,))
            self.assertLess(self.store.publication_lag_seconds(), 3_000_000_000)
            with self.store.transaction() as con:
                con.execute(restore, (self.service,))

    def publish(self, client=None):
        return publish_batch(
            self.service, self.batch, generation=self.generation, store=self.store, client=client or self.client
        )

    def ready(self, client=None):
        return readiness(
            self.service,
            start=self.dataset.coverage_start,
            end=self.dataset.coverage_end,
            source_snapshot=1,
            catalog_identity="test-catalog",
            source_table="logs_fixture",
            store=self.store,
            client=client or self.client,
        )

    def test_accepted_insert_lost_ack_retry_exact_before_merge(self):
        with self.assertRaises(RuntimeError):
            self.publish(AmbiguousClient(self.client))
        self.assertEqual(self.count(), 3)
        self.assertEqual(self.ready().reason, "no_active_generation")
        self.assertFalse(self.store.activate(self.service, self.generation, target_identity(self.client)))
        self.assertEqual(self.publish().status, "published")
        self.assertEqual(self.count(), 3)
        self.assertTrue(activate_generation(self.service, self.generation, store=self.store, client=self.client))
        self.assertTrue(self.ready().eligible)
        self.assertEqual(self.publish().status, "not_claimed")

    def test_dashboard_final_aggregates_with_duplicate_insert_and_nulls(self):
        rows = [
            ("2026-09-01 00:00:00.000000", "US", "192.0.2.1", "/", 1),
            ("2026-09-01 00:00:00.123456", "", "192.0.2.1", "/", None),
            ("2026-09-01 00:00:00.123456", None, "192.0.2.1", "/", 2),
            ("2026-09-02 00:00:00.000000", "GB", "192.0.2.1", "/", 6),
            ("2026-09-02 00:00:00.000001", "after", "192.0.2.1", "/", 1),
            ("2026-09-01 00:00:00.000000", "null-url", "192.0.2.1", None, 1),
            ("2026-09-01 00:00:00.000000", "beacon", "192.0.2.1", "/rum-beacon?x=1", 1),
            ("2026-09-01 00:00:00.000000", "empty-ip", "", "/", 1),
            ("2026-09-01 00:00:00.000000", "null-ip", None, "/", 1),
        ]
        inserts = [
            (self.service, self.artifact.batch_id, "placeholder", n, self.generation, *r) for n, r in enumerate(rows)
        ]
        # A single part contains both copies: no wait for a background merge,
        # no global STOP MERGES that would interfere with the real corpus.
        self.client.insert_rows("log_facts", list(CLICKHOUSE_FACT_COLUMNS), inserts + inserts)
        for interval in ("1 second", "1 minute", "1 hour", "1 day"):
            result = query_slice(
                self.client,
                self.service,
                self.generation,
                self.dataset.coverage_start,
                self.dataset.coverage_end,
                interval,
            )
            self.assertEqual(result["country"]["total"], 3)
            self.assertEqual({r["value"]: r["count"] for r in result["country"]["top"]}, {"US": 1, "GB": 1})
            self.assertEqual([p["value"] for p in result["time_series"]], [3.0, 1.0])
            self.assertEqual(datetime.fromisoformat(result["time_series"][-1]["time"]), self.dataset.coverage_end)

    def test_partial_insert_retry_and_no_incomplete_visibility(self):
        with self.assertRaises(RuntimeError):
            self.publish(AmbiguousClient(self.client, partial=True))
        self.assertEqual(self.count(), 1)
        self.assertFalse(self.ready().eligible)
        self.assertEqual(self.publish().status, "published")
        self.assertEqual(self.count(), 3)

    def test_stale_claimant_is_fenced_and_expired_claim_cannot_finish(self):
        old = self.store.claim(self.service, self.generation, self.artifact.batch_id)
        self.assertIsNone(self.store.claim(self.service, self.generation, self.artifact.batch_id))
        with self.store.transaction() as con:
            con.execute(
                "UPDATE clickhouse_publications SET lease_until=clock_timestamp()-interval '1 second' "
                "WHERE service_id=%s",
                (self.service,),
            )
        self.assertFalse(
            self.store.finish(
                self.service, self.generation, self.artifact.batch_id, old, digest=self.artifact.canonical_digest
            )
        )
        new = self.store.claim(self.service, self.generation, self.artifact.batch_id)
        self.assertEqual(new, old + 1)
        self.assertFalse(
            self.store.finish(
                self.service, self.generation, self.artifact.batch_id, old, digest=self.artifact.canonical_digest
            )
        )

    def test_crash_after_insert_before_postgres_update(self):
        with patch.object(self.store, "finish", side_effect=SystemExit):
            with self.assertRaises(SystemExit):
                self.publish()
        self.assertEqual(self.count(), 3)
        self.assertIsNone(self.store.selected(self.service))
        with self.store.transaction() as con:
            con.execute(
                "UPDATE clickhouse_publications SET lease_until=clock_timestamp() WHERE service_id=%s", (self.service,)
            )
        self.assertEqual(self.publish().status, "published")
        self.assertEqual(self.count(), 3)

    def test_full_rebuild_does_not_skip_old_publications_or_empty_target(self):
        self.publish()
        self.assertTrue(activate_generation(self.service, self.generation, store=self.store, client=self.client))
        self.client.execute(
            "ALTER TABLE log_facts DELETE WHERE service_id={service:String} SETTINGS mutations_sync=1",
            {"service": self.service},
        )
        self.assertEqual(self.ready().reason, "target_unverified")
        result = full_rebuild(
            self.service,
            self.dataset.dataset_id,
            loader=lambda artifact: self.batch,
            store=self.store,
            client=self.client,
        )
        self.assertNotEqual(result.generation, self.generation)
        self.assertTrue(result.activated)
        self.assertEqual(result.published, 1)
        self.assertEqual(self.count(result.generation), 3)
        self.assertTrue(self.ready().eligible)

    def test_replacement_table_uuid_rejected_and_rebuilds(self):
        self.publish()
        activate_generation(self.service, self.generation, store=self.store, client=self.client)
        # This database name is internally generated, not operator/user SQL.
        name = "test_prototype_" + uuid4().hex
        self.client.execute(f"CREATE DATABASE {name}")
        other = ClickHouseClient(replace(self.client._settings, database=name))
        identity = None
        try:
            create_clickhouse_schema(other)
            identity = target_identity(other)
            self.assertEqual(self.ready(other).reason, "target_unverified")
            result = full_rebuild(
                self.service,
                self.dataset.dataset_id,
                loader=lambda artifact: self.batch,
                store=self.store,
                client=other,
            )
            self.assertTrue(result.activated)
            self.assertTrue(self.ready(other).eligible)
        finally:
            other.close()
            self.client.execute(f"DROP DATABASE {name}")
            if identity:
                with self.store.transaction() as con:
                    con.execute("DELETE FROM clickhouse_schema_state WHERE target_identity=%s", (identity,))

    def test_late_complete_generation_cannot_replace_new_selection(self):
        second = self.store.begin_generation(self.service, self.dataset.dataset_id, target_identity(self.client))
        self.publish()
        publish_batch(self.service, self.batch, generation=second, store=self.store, client=self.client)
        self.assertTrue(activate_generation(self.service, second, store=self.store, client=self.client))
        self.assertFalse(activate_generation(self.service, self.generation, store=self.store, client=self.client))
        self.assertEqual(self.store.selected(self.service)["generation"], second)

    def test_real_transaction_rolls_back_and_manifest_is_immutable(self):
        with self.assertRaises(RuntimeError), self.store.transaction() as con:
            con.execute("UPDATE clickhouse_datasets SET expected_rows=0 WHERE service_id=%s", (self.service,))
            raise RuntimeError("rollback")
        self.assertEqual(self.store.dataset(self.service, self.dataset.dataset_id).expected_rows, 3)
        with self.assertRaises(Exception):
            self.store.seal(self.dataset, [self.artifact])
        self.assertEqual(len(self.store.artifacts(self.service, self.dataset.dataset_id)), 1)

    def test_wrong_content_same_count_is_not_published(self):
        class WrongClient(AmbiguousClient):
            def insert_rows(inner, table, columns, rows):
                wrong = [(*row[:6], "WRONG", *row[7:]) for row in rows]
                inner.client.insert_rows(table, columns, wrong)

        with self.assertRaises(RuntimeError):
            self.publish(WrongClient(self.client))
        self.assertEqual(self.count(), 3)
        self.assertFalse(self.store.activate(self.service, self.generation, target_identity(self.client)))

    def test_exporter_seals_real_postgres_then_rebuilds_from_tiny_fos(self):
        objects = FosArtifacts({"service_id": self.service, "bucket": "test-bucket"}, client=TinyFos())
        dataset = replace(self.dataset, dataset_id=uuid4().hex)
        with duckdb.connect() as con:
            reader = con.execute(
                "SELECT 'source' AS source_identity, TIMESTAMP '2026-09-01' AS timestamp, "
                "NULL::VARCHAR AS country, '192.0.2.1' AS ip, '/' AS url, 1::BIGINT AS conn_requests FROM range(5)"
            ).fetch_record_batch(2)
            exported = export_reader(reader, dataset=dataset, objects=objects, store=self.store, batch_rows=2)
        self.assertEqual(exported.expected_rows, 5)
        partial = full_rebuild(
            self.service, exported.dataset_id, loader=objects.load, limit=1, store=self.store, client=self.client
        )
        self.assertEqual(partial.published, 1)
        self.assertFalse(partial.activated)
        self.assertIsNone(self.store.selected(self.service))
        complete = replay_unpublished(
            self.service,
            generation=partial.generation,
            loader=objects.load,
            limit=2,
            store=self.store,
            client=self.client,
        )
        self.assertTrue(complete.activated)
        self.assertEqual(self.count(complete.generation), 5)

    def test_expiry_rejects_claim_replay_and_readiness(self):
        self.publish()
        activate_generation(self.service, self.generation, store=self.store, client=self.client)
        with self.store.transaction() as con:
            con.execute(
                "UPDATE clickhouse_datasets SET expires_at=clock_timestamp() WHERE service_id=%s", (self.service,)
            )
        self.assertEqual(self.ready().reason, "expired_or_unsupported_dataset")
        with self.assertRaisesRegex(ValueError, "expired"):
            full_rebuild(
                self.service,
                self.dataset.dataset_id,
                loader=lambda artifact: self.batch,
                store=self.store,
                client=self.client,
            )

    def test_snapshot_and_coverage_mismatch_never_eligible(self):
        self.publish()
        activate_generation(self.service, self.generation, store=self.store, client=self.client)
        kwargs = dict(
            start=self.dataset.coverage_start,
            end=self.dataset.coverage_end,
            source_snapshot=2,
            catalog_identity="test-catalog",
            source_table="logs_fixture",
            store=self.store,
            client=self.client,
        )
        self.assertEqual(readiness(self.service, **kwargs).reason, "source_snapshot_mismatch")
        kwargs["start"] -= timedelta(days=1)
        self.assertEqual(readiness(self.service, **kwargs).reason, "outside_coverage")


if __name__ == "__main__":
    unittest.main()
