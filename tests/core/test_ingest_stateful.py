"""Stateful property tests for the ingest metadata state machine.

Audit follow-up (R-9 tooling, deferred). Hypothesis-stateful explores
random sequences of metadata_db operations and checks that key
invariants hold after every rule. The unit tests in
``test_metadata_db_*.py`` cover happy-path correctness for individual
operations; this file pins the COMPOSITION of operations — the kind
of bug that only shows up when ``record_in_flight``,
``insert_ingested_files``, and ``clear_in_flight`` are interleaved
across a crash boundary.

Scope: SQLite-layer only. The full ingest tick includes filesystem
state (buffer Parquet exists / missing) that ``_recover_in_flight``
in backend/core/ingest.py keys on. That filesystem coupling is
exercised by the test_e2e_pipeline + test_ingest_partial_failure
suites; here we model the SQLite half so the rule explosion stays
tractable and shrinks land readable counter-examples.

Invariants pinned (checked after every rule):
  - DB's set of in_flight buffer-filenames matches our shadow.
  - DB's set of committed file-names matches our shadow.
  - The rollup ``ingested_files_summary.file_count`` matches the
    cardinality of committed files — catches a refactor that
    forgets to maintain the rollup in the same transaction.

Settings: ``max_examples=20`` keeps a single test under a few seconds
even with the full per-rule SQLite roundtrip. Default Hypothesis
behaviour shrinks on failure so a counter-example reads as the
minimal rule sequence that reproduces.
"""

from __future__ import annotations

import string

from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    consumes,
    initialize,
    invariant,
    rule,
)

from backend.core import metadata as metadata_db

# Small alphabets keep filenames human-readable in shrunk counter-examples.
_buffer_names = st.text(alphabet=string.ascii_lowercase, min_size=3, max_size=8).map(lambda s: f"batch_{s}.parquet")
_file_names = st.text(alphabet=string.ascii_lowercase + string.digits, min_size=3, max_size=8).map(
    lambda s: f"raw/2026-06-17/00/{s}.gz"
)
_row_counts = st.integers(min_value=0, max_value=10_000)
_file_sizes = st.one_of(st.none(), st.integers(min_value=0, max_value=1_000_000))

_file_manifest = st.tuples(_file_names, _row_counts, _file_sizes)
_file_manifests = st.lists(_file_manifest, min_size=1, max_size=4, unique_by=lambda t: t[0])


class IngestMetadataStateMachine(RuleBasedStateMachine):
    """Model the SQLite ingest-metadata half of the ingest tick.

    Each rule mirrors one production call site (record / promote /
    clear); the shadow state tracks what the DB SHOULD show after
    that call. Invariants then cross-check the DB against the shadow.

    Bundle('buffers') holds buffer-filenames that are currently
    in_flight — Hypothesis draws from it for the "commit" and "drop"
    rules so they only act on buffers that exist (avoiding the
    no-op-only state space).
    """

    buffers = Bundle("buffers")

    @initialize()
    def setup(self):
        # One service per state-machine instance. The autouse
        # ``isolate_metadata_db`` fixture sandboxes _DATA_DIR per pytest
        # function — but Hypothesis runs MANY state-machine instances
        # within a single pytest function, so the per-test SQLite file
        # is shared across instances. Tear down the service so each
        # instance starts from an empty DB; otherwise the second
        # instance's shadow is empty while the DB still carries rows
        # from the first instance → invariant violation → Hypothesis
        # raises FlakyStrategyDefinition.
        self.service_id = "svc-hypothesis-ingest"
        metadata_db.teardown(self.service_id)
        self.shadow_in_flight: dict[str, list[tuple[str, int, int | None]]] = {}
        self.shadow_ingested: set[str] = set()

    @rule(target=buffers, buffer=_buffer_names, files=_file_manifests)
    def record_in_flight(self, buffer, files):
        """Production call: ingest.py:_buffer_chunk() before writing parquet."""
        # Coerce to tuples so the equality compare with DB-roundtripped
        # tuples stays clean (lists vs tuples otherwise diff).
        files_t = [tuple(f) for f in files]
        metadata_db.record_in_flight(self.service_id, buffer, files_t)
        # Upsert semantics: re-record overwrites the prior manifest.
        self.shadow_in_flight[buffer] = files_t
        return buffer

    @rule(buffer=consumes(buffers))
    def promote_and_clear(self, buffer):
        """Happy-path commit: write_to_buffer succeeded, parquet on disk,
        cron promotes the manifest to ingested_files and clears the
        in_flight row. This is the production sequence inside
        ``commit_buffer`` + tombstone."""
        files = self.shadow_in_flight.pop(buffer, None)
        if files is None:
            # consumes() should have given us a live buffer; tolerate the
            # double-consume edge in case Hypothesis explores it.
            return
        metadata_db.insert_ingested_files(self.service_id, files)
        metadata_db.clear_in_flight(self.service_id, buffer)
        self.shadow_ingested.update(fname for (fname, _, _) in files)

    @rule(buffer=consumes(buffers))
    def crash_drop(self, buffer):
        """Recovery path for "crash before write_to_buffer": the
        in_flight row exists but the buffer parquet was never written.
        Recovery drops the row WITHOUT promoting — the files will be
        re-LISTed on the next ingest tick and re-ingested cleanly.

        Mirrors the ``else`` branch in ``_recover_in_flight``."""
        self.shadow_in_flight.pop(buffer, None)
        metadata_db.clear_in_flight(self.service_id, buffer)

    @rule(buffer=_buffer_names)
    def idempotent_clear(self, buffer):
        """``clear_in_flight`` on a buffer that doesn't exist is a no-op.
        Pinned because the production code calls it from a finally block
        regardless of upstream success — any future change that started
        raising on missing rows would break crash recovery."""
        # Only test "missing" — if the buffer happens to be in_flight,
        # this would behave like crash_drop and confuse the shadow.
        if buffer not in self.shadow_in_flight:
            metadata_db.clear_in_flight(self.service_id, buffer)

    @invariant()
    def in_flight_matches_shadow(self):
        db_state = {buffer: list(files) for buffer, files in metadata_db.list_in_flight(self.service_id)}
        assert set(db_state.keys()) == set(self.shadow_in_flight.keys()), (
            f"in_flight key drift: db={set(db_state.keys())!r} shadow={set(self.shadow_in_flight.keys())!r}"
        )
        for buffer, shadow_files in self.shadow_in_flight.items():
            assert db_state[buffer] == shadow_files, (
                f"in_flight payload drift for {buffer!r}: db={db_state[buffer]!r} shadow={shadow_files!r}"
            )

    @invariant()
    def ingested_matches_shadow(self):
        db_set = metadata_db.get_ingested_filenames(self.service_id)
        assert db_set == self.shadow_ingested, (
            f"ingested_files drift: db_only={db_set - self.shadow_ingested!r} "
            f"shadow_only={self.shadow_ingested - db_set!r}"
        )

    @invariant()
    def rollup_count_matches_shadow(self):
        """``ingested_files_summary.file_count`` must stay in lockstep
        with the actual ingested_files cardinality. The rollup is
        maintained in the same transaction; a refactor that split that
        transaction would surface here."""
        from backend.core.metadata import base as metadata_base

        con = metadata_base.get_con(self.service_id)
        row = con.execute(
            "SELECT file_count FROM ingested_files_summary WHERE source_name = ?",
            (self.service_id,),
        ).fetchone()
        rollup_count = row["file_count"] if row else 0
        assert rollup_count == len(self.shadow_ingested), (
            f"rollup file_count drift: rollup={rollup_count} shadow={len(self.shadow_ingested)}"
        )


# Hypothesis settings: 20 examples × ~5 rules each ≈ 100 SQLite roundtrips
# per test. That lands under 3s on a warm laptop. Bump max_examples
# locally with --hypothesis-seed when chasing a real-world flake.
#
# suppress_health_check: the autouse isolate_metadata_db fixture rebuilds
# the sandbox per pytest function; Hypothesis runs multiple state-machine
# instances inside one function, so each instance starts in a populated
# state from the prior instance's rules. We explicitly reset state in
# setup() so this is benign, but Hypothesis can't see that — silence the
# warning rather than let it pollute the CI output.
TestIngestMetadataStateMachine = IngestMetadataStateMachine.TestCase
TestIngestMetadataStateMachine.settings = settings(
    max_examples=20,
    deadline=None,  # SQLite-on-tmpfs jitter at <50ms blew past Hypothesis' 200ms default
    stateful_step_count=15,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
