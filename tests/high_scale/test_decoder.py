import gzip
import json

from backend.high_scale.archive_models import ArchiveSourceObject
from backend.high_scale.decoder import decode_source_object


def _source() -> ArchiveSourceObject:
    return ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:source", 100, "v7")


def test_decode_assigns_stable_identity_and_preserves_duplicate_payloads() -> None:
    payload = gzip.compress(b'{"url":"/same","status":200}\n{"url":"/same","status":200}\n')

    result = decode_source_object(_source(), payload, transform_version="normalize.v1")

    assert result.decoded_rows == 2
    assert result.accepted_rows == 2
    assert result.quarantined_rows == 0
    assert result.events[0]["event_id"] != result.events[1]["event_id"]
    assert result.events[0]["line_ordinal"] == 0
    assert result.events[1]["line_ordinal"] == 1


def test_decode_replays_with_identical_event_ids() -> None:
    payload = gzip.compress(b'{"url":"/same","status":200}\n')

    first = decode_source_object(_source(), payload, transform_version="normalize.v1")
    second = decode_source_object(_source(), payload, transform_version="normalize.v1")

    assert first.events[0]["event_id"] == second.events[0]["event_id"]


def test_decode_quarantines_malformed_records_losslessly() -> None:
    malformed = b'{"url":"/ok"}\nnot-json\n["not","an","object"]\n'

    result = decode_source_object(_source(), gzip.compress(malformed), transform_version="normalize.v1")

    assert result.accepted_rows == 1
    assert result.quarantined_rows == 2
    assert [item.raw_line for item in result.dead_letters] == [b"not-json", b'["not","an","object"]']
    assert json.loads(result.events[0]["raw_json"])["url"] == "/ok"
