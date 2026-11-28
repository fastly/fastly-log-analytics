"""scripts/lib/gce_ssh.sh, readiness probes, admin TLS, CLI, prom, streaming fixture, profile_insights, check_osv, port_forward, and run_contract_backend tests."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import re
import subprocess
import sys
import tarfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from scripts.lib import log_factory as lf

REPO = Path(__file__).resolve().parents[2]
LIB = REPO / "scripts" / "lib" / "gce_ssh.sh"
LAUNCHER = REPO / "scripts" / "run_contract_backend.py"
SCRIPT_PATH = REPO / "scripts" / "check_osv.py"
TRACKED_SCRIPTS = [
    REPO / "scripts" / "backup_service_configs.sh",
    REPO / "scripts" / "dev" / "snapshot_prod_to_dev.sh",
    REPO / "scripts" / "dev" / "sync-from-remote.sh",
]

HOSTILE = [
    "plain",
    "with space",
    "it's",
    'dq"uote',
    "semi;colon",
    "$(touch CANARY)",
    "`touch CANARY`",
    "a && touch CANARY",
    "pipe | touch CANARY",
    "new\nline",
    "back\\slash",
    "glob*?[x]",
    "$HOME ${HOME}",
    "-leading-dash",
    "",
    "~/tilde",
]

STUB = """#!{python}
import json, os, shutil, subprocess, sys
argv = sys.argv[1:]
with open(os.environ["STUB_LOG"], "a") as fh:
    fh.write(json.dumps(argv) + "\\n")
if argv[:2] == ["compute", "ssh"]:
    cmd = [a for a in argv if a.startswith("--command=")][0][len("--command="):]
    sys.exit(subprocess.call(["bash", "-c", cmd]))
if argv[:2] == ["storage", "cp"]:
    shutil.copy(argv[2], os.environ["STUB_UPLOAD"])
    sys.exit(0)
if argv[:2] == ["storage", "ls"]:
    print("2048  " + argv[-1])
    sys.exit(0)
sys.exit(97)
"""

DUMP = """import os, sys
with open(os.environ["DUMP_OUT"], "wb") as fh:
    fh.write(b"\\0".join(a.encode() for a in sys.argv[1:]))
"""


@pytest.fixture
def gce_stub(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    gcloud = bindir / "gcloud"
    gcloud.write_text(STUB.format(python=sys.executable))
    gcloud.chmod(0o755)
    sudo = bindir / "sudo"
    sudo.write_text('#!/bin/sh\nif [ "$1" = "-n" ]; then shift; fi\nexec "$@"\n')
    sudo.chmod(0o755)
    log = tmp_path / "calls.jsonl"
    env = {
        "PATH": f"{bindir}:{os.environ['PATH']}",
        "HOME": str(tmp_path),
        "STUB_LOG": str(log),
        "STUB_UPLOAD": str(tmp_path / "uploaded.tar.gz"),
        "DUMP_OUT": str(tmp_path / "dump.bin"),
    }

    def calls() -> list[list[str]]:
        return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

    return env, calls, tmp_path


def run_bash(body: str, env: dict[str, str], cwd: Path, *args: str) -> subprocess.CompletedProcess:
    cmd = ["bash", "-c", f'source "{LIB}"\n{body}']
    if args:
        cmd.extend(["_", *args])
    return subprocess.run(cmd, env=env, cwd=cwd, text=True, capture_output=True, timeout=30, check=False)


def test_exact_argv_for_simple_and_hostile_commands(gce_stub):
    env, calls, tmp = gce_stub
    r = run_bash("gce_ssh_run my-vm us-central1-a --quiet -- echo /a b", env, tmp)
    assert r.returncode == 0, r.stderr
    assert calls() == [["compute", "ssh", "my-vm", "--zone=us-central1-a", "--quiet", "--command=echo /a b"]]

    r2 = run_bash("gce_ssh_run vm z1 -- echo 'a b;$(x)'", env, tmp)
    assert r2.returncode == 0, r2.stderr
    assert calls()[-1] == ["compute", "ssh", "vm", "--zone=z1", r"--command=echo a\ b\;\$\(x\)"]


@pytest.mark.parametrize("value", HOSTILE)
def test_run_roundtrips_hostile_argument_without_executing(gce_stub, value):
    env, calls, tmp = gce_stub
    dump = tmp / "dump.py"
    dump.write_text(DUMP)
    r = run_bash('gce_ssh_run vm z1 -- "$1" "$2" "$3" "$4"', env, tmp, sys.executable, str(dump), value, value)
    assert r.returncode == 0, r.stderr
    assert (tmp / "dump.bin").read_bytes() == b"\0".join([value.encode(), value.encode()])
    assert not (tmp / "CANARY").exists()
    assert len(calls()) == 1


@pytest.mark.parametrize("value", HOSTILE)
def test_script_binds_hostile_args_as_positionals(gce_stub, value):
    env, calls, tmp = gce_stub
    script = 'printf "%s\\0" "$#" "$@" > "$DUMP_OUT"'
    r = run_bash('gce_ssh_script vm z1 --quiet -- "$1" "$2" "$3"', env, tmp, script, value, "second " + value)
    assert r.returncode == 0, r.stderr
    got = (tmp / "dump.bin").read_bytes().split(b"\0")[:-1]
    assert got == [b"2", value.encode(), ("second " + value).encode()]
    assert not (tmp / "CANARY").exists()
    (argv,) = calls()
    assert argv[:5] == ["compute", "ssh", "vm", "--zone=z1", "--quiet"]
    assert argv[5].startswith("--command=bash -c ")


@pytest.mark.parametrize(
    "bad", ["", "-oProxyCommand=x", "vm name", "vm;touch CANARY", "vm$(x)", "vm\nx", ".hidden", "a/b"]
)
@pytest.mark.parametrize("slot", ["instance", "zone"])
def test_rejects_unsafe_instance_or_zone(gce_stub, bad, slot):
    env, calls, tmp = gce_stub
    inst, zone = (bad, "z1") if slot == "instance" else ("vm", bad)
    assert run_bash('gce_ssh_run "$1" "$2" -- true', env, tmp, inst, zone).returncode == 2
    assert calls() == []


def test_rejects_non_flag_before_separator_and_missing_command_and_passes_exit(gce_stub):
    env, calls, tmp = gce_stub
    for cmd in (
        "gce_ssh_run vm z1 oops -- true",
        "gce_ssh_run vm z1 --quiet",
        "gce_ssh_run vm z1 --quiet --",
        "gce_ssh_script vm z1 --",
    ):
        assert run_bash(cmd, env, tmp).returncode == 2
    assert calls() == []

    r = run_bash("gce_ssh_script vm z1 -- 'echo out; exit 7'", env, tmp)
    assert (r.returncode, r.stdout) == (7, "out\n")


def test_no_ssh_command_string_building_or_eval_in_lib():
    text = LIB.read_text()
    assert not re.search(r"\beval\b", re.sub(r"^\s*#.*$", "", text, flags=re.M))


@pytest.mark.parametrize("script", TRACKED_SCRIPTS, ids=lambda p: p.name)
def test_tracked_scripts_use_the_shared_helper(script):
    code = "\n".join(line for line in script.read_text().splitlines() if not line.lstrip().startswith("#"))
    assert "source" in code and "lib/gce_ssh.sh" in code
    assert not re.search(r"^\s*(if\s+!?\s*)?(\w+=\"?\$\()?gcloud compute ssh\b", code, re.M)
    assert "--command=" not in code


def test_backup_script_end_to_end_with_hostile_paths(gce_stub):
    env, calls, tmp = gce_stub
    configs = tmp / "it's a dir; $(touch CANARY)" / "configs"
    configs.mkdir(parents=True)
    (configs / "svc.json").write_bytes(os.urandom(4096))
    marker = tmp / "mark dir" / "m'arker.json"
    env = {
        **env,
        "BACKUP_BUCKET": "gs://b",
        "BACKUP_INSTANCE": "vm-1",
        "BACKUP_ZONE": "us-central1-a",
        "BACKUP_CONFIGS_DIR": str(configs),
        "BACKUP_MARKER_VM_PATH": str(marker),
    }
    r = subprocess.run(
        ["bash", str(REPO / "scripts" / "backup_service_configs.sh")],
        env=env,
        cwd=tmp,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert r.returncode == 0, r.stderr + r.stdout
    assert not (tmp / "CANARY").exists()
    with tarfile.open(tmp / "uploaded.tar.gz") as tf:
        assert [m.name for m in tf.getmembers() if m.isfile()] == ["configs/svc.json"]
    assert json.loads(marker.read_text())["remote"].startswith("gs://b/configs/")
    ssh_calls = [c for c in calls() if c[:2] == ["compute", "ssh"]]
    assert len(ssh_calls) == 2
    assert all(c[2] == "vm-1" and c[3] == "--zone=us-central1-a" for c in ssh_calls)


# ── Merged from tests/scripts/test_scale_and_freshness_harness.py ──

NOW = datetime(2026, 10, 19, 12, 0, 0, tzinfo=UTC)

RECORD_KEYS = {
    "service_id",
    "timestamp",
    "geo_city",
    "geo_region",
    "geo_country_code",
    "server_pop",
    "browser",
    "os",
    "device",
    "rum_cid",
    "req_id",
    "fastly_req_id",
    "request_event_id",
    "url",
    "rum_body",
}


def _digest(obj: object) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode()).hexdigest()


def test_records_are_deterministic_for_a_seed_and_match_golden_digest() -> None:
    a = lf.generate_rum_records("svc", 200, NOW, random.Random(7))
    b = lf.generate_rum_records("svc", 200, NOW, random.Random(7))
    c = lf.generate_rum_records("svc", 200, NOW, random.Random(8))
    assert a == b and a != c
    assert _digest(a) == "884741d052c6a872939dce4c7b001739062f79dde8d457a4ec4e490d49555614"


def test_record_shape_matches_ingest_contract() -> None:
    records = lf.generate_rum_records("svc-1", 300, NOW, random.Random(1))
    assert len({r["timestamp"] for r in records}) == 300
    assert records[0]["timestamp"] == "2026-10-19T12:00:00.000Z"
    assert records[1]["timestamp"] == "2026-10-19T11:59:59.950Z"
    kinds: set[str] = set()
    for rec in records:
        assert set(rec) == RECORD_KEYS and rec["service_id"] == "svc-1"
        assert rec["req_id"].startswith("req-") and len(rec["req_id"]) == 4 + 16
        assert rec["fastly_req_id"] == rec["request_event_id"] == rec["req_id"]
        assert rec["rum_cid"].startswith("cid-") and rec["url"].startswith("http://localhost/")
        body = json.loads(rec["rum_body"])
        assert body["meta"]["page"]["url"] == rec["url"] and body["meta"]["os"]["name"] == rec["os"]
        assert body["meta"]["browser"] == {"name": rec["browser"], "mobile": rec["device"] == "Mobile"}
        (kind,) = set(body) - {"meta"}
        kinds.add(kind)
    assert kinds == {"measurements", "events", "exceptions"}
    assert len({r["req_id"] for r in records}) == 300


def test_vitals_ratings_follow_thresholds() -> None:
    profile = lf.FILE_RUM_PROFILE
    cases = [
        ("LCP", 2500, "good"),
        ("LCP", 4000, "needs_improvement"),
        ("LCP", 4001, "poor"),
        ("CLS", 0.1, "good"),
        ("CLS", 0.25, "needs_improvement"),
        ("CLS", 0.26, "poor"),
        ("INP", 200, "good"),
        ("INP", 500, "needs_improvement"),
        ("INP", 501, "poor"),
        ("TTFB", 800, "good"),
        ("TTFB", 801, "poor"),
        ("FCP", 100, "good"),
        ("FCP", 101, "poor"),
    ]
    for metric, value, rating in cases:
        assert lf.rum_rating(metric, value, profile) == rating
    assert lf.rum_rating("FID", 700, lf.HARNESS_RUM_PROFILE) == "good"


def test_harness_beacon_url_carries_vcl_identity_parameters_and_is_seeded() -> None:
    url = lf.build_rum_beacon_url("https://logs.example.com/", random.Random(3))
    assert url == lf.build_rum_beacon_url("https://logs.example.com", random.Random(3))
    parsed = urlparse(url)
    assert parsed.path == "/rum-beacon"
    qs = parse_qs(parsed.query)
    assert set(qs) == {"rum_metric_name", "rum_metric_value", "rum_cid", "cid", "req", "rum_pathname"}
    assert qs["cid"] == qs["rum_cid"]
    assert qs["req"][0] == "req-" + qs["cid"][0].removeprefix("cid-")
    assert len(qs["req"][0]) == 4 + 32


def test_harness_beacon_body_golden_and_shape() -> None:
    url = "https://h.example.com/rum-beacon?rum_metric_name=LCP&rum_metric_value=3500&rum_pathname=%2Fdashboard"
    bodies = [lf.build_rum_beacon_body(url, random.Random(s), NOW) for s in range(40)]
    assert bodies == [lf.build_rum_beacon_body(url, random.Random(s), NOW) for s in range(40)]
    assert _digest(bodies) == "4aaf0a1622b62558e7684d394595ce31be0232b4a2517ece00d8485dc184b2b4"
    seen = set()
    for body in bodies:
        assert body["meta"]["page"]["url"] == "https://h.example.com/dashboard"
        assert body["meta"]["browser"]["name"] in lf.HARNESS_RUM_PROFILE.browsers
        (kind,) = set(body) - {"meta"}
        seen.add(kind)
        if kind == "measurements":
            (m,) = body["measurements"]
            assert m["type"] == "web-vitals" and m["values"] == {"LCP": 3500.0}
            assert m["context"]["rating"] == "needs_improvement"
        if kind == "exceptions":
            assert body["exceptions"][0]["stacktrace"]["frames"][0]["filename"] == "static/chunks/main.js"
    assert seen == {"measurements", "events", "exceptions"}


def test_beacon_body_defaults_when_query_is_missing_or_bad() -> None:
    body = lf.build_rum_beacon_body("https://h.example.com/rum-beacon?rum_metric_value=abc", random.Random(0), NOW)
    assert body["meta"]["page"]["url"] == "https://h.example.com/"
    if "measurements" in body:
        assert body["measurements"][0]["values"] == {"LCP": 1200.0}


def test_source_ip_is_seeded_and_in_range() -> None:
    assert lf.random_source_ip(random.Random(5)) == lf.random_source_ip(random.Random(5))
    rng = random.Random(9)
    for _ in range(200):
        a, b, c, d = (int(x) for x in lf.random_source_ip(rng).split("."))
        assert 12 <= a <= 223 and all(1 <= x <= 254 for x in (b, c, d))


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_global_random_state_is_untouched_when_rng_is_injected(seed: int) -> None:
    random.seed(1234)
    expected = random.random()
    random.seed(1234)
    lf.generate_rum_records("svc", 50, NOW, random.Random(seed))
    lf.build_rum_beacon_url("https://x.example.com", random.Random(seed))
    assert random.random() == expected


# ── Merged from tests/scripts/test_section_measurements.py ──

from copy import deepcopy

import pytest

from scripts.lib.section_measurements import describe_response


def test_controlled_sampling_flag_does_not_bypass_prerequisite_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts.dev import section_sweep

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("live transport must not run")

    monkeypatch.setattr(section_sweep, "_post", forbidden)
    assert section_sweep.main(["--env", "local-standard", "--controlled-cache-pair"]) == 2


def test_follower_sections_are_not_current_execution() -> None:
    result = describe_response(
        {
            "_response_cache": {
                "state": "unknown",
                "ttl_seconds": 15,
                "prewarmer": False,
                "single_flight": "follower",
                "wait_ms": 8,
            },
            "_section_timings": [{"section": "query", "time_ms": 5, "current_execution": True, "source": "fact"}],
        }
    )
    component = result["components"][0]
    assert component["single_flight"] == "follower"
    assert component["wait_ms"] == 8
    assert component["sections"][0]["current_execution"] is False
    assert "shared_execution" in component["gaps"]


def test_self_scoped_health_hit_does_not_suppress_independent_shielding() -> None:
    child = {
        "_response_cache": {"state": "miss", "prewarmer": False},
        "_section_timings": [{"section": "shielding", "time_ms": 10, "current_execution": True, "source": "live"}],
    }
    payload = {"_response_cache": {"state": "hit", "scope": "self", "prewarmer": False}, "shielding_analysis": child}
    result = describe_response(payload)
    assert result["components"][1]["sections"][0]["current_execution"] is True
    enclosing = describe_response({"_is_cached": True, "health": payload})
    assert enclosing["components"][2]["sections"][0]["current_execution"] is False


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.waits: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.now += seconds


def _component(state: str, **extra: object) -> dict:
    return {"_response_cache": {"state": state, "ttl_seconds": 15, "prewarmer": False, **extra}}


@pytest.mark.parametrize("rows", [[], [{"pop": "PRIVATE_ROW_VALUE", "requests": 7}]])
@pytest.mark.parametrize("state", ["bypass", "hit"])
def test_list_component_remains_unknown_without_copying_rows(rows: list[dict], state: str) -> None:
    result = describe_response({"_response_cache": {"state": state}, "pop_health": rows})
    components = {row["component"]: row for row in result["components"]}
    pop = components["pop_health"]
    assert pop["cache_state"] == "unknown"
    assert pop["sections"] == []
    assert pop["reader_coverage"] == []
    assert pop["history"] is None
    assert "component_observation_envelope_missing" in pop["gaps"]
    assert "response_cache_unknown" in pop["gaps"]
    assert pop["execution_reused"] is (state == "hit")
    assert "PRIVATE_ROW_VALUE" not in str(result)


@pytest.mark.parametrize("child", [None, "absent"])
def test_absent_list_component_is_not_fabricated(child: object) -> None:
    payload = {"pop_health": child} if child is None else {}
    assert [row["component"] for row in describe_response(payload)["components"]] == ["response"]


@pytest.mark.parametrize("reuse", ["none", "hit", "follower"])
def test_list_component_uses_owned_sidecar_without_promoting_reused_execution(reuse: str) -> None:
    payload = {
        "_response_cache": {"state": "bypass", "prewarmer": False},
        "pop_health": [{"pop": "PRIVATE_ROW_VALUE"}],
        "_component_observations": {
            "pop_health": {
                "_response_cache": {"state": "bypass", "prewarmer": False},
                "_section_timings": [
                    {"section": "pop_health", "time_ms": 3, "source": "live", "current_execution": True}
                ],
            }
        },
    }
    if reuse == "hit":
        payload["_response_cache"]["state"] = "hit"
    elif reuse == "follower":
        payload["_response_cache"].update(state="unknown", single_flight="follower")
    result = describe_response(payload)
    pop = next(row for row in result["components"] if row["component"] == "pop_health")
    assert pop["cache_state"] == "bypass"
    assert pop["lookup_current"] is (reuse == "none")
    assert pop["sections"][0]["current_execution"] is (reuse == "none")
    assert "component_observation_envelope_missing" not in pop["gaps"]
    assert "reader_coverage_missing" in pop["gaps"]
    assert "history_evidence_missing" in pop["gaps"]
    assert "PRIVATE_ROW_VALUE" not in str(result)


def test_list_component_prevents_false_composite_cache_pair_success() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    answers = iter(["hit", "miss", "hit"])

    def request(body: dict, timeout: float) -> dict:
        return {
            "_response_cache": {"state": "bypass", "prewarmer": False},
            "health": _component(next(answers)),
            "pop_health": [],
        }

    result = sample_cache_pair(request, {}, max_seconds=60, monotonic=clock.monotonic, sleep=clock.sleep)
    assert result["status"] == "incomplete"
    assert result["components"]["health"]["status"] == "observed"
    assert result["components"]["pop_health"]["status"] == "incomplete"
    assert "miss_hit_pair_unobserved" in result["gaps"]


def test_expiry_sampler_keeps_payload_exact_and_requires_miss_then_hit() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    payload = {"anchor": "2026-10-22T00:00:00Z", "filters": {"status": {"values": ["200"]}}}
    expected = deepcopy(payload)
    calls = []
    answers = iter([_component("hit"), _component("miss"), _component("hit")])

    def request(body: dict, timeout: float) -> dict:
        calls.append((deepcopy(body), timeout, clock.now))
        body.clear()
        return next(answers)

    result = sample_cache_pair(request, payload, max_seconds=60, monotonic=clock.monotonic, sleep=clock.sleep)
    assert result["status"] == "observed"
    assert result["components"]["response"] == {"status": "observed", "miss_sample": 2, "hit_sample": 3}
    assert [call[0] for call in calls] == [expected] * 3
    assert calls[1][2] >= 15
    assert payload == expected
    assert result["database_cache_state"] == "unknown"


@pytest.mark.parametrize(
    "answer", [_component("unknown"), _component("miss", single_flight="follower"), {"_is_cached": False}]
)
def test_unknown_and_shared_results_cannot_establish_a_pair(answer: dict) -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    result = sample_cache_pair(
        lambda body, timeout: answer, {}, max_seconds=30, monotonic=clock.monotonic, sleep=clock.sleep
    )
    assert result["status"] == "incomplete"
    assert len(result["samples"]) <= 4
    assert result["gaps"]


def test_repopulation_and_independent_components_do_not_manufacture_a_pair() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    answers = iter(
        [
            {"aggregates": _component("hit"), "top_bots": _component("hit")},
            {"aggregates": _component("miss"), "top_bots": _component("hit")},
            {"aggregates": _component("hit"), "top_bots": _component("hit")},
            {"aggregates": _component("hit"), "top_bots": _component("hit")},
        ]
    )
    result = sample_cache_pair(
        lambda body, timeout: next(answers), {}, max_seconds=45, monotonic=clock.monotonic, sleep=clock.sleep
    )
    assert result["components"]["aggregates"]["status"] == "observed"
    assert result["components"]["top_bots"]["status"] == "incomplete"
    assert result["status"] == "incomplete"


def test_bypass_is_not_a_miss_and_failures_are_safe() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    bypass = sample_cache_pair(
        lambda body, timeout: _component("bypass"), {}, monotonic=clock.monotonic, sleep=clock.sleep
    )
    assert bypass["status"] == "not_applicable"
    assert bypass["components"]["response"]["status"] == "bypass"

    def fail(body: dict, timeout: float) -> dict:
        raise OSError("secret URL or cookie")

    failed = sample_cache_pair(fail, {}, monotonic=clock.monotonic, sleep=clock.sleep)
    assert failed["status"] == "incomplete"
    assert failed["samples"][0]["error"] == "OSError"
    assert "secret" not in str(failed)


def test_initial_miss_is_only_a_baseline_until_ttl_expiry() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    answers = iter([_component("miss"), _component("hit"), _component("miss"), _component("hit")])
    result = sample_cache_pair(
        lambda body, timeout: next(answers),
        {},
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    assert result["status"] == "observed"
    assert result["components"]["response"] == {"status": "observed", "miss_sample": 3, "hit_sample": 4}
    assert sum(clock.waits) >= 15


def test_historical_child_hit_cannot_complete_a_current_miss_pair() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    answers = iter(
        [
            {"_response_cache": {"state": "bypass", "prewarmer": False}, "aggregates": _component("miss")},
            {
                "_response_cache": {"state": "hit", "ttl_seconds": 15, "prewarmer": False},
                "aggregates": _component("hit"),
            },
        ]
    )
    result = sample_cache_pair(
        lambda body, timeout: next(answers),
        {},
        max_samples=2,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    assert result["components"]["aggregates"]["status"] == "incomplete"
    assert "hit_sample" not in result["components"]["aggregates"]


def test_expiry_sampler_honors_attempt_and_wall_time_bounds() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    timeouts = []

    def request(body: dict, timeout: float) -> dict:
        timeouts.append(timeout)
        clock.now += timeout
        return _component("miss")

    result = sample_cache_pair(request, {}, max_seconds=10, monotonic=clock.monotonic, sleep=clock.sleep)
    assert timeouts == [10]
    assert result["status"] == "incomplete"
    assert clock.now == 10
    with pytest.raises(ValueError):
        sample_cache_pair(request, {}, max_seconds=float("nan"))


# ── Merged from test_section_measurements.py ──────────────────────────────────────────────────────

import math
from pathlib import Path

import pytest

from scripts.lib.section_measurements import (
    distribution,
    rank_sections,
    visible_results_budget_ms,
)


def test_false_cache_marker_does_not_establish_a_miss() -> None:
    result = describe_response({"_is_cached": False, "_section_timings": []})
    assert result["components"][0]["cache_state"] == "unknown"
    assert "response_cache_unknown" in result["components"][0]["gaps"]
    assert "section_timings_missing" in result["components"][0]["gaps"]


@pytest.mark.parametrize("state", ["hit", "miss", "bypass", "unknown"])
def test_explicit_cache_outcomes_are_separate_from_source(state: str) -> None:
    result = describe_response(
        {
            "_response_cache": {"state": state, "reason": "lookup", "prewarmer": False},
            "_section_timings": [{"section": "summary", "time_ms": 12, "source": "projection"}],
        }
    )
    component = result["components"][0]
    assert component["cache_state"] == state
    assert component["prewarmer"] is False
    assert component["sections"][0]["source"] == "projection"
    assert component["sections"][0]["current_execution"] is (False if state == "hit" else None)


def test_nested_components_keep_independent_cache_states_and_cached_timings() -> None:
    result = describe_response(
        {
            "_is_cached": False,
            "aggregates": {
                "_is_cached": True,
                "_section_timings": [{"section": "count_rollup", "time_ms": 900}],
            },
            "top_bots": {"_section_timings": [{"section": "ua_query", "elapsed_ms": 25}]},
        }
    )
    components = {row["component"]: row for row in result["components"]}
    assert set(components) == {"response", "aggregates", "top_bots"}
    assert components["response"]["cache_state"] == "unknown"
    assert components["aggregates"]["cache_state"] == "hit"
    assert components["aggregates"]["sections"][0]["current_execution"] is False
    assert components["top_bots"]["sections"][0]["ms"] == 25
    assert components["top_bots"]["sections"][0]["source"] == "unknown"
    assert "historical_or_unverified_timings" in components["aggregates"]["gaps"]


@pytest.mark.parametrize("value", [None, True, -1, math.nan, math.inf, "12"])
def test_invalid_durations_are_unknown_not_zero(value: object) -> None:
    section = describe_response({"_section_timings": [{"section": "scan", "time_ms": value}]})["components"][0][
        "sections"
    ][0]
    assert section["ms"] is None
    assert "duration_unknown" in section["gaps"]


def test_response_cache_hit_is_not_a_rollup_and_parallel_spans_are_not_summed() -> None:
    result = describe_response(
        {
            "_section_timings": [
                {"section": "summary_cache_hit", "time_ms": 1},
                {"section": "foo_rollup", "time_ms": 40},
                {"section": "bar_rollup", "time_ms": 50},
                {"section": "connection_acquire", "time_ms": 10},
            ]
        }
    )
    sections = result["components"][0]["sections"]
    assert sections[0]["source"] == "unknown"
    assert sections[0]["activity"] == "cache"
    assert sections[1]["source"] == "rollup"
    assert sections[1]["source_evidence"] == "section_name"
    assert sections[3]["activity"] == "queue"
    assert "totals_ms" not in result
    assert "totals_ms" not in result["components"][0]


def test_zero_and_elapsed_ms_are_supported_without_fallback_overwriting_zero() -> None:
    result = describe_response(
        {"section_timings": [{"section": "zero", "time_ms": 0, "elapsed_ms": 50}, {"section": "b", "elapsed_ms": 9}]}
    )
    assert [row["ms"] for row in result["components"][0]["sections"]] == [0, 9]


def test_report_never_copies_raw_debug_queries_headers_or_data() -> None:
    result = describe_response(
        {
            "_debug_queries": [{"sql": "SECRET SQL", "params": ["PRIVATE"]}],
            "headers": {"Cookie": "BEARER"},
            "data": {"ip": "192.0.2.1"},
            "_section_timings": [{"section": "summary", "time_ms": 1, "sql": "SECRET SQL"}],
        }
    )
    assert all(secret not in str(result) for secret in ("SECRET SQL", "PRIVATE", "BEARER", "192.0.2.1"))


def test_non_object_response_is_incomplete() -> None:
    assert describe_response([]) == {"components": [], "gaps": ["response_not_object"]}


def test_malformed_metadata_is_unknown_and_section_gaps_reach_top_level() -> None:
    result = describe_response(
        {"_response_cache": {"state": []}, "_section_timings": [{"section": "scan", "source": {}, "time_ms": 1}]}
    )
    assert result["components"][0]["cache_state"] == "unknown"
    assert "source_unverified" in result["gaps"]
    assert "execution_freshness_unknown" in result["gaps"]


def test_conflicting_cache_markers_are_not_accepted_as_miss() -> None:
    result = describe_response({"_response_cache": {"state": "miss"}, "_is_cached": True})
    assert result["components"][0]["cache_state"] == "unknown"
    assert "response_cache_conflict" in result["gaps"]


def test_enclosing_hit_and_conflicting_marker_cannot_manufacture_current_misses() -> None:
    cached = _sample(1)
    cached["response"] = describe_response(
        {
            "_is_cached": True,
            "aggregates": {
                "_response_cache": {"state": "miss"},
                "_section_timings": [{"section": "scan", "time_ms": 900, "current_execution": True}],
            },
        }
    )
    conflict = _sample(2)
    conflict["response"] = describe_response(
        {
            "_is_cached": True,
            "_response_cache": {"state": "miss"},
            "_section_timings": [{"section": "scan", "time_ms": 900, "current_execution": True}],
        }
    )
    assert cached["response"]["components"][1]["sections"][0]["current_execution"] is False
    assert conflict["response"]["components"][0]["sections"][0]["current_execution"] is False
    assert rank_sections([cached, conflict]) == []


@pytest.mark.parametrize(
    "filters",
    [
        {"status": {"values": []}},
        {"status": {"values": [""]}},
        {"status": {"values": ["200"], "mode": "ignored"}},
        {"status": ["200"]},
    ],
)
def test_inactive_or_invalid_filters_cannot_receive_filtered_budget(filters: dict) -> None:
    with pytest.raises(ValueError, match="filter"):
        visible_results_budget_ms("30d", filters)


def test_distribution_is_nearest_rank_and_reports_sample_support() -> None:
    stats = distribution(list(range(1, 101)))
    assert (stats["count"], stats["p50"], stats["p90"], stats["p95"], stats["p99"]) == (100, 50, 90, 95, 99)
    assert stats["mean"] == 50.5
    assert stats["min"] == 1
    assert stats["max"] == 100
    assert stats["p95_sample_floor_met"] is True
    assert stats["p99_sample_floor_met"] is True
    assert distribution([0])["p95_sample_floor_met"] is False
    assert distribution([])["p95"] is None


@pytest.mark.parametrize("values", [[-1], [math.inf], [math.nan], [True]])
def test_distribution_rejects_invalid_samples(values: list[float]) -> None:
    with pytest.raises(ValueError, match="finite nonnegative"):
        distribution(values)


def test_visible_results_budgets_are_not_request_or_section_budgets() -> None:
    assert visible_results_budget_ms("24h", {}) == 2000
    assert visible_results_budget_ms("3d", {}) == 5000
    assert visible_results_budget_ms("7d", {"status": {"values": ["200"], "mode": "include"}}) == 10000
    assert visible_results_budget_ms("30d", {"status": {"values": ["200"]}, "method": {"values": ["GET"]}}) == 30000
    with pytest.raises(ValueError, match="Unsupported window"):
        visible_results_budget_ms("1h", {})


def _sample(number: int, *, role: str = "admin", cache: str = "miss", ms: float = 900) -> dict:
    return {
        "sample_id": str(number),
        "environment": "remote-standard",
        "role": role,
        "endpoint": "security",
        "window": "7d",
        "payload": {"range_token": "7d", "filters": {}},
        "response": describe_response(
            {
                "_response_cache": {"state": cache},
                "_section_timings": [
                    {"section": "proxies", "time_ms": ms, "source": "live", "current_execution": True}
                ],
            }
        ),
    }


def test_ranking_preserves_role_filters_cache_and_repeated_evidence() -> None:
    samples = [_sample(1), _sample(2, ms=1100), _sample(3, role="analyst-no-pii"), _sample(4, cache="hit")]
    filtered = _sample(5)
    filtered["payload"]["filters"] = {"status": {"values": ["200"]}, "country": {"values": ["US"]}}
    samples.append(filtered)
    rows = rank_sections(samples)
    admin_unfiltered = next(row for row in rows if row["role"] == "admin" and not row["payload"]["filters"])
    assert admin_unfiltered["cache_state"] == "miss"
    assert admin_unfiltered["distribution_ms"]["count"] == 2
    assert admin_unfiltered["repeatable"] is True
    assert len(rows) == 3
    assert all(row["cache_state"] != "hit" for row in rows)


def test_sample_id_reuse_across_environments_does_not_drop_evidence() -> None:
    local = _sample(1)
    remote = _sample(1)
    local["environment"] = "local-standard"
    assert len(rank_sections([local, remote])) == 2


def test_python_and_browser_reporters_agree_on_evidence() -> None:
    payloads = [
        {},
        {"_is_cached": False},
        {
            "_response_cache": {"state": "miss"},
            "_debug_queries": [
                {
                    "sql": "PRIVATE",
                    "clickhouse": {
                        "query_id": "query-test",
                        "section_id": "section-test",
                        "request_id": "request-test",
                        "service_id": "service-test",
                        "section": "cmcd.overview",
                        "source": "projected_fact",
                        "outcome": "success",
                        "client_elapsed_ms": 100,
                        "network_ms": 50,
                        "admission_wait_ms": 20,
                        "decode_ms": 30,
                        "server_cpu_ms": None,
                    },
                }
            ],
            "_section_timings": [
                {
                    "section": "cmcd.overview",
                    "section_id": "section-test",
                    "time_ms": 110,
                    "source": "projected_fact",
                    "current_execution": True,
                }
            ],
        },
        {
            "_debug_queries": [
                {"clickhouse": {"query_id": "PRIVATE VALUE", "outcome": "success", "client_elapsed_ms": 5}}
            ]
        },
        {
            "_response_cache": {"state": "miss"},
            "_reader_coverage": [
                {
                    "reader": "dashboard.count",
                    "minimum_window_hours": None,
                    "closed_hour_heal": "used",
                    "scalar_plan": {
                        "reason": "planned",
                        "closed_hours": 168,
                        "prefix_hours": 144,
                        "trailing_start": "2026-01-07T00:00:00+00:00",
                        "trailing": {"reason": "planned", "closed_hours": 24, "bundle_hours": 24},
                    },
                }
            ],
            "_history": {
                "extent_source": "live_query",
                "extent_rows": 10,
                "window_rows": 5,
                "window_count_source": "raw",
                "earliest_event_at": "2026-01-01T00:00:00+00:00",
                "latest_event_at": "2026-01-08T00:00:00+00:00",
            },
        },
        {
            "_reader_coverage": [
                {
                    "reader": "dashboard.top_n",
                    "top_n_plan": {
                        "coverage_scope": "any_field_file_presence",
                        "field_complete_hours": None,
                        "any_field_covered_hours": 1,
                        "heal_missing_hours": 0,
                    },
                }
            ]
        },
        {"_history": {"extent_source": "status_cache", "earliest_event_at": "PRIVATE"}},
        {
            "_reader_coverage": [
                {
                    "reader": "dashboard.ngwaf_bots",
                    "minimum_window_hours": 2,
                    "outcome": "served",
                    "live_top_up": "unavailable",
                    "attempts": [{"bundle": "ngwaf_bots", "reason": "planned", "closed_hours": 4, "covered_hours": 2}],
                }
            ]
        },
        {"_reader_coverage": [{"reader": "unknown", "minimum_window_hours": True}]},
        {
            "_reader_coverage": [
                {"reader": "dashboard.ngwaf_bots", "minimum_window_hours": 2, "window_start": "PRIVATE"}
            ]
        },
        {
            "_response_cache": {"state": "hit"},
            "aggregates": {
                "_response_cache": {"state": "miss"},
                "_reader_coverage": [
                    {"reader": "dashboard.conn_requests_histogram", "minimum_window_hours": 2, "outcome": "served"}
                ],
            },
        },
        {
            "_response_cache": {"state": "unknown", "single_flight": "follower"},
            "_reader_coverage": [{"reader": "dashboard.ngwaf_bots", "minimum_window_hours": 2, "outcome": "served"}],
        },
        {"_response_cache": {"state": "miss"}, "_is_cached": True},
        {
            "_response_cache": {"state": []},
            "_section_timings": [{"section": "scan", "source": {}, "activity": [], "time_ms": "12"}],
        },
        {
            "aggregates": {"_is_cached": True, "_section_timings": [{"section": "foo_rollup", "elapsed_ms": 15}]},
            "top_bots": {},
        },
        {
            "_response_cache": {"state": "bypass", "prewarmer": False},
            "_section_timings": [
                {"section": "summary", "time_ms": 0, "source": "fact", "activity": "compute", "current_execution": True}
            ],
        },
    ]
    script = "const fs=require('node:fs');const {describeResponse}=require('./scripts/lib/section_measurements.cjs');process.stdout.write(JSON.stringify(JSON.parse(fs.readFileSync(0,'utf8')).map(describeResponse)));"
    result = subprocess.run(
        ["node", "-e", script],
        input=json.dumps(payloads),
        capture_output=True,
        text=True,
        check=True,
        cwd=Path(__file__).resolve().parents[2],
    )
    assert json.loads(result.stdout) == [describe_response(payload) for payload in payloads]


@pytest.mark.parametrize("cache", ["miss", "bypass", "hit", "unknown"])
def test_reader_coverage_keeps_planning_distinct_from_history_and_execution(cache: str) -> None:
    observation = {
        "reader": "dashboard.conn_requests_histogram",
        "minimum_window_hours": 2,
        "eligible": True,
        "eligibility_reason": "eligible",
        "outcome": "served",
        "live_top_up": "failed",
        "attempts": [
            {
                "bundle": "security_conn_reuse",
                "reason": "planned",
                "closed_hours": 4,
                "covered_hours": 2,
                "coverage_fraction": 0.5,
                "path": "PRIVATE",
            }
        ],
        "sql": "PRIVATE",
    }
    report = describe_response({"_response_cache": {"state": cache}, "_reader_coverage": [observation]})
    reader = report["components"][0]["reader_coverage"][0]
    assert reader["observation"]["attempts"][0]["covered_hours"] == 2
    assert reader["current_observation"] is (False if cache == "hit" else True if cache in {"miss", "bypass"} else None)
    assert "history_evidence_missing" in report["gaps"]
    assert "reader_live_top_up_failed" in report["gaps"]
    assert "PRIVATE" not in json.dumps(report)
    assert "reader_coverage" not in observation


def test_clickhouse_observations_keep_safe_correlation_and_client_phases_only() -> None:
    operation = {
        "query_id": "query-test",
        "request_id": "request-test",
        "service_id": "service-test",
        "section_id": "section-test",
        "section": "cmcd.overview",
        "source": "projected_fact",
        "outcome": "success",
        "client_elapsed_ms": 100,
        "admission_wait_ms": 20,
        "network_ms": 50,
        "decode_ms": 30,
        "server_cpu_ms": None,
        "sql": "PRIVATE",
        "url": "PRIVATE",
    }
    report = describe_response(
        {
            "_response_cache": {"state": "miss"},
            "_debug_queries": [{"sql": "PRIVATE", "clickhouse": operation}],
            "_section_timings": [
                {
                    "section": "cmcd.overview",
                    "section_id": "section-test",
                    "time_ms": 105,
                    "source": "projected_fact",
                    "current_execution": True,
                }
            ],
        }
    )
    query = report["components"][0]["query_operations"][0]
    assert query["observation"]["query_id"] == "query-test"
    assert query["observation"]["network_ms"] == 50
    assert query["observation"]["server_cpu_ms"] is None
    assert query["current_observation"] is True
    assert report["components"][0]["sections"][0]["section_id"] == "section-test"
    assert "PRIVATE" not in json.dumps(report)
    assert "server_cpu_unobserved" in report["gaps"]
    hit = describe_response(
        {"_response_cache": {"state": "bypass"}, "_debug_queries": [{"is_cached": True, "clickhouse": operation}]}
    )
    assert hit["components"][0]["query_operations"][0]["current_observation"] is False


def test_history_retains_provenance_without_certifying_cached_or_rolled_counts() -> None:
    for state, source in [("hit", "live_query"), ("miss", "status_cache"), ("miss", "live_query")]:
        report = describe_response(
            {
                "_response_cache": {"state": state},
                "_history": {
                    "extent_source": source,
                    "extent_rows": 5,
                    "window_rows": 5,
                    "window_count_source": "rollup",
                    "earliest_event_at": "2026-01-01T00:00:00+00:00",
                    "latest_event_at": "2026-01-02T00:00:00+00:00",
                    "sql": "PRIVATE",
                },
            }
        )
        history = report["components"][0]["history"]
        assert history["observation"]["extent_rows"] == 5
        assert history["current_observation"] is (state == "miss")
        assert "history_window_rows_unverified" in report["gaps"]
        assert "history_continuity_unverified" in report["gaps"]
        assert "PRIVATE" not in json.dumps(report)
        if source == "status_cache":
            assert "history_extent_unverified" in report["gaps"]


def test_history_reports_when_raw_extent_starts_after_requested_window() -> None:
    report = describe_response(
        {
            "_response_cache": {"state": "miss"},
            "_history": {
                "extent_source": "live_query",
                "extent_rows": 5,
                "window_rows": 5,
                "window_count_source": "rollup",
                "window_start": "2026-09-25T05:00:00+00:00",
                "earliest_event_at": "2026-09-25T05:38:42+00:00",
                "raw_extent_reaches_window_start": False,
            },
        }
    )

    assert "history_raw_prefix_uncovered" in report["gaps"]
    assert "history_continuity_unverified" in report["gaps"]
    assert report["components"][0]["history"]["observation"]["raw_extent_reaches_window_start"] is False


def test_reader_observation_limit_does_not_truncate_normal_multi_reader_payload() -> None:
    rows = [
        {"reader": reader, "minimum_window_hours": None}
        for reader in (
            "dashboard.top_n",
            "dashboard.count",
            "dashboard.time_series",
            "dashboard.conn_requests_histogram",
        )
    ]
    report = describe_response({"_reader_coverage": rows})
    assert len(report["components"][0]["reader_coverage"]) == 4
    assert "reader_coverage_truncated" not in report["gaps"]


def test_bundle_wrapper_does_not_claim_missing_coverage_owned_by_children() -> None:
    report = describe_response(
        {
            "aggregates": {
                "_reader_coverage": [{"reader": "dashboard.conn_requests_histogram", "minimum_window_hours": 2}]
            },
            "top_bots": {"_reader_coverage": [{"reader": "dashboard.ngwaf_bots", "minimum_window_hours": 2}]},
        }
    )
    assert "reader_coverage_missing" not in report["components"][0]["gaps"]
    assert "reader_coverage_missing" not in report["gaps"]
    assert "history_evidence_missing" in report["gaps"]


def test_missing_reader_metadata_stays_a_gap() -> None:
    report = describe_response({"_response_cache": {"state": "miss"}})
    assert report["components"][0]["reader_coverage"] == []
    assert "reader_coverage_missing" in report["gaps"]


def test_duplicate_sample_ids_do_not_manufacture_repeatability() -> None:
    sample = _sample(1)
    assert rank_sections([sample, sample])[0]["distribution_ms"]["count"] == 1
    assert rank_sections([sample, sample])[0]["repeatable"] is False


# ── Merged from test_section_sweep.py ──────────────────────────────────────────────────────

"""Pure-logic measurement-harness tests; no environment access."""

import stat

import pytest

from scripts.dev import section_sweep as sweep


def _rec(endpoint: str, window: str, sections: list[dict]) -> dict:
    sample = {
        "sample_id": endpoint,
        "environment": "remote-standard",
        "role": "admin",
        "endpoint": endpoint,
        "window": window,
        "payload": {"range_token": window, "filters": {}},
        "response": describe_response({"_section_timings": sections}),
        "wall_ms": 1.0,
        "gaps": ["response_cache_unknown"],
    }
    return {"endpoint": endpoint, "window": window, "samples": [sample], "payload": sample["payload"]}


def test_find_offenders_ranks_sections_and_keeps_unknown_evidence() -> None:
    records = [
        _rec("security", "7d", [{"section": "proxies", "time_ms": 1500, "source": "live"}]),
        _rec("origin", "3d", [{"section": "slow_urls_query", "time_ms": 2500}, {"section": "tiny", "time_ms": 10}]),
        {"endpoint": "network", "window": "3d", "samples": [], "error": "HTTPError"},
    ]
    offenders = sweep.find_offenders(records, budget_ms=500)
    assert [(row["endpoint"], row["section"]) for row in offenders] == [
        ("origin", "slow_urls_query"),
        ("security", "proxies"),
    ]
    assert all(row["cache_state"] == "unknown" and not row["repeatable"] for row in offenders)


def test_render_report_lists_rows_errors_and_offenders() -> None:
    text = sweep.render_report(
        [
            _rec("security", "3d", [{"section": "temp_table_create", "time_ms": 900}]),
            {"endpoint": "origin", "window": "3d", "samples": [], "error": "URLError"},
        ],
        budget_ms=500,
    )
    assert "| security | 3d |" in text
    assert "URLError" in text
    assert "temp_table_create" in text
    assert "incomplete" in text
    assert "cold" not in text.lower()
    assert "warm" not in text.lower()


def test_repeated_sampling_keeps_exact_payload_errors_and_independent_cache_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = {
        "range_token": "7d",
        "anchor": "2026-10-21T00:00:00Z",
        "filters": {"status": {"mode": "include", "values": ["200"]}, "method": {"mode": "include", "values": ["GET"]}},
    }
    expected = deepcopy(body)
    responses = iter([({"_is_cached": True}, 10), OSError("PRIVATE URL token"), ({"_is_cached": False}, 900)])
    calls = []

    def post(*args: object, **kwargs: object) -> tuple[dict, float]:
        calls.append(deepcopy(args[3]))
        value = next(responses)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(sweep, "_post", post)
    result = sweep.sweep_one(
        "https://example.com",
        "TestLogSvcABC123",
        "security",
        "/api/security/aggregates",
        body,
        "7d",
        5,
        sample_count=3,
        environment="remote-standard",
    )
    assert calls == [expected] * 3
    assert result["payload"] == expected
    assert body == expected
    samples = result["samples"]
    assert len(samples) == 3
    assert samples[0]["response"]["components"][0]["cache_state"] == "hit"
    assert samples[1]["error"] == "OSError"
    assert samples[2]["response"]["components"][0]["cache_state"] == "unknown"
    assert "PRIVATE" not in str(result)
    assert result["qualification"] == "incomplete"
    assert result["visible_results_budget_ms"] == 10000
    assert result["whole_request_budget_policy"] == "diagnostic_only"
    assert result["whole_request_budget_ms"] is None
    assert "page_ready_unmeasured" in result["gaps"]
    assert not any("cold" in key or "warm" in key for key in result)


def test_mixed_component_cache_states_are_not_dropped_from_distributions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sweep, "_post", lambda *args, **kwargs: ({"_is_cached": False, "aggregates": {"_is_cached": True}}, 10)
    )
    result = sweep.sweep_one(
        "https://example.com",
        "TestLogSvcABC123",
        "dashboard",
        "/api/dashboard/bundle",
        {"filters": {}},
        "24h",
        5,
        sample_count=1,
    )
    assert result["request_distribution_by_cache_state_ms"]["mixed"]["count"] == 1
    assert sum(row["count"] for row in result["request_distribution_by_cache_state_ms"].values()) == 1


def test_sweep_cli_blocks_network_without_verified_prerequisites(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Prerequisite failure must happen before any live request")

    monkeypatch.setattr(sweep, "run_sweep", forbidden)
    assert sweep.main(["--env", "remote-standard", "--service-id", "TestLogSvcABC123"]) == 2
    evidence = tmp_path / "evidence.json"
    evidence.write_text('{"environment":"remote-standard", "checks":{}}')
    assert (
        sweep.main(["--env", "remote-standard", "--service-id", "TestLogSvcABC123", "--prerequisites", str(evidence)])
        == 2
    )


def test_gate_rejects_other_environment_missing_or_unverified_evidence() -> None:
    assert "environment_mismatch" in sweep.prerequisite_gaps({"environment": "local-standard"}, "remote-standard")
    assert "cache_reporting" in sweep.prerequisite_gaps(
        {"environment": "remote-standard", "checks": {"cache_reporting": {"status": "unknown"}}}, "remote-standard"
    )


def test_exact_filter_payload_artifacts_are_owner_only_and_symlinks_are_rejected(tmp_path) -> None:
    report = {"payload": {"filters": {"url": {"values": ["/video?signature=PRIVATE"]}}}}
    path = tmp_path / "report.json"
    path.write_text("old")
    path.chmod(0o644)
    sweep.write_report(path, report)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert "signature=PRIVATE" in path.read_text()
    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises(OSError):
        sweep.write_report(link, {"changed": True})
    assert "signature=PRIVATE" in path.read_text()


def test_gate_rejects_claimed_checks_without_evidence() -> None:
    report = {"environment": "remote-standard", "checks": {key: {"status": "verified"} for key in sweep.PREREQUISITES}}
    assert set(sweep.prerequisite_gaps(report, "remote-standard")) == set(sweep.PREREQUISITES)


# ── Merged from tests/load/test_clickhouse_serving_benchmark.py ──

"""Safety and measurement contracts; no Docker or network required."""

import copy
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from tests.load.clickhouse_serving_benchmark import (
    BenchmarkReport,
    Freshness,
    RecoveryEvidence,
    Sample,
    Stage,
    attempt,
    parser,
    quantile,
    request_for,
    runtime,
    summarize_payload,
    validate_args,
)


def args(*extra):
    return parser().parse_args(
        [
            "--engine",
            "baseline",
            "--output",
            "local-docs/test-evidence.json",
            "--apply",
            "--allow-prototype",
            "--base-url",
            "http://127.0.0.1",
            "--backend-container",
            "local-backend",
            *extra,
        ]
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1",
        "http://localhost",
        "http://example.com",
        "http://127.0.0.1.example.com",
        "http://user:secret@127.0.0.1",
        "http://127.0.0.1/path",
        "http://127.0.0.1?q=x",
    ],
)
def test_nonlocal_and_ambiguous_origins_refused(url):
    with pytest.raises(ValueError):
        validate_args(args("--base-url", url))


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--attempts", "29"),
        ("--attempts", "257"),
        ("--timeout", "0"),
        ("--timeout", "61"),
        ("--timeout", "nan"),
        ("--stage-seconds", "601"),
        ("--memory-abort-mb", "2049"),
        ("--concurrency", "128"),
        ("--output", "report.json"),
    ],
)
def test_bounds_refused(flag, value):
    with pytest.raises(ValueError):
        validate_args(args(flag, value))


@pytest.mark.parametrize("flag", ["apply", "allow_prototype", "base_url", "backend_container"])
def test_explicit_optin_required(flag):
    a = args()
    setattr(a, flag, None)
    with pytest.raises(ValueError):
        validate_args(a)


def test_allowed_budget_and_quantiles():
    with patch("subprocess.run") as run:
        run.return_value.returncode = 0
        validate_args(args())
    assert quantile([1.0, 2.0, 100.0], 0.95) == pytest.approx(90.2)


def test_live_hybrid_benchmark_refused_before_docker_or_network():
    with patch("subprocess.run") as run:
        with pytest.raises(ValueError, match="archived"):
            validate_args(args("--engine", "clickhouse_hybrid"))
        run.assert_not_called()


def test_removed_dashboard_route_cannot_be_reported_as_hybrid():
    payload = top_n_payload()
    payload["_section_timings"] = [{"section": "agg:query", "time_ms": 1.0}]
    summary = summarize_payload(payload, "top_n", "clickhouse_hybrid")
    assert summary["error"] == "engine"
    assert summary["clickhouse_calls"] == 0


def test_current_baseline_dashboard_needs_no_retired_dispatch_marker():
    payload = top_n_payload()
    payload["_section_timings"] = [{"section": "agg:query", "time_ms": 1.0}]
    summary = summarize_payload(payload, "top_n", "baseline")
    assert summary["error"] is None
    assert summary["dispatch"] == "baseline"
    assert summary["debug_queries"] == 1


@pytest.mark.parametrize("enabled", ["true", "false"])
def test_baseline_benchmark_does_not_require_disabling_diagnostics(enabled):
    container = {
        "Config": {
            "Env": [
                f"CLICKHOUSE_ENABLED={enabled}",
                "METADATA_DSN=postgresql://fixture",
                "CLICKHOUSE_HOST=clickhouse",
            ],
            "Labels": {
                "com.docker.compose.project.config_files": (
                    "docker-compose.multipod.yml,docker-compose.observability.yml,"
                    "docker-compose.clickhouse-prototype.yml"
                )
            },
        },
        "Image": "sha256:" + "a" * 64,
        "HostConfig": {"Memory": 0},
    }
    with patch("subprocess.check_output", return_value=json.dumps([container])):
        assert runtime("local-backend") == (container["Image"], 0)


def test_mixed_corpus_preserves_narrow_adapter_support():
    assert request_for("bundle")[1]["sections"] == ["core", "topten"]
    assert request_for("top_n")[1]["sections"] == ["topten"]
    assert request_for("time_series")[1]["include_conn_requests"] is False
    assert request_for("empty")[1]["start_time"] == "2026-09-06T00:00:00Z"


@pytest.mark.parametrize("status", [429, 500, 503])
def test_failures_retained_and_response_secrets_omitted(status):
    async def run():
        async with httpx.AsyncClient(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(
                lambda _: httpx.Response(status, json={"secret": "NEVER_COPY_SECRET", "sql": "SELECT PRIVATE"})
            ),
        ) as client:
            return await attempt(client, "PRIVATE_SERVICE", "bundle", "baseline", 1)

    sample = asyncio.run(run())
    assert sample.error == "http" and sample.status_code == status and sample.duration_ms > 0
    assert sample.rows_read is None and sample.bytes_read is None and sample.readiness_ms is None
    assert "PRIVATE" not in sample.model_dump_json() and "SECRET" not in sample.model_dump_json()


def test_deadline_retains_slow_attempt():
    async def handler(_):
        await asyncio.sleep(1)
        return httpx.Response(200)

    async def run():
        async with httpx.AsyncClient(base_url="http://127.0.0.1", transport=httpx.MockTransport(handler)) as client:
            return await attempt(client, "fixture", "bundle", "baseline", 0.01)

    sample = asyncio.run(run())
    assert sample.error == "timeout" and sample.duration_ms >= 10


def test_schema_rejects_arbitrary_payload_and_false_complete():
    with pytest.raises(ValidationError):
        Sample.model_validate({"sql": "PRIVATE"})
    with pytest.raises(ValidationError):
        Stage(
            concurrency=1,
            requested_attempts=30,
            peak_in_flight=1,
            status="complete",
            elapsed_seconds=1.0,
            backend_peak_memory_bytes=1,
            warmups=[],
            samples=[],
            report=None,
        )
    assert "sql" not in json.dumps(Sample.model_json_schema()["properties"])


def top_n_payload(dispatch="disabled"):
    return {
        "_is_cached": False,
        "_section_timings": [{"section": "engine:" + dispatch, "time_ms": 0.0}],
        "_debug_queries": [{"sql": "PRIVATE_SQL", "time_ms": 1.0}],
        "total_rows": 184374,
        "interval": "1 minute",
        "metric": "requests",
        "data": {"country": {"total": 184374, "top": [{"value": "US", "count": 184374, "label": None}]}},
        "time_series": [],
        "map_data": [],
    }


def test_top_n_has_no_chart_and_uses_existing_minute_metadata():
    summary = summarize_payload(top_n_payload(), "top_n", "baseline")
    assert summary["error"] is None
    assert summary["semantic_digest"] is not None
    assert "PRIVATE" not in json.dumps(summary)


def test_disabled_engine_must_not_be_mislabeled_fallback():
    summary = summarize_payload(top_n_payload("unsupported_sections"), "top_n", "baseline")
    assert summary["error"] == "engine"
    assert summarize_payload(top_n_payload("unsupported_sections"), "top_n", "clickhouse_hybrid")["error"] is None


def test_telemetry_counts_and_errors_are_retained_without_details():
    payload = top_n_payload("unsupported_sections")
    payload["_debug_calls"] = [
        {
            "service": "ClickHouse",
            "url": "PRIVATE_HOST",
            "details": json.dumps(
                {
                    "query_id": "PRIVATE_ID",
                    "outcome": "error",
                    "rows_read": 2,
                    "bytes_read": 100,
                }
            ),
        }
    ]
    summary = summarize_payload(payload, "top_n", "clickhouse_hybrid")
    assert summary["clickhouse_errors"] == 1
    assert summary["rows_read"] == 2 and summary["bytes_read"] == 100
    assert "PRIVATE" not in json.dumps(summary)


@pytest.mark.parametrize("memory", [1024, 2 * 1024**3])
def test_scheduler_respects_budget_and_records_errors(monkeypatch, memory):
    from tests.load import clickhouse_serving_benchmark as module

    calls = 0

    async def fake_attempt(*_):
        nonlocal calls
        calls += 1
        n = calls
        await asyncio.sleep(0)
        return Sample(
            kind="bundle",
            duration_ms=float(n),
            status_code=200 if n <= 3 else 503,
            error=None if n <= 3 else "http",
            dispatch="disabled",
            response_cached=False,
            debug_queries=0,
            clickhouse_calls=0,
            clickhouse_errors=0,
            rows_read=0,
            bytes_read=0,
            readiness_ms=0.0,
            semantic_digest=None,
        )

    async def fake_memory(*_):
        return memory

    monkeypatch.setattr(module, "attempt", fake_attempt)
    monkeypatch.setattr(module, "memory_bytes", fake_memory)
    now = datetime(2026, 9, 7, tzinfo=UTC)
    fresh = Freshness(
        observed_at=now,
        event_watermark=now,
        window_start=datetime(2026, 9, 1, tzinfo=UTC),
        window_end=datetime(2026, 9, 2, tzinfo=UTC),
        last_commit_at=now,
        last_publication_at=now,
    )
    measured = asyncio.run(module.stage(None, "fixture", args(), 8, fresh, 0.0))
    if memory > 1024:
        assert measured.status == "memory_abort"
        assert measured.samples == [] and measured.report is None and calls == 0
    else:
        assert measured.status == "complete" and measured.peak_in_flight == 8
        assert len(measured.samples) == 64 and calls == 67
        assert measured.report.error_count == 64 and measured.report.achieved_throughput_rps == 0
        assert measured.max_ms == 67
        assert measured.report.p99_ms == quantile([float(n) for n in range(4, 68)], 0.99)


def test_recovery_contract_never_accepts_credentials_or_nonfinite_durations():
    with pytest.raises(ValidationError):
        RecoveryEvidence.model_validate({"password": "NEVER_COPY"})
    assert "password" not in RecoveryEvidence.model_json_schema()["properties"]


# ── Merged from tests/load/test_clickhouse_serving_contract.py ──


@pytest.fixture
def report():
    return {
        "schema_version": 1,
        "engine": "baseline",
        "dataset_size_rows": 190334,
        "eligible_window_rows": 184374,
        "concurrency": 1,
        "warmup_requests": 3,
        "measured_requests": 30,
        "elapsed_seconds": 3.0,
        "achieved_throughput_rps": 10.0,
        "p50_ms": 90.0,
        "p95_ms": 110.0,
        "p99_ms": 120.0,
        "ingest_lag_seconds": 128.0,
        "error_count": 0,
        "freshness": {
            "observed_at": datetime(2026, 9, 7, 23, tzinfo=UTC),
            "event_watermark": datetime(2026, 9, 7, 19, tzinfo=UTC),
            "window_start": datetime(2026, 9, 1, tzinfo=UTC),
            "window_end": datetime(2026, 9, 2, tzinfo=UTC),
            "last_commit_at": datetime(2026, 9, 7, 20, tzinfo=UTC),
            "last_publication_at": datetime(2026, 9, 7, 21, tzinfo=UTC),
        },
        "clickhouse_ingest_lag_seconds": None,
        "clickhouse_rebuild_seconds": None,
    }


def test_report_json_roundtrip(report):
    validated = BenchmarkReport.model_validate(report)
    assert BenchmarkReport.model_validate_json(validated.model_dump_json()) == validated
    assert validated.model_dump()["clickhouse_rebuild_seconds"] is None


@pytest.mark.parametrize(
    "field",
    [
        "schema_version",
        "engine",
        "dataset_size_rows",
        "eligible_window_rows",
        "concurrency",
        "warmup_requests",
        "measured_requests",
        "elapsed_seconds",
        "achieved_throughput_rps",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "ingest_lag_seconds",
        "error_count",
        "freshness",
        "clickhouse_ingest_lag_seconds",
        "clickhouse_rebuild_seconds",
    ],
)
def test_missing_report_field_rejected(report, field):
    del report[field]
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), -float("inf"), "1", True, None])
@pytest.mark.parametrize(
    "field", ["elapsed_seconds", "achieved_throughput_rps", "p50_ms", "p95_ms", "p99_ms", "ingest_lag_seconds"]
)
def test_invalid_measurement_rejected(report, field, value):
    report[field] = value
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize("value", [-1, 1.5, True, "30", None])
@pytest.mark.parametrize(
    "field",
    ["dataset_size_rows", "eligible_window_rows", "concurrency", "warmup_requests", "measured_requests", "error_count"],
)
def test_invalid_count_rejected(report, field, value):
    report[field] = value
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize(
    "updates",
    [
        {"p50_ms": 111.0},
        {"p99_ms": 109.0},
        {"error_count": 31},
        {"eligible_window_rows": 190335},
        {"dataset_size_rows": 0},
        {"eligible_window_rows": 0},
        {"concurrency": 0},
        {"warmup_requests": 2},
        {"measured_requests": 29},
        {"elapsed_seconds": 0.0},
        {"achieved_throughput_rps": 0.0},
        {"clickhouse_ingest_lag_seconds": 0.0},
        {"clickhouse_rebuild_seconds": 0.0},
        {"schema_version": True},
        {"engine": "unknown"},
        {"unexpected": "not allowed"},
    ],
)
def test_inconsistent_report_rejected(report, updates):
    report.update(updates)
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize(
    "field", ["observed_at", "event_watermark", "window_start", "window_end", "last_commit_at", "last_publication_at"]
)
def test_missing_or_naive_freshness_rejected(report, field):
    missing = copy.deepcopy(report)
    del missing["freshness"][field]
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(missing)
    report["freshness"][field] = datetime(2026, 9, 1)
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize(
    "field,value",
    [
        ("window_end", datetime(2026, 9, 1, tzinfo=UTC)),
        ("event_watermark", datetime(2026, 9, 8, tzinfo=UTC)),
        ("last_commit_at", datetime(2026, 9, 8, tzinfo=UTC)),
        ("last_publication_at", datetime(2026, 9, 8, tzinfo=UTC)),
        ("unexpected", datetime(2026, 9, 1, tzinfo=UTC)),
    ],
)
def test_invalid_freshness_order_rejected(report, field, value):
    report["freshness"][field] = value
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


def test_errors_reduce_successful_throughput(report):
    report.update(error_count=3, achieved_throughput_rps=9.0)
    assert BenchmarkReport.model_validate(report).error_count == 3
    report["achieved_throughput_rps"] = 10.0
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


def test_equal_quantiles_and_all_failed_requests_are_representable(report):
    report.update(p50_ms=0.0, p95_ms=0.0, p99_ms=0.0, error_count=30, achieved_throughput_rps=0.0)
    assert BenchmarkReport.model_validate(report).error_count == 30


def test_later_hybrid_measurements_are_explicit(report):
    report.update(engine="clickhouse_hybrid", clickhouse_ingest_lag_seconds=1.0, clickhouse_rebuild_seconds=42.0)
    assert BenchmarkReport.model_validate(report).clickhouse_rebuild_seconds == 42.0
    report["clickhouse_rebuild_seconds"] = float("nan")
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


def test_section_query_correlation_missing_flags_uncorrelated_current_fact_section() -> None:
    """A current fact/projection section with no matching current query op is a gap.

    Mirrors the cross-check at scripts/lib/section_measurements.py: a section whose
    source resolves to fact/projection/projected_fact/mixed AND is the current
    execution MUST have a correlated current ClickHouse query op whose section_id
    matches, or the harness cannot attribute the section's work to a real query.
    """
    base_payload = {
        "_response_cache": {"state": "miss"},
        "_section_timings": [
            {
                "section": "cmcd.overview",
                "section_id": "section-test",
                "time_ms": 105,
                "source": "projected_fact",
                "current_execution": True,
            }
        ],
    }

    # gap PRESENT: no current query op correlates to section-test
    uncorrelated = describe_response(base_payload)
    assert "section_query_correlation_missing" in uncorrelated["gaps"]

    # gap ABSENT: a valid strict current ClickHouse op whose section_id matches
    operation = {
        "query_id": "query-test",
        "request_id": "request-test",
        "service_id": "service-test",
        "section_id": "section-test",
        "section": "cmcd.overview",
        "source": "projected_fact",
        "outcome": "success",
        "client_elapsed_ms": 100,
        "admission_wait_ms": 20,
        "network_ms": 50,
        "decode_ms": 30,
        "server_cpu_ms": None,
        "sql": "PRIVATE",
        "url": "PRIVATE",
    }
    correlated_payload = deepcopy(base_payload)
    correlated_payload["_debug_queries"] = [{"sql": "PRIVATE", "clickhouse": operation}]
    correlated = describe_response(correlated_payload)
    assert "section_query_correlation_missing" not in correlated["gaps"]
    assert "PRIVATE" not in json.dumps(correlated)


def test_generate_synthetic_traffic_rejects_nonpositive_arguments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """E5: Nonpositive file_rows / batch_size or negative rows must fail before side effects."""
    from datetime import timedelta

    from scripts.load_test import generate_synthetic_traffic

    now = datetime(2026, 8, 5, 12, 0, 0, tzinfo=UTC)
    start_dt = now - timedelta(hours=1)
    src = {"service_id": "svc-test", "name": "svc-test"}

    for bad_kwargs in (
        {"rows": -1, "batch_size": 100, "file_rows": 100},
        {"rows": 100, "batch_size": 0, "file_rows": 100},
        {"rows": 100, "batch_size": -5, "file_rows": 100},
        {"rows": 100, "batch_size": 100, "file_rows": 0},
        {"rows": 100, "batch_size": 100, "file_rows": -10},
    ):
        with pytest.raises(ValueError, match="must be"):
            generate_synthetic_traffic.run_target_local(
                src=src,
                scenario="diurnal",
                rows=bad_kwargs["rows"],
                start_dt=start_dt,
                end_dt=now,
                batch_size=bad_kwargs["batch_size"],
                file_rows=bad_kwargs["file_rows"],
                seed=42,
                commit=False,
                clean=False,
                dry_run=True,
                rate_rps=0,
                burst_rps=0,
                burst_duration=30,
                burst_interval=120,
            )

    for bad_kwargs in (
        {"rows": -1, "batch_size": 100},
        {"rows": 100, "batch_size": 0},
        {"rows": 100, "batch_size": -5},
    ):
        with pytest.raises(ValueError, match="must be"):
            generate_synthetic_traffic.run_target_fos(
                src=src,
                scenario="diurnal",
                rows=bad_kwargs["rows"],
                start_dt=start_dt,
                end_dt=now,
                batch_size=bad_kwargs["batch_size"],
                seed=42,
                dry_run=True,
                rate_rps=0,
                burst_rps=0,
                burst_duration=30,
                burst_interval=120,
                upload_workers=1,
            )

        with pytest.raises(ValueError, match="must be"):
            generate_synthetic_traffic.run_target_clickhouse(
                src=src,
                scenario="diurnal",
                rows=bad_kwargs["rows"],
                start_dt=start_dt,
                end_dt=now,
                batch_size=bad_kwargs["batch_size"],
                seed=42,
                dry_run=True,
                clickhouse_host="127.0.0.1",
                clickhouse_port=8123,
                clickhouse_database="default",
                clickhouse_user="default",
                clickhouse_password="",
                rate_rps=0,
                burst_rps=0,
                burst_duration=30,
                burst_interval=120,
            )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "generate_synthetic_traffic.py",
            "--target",
            "local",
            "--rows",
            "100",
            "--file-rows",
            "0",
        ],
    )
    with pytest.raises(SystemExit) as exc_info:
        generate_synthetic_traffic.main()
    assert exc_info.value.code == 2


def test_generate_synthetic_traffic_fos_zstd_and_pop_wave_simulation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify .zst/.gz chunk encoding, POP-wave allocation, RUM ratio, and High-Scale decoder parity."""
    from datetime import timedelta
    from typing import Any

    from backend.high_scale.archive import ArchiveSourceObject
    from backend.high_scale.query import decode_source_object
    from scripts.lib.log_factory import build_pop_wave_weights
    from scripts.load_test import generate_synthetic_traffic

    pops, weights = build_pop_wave_weights(20)
    assert len(pops) == 20
    assert pops[0] == "IAD"
    assert abs(sum(weights) - 1.0) < 1e-9
    assert weights[0] > weights[-1]

    chunks = generate_synthetic_traffic._allocate_pop_wave_chunks(total_rows=1000, pop_count=10, max_chunk_rows=120)
    assert sum(c for _, c in chunks) == 1000
    assert all(0 < c <= 120 for _, c in chunks)

    now = datetime(2026, 8, 5, 12, 0, 0, tzinfo=UTC)
    start_dt = now - timedelta(minutes=10)
    start_ms, end_ms = int(start_dt.timestamp() * 1000), int(now.timestamp() * 1000)

    zst_bytes, n_zst, ext_zst, ct_zst = generate_synthetic_traffic._build_fos_chunk_worker(
        25, start_ms, end_ms, "diurnal", 42, "zstd", "LHR"
    )
    assert n_zst == 25
    assert ext_zst == "zst"
    assert ct_zst == "application/zstd"
    assert zst_bytes.startswith(b"\x28\xb5\x2f\xfd")

    decoded = decode_source_object(
        ArchiveSourceObject(
            service_id="svc-hs",
            domain="request",
            object_key="raw/request/year=2026/month=08/day=05/hour=12/minute=00/2026-08-05T12:00:00.000-svc-hs-LHR-00000.log.zst",
            checksum="etag1",
            size_bytes=len(zst_bytes),
            version="v1",
        ),
        zst_bytes,
        transform_version="v1",
    )
    assert len(decoded.events) == 25
    assert decoded.dead_letters == ()
    assert all(r["pop"] == "LHR" for r in decoded.events)

    gz_bytes, n_gz, ext_gz, ct_gz = generate_synthetic_traffic._build_fos_chunk_worker(
        10, start_ms, end_ms, "diurnal", 42, "gzip", "NRT"
    )
    assert n_gz == 10
    assert ext_gz == "gz"
    assert ct_gz == "application/gzip"
    assert gz_bytes.startswith(b"\x1f\x8b")

    uploaded: list[dict[str, Any]] = []

    class _FakeS3:
        def put_object(self, **kwargs: Any) -> None:
            uploaded.append(kwargs)

    import boto3

    monkeypatch.setattr(boto3, "client", lambda *a, **kw: _FakeS3())
    rc = generate_synthetic_traffic.run_target_fos(
        src={
            "service_id": "svc-hs",
            "s3_bucket": "test-bucket",
            "s3_endpoint": "https://us-east.object.fastlystorage.app",
            "s3_access_key": "k",
            "s3_secret_key": "s",
        },
        scenario="diurnal",
        rows=200,
        start_dt=start_dt,
        end_dt=now,
        batch_size=50,
        seed=42,
        dry_run=False,
        upload_workers=2,
        compression="zstd",
        gen_workers=1,
        simulate_pops=True,
        pop_count=5,
        log_period=10,
        rum_ratio=0.1,
    )
    assert rc == 0
    req_uploads = [u for u in uploaded if "/raw/request/" in f"/{u['Key']}"]
    rum_uploads = [u for u in uploaded if "/raw/rum/" in f"/{u['Key']}"]
    assert len(req_uploads) >= 5
    assert len(rum_uploads) >= 1
    assert all(u["Key"].endswith(".log.zst") for u in uploaded)
    assert all(u["ContentType"] == "application/zstd" for u in uploaded)


def test_wave_window_is_the_last_log_period_when_live_and_the_run_window_otherwise():
    from scripts.load_test.generate_synthetic_traffic import _wave_window_ms

    now = 1_800_000_000_000
    run_window = (now - 3_600_000, now - 5_000)
    # Live feed: event times sit inside the last log period, as in real Fastly delivery, so only the current hour is
    # touched and closed-hour rollups are not re-run on every tick.
    assert _wave_window_ms(now, 10, True, run_window) == (now - 10_000, now)
    # Backfill keeps the run window, stretched up to now (the previous behaviour).
    assert _wave_window_ms(now, 10, False, run_window) == (now - 3_600_000, now)
    assert _wave_window_ms(now, 10, False, (now - 3_600_000, now + 5_000)) == (now - 3_600_000, now + 5_000)
