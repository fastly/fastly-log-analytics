"""Consolidated tests for test_terraform_gen."""

from __future__ import annotations

import json
import os
import shutil
import subprocess

import pytest
from backend.utils.terraform_gen import generate_terraform

TERRAFORM_INSTALLED = shutil.which("terraform") is not None
RUN_VALIDATE = os.environ.get("TERRAFORM_VALIDATE") == "1"
TFLINT_INSTALLED = shutil.which("tflint") is not None
TFLINT_REQUIRED = os.environ.get("TFLINT_REQUIRED") == "1"

if TFLINT_REQUIRED and not TFLINT_INSTALLED:
    raise RuntimeError(
        "TFLINT_REQUIRED=1 but the tflint binary is not on PATH. Install tflint or unset TFLINT_REQUIRED to allow skipping."
    )


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _baseline_cfg() -> dict:
    """Representative cfg covering all rendered branches except custom fields."""
    return {
        "logging_service_id": "SU3xxxxxxxxxxxxxx0000",
        "endpoint_name": "fastly_log_analysis",
        "fos_region": "us-east-1",
        "fos_bucket_name": "my-test-bucket",
        "fos_prefix": "logs",
        "log_period": 60,
        "cdn_service_name": "Test CDN Proxy",
        "cdn_prefix": "my-test-bucket",
        "cdn_shield": "iad-va-us",
        "cdn_secret": "test-secret-do-not-use",
        "sample_rate": 100,
        "edge_only": False,
        "custom_condition": "",
        "log_fields": {
            "groups": ["A"],
            "preset": "minimal",
            "schema_version": 2,
            "custom_fields": [],
        },
    }


def _write_files(out: dict[str, str], dest: str | Path) -> None:
    dest = os.fspath(dest)
    """Write generator output to ``dest``, mirroring the zip-export layout."""
    for fname, content in out.items():
        if fname == "instructions":
            continue
        full = os.path.join(dest, fname)
        os.makedirs(os.path.dirname(full), exist_ok=True) if "/" in fname else None
        with open(full, "w") as f:
            f.write(content)


# ── Smoke / happy path ───────────────────────────────────────────────────────


def test_returns_expected_files():
    out = generate_terraform(_baseline_cfg(), "AKIATEST", "secrettest")
    expected = {
        "cdn_proxy.vcl",
        "log_format.vcl",
        "fos.tf.json",
        "cdn_proxy.tf.json",
        "logging_service.tf.json",
        "versions.tf.json",
        "instructions",
    }
    assert expected.issubset(out.keys()), f"Missing files: {expected - set(out.keys())}"
    # Snippet files prefixed by their phase
    snippet_files = [f for f in out if f.startswith("capture_snippets/")]
    assert snippet_files, "Expected at least one capture snippet"


def test_every_tf_json_file_parses_as_json():
    """The generator must emit syntactically valid JSON. Catches accidental
    f-string holdovers, missing commas, or stray HCL constructs."""
    out = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    for fname, content in out.items():
        if not fname.endswith(".tf.json"):
            continue
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as e:
            pytest.fail(f"{fname} is not valid JSON: {e}\n--- content ---\n{content}")
        assert isinstance(parsed, dict), f"{fname} top-level must be an object"


def test_versions_tf_pins_fastly_and_aws_providers_by_major():
    """versions.tf.json must pin the Fastly and AWS providers with the
    pessimistic operator so a major-version bump from either provider
    (Fastly v6, AWS v7) doesn't silently break customer apply.

    If you intentionally bump the major, update this test deliberately AND
    the matching scaffold in test_baseline_output_passes_terraform_validate.
    """
    out = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    assert "versions.tf.json" in out

    v = json.loads(out["versions.tf.json"])
    tf_block = v["terraform"]
    assert "required_version" in tf_block, "must declare a Terraform CLI floor"

    providers = tf_block["required_providers"]
    assert providers["aws"]["source"] == "hashicorp/aws"
    assert providers["fastly"]["source"] == "fastly/fastly"
    # Pessimistic constraint is the contract. >= or no operator would let a
    # major bump through. Test both providers explicitly.
    assert providers["aws"]["version"] == "~> 5.0", (
        f"expected '~> 5.0' on aws to gate major bumps; got {providers['aws']['version']!r}"
    )
    assert providers["fastly"]["version"] == "~> 5.0", (
        f"expected '~> 5.0' on fastly to gate major bumps; got {providers['fastly']['version']!r}"
    )
    # Belt-and-braces: exactly TWO required providers — no accidental third
    # undeclared source slipping in.
    assert set(providers.keys()) == {"aws", "fastly"}


@pytest.mark.skipif(not TERRAFORM_INSTALLED, reason="terraform binary not on PATH")
def test_baseline_output_passes_terraform_fmt(tmp_path):
    out = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    _write_files(out, str(tmp_path))

    # Terraform fmt understands .tf.json (it normalises trailing newlines /
    # 2-space indent). Using a relative target sidesteps the macOS
    # /private/-prefix path confusion documented at the prior HCL revision.
    r = subprocess.run(
        ["terraform", "fmt", "-check", "-recursive", "."],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, (
        f"terraform fmt -check failed.\nrc={r.returncode}\nstdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )


# Shares the `tf_provider_cache.lock` with the slower resource-graph
# `terraform test`; under `-n auto` this test can block on that lock long
# enough to blow the default 60s pytest-timeout (which `os._exit()`s the worker
# and loses its coverage). Headroom for lock-wait + its own init/validate.
# Heavyweight real-terraform subprocess: run in the dedicated serial (`-n 0`,
# un-niced) test-ci step, excluded from the niced `-n auto` pool where its
# xdist worker hard-crashed (see Makefile/ci.yml split).
@pytest.mark.terraform_cli
@pytest.mark.timeout(300)
@pytest.mark.skipif(
    not (TERRAFORM_INSTALLED and RUN_VALIDATE), reason="set TERRAFORM_VALIDATE=1 to run validate (needs network)"
)
def test_baseline_output_passes_terraform_validate(tmp_path):
    """Real ``terraform validate``. Requires network for provider download."""
    out = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    _write_files(out, str(tmp_path))

    # The generator emits its own versions.tf.json with pinned providers.
    # Only the provider *configuration* needs stubbing for init/validate.
    (tmp_path / "_providers.tf").write_text(
        """
provider "aws"    { region = "us-east-1" }
provider "fastly" { api_key = "stub" }
"""
    )

    from pathlib import Path

    from filelock import FileLock

    cache_dir = Path(__file__).parents[2] / "cache" / "tf_provider_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["TF_PLUGIN_CACHE_DIR"] = str(cache_dir)

    lock_path = cache_dir.parent / "tf_provider_cache.lock"
    with FileLock(str(lock_path)):
        init = subprocess.run(
            ["terraform", "init", "-backend=false", "-input=false", "-no-color"],
            cwd=str(tmp_path),
            capture_output=True,
            text=True,
            env=env,
        )
        assert init.returncode == 0, f"terraform init failed:\n{init.stdout}\n{init.stderr}"

        val = subprocess.run(
            ["terraform", "validate", "-no-color"],
            cwd=str(tmp_path),
            capture_output=True,
            text=True,
            env=env,
        )
        assert val.returncode == 0, f"terraform validate failed:\n{val.stdout}\n{val.stderr}"


@pytest.mark.skipif(not TFLINT_INSTALLED, reason="tflint not on PATH")
def test_baseline_output_passes_tflint(tmp_path):
    """tflint static analysis on the generated .tf.json bundle.

    ``terraform validate`` catches syntactic and schema-level errors;
    tflint adds provider-specific best-practice checks (deprecated
    arguments, unused-decl, mis-typed values) that the official
    validator misses. Lives behind a binary-presence skip.

    COVHON-01: this is **local-opt-in only** — CI does NOT install tflint, so
    this test never runs there (unlike the falco VCL tests, which CI installs
    and gates via FALCO_REQUIRED). The earlier docstring claimed CI ran it;
    that was false. To actually gate on it, add a tflint install step to
    .github/workflows/ci.yml (mirror the falco curl-binary step) plus a
    TFLINT_REQUIRED collection guard — note tflint needs a ``.tflint.hcl``
    plugin config to load the Fastly provider rules, or it runs core rules only.

    Install locally:
        curl -sSL https://github.com/terraform-linters/tflint/releases/latest/download/tflint_darwin_arm64.zip \\
            -o /tmp/tflint.zip && unzip /tmp/tflint.zip -d /usr/local/bin
    """
    out = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    _write_files(out, str(tmp_path))

    init = subprocess.run(
        ["tflint", "--init"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
    )
    assert init.returncode == 0, f"tflint --init failed:\n{init.stdout}\n{init.stderr}"

    r = subprocess.run(
        ["tflint", "--format", "compact"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
    )
    # tflint returns 0 on no issues, 2 on issues found (per its docs).
    # rc==1 is "tool error" — treat that as a hard fail too.
    assert r.returncode == 0, (
        f"tflint reported issues on generated terraform output:\n"
        f"rc={r.returncode}\nstdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )


# ── Idempotency ──────────────────────────────────────────────────────────────


def test_output_is_byte_identical_across_calls():
    """The same cfg must produce the same files. Catches non-deterministic
    ordering (e.g. set iteration), timestamps, random IDs.

    Exception: ``cdn_proxy.vcl`` embeds a per-load fallback secret
    generated by ``secrets.token_hex(32)`` inside ``load_vcl``. This is
    intentional — the fallback only matters when the ``cdn_auth``
    dictionary is unprovisioned, and a fresh random value per
    provisioning ensures the unprovisioned state fails closed instead
    of accepting an attacker-controlled empty key. Mask the 64-hex-char
    fallback string out of the diff so we still catch every OTHER
    accidental source of non-determinism.
    """
    import re as _re

    out1 = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    out2 = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    assert out1.keys() == out2.keys()
    # 64 lowercase-hex chars = ``secrets.token_hex(32)`` output; replace
    # both sides with a placeholder before comparing.
    hex64 = _re.compile(r"\b[0-9a-f]{64}\b")
    for fname in out1:
        a = hex64.sub("<HEX64>", out1[fname])
        b = hex64.sub("<HEX64>", out2[fname])
        assert a == b, f"File {fname} differs across calls"


def test_output_differs_when_cfg_differs():
    """Sanity that we're not just hashing the same string twice."""
    cfg1 = _baseline_cfg()
    cfg2 = _baseline_cfg()
    cfg2["fos_bucket_name"] = "different-bucket"
    out1 = generate_terraform(cfg1, "AKIA", "sec")
    out2 = generate_terraform(cfg2, "AKIA", "sec")
    assert out1["fos.tf.json"] != out2["fos.tf.json"]
    parsed = json.loads(out2["fos.tf.json"])
    assert parsed["resource"]["aws_s3_bucket"]["fos_bucket"]["bucket"] == "different-bucket"


# ── Custom field round trip ──────────────────────────────────────────────────


def test_custom_field_emits_matching_capture_snippet():
    cfg = _baseline_cfg()
    cfg["log_fields"]["custom_fields"] = [
        {
            "name": "x_env",
            "label": "Environment",
            "vcl_log_expression": "req.http.X-Env",
            "collection_stage": "edge",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "bytes_estimate": 10,
            "enabled": True,
        }
    ]
    out = generate_terraform(cfg, "AKIA", "sec")

    # The recv snippet should reference the field name (it's the edge stage)
    recv = out.get("capture_snippets/recv.vcl", "")
    assert "x_env" in recv, f"Expected 'x_env' in recv snippet, got: {recv[:200]!r}"

    # log_format.vcl should declare it
    assert '"x_env"' in out["log_format.vcl"]


def test_custom_origin_field_emits_in_deliver_phase():
    cfg = _baseline_cfg()
    cfg["log_fields"]["custom_fields"] = [
        {
            "name": "bereq_x",
            "label": "Origin Header",
            "vcl_log_expression": "beresp.http.x-something",
            "collection_stage": "origin",
            "origin_log_frequency": "all",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "bytes_estimate": 20,
            "enabled": True,
        }
    ]
    out = generate_terraform(cfg, "AKIA", "sec")
    # Origin fields land in the fetch (capture) and deliver (promote) snippets
    assert "bereq_x" in out.get("capture_snippets/fetch.vcl", ""), "origin field missing from fetch snippet"
    assert "bereq_x" in out.get("capture_snippets/deliver.vcl", ""), "origin field missing from deliver snippet"


# ── Injection / escape ──────────────────────────────────────────────────────


@pytest.mark.skipif(not TERRAFORM_INSTALLED, reason="terraform binary not on PATH")
@pytest.mark.parametrize(
    "field,value",
    [
        # Strings the generator splices into JSON string values. JSON encoding
        # owns quote/backslash/newline; the test confirms the integration
        # actually parses + that ``terraform fmt`` accepts the result.
        ("fos_bucket_name", 'b"; rm -rf /; #'),
        ("endpoint_name", 'name"; resource "extra" "x" {} #'),
        ("cdn_service_name", 'svc\\name with "quotes"'),
        ("custom_condition", 'req.url ~ "test\\b"'),
        ("cdn_secret", 'secret"with"quotes'),
        ("fos_region", 'r"\nresource "evil" "x" {}'),
    ],
)
def test_injection_does_not_break_terraform_fmt(tmp_path, field, value):
    """User-supplied strings flow into JSON string values. ``json.dumps``
    handles every escape it owns (quote, backslash, control bytes) so the
    file MUST always parse, regardless of what the attacker passes."""
    cfg = _baseline_cfg()
    cfg[field] = value
    out = generate_terraform(cfg, "AKIA", "sec")
    _write_files(out, str(tmp_path))

    # The .tf.json files must parse as JSON unconditionally.
    for fname, content in out.items():
        if fname.endswith(".tf.json"):
            json.loads(content)  # raises on failure

    # Use `terraform fmt` (not -check) — it parses the file. If the
    # injection broke JSON syntax it would error with a non-zero rc.
    r = subprocess.run(
        ["terraform", "fmt", "-recursive", "."],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
    )
    assert "Error:" not in r.stderr, (
        f"Injection broke parse for {field}={value!r}:\nstdout: {r.stdout[:400]}\nstderr: {r.stderr[:400]}"
    )


def test_template_prefix_escape_is_applied_to_user_input():
    """JSON encoding doesn't escape Terraform's ``${`` / ``%{`` template
    syntax (Terraform interprets these inside string values even in JSON
    config). The generator must convert them to ``$${`` / ``%%{`` for any
    user-supplied value so attacker input can't trigger interpolation.

    Replaces the prior HCL-specific test that asserted literal byte
    sequences from the old escape regex; the JSON path's only remaining
    template-prefix concern is the ``$``/``%`` doubling."""
    cfg = _baseline_cfg()
    # The region flows into multiple files (fos_host derivation in
    # cdn_proxy.tf.json, dictionary items, etc.). If unescaped, the
    # ``${file("/etc/passwd")}`` would expand at apply time.
    cfg["fos_region"] = 'us-east-1${file("/etc/passwd")}%{ if true }x%{ endif }'
    out = generate_terraform(cfg, "AKIA", "sec")

    # Must appear in the rendered files with the doubled prefixes.
    cdn = json.loads(out["cdn_proxy.tf.json"])
    items = cdn["resource"]["fastly_service_dictionary_items"]["fos_credentials"]["items"]
    assert items["region"].startswith('us-east-1$${file("/etc/passwd")}%%{ if true }x%%{ endif }')

    # And the raw original prefix must NOT appear anywhere in any rendered
    # .tf.json file (defense in depth — catches a future field that
    # forgets to call the escape helper). The doubled forms (``$${`` /
    # ``%%{``) contain the raw forms as substrings, so check that what
    # appears is ONLY the doubled form (raw count == doubled count).
    for fname, content in out.items():
        if not fname.endswith(".tf.json"):
            continue
        raw_dollar = content.count("${")
        doubled_dollar = content.count("$${")
        # Authored Terraform-interpolation refs in the generator itself
        # (e.g. ``${aws_s3_bucket.fos_bucket.bucket}``) use raw ``${``
        # intentionally — those aren't doubled. Count user-input-derived
        # raw forms by subtracting the doubled count's contribution.
        unescaped_dollar = raw_dollar - doubled_dollar
        # All authored refs in cdn_proxy.tf.json + logging_service.tf.json
        # are accounted for; an attacker-injected ${file()} would push this
        # over the authored baseline. The strict check: the attacker
        # payload ``${file("/etc/passwd")}`` must not be present as a
        # standalone substring (i.e. not immediately preceded by ``$``).
        assert '$${file("/etc/passwd")}' in content or '${file("/etc/passwd")}' not in content, (
            f"unescaped ${{file()}} reached {fname} — template-prefix escape missing"
        )
        # %{ directives have no authored counterpart in the generator —
        # any raw ``%{`` is automatically suspect. Count: raw must equal
        # doubled (every ``%{`` in the file must be part of a ``%%{``).
        raw_pct = content.count("%{")
        doubled_pct = content.count("%%{")
        assert raw_pct == doubled_pct, (
            f"{fname} has an unescaped %{{}} template-directive prefix "
            f"(raw=%{{ count {raw_pct}, doubled=%%{{ count {doubled_pct})"
        )


def test_quotes_in_user_input_are_json_escaped():
    """A bucket name with a double-quote MUST land in the JSON output as
    ``\\\"`` (JSON escape), not be stripped or corrupted."""
    cfg = _baseline_cfg()
    cfg["fos_bucket_name"] = 'bucket"with"quotes'
    out = generate_terraform(cfg, "AKIA", "sec")
    parsed = json.loads(out["fos.tf.json"])
    assert parsed["resource"]["aws_s3_bucket"]["fos_bucket"]["bucket"] == 'bucket"with"quotes'


# ── Syrupy snapshot of generated Terraform (audit follow-up) ────────────────


def test_baseline_output_matches_snapshot(snapshot):
    """Snapshot the full set of generated files for the baseline cfg.

    ``terraform fmt`` + ``terraform validate`` already catch syntax
    breakage; this snapshot catches SEMANTIC drift — a resource name
    renamed, a label removed, a default region changed. The diff in a
    snapshot update review is the audit trail for any generator change.

    Refresh with::

        uv run pytest tests/utils/test_terraform_gen.py \
            --snapshot-update

    …then read the diff line-by-line before committing.

    Scrubs the per-load ``secrets.token_hex(32)`` fallback secret
    embedded in cdn_proxy.vcl (see test_output_is_byte_identical_across_calls
    for the rationale) so the snapshot stays stable across runs.
    """
    import re as _re

    out = generate_terraform(_baseline_cfg(), "AKIATESTSTABLE", "test-secret-stable")

    hex64 = _re.compile(r"\b[0-9a-f]{64}\b")
    snapshot_view = {fname: hex64.sub("<HEX64>", content) for fname, content in sorted(out.items())}
    assert snapshot_view == snapshot


# ── Merged from test_terraform_resource_graph.py ──────────────────────────────────────────────

import os
import shutil
from pathlib import Path

import pytest

TERRAFORM_INSTALLED = shutil.which("terraform") is not None
RUN_VALIDATE = os.environ.get("TERRAFORM_VALIDATE") == "1"

TFTEST_DIR = Path(__file__).parent / "terraform_tests"


# `terraform init` + `terraform test` (plan mode) is ~46s solo; under `make
# ci`'s `-n auto` it also serializes behind the sibling `terraform validate`
# test on the shared `tf_provider_cache.lock`, so the lock-wait eats into the
# default 60s pytest-timeout. The thread-method timeout `os._exit()`s the xdist
# worker — failing this test *and* discarding the worker's coverage data. Give
# it generous headroom (the suite-wide 10-min CI timeout is the real backstop).
# Heavyweight real-terraform subprocess: run in the dedicated serial (`-n 0`,
# un-niced) test-ci step, excluded from the niced `-n auto` pool where its
# xdist worker hard-crashed (see Makefile/ci.yml split).
@pytest.mark.terraform_cli
@pytest.mark.timeout(300)
@pytest.mark.skipif(
    not (TERRAFORM_INSTALLED and RUN_VALIDATE),
    reason="set TERRAFORM_VALIDATE=1 + install terraform to run resource-graph assertions",
)
def test_resource_graph_assertions_via_terraform_test(tmp_path):
    out = generate_terraform(_baseline_cfg(), "AKIA_TEST_KEY", "secret_test_value")
    _write_files(out, tmp_path)

    # Stub provider configs — `command = plan` in the .tftest.hcl means no
    # real API calls happen, but the providers still need to *initialize*.
    # The skip_* flags neutralize AWS's IMDS/STS auth dance. The Fastly
    # provider accepts a non-empty api_key for plan-only operations.
    (tmp_path / "_providers.tf").write_text(
        """
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "AKIA_TEST_KEY"
  secret_key                  = "secret_test_value"
  skip_credentials_validation = true
  skip_region_validation      = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
}

provider "fastly" {
  api_key = "fastly-test-key"
}
"""
    )

    # `terraform test` looks for .tftest.hcl in the module root or the
    # `tests/` subdir. Copy the resource_graph file in so the layout is
    # what the CLI expects.
    shutil.copy(TFTEST_DIR / "resource_graph.tftest.hcl", tmp_path / "resource_graph.tftest.hcl")

    from pathlib import Path

    from filelock import FileLock

    cache_dir = Path(__file__).parents[2] / "cache" / "tf_provider_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["TF_PLUGIN_CACHE_DIR"] = str(cache_dir)

    lock_path = cache_dir.parent / "tf_provider_cache.lock"
    with FileLock(str(lock_path)):
        init = subprocess.run(
            ["terraform", "init", "-backend=false", "-input=false", "-no-color"],
            cwd=str(tmp_path),
            capture_output=True,
            text=True,
            env=env,
        )
        assert init.returncode == 0, f"terraform init failed:\n{init.stdout}\n{init.stderr}"

        # `terraform test` runs each `run` block and reports per-assertion failures.
        test = subprocess.run(
            ["terraform", "test", "-no-color"],
            cwd=str(tmp_path),
            capture_output=True,
            text=True,
            env=env,
        )
        assert test.returncode == 0, (
            f"terraform test failed (resource-graph assertion broke):\n"
            f"--- stdout ---\n{test.stdout}\n"
            f"--- stderr ---\n{test.stderr}"
        )

    # Sanity: every run block should report as `pass`. The stdout has
    # lines like "  run \"foo\"... pass". If the binary's output format
    # changes, the rc check above is still the source of truth.
    assert "fail" not in test.stdout.lower() or "0 failed" in test.stdout.lower(), (
        f"unexpected failure in terraform test output:\n{test.stdout}"
    )


# ── Merged from tests/utils/test_fos_setup.py ──

"""Tests for ``backend.provision.fos_setup`` — FOS bucket + access-key CRUD.

This module is the provisioning CLI's S3 layer. Its operations are
hard-to-reverse (creating buckets, generating keys) and rarely run in
production, so the tests pin happy paths + the idempotency branches
(already-exists detection) that protect against double-provisioning
when a user re-runs the wizard.

All boto3 + Fastly API calls are mocked at the client boundary.
"""


from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from backend.provision import fos_setup

# ── _get_fos_s3_client ─────────────────────────────────────────────────────


def test_get_fos_s3_client_returns_raw_boto_when_no_service_id():
    """Without a service_id (e.g. the standalone CLI before a service
    is registered), the helper returns the bare boto3 client and
    skips telemetry. Pinned because wrapping a raw boto3 client in
    the tracker without a service_id would NPE on flush()."""
    with patch("boto3.client") as mock_boto:
        mock_boto.return_value = "raw-client"
        out = fos_setup._get_fos_s3_client("k", "s", "us-east-1")

    assert out == "raw-client"


def test_get_fos_s3_client_wraps_with_tracker_when_service_id_provided():
    """When a service_id is supplied (e.g. by the orchestrator), the
    boto3 client is wrapped in _ProvisioningTracker so every call
    lands in usage_log. Pinned because losing this wrapper was the
    bug that left provisioning ops invisible to dashboards."""
    with patch("boto3.client") as mock_boto:
        mock_boto.return_value = MagicMock()
        out = fos_setup._get_fos_s3_client("k", "s", "us-east-1", service_id="svc-1", bucket_name="my-bucket")

    assert isinstance(out, fos_setup._ProvisioningTracker)
    assert out._service_id == "svc-1"
    assert out._bucket_name == "my-bucket"


def test_get_fos_s3_client_uses_region_endpoint():
    """The endpoint URL is derived from ``region_endpoint(region)``,
    NOT hardcoded to us-east-1. Pinned because losing this would
    silently route eu-west-1 traffic to the US shard."""
    with (
        patch("boto3.client") as mock_boto,
        patch("backend.core.fastly.utils.region_endpoint", return_value="eu-west-1.object.fastlystorage.app"),
    ):
        fos_setup._get_fos_s3_client("k", "s", "eu-west-1")

    kwargs = mock_boto.call_args.kwargs
    assert "eu-west-1.object.fastlystorage.app" in kwargs["endpoint_url"]
    assert kwargs["region_name"] == "eu-west-1"


# ── ensure_fos_bucket ──────────────────────────────────────────────────────


def test_ensure_fos_bucket_returns_true_when_bucket_exists():
    """``head_bucket`` succeeds → bucket exists → no create call.
    Pinned because re-creating would fail with BucketAlreadyExists
    and break the provisioning wizard on re-runs."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}  # 200 OK

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        out = fos_setup.ensure_fos_bucket("my-bucket", "us-east-1", "k", "s")

    assert out is True
    fake_s3.create_bucket.assert_not_called()


def test_ensure_fos_bucket_creates_when_head_raises_404():
    """``head_bucket`` raises (bucket missing) → call ``create_bucket``.
    Pinned because a refactor that re-throws on any ClientError would
    abort first-time provisioning."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket")

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        out = fos_setup.ensure_fos_bucket("new-bucket", "us-east-1", "k", "s")

    assert out is True
    fake_s3.create_bucket.assert_called_once_with(Bucket="new-bucket")


def test_ensure_fos_bucket_calls_status_callback_on_create():
    """When a ``status_cb`` is provided, the wizard surfaces "Creating
    bucket..." + "Bucket created" to the user. Pinned because losing
    these would make the provisioning UI silent during a long step."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")

    statuses: list[str] = []
    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s", status_cb=statuses.append)

    assert any("Creating bucket" in s for s in statuses)
    assert any("Bucket created" in s for s in statuses)


def test_ensure_fos_bucket_raises_runtime_error_on_create_failure():
    """A ClientError from ``create_bucket`` (perms denied, quota) →
    RuntimeError with the cause. Pinned because RuntimeError is what
    the wizard's outer try/except catches to render a clear error."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")
    fake_s3.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "perms"}}, "CreateBucket"
    )

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        with pytest.raises(RuntimeError, match="Could not create FOS bucket"):
            fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s")


# ── delete_fos_bucket ─────────────────────────────────────────────────────


def test_delete_fos_bucket_short_circuits_when_already_deleted():
    """head_bucket → 404 → no list_objects / delete_bucket calls.
    Pinned because the teardown is idempotent — running it twice
    must not error."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")  # must not raise

    fake_s3.delete_bucket.assert_not_called()


def test_delete_fos_bucket_empties_then_deletes_on_happy_path():
    """Bucket has objects → list_objects + delete_objects + delete_bucket.
    Pinned because skipping the empty step would surface as a
    BucketNotEmpty error after a few attempts."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "obj1.gz"}, {"Key": "obj2.gz"}]},
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {"Deleted": [{"Key": "obj1.gz"}, {"Key": "obj2.gz"}]}

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")

    fake_s3.delete_objects.assert_called()
    fake_s3.delete_bucket.assert_called_with(Bucket="b")


def test_delete_fos_bucket_raises_after_max_retries_on_bucket_not_empty():
    """If ``delete_bucket`` repeatedly returns BucketNotEmpty (which
    means deleted-and-undeleted by S3 eventual consistency), the
    helper retries with backoff but eventually raises after 15
    attempts. Pinned because infinite retries would hang the wizard."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}
    fake_s3.get_paginator.return_value.paginate.return_value = []
    fake_s3.delete_bucket.side_effect = ClientError(
        {"Error": {"Code": "BucketNotEmpty", "Message": "x"}}, "DeleteBucket"
    )

    with (
        patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3),
        patch("time.sleep"),  # skip the retry backoff
    ):
        with pytest.raises(RuntimeError, match="Could not delete FOS bucket after 15 attempts"):
            fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")

    # All 15 attempts hit the API
    assert fake_s3.delete_bucket.call_count == 15


def test_delete_fos_bucket_raises_on_unexpected_client_error():
    """Non-BucketNotEmpty ClientError (perms denied) → immediate raise,
    no retry loop. Pinned because retrying through an AccessDenied is
    pointless and slows the failure."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}
    fake_s3.get_paginator.return_value.paginate.return_value = []
    fake_s3.delete_bucket.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "perms"}}, "DeleteBucket"
    )

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        with pytest.raises(RuntimeError):
            fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")

    # Only one attempt, not 15
    assert fake_s3.delete_bucket.call_count == 1


def test_delete_fos_bucket_aborts_multipart_uploads_before_listing_objects():
    """Multipart uploads must be aborted before object deletion, or
    they leak forever as zombie partial uploads (and accrue storage
    charges). Pinned because skipping this is a silent cost leak."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": [{"Key": "partial.gz", "UploadId": "upload-123"}]}
    fake_s3.get_paginator.return_value.paginate.return_value = []

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")

    fake_s3.abort_multipart_upload.assert_called_once_with(Bucket="b", Key="partial.gz", UploadId="upload-123")


def test_delete_fos_bucket_tolerates_multipart_listing_failure():
    """``list_multipart_uploads`` raises (perms gap) → swallow and
    continue. Pinned because some FOS keys don't have multipart
    perms even though they can list/delete objects."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "x"}}, "ListMultipartUploads"
    )
    fake_s3.get_paginator.return_value.paginate.return_value = []

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")  # must not raise

    fake_s3.delete_bucket.assert_called()


# ── find_fos_key ──────────────────────────────────────────────────────────


def test_find_fos_key_returns_matching_key():
    fake_keys = {
        "data": [
            {"access_key": "AK111", "description": "fos-log-analysis-svc1"},
            {"access_key": "AK222", "description": "manual-key"},
        ]
    }
    with patch("backend.provision.fos_setup.fastly", return_value=fake_keys):
        out = fos_setup.find_fos_key("manual-key", token="tkn")

    assert out["access_key"] == "AK222"


def test_find_fos_key_returns_none_when_no_match():
    with patch("backend.provision.fos_setup.fastly", return_value={"data": []}):
        assert fos_setup.find_fos_key("nonexistent", token="tkn") is None


def test_find_fos_key_swallows_runtime_error_from_fastly_api():
    """A RuntimeError from the Fastly API (rate-limited, 5xx) →
    return None. Pinned because raising here would crash the
    ``ensure_fos_access_key`` idempotency check."""
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("rate limit")):
        assert fos_setup.find_fos_key("any", token="tkn") is None


# ── ensure_fos_access_key ─────────────────────────────────────────────────


def test_ensure_fos_access_key_creates_new_key():
    """No existing key → POST /access-keys → return id+secret."""
    fake_resp = {"access_key": "AK999", "secret_key": "SK999"}
    with (
        patch("backend.provision.fos_setup.find_fos_key", return_value=None),
        patch("backend.provision.fos_setup.fastly", return_value=fake_resp) as mock_fastly,
    ):
        out = fos_setup.ensure_fos_access_key("fos-log-analysis-test", state={}, token="tkn", buckets=["b1"])

    assert out == {"id": "AK999", "access_key": "AK999", "secret_key": "SK999"}
    # The POST body must include both permission and buckets
    args, kwargs = mock_fastly.call_args[:2] if len(mock_fastly.call_args) > 1 else (mock_fastly.call_args[0], {})
    payload = mock_fastly.call_args[0][2] if len(mock_fastly.call_args[0]) > 2 else {}
    assert payload.get("permission") == "read-write-objects"
    assert payload.get("buckets") == ["b1"]


def test_ensure_fos_access_key_recreates_managed_key():
    """An existing key with our ``fos-log-analysis-*`` prefix is a
    leftover from a prior run → DELETE then re-POST. Pinned because
    losing this would cause the provisioning to fail on every
    re-run after a previous partial completion."""
    existing = {"access_key": "AK_OLD", "description": "fos-log-analysis-svc1"}
    new_key = {"access_key": "AK_NEW", "secret_key": "SK_NEW"}

    calls: list[tuple] = []

    def _fastly_spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path))
        if method == "POST":
            return new_key
        return {}

    with (
        patch("backend.provision.fos_setup.find_fos_key", return_value=existing),
        patch("backend.provision.fos_setup.fastly", side_effect=_fastly_spy),
    ):
        out = fos_setup.ensure_fos_access_key("fos-log-analysis-svc1", state={}, token="tkn")

    # DELETE happened first, then POST
    assert ("DELETE", "/resources/object-storage/access-keys/AK_OLD") in calls
    assert any(call[0] == "POST" for call in calls)
    assert out["access_key"] == "AK_NEW"


def test_ensure_fos_access_key_raises_on_unmanaged_existing_key():
    """If an existing key has a description NOT starting with
    ``fos-log-analysis-``, it was created manually → don't delete it,
    raise instead. Pinned because clobbering a customer's hand-rolled
    key would be a data-loss bug."""
    existing = {"access_key": "AK_MANUAL", "description": "my-prod-key"}

    with patch("backend.provision.fos_setup.find_fos_key", return_value=existing):
        with pytest.raises(RuntimeError, match="not managed by this tool"):
            fos_setup.ensure_fos_access_key("my-prod-key", state={}, token="tkn")


def test_ensure_fos_access_key_omits_buckets_when_none_supplied():
    """``buckets=None`` → no ``buckets`` key in the POST payload (so
    the key gets account-wide perms). Pinned because sending
    ``"buckets": null`` would trip Fastly's schema validation."""
    fake_resp = {"access_key": "AK", "secret_key": "SK"}
    with (
        patch("backend.provision.fos_setup.find_fos_key", return_value=None),
        patch("backend.provision.fos_setup.fastly", return_value=fake_resp) as mock_fastly,
    ):
        fos_setup.ensure_fos_access_key("d", state={}, token="tkn", buckets=None)

    payload = mock_fastly.call_args[0][2]
    assert "buckets" not in payload


# ── delete_fos_access_key ─────────────────────────────────────────────────


def test_delete_fos_access_key_calls_delete_endpoint():
    with patch("backend.provision.fos_setup.fastly") as mock_fastly:
        fos_setup.delete_fos_access_key("AK123", token="tkn")

    args, kwargs = mock_fastly.call_args[0], mock_fastly.call_args[1]
    assert args[0] == "DELETE"
    assert "AK123" in args[1]


def test_delete_fos_access_key_treats_404_as_already_deleted():
    """404 from the DELETE is idempotency-friendly — treat as success.
    Pinned because the teardown CLI may run twice (manual retry); the
    second run must not error on a missing key."""
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("404 Not Found")):
        fos_setup.delete_fos_access_key("AK123", token="tkn")  # must not raise


def test_delete_fos_access_key_raises_on_other_errors():
    """Non-404 RuntimeError → re-raise. Pinned distinct from the
    404-tolerant path so transient 5xx errors aren't silently
    swallowed (the teardown would think it succeeded and clean up
    state pointing to a still-existing key)."""
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("500 Internal")):
        with pytest.raises(RuntimeError):
            fos_setup.delete_fos_access_key("AK123", token="tkn")


def test_ensure_fos_access_key_emits_status_callbacks_on_recreate():
    """When recreating a managed key, status_cb receives "Recreating
    existing key..." then "Creating ... access key..." then
    "Access key created.". Pinned because the SSE wizard renders
    these as user-visible progress steps — losing them would leave
    the user staring at a stalled progress bar."""
    statuses = []
    fastly_calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        fastly_calls.append((method, path))
        if method == "POST" and "/access-keys" in path:
            return {"access_key": "AKNEW", "secret_key": "SKNEW", "id": "AKNEW"}
        return {}

    with (
        patch(
            "backend.provision.fos_setup.find_fos_key",
            return_value={"access_key": "AKEXISTING"},
        ),
        patch("backend.provision.fos_setup.fastly", side_effect=fake_fastly),
    ):
        result = fos_setup.ensure_fos_access_key(
            "fos-log-analysis-svc-1",
            state={},
            token="tok",
            permission="read-write-objects",
            buckets=["b"],
            status_cb=statuses.append,
        )

    # Status callbacks fired in expected order
    joined = " ".join(statuses)
    assert "Recreating" in joined
    # The existing key was DELETEd then a new POST happened
    assert any(m == "DELETE" for m, _ in fastly_calls)
    assert any(m == "POST" for m, _ in fastly_calls)
    assert result["access_key"] == "AKNEW"


# ── provisioning capture (writes to usage_log) ────────────────────────────


def test_provisioning_tracker_records_create_bucket_to_usage_log():
    """The headline pin: when service_id is supplied, every S3 call
    made during ``ensure_fos_bucket`` lands in usage_log via
    ``metadata_db.log_usage_calls``. Pinned because provisioning ops
    were silently dropped pre-fix — the wrapper did not exist, so
    head_bucket / create_bucket attempts never appeared in any
    dashboard, hiding a class of cost from the operator."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")
    fake_s3.create_bucket.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls") as mock_log,
    ):
        fos_setup.ensure_fos_bucket("new-bucket", "us-east-1", "k", "s", service_id="svc-x")

    assert mock_log.called, "log_usage_calls must be invoked on flush"
    args, kwargs = mock_log.call_args
    assert args[0] == "svc-x"
    methods = [c["method"] for c in args[1]]
    # Both the existence probe and the creation land as rows
    assert "HEAD_BUCKET" in methods
    assert "CREATE_BUCKET" in methods
    assert kwargs["process_context"] == "provision:new-bucket"


def test_provisioning_tracker_marks_create_bucket_as_class_a_in_details():
    """``log_usage_calls`` auto-classifies as Class B by default; the
    "Class A" sentinel in details is the documented escape hatch.
    Pinned because miss-classifying CreateBucket as Class B would
    under-count A-class billable ops for every provisioning run."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")
    fake_s3.create_bucket.return_value = {}

    captured: list[dict] = []

    def _capture(_sid, calls, **_):
        captured.extend(calls)

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s", service_id="svc-x")

    create_row = next(c for c in captured if c["method"] == "CREATE_BUCKET")
    assert "Class A" in create_row["details"]
    head_row = next(c for c in captured if c["method"] == "HEAD_BUCKET")
    assert "Class A" not in head_row["details"]  # HEAD is Class B


def test_provisioning_tracker_records_errors_as_rows_too():
    """A ClientError on create_bucket still results in a usage_log
    row (status="Error:<code>"). Pinned because failed provisioning
    attempts also incur API charges — pretending they didn't happen
    would under-count cost."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")
    fake_s3.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "CreateBucket"
    )

    captured: list[dict] = []

    def _capture(_sid, calls, **_):
        captured.extend(calls)

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        with pytest.raises(RuntimeError):
            fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s", service_id="svc-x")

    create_row = next(c for c in captured if c["method"] == "CREATE_BUCKET")
    assert "Error" in str(create_row["status"])
    assert "AccessDenied" in str(create_row["status"])


def test_provisioning_tracker_tags_teardown_context_separately_from_provision():
    """delete_fos_bucket flushes with process_context="teardown:<name>"
    so dashboards can distinguish setup cost from teardown cost.
    Pinned because mixing the two would hide the teardown-Class-A
    spike that happens when emptying a large bucket."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}
    fake_s3.get_paginator.return_value.paginate.return_value = []
    fake_s3.delete_bucket.return_value = {}

    contexts: list[str] = []

    def _capture(_sid, _calls, **kwargs):
        contexts.append(kwargs.get("process_context"))

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        fos_setup.delete_fos_bucket("doomed-bucket", "us-east-1", "k", "s", service_id="svc-x")

    assert "teardown:doomed-bucket" in contexts


def test_provisioning_tracker_skips_logging_when_no_service_id():
    """Standalone CLI runs without a service_id (e.g. the bootstrap
    wizard runs ensure_fos_bucket before logging_service_id is even
    chosen). The wrapper must not be applied and log_usage_calls
    must not be called. Pinned because a NameError trying to flush
    to a missing service file would crash the CLI."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls") as mock_log,
    ):
        fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s")  # no service_id

    mock_log.assert_not_called()


def test_provisioning_tracker_records_delete_objects_pagination():
    """delete_fos_bucket calls list_objects_v2 via paginator and
    delete_objects from a ThreadPoolExecutor. Both must land in
    usage_log. Pinned because a large-bucket teardown can issue
    thousands of Class A delete_objects calls — losing them would
    hide a major cost spike on every teardown."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "obj1.gz"}, {"Key": "obj2.gz"}]},
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {"Deleted": [{"Key": "obj1.gz"}, {"Key": "obj2.gz"}]}
    fake_s3.delete_bucket.return_value = {}

    captured: list[dict] = []

    def _capture(_sid, calls, **_):
        captured.extend(calls)

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        fos_setup.delete_fos_bucket("doomed-bucket", "us-east-1", "k", "s", service_id="svc-x")

    methods = [c["method"] for c in captured]
    assert "LIST_OBJECTS_V2" in methods
    assert "DELETE_OBJECTS" in methods
    assert "DELETE_BUCKET" in methods


def test_ensure_fos_bucket_calls_status_callback_on_already_exists():
    """When the bucket already exists (head_bucket succeeds), the
    status_cb receives an "already exists" message. Pinned because
    the wizard's idempotent re-run UX depends on this distinction
    vs the "creating..." message admins see on first run."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}  # success

    statuses = []

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        result = fos_setup.ensure_fos_bucket(
            "my-bucket",
            "us-east-1",
            "AK",
            "SK",
            status_cb=statuses.append,
        )

    assert result is True
    # create_bucket was NOT called (idempotency)
    fake_s3.create_bucket.assert_not_called()
    # Status callback got the "already exists" signal
    assert any("already exists" in s.lower() for s in statuses)


def test_delete_fos_prefix_happy_path():
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "raw/log1.gz"}, {"Key": "raw/log2.gz"}]},
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {}

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_prefix("my-bucket", "us-east-1", "k", "s", "raw/")

    fake_s3.delete_objects.assert_called_once_with(
        Bucket="my-bucket", Delete={"Objects": [{"Key": "raw/log1.gz"}, {"Key": "raw/log2.gz"}]}
    )


def test_delete_fos_prefix_excludes_matching():
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "raw/log1.gz"},
                {"Key": "raw/rum/log2.gz"},
                {"Key": "raw/log3.gz"},
            ]
        },
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {}

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_prefix("my-bucket", "us-east-1", "k", "s", "raw/", exclude_prefix="raw/rum/")

    fake_s3.delete_objects.assert_called_once_with(
        Bucket="my-bucket", Delete={"Objects": [{"Key": "raw/log1.gz"}, {"Key": "raw/log3.gz"}]}
    )
