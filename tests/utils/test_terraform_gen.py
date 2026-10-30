"""Unit and CLI integration tests for Terraform configuration generation."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
from filelock import FileLock

from backend import config as svcconfig
from backend.utils.terraform_gen import generate_terraform

TERRAFORM_INSTALLED = shutil.which("terraform") is not None
RUN_VALIDATE = os.environ.get("TERRAFORM_VALIDATE") == "1"
TFLINT_INSTALLED = shutil.which("tflint") is not None
TFLINT_REQUIRED = os.environ.get("TFLINT_REQUIRED") == "1"

if TFLINT_REQUIRED and not TFLINT_INSTALLED:
    raise RuntimeError(
        "TFLINT_REQUIRED=1 but the tflint binary is not on PATH. Install tflint or unset TFLINT_REQUIRED to allow skipping."
    )

TFTEST_DIR = Path(__file__).parent / "terraform_tests"


@pytest.fixture(autouse=True)
def _isolate_configs_dir(tmp_path, monkeypatch):
    """Redirect svcconfig.CONFIGS_DIR to tmp_path so tests never write into repo configs/."""
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", cfg_dir)


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
    """Write generator output to ``dest``, mirroring the zip-export layout."""
    dest_path = os.fspath(dest)
    for fname, content in out.items():
        if fname == "instructions":
            continue
        full = os.path.join(dest_path, fname)
        if "/" in fname:
            os.makedirs(os.path.dirname(full), exist_ok=True)
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
    snippet_files = [f for f in out if f.startswith("capture_snippets/")]
    assert snippet_files, "Expected at least one capture snippet"


def test_every_tf_json_file_parses_as_json():
    """The generator must emit syntactically valid JSON."""
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
    """versions.tf.json must pin the Fastly and AWS providers with the pessimistic operator."""
    out = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    assert "versions.tf.json" in out

    v = json.loads(out["versions.tf.json"])
    tf_block = v["terraform"]
    assert "required_version" in tf_block, "must declare a Terraform CLI floor"

    providers = tf_block["required_providers"]
    assert providers["aws"]["source"] == "hashicorp/aws"
    assert providers["fastly"]["source"] == "fastly/fastly"
    assert providers["aws"]["version"] == "~> 5.0", (
        f"expected '~> 5.0' on aws to gate major bumps; got {providers['aws']['version']!r}"
    )
    assert providers["fastly"]["version"] == "~> 5.0", (
        f"expected '~> 5.0' on fastly to gate major bumps; got {providers['fastly']['version']!r}"
    )
    assert set(providers.keys()) == {"aws", "fastly"}


@pytest.mark.skipif(not TERRAFORM_INSTALLED, reason="terraform binary not on PATH")
def test_baseline_output_passes_terraform_fmt(tmp_path):
    out = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    _write_files(out, str(tmp_path))

    r = subprocess.run(
        ["terraform", "fmt", "-check", "-recursive", "."],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, (
        f"terraform fmt -check failed.\nrc={r.returncode}\nstdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )


@pytest.mark.terraform_cli
@pytest.mark.timeout(300)
@pytest.mark.skipif(
    not (TERRAFORM_INSTALLED and RUN_VALIDATE), reason="set TERRAFORM_VALIDATE=1 to run validate (needs network)"
)
def test_baseline_output_passes_terraform_validate(tmp_path):
    """Real ``terraform validate``. Requires network for provider download."""
    out = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    _write_files(out, str(tmp_path))

    (tmp_path / "_providers.tf").write_text(
        """
provider "aws"    { region = "us-east-1" }
provider "fastly" { api_key = "stub" }
"""
    )

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
    """tflint static analysis on the generated .tf.json bundle."""
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
    assert r.returncode == 0, (
        f"tflint reported issues on generated terraform output:\n"
        f"rc={r.returncode}\nstdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )


# ── Idempotency ──────────────────────────────────────────────────────────────


def test_output_is_byte_identical_across_calls():
    """The same cfg must produce the same files (masking per-load random fallback secret)."""
    out1 = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    out2 = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    assert out1.keys() == out2.keys()
    hex64 = re.compile(r"\b[0-9a-f]{64}\b")
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

    recv = out.get("capture_snippets/recv.vcl", "")
    assert "x_env" in recv, f"Expected 'x_env' in recv snippet, got: {recv[:200]!r}"
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
    assert "bereq_x" in out.get("capture_snippets/fetch.vcl", ""), "origin field missing from fetch snippet"
    assert "bereq_x" in out.get("capture_snippets/deliver.vcl", ""), "origin field missing from deliver snippet"


# ── Injection / escape ──────────────────────────────────────────────────────


@pytest.mark.skipif(not TERRAFORM_INSTALLED, reason="terraform binary not on PATH")
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("fos_bucket_name", 'b"; rm -rf /; #'),
        ("endpoint_name", 'name"; resource "extra" "x" {} #'),
        ("cdn_service_name", 'svc\\name with "quotes"'),
        ("custom_condition", 'req.url ~ "test\\b"'),
        ("cdn_secret", 'secret"with"quotes'),
        ("fos_region", 'r"\nresource "evil" "x" {}'),
    ],
)
def test_injection_does_not_break_terraform_fmt(tmp_path, field, value):
    """User-supplied strings flow into JSON string values and must never break JSON/terraform syntax."""
    cfg = _baseline_cfg()
    cfg[field] = value
    out = generate_terraform(cfg, "AKIA", "sec")
    _write_files(out, str(tmp_path))

    for fname, content in out.items():
        if fname.endswith(".tf.json"):
            json.loads(content)

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
    """The generator must convert ${ / %{ to $${ / %%{ for any user-supplied value."""
    cfg = _baseline_cfg()
    cfg["fos_region"] = 'us-east-1${file("/etc/passwd")}%{ if true }x%{ endif }'
    out = generate_terraform(cfg, "AKIA", "sec")

    cdn = json.loads(out["cdn_proxy.tf.json"])
    items = cdn["resource"]["fastly_service_dictionary_items"]["fos_credentials"]["items"]
    assert items["region"].startswith('us-east-1$${file("/etc/passwd")}%%{ if true }x%%{ endif }')

    for fname, content in out.items():
        if not fname.endswith(".tf.json"):
            continue
        assert '$${file("/etc/passwd")}' in content or '${file("/etc/passwd")}' not in content, (
            f"unescaped ${{file()}} reached {fname} — template-prefix escape missing"
        )
        raw_pct = content.count("%{")
        doubled_pct = content.count("%%{")
        assert raw_pct == doubled_pct, (
            f"{fname} has an unescaped %{{}} template-directive prefix "
            f"(raw=%{{ count {raw_pct}, doubled=%%{{ count {doubled_pct})"
        )


def test_quotes_in_user_input_are_json_escaped():
    """A bucket name with a double-quote MUST land in the JSON output as JSON-escaped."""
    cfg = _baseline_cfg()
    cfg["fos_bucket_name"] = 'bucket"with"quotes'
    out = generate_terraform(cfg, "AKIA", "sec")
    parsed = json.loads(out["fos.tf.json"])
    assert parsed["resource"]["aws_s3_bucket"]["fos_bucket"]["bucket"] == 'bucket"with"quotes'


# ── Syrupy snapshot of generated Terraform (audit follow-up) ────────────────


def test_baseline_output_matches_snapshot(snapshot):
    """Snapshot the full set of generated files for the baseline cfg."""
    out = generate_terraform(_baseline_cfg(), "AKIATESTSTABLE", "test-secret-stable")

    hex64 = re.compile(r"\b[0-9a-f]{64}\b")
    snapshot_view = {fname: hex64.sub("<HEX64>", content) for fname, content in sorted(out.items())}
    assert snapshot_view == snapshot


# ── Resource-graph assertions via terraform test ─────────────────────────────


@pytest.mark.terraform_cli
@pytest.mark.timeout(300)
@pytest.mark.skipif(
    not (TERRAFORM_INSTALLED and RUN_VALIDATE),
    reason="set TERRAFORM_VALIDATE=1 + install terraform to run resource-graph assertions",
)
def test_resource_graph_assertions_via_terraform_test(tmp_path):
    out = generate_terraform(_baseline_cfg(), "AKIA_TEST_KEY", "secret_test_value")
    _write_files(out, tmp_path)

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

    shutil.copy(TFTEST_DIR / "resource_graph.tftest.hcl", tmp_path / "resource_graph.tftest.hcl")

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

    assert "fail" not in test.stdout.lower() or "0 failed" in test.stdout.lower(), (
        f"unexpected failure in terraform test output:\n{test.stdout}"
    )
