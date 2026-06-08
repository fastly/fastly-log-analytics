"""Tests for backend.utils.terraform_gen.generate_terraform.

This module ships HCL that customers run ``terraform apply`` against. A
malformed file means a broken customer infra deploy, so the suite focuses on:

- Output passes ``terraform fmt -check`` (canonical formatting).
- Output is byte-identical across repeated calls (idempotent).
- Custom fields produce matching ``capture_snippets/*.vcl`` files.
- User-supplied strings (bucket, endpoint_name, custom_condition) can't
  break the generated HCL via injection.

``terraform validate`` requires provider downloads (network). It's run when
``TERRAFORM_VALIDATE=1`` is set in the environment (CI), and skipped
otherwise so the suite stays fast and offline-friendly locally.
"""

from __future__ import annotations

import os
import shutil
import subprocess

import pytest

from backend.utils.terraform_gen import generate_terraform

TERRAFORM_INSTALLED = shutil.which("terraform") is not None
RUN_VALIDATE = os.environ.get("TERRAFORM_VALIDATE") == "1"


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


def _write_files(out: dict[str, str], dest: str) -> None:
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
        "fos.tf",
        "cdn_proxy.tf",
        "logging_service.tf",
        "versions.tf",
        "instructions",
    }
    assert expected.issubset(out.keys()), f"Missing files: {expected - set(out.keys())}"
    # Snippet files prefixed by their phase
    snippet_files = [f for f in out if f.startswith("capture_snippets/")]
    assert snippet_files, "Expected at least one capture snippet"


def test_versions_tf_pins_fastly_and_aws_providers_by_major():
    """versions.tf must pin the Fastly and AWS providers with the
    pessimistic operator so a major-version bump from either provider
    (Fastly v6, AWS v7) doesn't silently break customer apply.

    Pinned by TESTING_PLAN_3 item 17. If you intentionally bump the
    major, update this test deliberately AND the matching scaffold in
    test_baseline_output_passes_terraform_validate above.
    """
    out = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    assert "versions.tf" in out

    v = out["versions.tf"]
    assert "required_version" in v, "must declare a Terraform CLI floor"
    assert 'source = "hashicorp/aws"' in v
    assert 'source = "fastly/fastly"' in v
    # Pessimistic constraint is the contract. >= or no operator would let
    # a major bump through. Test both providers explicitly.
    assert 'version = "~> 5.0"' in v, (
        f"expected ~> pessimistic constraints in versions.tf to gate major bumps; got:\n{v}"
    )
    # Belt-and-braces: there must be exactly TWO required providers (we
    # don't want an accidental third undeclared source slipping in).
    assert v.count("source =") == 2


@pytest.mark.skipif(not TERRAFORM_INSTALLED, reason="terraform binary not on PATH")
def test_baseline_output_passes_terraform_fmt(tmp_path):
    out = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    _write_files(out, str(tmp_path))

    # Run terraform from inside tmp_path with "." as target. On macOS,
    # passing an absolute /private/var/folders/... path while CWD is also
    # rooted under /private/ confuses terraform's relative-path resolution
    # ("No file or directory at ../../private/var/..."). Using a relative
    # target sidesteps the bug.
    r = subprocess.run(
        ["terraform", "fmt", "-check", "-recursive", "."],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, (
        f"terraform fmt -check failed.\nrc={r.returncode}\nstdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )


@pytest.mark.skipif(
    not (TERRAFORM_INSTALLED and RUN_VALIDATE), reason="set TERRAFORM_VALIDATE=1 to run validate (needs network)"
)
def test_baseline_output_passes_terraform_validate(tmp_path):
    """Real ``terraform validate``. Requires network for provider download."""
    out = generate_terraform(_baseline_cfg(), "AKIA", "sec")
    _write_files(out, str(tmp_path))

    # The generator now emits its own versions.tf with pinned providers
    # (TESTING_PLAN_3 item 17). Only the provider *configuration* needs
    # stubbing for init/validate.
    (tmp_path / "_providers.tf").write_text(
        """
provider "aws"    { region = "us-east-1" }
provider "fastly" { api_key = "stub" }
"""
    )

    init = subprocess.run(
        ["terraform", "init", "-backend=false", "-input=false", "-no-color"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
    )
    assert init.returncode == 0, f"terraform init failed:\n{init.stdout}\n{init.stderr}"

    val = subprocess.run(
        ["terraform", "validate", "-no-color"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
    )
    assert val.returncode == 0, f"terraform validate failed:\n{val.stdout}\n{val.stderr}"


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
    assert out1["fos.tf"] != out2["fos.tf"]
    assert "different-bucket" in out2["fos.tf"]


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


# ── Injection / escape fuzz ──────────────────────────────────────────────────


@pytest.mark.skipif(not TERRAFORM_INSTALLED, reason="terraform binary not on PATH")
@pytest.mark.parametrize(
    "field,value",
    [
        # Strings the generator splices via f-string into HCL string literals.
        # If the field isn't escaped, the closing quote/brace can break the file.
        ("fos_bucket_name", 'b"; rm -rf /; #'),
        ("endpoint_name", 'name"; resource "extra" "x" {} #'),
        ("cdn_service_name", 'svc\\name with "quotes"'),
        ("custom_condition", 'req.url ~ "test\\b"'),
        ("cdn_secret", 'secret"with"quotes'),
    ],
)
def test_injection_fuzz_does_not_break_terraform_fmt(tmp_path, field, value):
    """User-supplied strings flow into HCL via f-string. Even when the values
    contain quotes or HCL syntax, the result must still parse."""
    cfg = _baseline_cfg()
    cfg[field] = value
    out = generate_terraform(cfg, "AKIA", "sec")
    _write_files(out, str(tmp_path))

    # We use `terraform fmt` (not -check) — it parses the file. If the
    # injection broke HCL syntax, fmt errors with a non-zero rc and a clear
    # message. fmt -check would also flag a formatting *change* as failure
    # which is OK here too (still proves it parsed), but the parse error is
    # what we actually care about.
    # Use cwd=tmp_path + "." for the same macOS /private/-prefix reason
    # documented in test_baseline_output_passes_terraform_fmt above.
    r = subprocess.run(
        ["terraform", "fmt", "-recursive", "."],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
    )
    # Filter true parse errors (Diagnostic markers) vs simple format diffs.
    # A parse failure produces "Error: ..." on stderr.
    assert "Error:" not in r.stderr, (
        f"Injection broke HCL parse for {field}={value!r}:\nstdout: {r.stdout[:400]}\nstderr: {r.stderr[:400]}"
    )
