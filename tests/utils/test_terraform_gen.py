"""Tests for backend.utils.terraform_gen.generate_terraform.

This module ships ``.tf.json`` files that customers run ``terraform apply``
against. A malformed file means a broken customer infra deploy, so the
suite focuses on:

- Output is valid JSON and passes ``terraform fmt -check`` (canonical formatting).
- Output is byte-identical across repeated calls (idempotent).
- Custom fields produce matching ``capture_snippets/*.vcl`` files.
- User-supplied strings (bucket, endpoint_name, custom_condition) flow through
  ``json.dumps`` so quote / backslash / newline injection is structurally
  impossible. The remaining Terraform-template prefix (``${``, ``%{``) is
  still escaped explicitly — covered by :func:`test_template_prefix_escape`.

``terraform validate`` requires provider downloads (network). It's run when
``TERRAFORM_VALIDATE=1`` is set in the environment (CI), and skipped
otherwise so the suite stays fast and offline-friendly locally.

5b.3a migrated the generator from f-string HCL to ``.tf.json`` (Terraform's
JSON config syntax). The output filenames carry the ``.tf.json`` suffix;
Terraform accepts ``.tf`` and ``.tf.json`` interchangeably in the same
module.
"""

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
