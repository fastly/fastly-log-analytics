"""``.tftest.hcl`` resource-graph assertions (TESTING_PLAN_3 item 16).

``test_terraform_gen.py``'s ``terraform validate`` test proves the
output is syntactically valid HCL the providers accept. That's a
necessary but insufficient bar: a generator that dropped the
``cdn-no-cache-404`` snippet would still pass validate. This test fills
the gap by running ``terraform test`` with ``command = plan``
assertions that pin the *shape* of the planned resource graph
(specific dictionaries, snippet names, backend addresses, dictionary
items).

The ``.tftest.hcl`` lives next to this file at
``terraform_tests/resource_graph.tftest.hcl``. It runs in plan mode
only — no real Fastly/AWS calls — so the test is gated on the same
``TERRAFORM_VALIDATE=1`` flag CI already sets (it still needs the
provider binaries downloaded, which is a network operation).

Why ``terraform test`` and not Python HCL parsing: the .tftest.hcl
assertions get to use Terraform's expression language to traverse
nested blocks (``[for d in fastly_service_vcl.cdn_proxy.dictionary :
d if d.name == "fos_credentials"]``), which catches a much broader
class of regressions than substring matching on raw HCL. They also
travel with the generator output — a customer's own CI can run the
same test against their generated module.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from backend.utils.terraform_gen import generate_terraform

TERRAFORM_INSTALLED = shutil.which("terraform") is not None
RUN_VALIDATE = os.environ.get("TERRAFORM_VALIDATE") == "1"

TFTEST_DIR = Path(__file__).parent / "terraform_tests"


def _baseline_cfg() -> dict:
    """Mirror of test_terraform_gen.py::_baseline_cfg so the .tftest.hcl
    assertions can target known values (bucket name, region, etc.)."""
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


def _write_files(out: dict[str, str], dest: Path) -> None:
    for fname, content in out.items():
        if fname == "instructions":
            continue
        full = dest / fname
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(content)


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
