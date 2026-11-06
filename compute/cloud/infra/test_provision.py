"""Synthetic boundary tests; these responses are not recorded API evidence."""

import base64
import hashlib
import io
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tarfile
import uuid
from pathlib import Path

import httpx
import provision
import pytest


def test_plan_is_deterministic_isolated_and_contains_no_credentials() -> None:
    first = provision.resource_plan("team-a")
    assert first == provision.resource_plan("team-a")
    assert first["services"] == {
        "compute": "team-a",
        "worker": "team-a-worker",
        "front": "team-a-front",
    }
    assert first["stores"] == {
        "secret": "team-a_secrets",
        "config": "team-a_config",
        "kv": "team-a_state",
    }
    assert first["links"] == {
        "secret": "flp_secrets",
        "config": "flp_config",
        "kv": "flp_state",
    }
    assert first["public_base_url"] == "https://team-a.global.ssl.fastly.net"
    assert set(first["secret_keys"]) == {
        "session_key",
        "front_key",
        "cdn_key",
        "worker_secret",
        "admin_setup_hash",
        "fastly_token",
    }
    assert "secret_values" not in json.dumps(first)
    assert first != provision.resource_plan("team-b")


@pytest.mark.parametrize("name", ["", "../flp", "Team", "a_b", "-flp", "flp\nx"])
def test_unsafe_install_names_are_rejected(name: str) -> None:
    with pytest.raises(ValueError, match="name"):
        provision.resource_plan(name)


@pytest.mark.parametrize(
    "host",
    ["127.0.0.1", "::1", "http://origin.example", "origin.example/path", "x\nY:z"],
)
def test_backend_rejects_non_domain_origins(host: str) -> None:
    with pytest.raises(ValueError, match="domain"):
        provision.backend("flp", host)


def test_static_tls_backend_has_matching_hostnames() -> None:
    assert provision.backend("flp_tick", "team-a.edgecompute.app", shield=True) == {
        "name": "flp_tick",
        "address": "team-a.edgecompute.app",
        "override_host": "team-a.edgecompute.app",
        "ssl_sni_hostname": "team-a.edgecompute.app",
        "ssl_cert_hostname": "team-a.edgecompute.app",
        "use_ssl": "1",
        "ssl_check_cert": "1",
        "port": "443",
        "prefer_ipv6": "0",
        "connect_timeout": "5000",
        "first_byte_timeout": "60000",
        "between_bytes_timeout": "60000",
        "shield": "iad-va-us",
    }


def test_storage_backends_use_the_shared_front_peer_and_fixed_regional_hosts() -> None:
    plan = provision.resource_plan("team-a")
    for role in ("compute", "worker"):
        backends = {item["name"]: item for item in plan["backends"][role]}
        assert backends["flp_front"]["address"] == "team-a.global.ssl.fastly.net"
        assert backends["fos_eu_central_1"]["address"] == ("eu-central-1.object.fastlystorage.app")
        assert all(
            item["address"] == item["override_host"] == item["ssl_cert_hostname"] == item["ssl_sni_hostname"]
            for item in backends.values()
        )


def test_front_app_and_tick_backends_are_shielded() -> None:
    front = {item["name"]: item for item in provision.resource_plan("team-a")["backends"]["front"]}
    assert front["flp"]["shield"] == "iad-va-us"
    assert front["flp_tick"]["shield"] == "iad-va-us"
    assert "shield" not in front["flp_clock"]
    assert "shield" not in front["flp_flush_clock"]


def test_front_has_separate_tick_and_flush_clocks_routed_to_the_shielded_tick_backend() -> None:
    plan = provision.resource_plan("team-a")
    front = {item["name"]: item for item in plan["backends"]["front"]}
    assert (plan["healthcheck"]["name"], plan["healthcheck"]["path"]) == (
        "flp_clock",
        "/_tick",
    )
    flush = plan["flush_healthcheck"]
    assert (flush["name"], flush["path"]) == ("flp_flush", "/_flush")
    assert {k: v for k, v in flush.items() if k not in ("name", "path")} == {
        k: v for k, v in plan["healthcheck"].items() if k not in ("name", "path")
    }
    assert front["flp_flush_clock"]["healthcheck"] == "flp_flush"
    assert front["flp_flush_clock"]["request_condition"] == "flp_clock_never"
    assert provision.TICK_CONDITION == ('req.url.path == "/_tick" || req.url.path == "/_flush"')
    assert '"/_flush"' in provision.APP_CONDITION


def test_shield_safe_vcl_contracts_falco_cannot_inject() -> None:
    # Falco 2.0.1 hardcodes fastly.ff.visits_this_service and fastly_info.state.
    vcl = (Path(__file__).parents[1] / "vcl/front/main.vcl").read_text()
    recv = vcl[: vcl.index("sub vcl_hash")]
    assert re.search(
        r"# Edge and shield cache across seconds for the sleep TTL Compute returns in x-flp-tick-ttl\.\s+"
        r"set req\.url = req\.url\.path;",
        recv,
    )
    fetch = vcl[vcl.index("sub vcl_fetch") : vcl.index("sub vcl_deliver")]
    assert re.search(
        r"if \(fastly\.ff\.visits_this_service == 0\) \{\s+"
        r"unset beresp\.http\.x-flp-tick-ttl;\s+\}",
        fetch,
    )
    deliver = vcl[vcl.index("sub vcl_deliver") :]
    assert re.search(
        r"#RATELIMIT_END\s+if \(fastly\.ff\.visits_this_service == 0\) \{\s+"
        r"unset resp\.http\.x-flp-auth-fail;",
        deliver,
    )
    assert (
        'set resp.http.x-flp-fos-cache = if(fastly_info.state ~ "^HIT" '
        '&& fastly_info.state !~ "(SYNTH|PASS)", "HIT", "MISS");'
    ) in deliver


def test_existing_install_storage_backends_are_added_without_replacing_others() -> None:
    class BackendsAPI:
        def __init__(self) -> None:
            self.items = [{"name": "customer", "address": "customer.example"}]

        def request(self, method, path, payload=None, *, form=False):
            assert path == "/service/app/version/3/backend"
            if method == "GET":
                return list(self.items)
            assert method == "POST"
            self.items.append(payload)
            return payload

    api = BackendsAPI()
    provision.prepare_storage_backends(api, "/service/app/version/3", "front.example")
    assert api.items[0] == {"name": "customer", "address": "customer.example"}
    assert len(api.items) == 11
    provision.prepare_storage_backends(api, "/service/app/version/3", "front.example")
    assert len(api.items) == 11
    api.items[1]["address"] = "different.example"
    with pytest.raises(ValueError, match="backend"):
        provision.prepare_storage_backends(api, "/service/app/version/3", "front.example")


def test_setup_code_only_persists_hash_and_generated_keys_are_independent(
    monkeypatch,
) -> None:
    monkeypatch.setattr(provision.time, "time", lambda: 1_799_999_999.875)
    values = provision.initial_secrets("api-token")
    assert set(values) == {
        "session_key",
        "front_key",
        "cdn_key",
        "worker_secret",
        "admin_setup_hash",
        "fastly_token",
    }
    assert json.loads(values["admin_setup_hash"])["expires_at"] == 0
    assert values["fastly_token"] == "api-token"
    keys = [values[key] for key in ("session_key", "front_key", "cdn_key", "worker_secret")]
    assert len(set(keys)) == 4
    assert all(len(key) >= 43 for key in keys)


def test_initial_secrets_requires_api_token() -> None:
    with pytest.raises(ValueError, match="Fastly API token"):
        provision.initial_secrets("")


def test_no_erl_render_strips_all_marked_declarations_and_calls() -> None:
    vcl = provision.load_vcl(rate_limiting=False)
    assert "#RATELIMIT" not in vcl
    assert "ratelimit." not in vcl
    assert "ratecounter " not in vcl
    assert "penaltybox " not in vcl
    assert 'set req.http.x-flp-front-rl = "0";' in vcl
    assert "unset resp.http.x-flp-auth-fail;" in vcl
    assert 'table.lookup(flp_front_auth, "front_key", "")' in vcl


class SyntheticAPI:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, object, bool]] = []

    def request(self, method: str, path: str, payload: object = None, *, form: bool = False) -> object:
        self.calls.append((method, path, payload, form))
        if path.endswith("/version"):
            return [{"number": 2, "active": True}, {"number": 3, "active": False}]
        if path.endswith("/generated_vcl"):
            return {"content": "pragma optional_param ratelimit_opt_in true;"}
        raise AssertionError(f"Unexpected synthetic API request: {method} {path}")


def test_erl_detection_reads_active_generated_vcl_only() -> None:
    api = SyntheticAPI()
    assert provision.account_has_rate_limiting(api, "front-id") is True
    assert [call[:2] for call in api.calls] == [
        ("GET", "/service/front-id/version"),
        ("GET", "/service/front-id/version/2/generated_vcl"),
    ]


def test_erl_detection_distinguishes_unavailable_from_unknown() -> None:
    class NoERL(SyntheticAPI):
        def request(self, method, path, payload=None, *, form=False):
            if path.endswith("/generated_vcl"):
                return {"content": "pragma optional_param ratelimit_opt_in false;"}
            return super().request(method, path, payload, form=form)

    class Unavailable(SyntheticAPI):
        def request(self, *args, **kwargs):
            raise provision.APIError("Fastly GET /service/front-id/version returned 403")

    assert provision.account_has_rate_limiting(NoERL(), "front-id") is False
    assert provision.account_has_rate_limiting(Unavailable(), "front-id") is None


def test_api_errors_never_echo_response_bodies_or_tokens(monkeypatch, caplog) -> None:
    def denied(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Fastly-Key") == "sensitive-deploy-token"
        return httpx.Response(400, content=b"sensitive-deploy-token")

    monkeypatch.setattr(provision, "HTTP_TRANSPORT", httpx.MockTransport(denied))
    api = provision.FastlyAPI("sensitive-deploy-token")
    with pytest.raises(provision.APIError) as failure:
        api.request("POST", "/service", {"name": "test"}, form=True)
    assert str(failure.value) == "Fastly POST /service returned 400"
    assert "sensitive" not in repr(api)
    assert "sensitive" not in caplog.text
    event = json.loads(caplog.records[-1].message)
    assert event["status"] == 400
    assert event["duration_ms"] >= 0


def test_secret_api_uses_base64_json_without_a_subprocess(monkeypatch) -> None:
    captured: list[httpx.Request] = []

    def accepted(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, content=b'{"name":"front_key"}')

    monkeypatch.setattr(provision, "HTTP_TRANSPORT", httpx.MockTransport(accepted))
    provision.put_secret(provision.FastlyAPI("deploy-token"), "store-id", "front_key", "abc")
    request = captured[0]
    assert request.method == "PUT"
    assert str(request.url).endswith("/resources/stores/secret/store-id/secrets")
    assert json.loads(request.content) == {
        "name": "front_key",
        "secret": base64.b64encode(b"abc").decode(),
    }


def test_default_cli_dry_run_needs_no_token_and_performs_no_writes() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(Path(provision.__file__)),
            "bootstrap",
            "--name",
            "dry-test",
        ],
        text=True,
        capture_output=True,
        check=True,
        env={"PATH": "", "HOME": str(Path.cwd())},
    )
    assert json.loads(result.stdout)["services"]["front"] == "dry-test-front"
    assert result.stderr == ""


def test_admin_reset_replaces_hash_without_seeding_or_resetting_other_keys(
    monkeypatch,
) -> None:
    monkeypatch.setattr(provision.time, "time", lambda: 1_799_999_999.875)

    class SecretAPI:
        def __init__(self):
            self.writes = []

        def request(self, method, path, payload=None, *, form=False):
            self.writes.append((method, path, payload))
            return {}

    api = SecretAPI()
    previous_code = provision.admin_reset(api, "store-id")
    previous_value = api.writes[0][2]["secret"]
    api.writes.clear()
    code = provision.admin_reset(api, "store-id")
    assert code != previous_code
    assert api.writes[0][2]["secret"] != previous_value
    value = base64.b64decode(api.writes[0][2]["secret"]).decode()
    assert json.loads(value) == {
        "sha256": hashlib.sha256(code.encode()).hexdigest(),
        "expires_at": 1_800_000_899,
    }
    assert code not in value
    assert api.writes == [
        (
            "PUT",
            "/resources/stores/secret/store-id/secrets",
            {
                "name": "admin_setup_hash",
                "secret": base64.b64encode(value.encode()).decode(),
            },
        )
    ]


@pytest.mark.parametrize("command", ["admin-reset", "admin-setup-code"])
def test_cli_prints_setup_code_once_with_expiry_and_replacement_guidance(
    command, inventory_path, monkeypatch, capsys
) -> None:
    api = SyntheticBootstrapAPI()
    monkeypatch.setattr(provision, "FastlyAPI", lambda token: api)
    monkeypatch.setenv("FASTLY_API_TOKEN", "synthetic-deploy-token")
    monkeypatch.delenv("FLP_APP_TOKEN", raising=False)

    def unexpected_prompt(message):
        pytest.fail("No additional app credential should be requested")

    monkeypatch.setattr(provision.getpass, "getpass", unexpected_prompt)
    assert provision.main([command, "--apply", "--secret-store-id", "store-id"]) == 0
    output = capsys.readouterr()
    prefix = "One-time admin setup code (shown only once; only its hash is stored): "
    code = output.out.split(prefix)[1].splitlines()[0]
    assert output.out.count(code) == 1
    assert "15 minutes" in output.out
    assert "invalidates" in output.out
    assert "previous" in output.out
    assert "synthetic-deploy-token" not in output.out + output.err
    secret = next(
        payload["secret"]
        for _, _, payload, _ in api.calls
        if isinstance(payload, dict) and payload.get("name") == "admin_setup_hash"
    )
    assert json.loads(base64.b64decode(secret))["sha256"] == hashlib.sha256(code.encode()).hexdigest()


def test_bootstrap_prints_no_setup_code_and_stores_an_expired_placeholder(inventory_path, monkeypatch, capsys) -> None:
    api = SyntheticBootstrapAPI()
    monkeypatch.setattr(provision, "FastlyAPI", lambda token: api)
    monkeypatch.setenv("FASTLY_API_TOKEN", "synthetic-deploy-token")
    args = ["bootstrap", "--apply", "--inventory", str(inventory_path), "--no-erl"]
    assert provision.main(args) == 0
    assert "setup code" not in capsys.readouterr().out
    secret = next(
        payload["secret"]
        for _, _, payload, _ in api.calls
        if isinstance(payload, dict) and payload.get("name") == "admin_setup_hash"
    )
    assert json.loads(base64.b64decode(secret))["expires_at"] == 0


@pytest.mark.parametrize("legacy_app_token", [None, "synthetic-deploy-token"])
def test_bootstrap_prompts_only_for_the_deploy_token(inventory_path, monkeypatch, capsys, legacy_app_token) -> None:
    api = SyntheticBootstrapAPI()
    monkeypatch.delenv("FASTLY_API_TOKEN", raising=False)
    if legacy_app_token is None:
        monkeypatch.delenv("FLP_APP_TOKEN", raising=False)
    else:
        monkeypatch.setenv("FLP_APP_TOKEN", legacy_app_token)
    prompts = []
    credentials = {
        "Local deploy token: ": "synthetic-deploy-token",
    }

    def prompt(message):
        prompts.append(message)
        return credentials[message]

    def connect(token):
        assert token == "synthetic-deploy-token"
        return api

    monkeypatch.setattr(provision.getpass, "getpass", prompt)
    monkeypatch.setattr(provision, "FastlyAPI", connect)
    assert provision.main(["bootstrap", "--apply", "--inventory", str(inventory_path), "--no-erl"]) == 0
    assert prompts == ["Local deploy token: "]
    writes = {
        fields["name"]: base64.b64decode(fields["secret"]).decode()
        for method, path, fields, _ in api.calls
        if method == "PUT" and path.endswith("/secrets")
    }
    assert set(writes) == set(provision.resource_plan()["secret_keys"])
    assert writes["fastly_token"] == "synthetic-deploy-token"
    assert not any(method == "DELETE" for method, *_ in api.calls)
    output = capsys.readouterr()
    for token in credentials.values():
        assert token not in output.out + output.err + inventory_path.read_text()


def test_admin_reset_securely_prompts_without_environment_token(monkeypatch, capsys) -> None:
    api = SyntheticBootstrapAPI()
    monkeypatch.delenv("FASTLY_API_TOKEN", raising=False)
    prompts = []

    def prompt(message):
        prompts.append(message)
        return "synthetic-prompt-token"

    def connect(token):
        assert token == "synthetic-prompt-token"
        return api

    monkeypatch.setattr(provision.getpass, "getpass", prompt)
    monkeypatch.setattr(provision, "FastlyAPI", connect)
    assert provision.main(["admin-reset", "--secret-store-id", "store-id", "--apply"]) == 0
    assert prompts == ["Local deploy token: "]
    output = capsys.readouterr()
    assert "synthetic-prompt-token" not in output.out + output.err


def test_failed_admin_reset_does_not_print_a_code(monkeypatch, capsys) -> None:
    class FailedAPI:
        def request(self, *args, **kwargs):
            raise provision.APIError("Fastly PUT secrets returned 403")

    monkeypatch.setattr(provision, "FastlyAPI", lambda token: FailedAPI())
    monkeypatch.setenv("FASTLY_API_TOKEN", "synthetic-deploy-token")
    assert provision.main(["admin-reset", "--secret-store-id", "store-id", "--apply"]) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "403" in output.err


@pytest.fixture
def reset_http(monkeypatch):
    state: dict[str, object] = {"calls": [], "fail_method": None}
    monkeypatch.setenv("FASTLY_API_TOKEN", "synthetic-token-never-real")
    monkeypatch.setattr(provision.secrets, "token_urlsafe", lambda size: "synthetic-setup-code")

    def respond(request: httpx.Request) -> httpx.Response:
        calls = state["calls"]
        assert isinstance(calls, list)
        calls.append(request)
        if request.method == state["fail_method"]:
            return httpx.Response(403, content=b"synthetic-token-never-real")
        if request.method == "GET":
            assert str(request.url) == ("https://api.fastly.com/resources/stores/secret")
            return httpx.Response(200, json=[{"id": "test-store", "name": "flp_secrets"}])
        assert request.method == "PUT"
        assert str(request.url).endswith("/secret/test-store/secrets")
        return httpx.Response(200, json={})

    state["respond"] = respond
    monkeypatch.setattr(provision, "HTTP_TRANSPORT", httpx.MockTransport(respond))
    return state


@pytest.mark.parametrize("command", ["admin-reset", "admin-setup-code"])
def test_setup_code_guidance_does_not_claim_edge_readiness_or_extend_expiry(
    reset_http, monkeypatch, capsys, command
) -> None:
    clock = [1_800_000_000]
    monkeypatch.setattr(provision.time, "time", lambda: clock[0])
    respond = reset_http["respond"]

    def delayed_write(request: httpx.Request) -> httpx.Response:
        response = respond(request)
        if request.method == "PUT":
            clock[0] += 75
        return response

    def unexpected_sleep(seconds):
        pytest.fail("Code rotation must not sleep or poll for readiness")

    monkeypatch.setattr(provision, "HTTP_TRANSPORT", httpx.MockTransport(delayed_write))
    monkeypatch.setattr(provision.time, "sleep", unexpected_sleep)
    assert provision.main([command, "--apply"]) == 0
    output = capsys.readouterr()
    assert "about a minute or longer" in output.out
    assert "API write success does not confirm edge readiness" in output.out
    assert "Wait before retrying" in output.out
    assert "repeated submissions or regenerating codes" in output.out
    assert "Expires 15 minutes after generation" in output.out
    assert output.out.count("synthetic-setup-code") == 1
    assert output.err == ""
    assert [request.method for request in reset_http["calls"]] == ["GET", "PUT"]
    payload = json.loads(reset_http["calls"][1].content)
    stored = json.loads(base64.b64decode(payload["secret"]))
    assert stored["expires_at"] == 1_800_000_900


@pytest.mark.parametrize(
    "command,verbose,expected_events",
    [
        ("admin-reset", False, 0),
        ("admin-reset", True, 2),
        ("admin-setup-code", False, 2),
    ],
)
def test_reset_http_diagnostics_are_opt_in_only_for_admin_reset(
    reset_http, command, verbose, expected_events, caplog, capsys
) -> None:
    args = [command, "--apply"] + (["--verbose"] if verbose else [])
    with caplog.at_level(logging.INFO, logger=provision.LOG.name):
        assert provision.main(args) == 0
    events = [json.loads(record.message) for record in caplog.records]
    assert len(events) == expected_events
    assert all(event["status"] == 200 for event in events)
    assert all(event["event"] == "fastly.api" for event in events)
    assert len(reset_http["calls"]) == 2
    output = capsys.readouterr()
    assert output.out.count("synthetic-setup-code") == 1
    assert "synthetic-token-never-real" not in output.out + output.err + caplog.text


@pytest.mark.parametrize("tty", [False, True])
@pytest.mark.parametrize("no_color", [None, "1", ""])
def test_admin_reset_output_colors_only_for_terminal_without_no_color(
    reset_http, monkeypatch, capsys, tty, no_color
) -> None:
    monkeypatch.setattr(provision.sys.stdout, "isatty", lambda: tty)
    if no_color is None:
        monkeypatch.delenv("NO_COLOR", raising=False)
    else:
        monkeypatch.setenv("NO_COLOR", no_color)
    assert provision.main(["admin-reset", "--apply"]) == 0
    output = capsys.readouterr()
    assert ("\x1b[" in output.out) == (tty and no_color is None)
    assert re.sub(r"\x1b\[[0-9;]*m", "", output.out) == (
        "One-time admin setup code (shown only once; only its hash is stored): synthetic-setup-code\n"
        "Expires 15 minutes after generation. Generating a replacement invalidates the previous code.\n"
        "New codes may take about a minute or longer to reach the app. "
        "API write success does not confirm edge readiness.\n"
        "Wait before retrying; avoid repeated submissions or regenerating codes.\n"
    )
    assert output.err == ""


@pytest.mark.parametrize("fail_method", ["GET", "PUT"])
def test_quiet_reset_keeps_http_failures_visible_and_restores_other_command_logging(
    reset_http, fail_method, caplog, capsys
) -> None:
    reset_http["fail_method"] = fail_method
    with caplog.at_level(logging.INFO, logger=provision.LOG.name):
        assert provision.main(["admin-reset", "--apply"]) == 1
        output = capsys.readouterr()
        assert output.out == ""
        assert f"Error: Fastly {fail_method}" in output.err
        assert "403" in output.err
        errors = [json.loads(record.message) for record in caplog.records if record.levelno >= logging.ERROR]
        assert len(errors) == 1
        assert errors[0]["status"] == 403
        assert "synthetic-token-never-real" not in output.err + caplog.text
        caplog.clear()
        reset_http["fail_method"] = None
        assert provision.main(["admin-setup-code", "--apply"]) == 0
        assert len(caplog.records) == 2


@pytest.fixture
def make_project():
    root = Path(__file__).parent / f".test-make-{uuid.uuid4()} with spaces"
    try:
        (root / "compute" / "cloud" / "infra").mkdir(parents=True)
        yield root
    finally:
        shutil.rmtree(root)


@pytest.mark.parametrize("directory", [".", "compute/cloud"])
@pytest.mark.parametrize("name", [None, "team-a"])
@pytest.mark.parametrize("verbose", [False, True])
@pytest.mark.parametrize(
    "dotenv_value",
    [None, "synthetic dotenv token with spaces", '$(touch "$FAKE_UV_MARKER")'],
)
def test_make_admin_reset_uses_project_env_without_exposing_credentials(
    make_project, directory, name, dotenv_value, verbose
) -> None:
    source = Path(provision.__file__).resolve().parents[3]
    for relative in ("Makefile", "compute/cloud/Makefile"):
        if (source / relative).exists():
            shutil.copyfile(source / relative, make_project / relative)
    fake_bin = make_project / "bin"
    fake_bin.mkdir()
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, sys\n"
        "pathlib.Path(os.environ['FAKE_UV_CAPTURE']).write_text(json.dumps({\n"
        "    'args': sys.argv[1:], 'cwd': os.getcwd(),\n"
        "    'token': os.environ.get('FASTLY_API_TOKEN'),\n"
        "}))\n"
    )
    fake_uv.chmod(0o700)
    capture = make_project / "capture.json"
    marker = make_project / "shell-executed"
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"MAKEFLAGS", "MFLAGS", "MAKELEVEL", "FASTLY_API_TOKEN"}
    }
    env.update(
        PATH=str(fake_bin) + os.pathsep + os.environ["PATH"],
        FAKE_UV_CAPTURE=str(capture),
        FAKE_UV_MARKER=str(marker),
    )
    expected_token = None
    if dotenv_value is not None:
        (make_project / ".env").write_text(f"FASTLY_API_TOKEN={dotenv_value}\n")
    if name is not None:
        expected_token = "synthetic-environment-token"
        env["FASTLY_API_TOKEN"] = expected_token
    args = ["make", "admin-reset"]
    if name is not None:
        args.append(f"NAME={name}")
    if verbose:
        args.append("VERBOSE=1")
    result = subprocess.run(
        args,
        cwd=make_project / directory,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert not marker.exists(), "Make executed dotenv content as a shell command"
    assert result.returncode == 0, result.stdout + result.stderr
    dotenv_args = ["--env-file", str(make_project / ".env")] if dotenv_value is not None else []
    assert json.loads(capture.read_text()) == {
        "args": [
            "run",
            *dotenv_args,
            "infra/provision.py",
            "admin-reset",
            "--name",
            name or "flp",
            "--apply",
            *(["--verbose"] if verbose else []),
        ],
        "cwd": str(make_project / "compute" / "cloud"),
        "token": expected_token,
    }
    assert result.stdout == ""
    assert result.stderr == ""
    if expected_token:
        assert expected_token not in result.stdout + result.stderr
    if dotenv_value is not None:
        assert dotenv_value not in result.stdout + result.stderr
        assert (make_project / ".env").read_text() == (f"FASTLY_API_TOKEN={dotenv_value}\n")
    assert not list((make_project / "compute" / "cloud" / "infra").iterdir())


def test_root_make_without_target_only_prints_help(make_project, monkeypatch) -> None:
    source = Path(provision.__file__).resolve().parents[3]
    for relative in ("Makefile", "compute/cloud/Makefile"):
        shutil.copyfile(source / relative, make_project / relative)
    fake_bin = make_project / "bin"
    fake_bin.mkdir()
    marker = make_project / "uv-was-invoked"
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(f"#!{sys.executable}\nfrom pathlib import Path\nPath({str(marker)!r}).touch()\n")
    fake_uv.chmod(0o700)
    monkeypatch.setenv("PATH", str(fake_bin) + os.pathsep + os.environ["PATH"])
    for key in ("MAKEFLAGS", "MFLAGS", "MAKELEVEL"):
        monkeypatch.delenv(key, raising=False)
    result = subprocess.run(
        ["make", "--no-print-directory"],
        cwd=make_project,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert not marker.exists(), "Bare make invoked a credential-changing command"
    assert "make admin-reset" in result.stdout
    assert "NAME" in result.stdout
    assert result.stderr == ""


@pytest.fixture
def inventory_path():
    path = Path(__file__).parent / f".test-{uuid.uuid4()}.local.json"
    try:
        yield path
    finally:
        path.unlink(missing_ok=True)


class SyntheticBootstrapAPI:
    """Deliberately synthetic write responses; no recorded-API claim."""

    def __init__(self) -> None:
        self.calls = []

    def request(self, method, path, payload=None, *, form=False):
        self.calls.append((method, path, payload, form))
        if method == "GET":
            if path.endswith("/validate"):
                return {"status": "ok"}
            return []
        if path == "/service":
            return {"id": payload["name"] + "-id"}
        if path.startswith("/resources/stores/") and path.count("/") == 3:
            return {"id": payload["name"] + "-id"}
        if path.endswith("/dictionary"):
            return {"id": payload["name"] + "-id"}
        return {}


def test_bootstrap_drafts_wire_isolated_resources_and_never_activate(
    inventory_path,
) -> None:
    api = SyntheticBootstrapAPI()
    inventory = provision.bootstrap(
        api,
        provision.resource_plan("team-a"),
        inventory_path,
        rate_limiting=False,
        api_token="api-token",
    )
    saved = inventory_path.read_text()
    assert json.loads(saved) == inventory
    assert inventory["activated"] is False
    assert inventory["configured"] is True
    assert all(secret not in saved for secret in ("app-token", "api-token", "admin_setup_hash"))
    links = [(path, fields) for method, path, fields, _ in api.calls if method == "POST" and path.endswith("/resource")]
    assert len(links) == 6
    for role in ("team-a", "team-a-worker"):
        assert {
            entry["name"]: entry["resource_id"]
            for path, entry in links
            if path == f"/service/{role}-id/version/1/resource"
        } == {
            "flp_secrets": "team-a_secrets-id",
            "flp_config": "team-a_config-id",
            "flp_state": "team-a_state-id",
        }
    backends = [fields for _, path, fields, _ in api.calls if path.endswith("/backend")]
    assert len(backends) == 28
    assert all(fields["prefer_ipv6"] == "0" for fields in backends)
    tick = next(fields for fields in backends if fields["name"] == "flp_tick")
    assert tick["shield"] == "iad-va-us"
    assert tick["request_condition"] == "flp_tick"
    assert all(
        fields["address"] == fields["override_host"] == fields["ssl_sni_hostname"] == fields["ssl_cert_hostname"]
        for fields in backends
    )
    calls = [(method, path) for method, path, *_ in api.calls]
    assert not any("/activate" in path or "/package" in path for _, path in calls)
    assert not any("object-storage" in path for _, path in calls)
    secret_writes = {
        fields["name"]: base64.b64decode(fields["secret"]).decode()
        for method, path, fields, _ in api.calls
        if method == "PUT" and path.endswith("/secrets")
    }
    front_key = next(fields["item_value"] for method, path, fields, _ in api.calls if path.endswith("/item/front_key"))
    assert front_key == secret_writes["front_key"]
    assert set(secret_writes) == set(provision.resource_plan()["secret_keys"])
    assert secret_writes["fastly_token"] == "api-token"
    assert not any(method == "DELETE" for method, *_ in api.calls)
    assert json.loads(secret_writes["admin_setup_hash"])["expires_at"] == 0


def test_bootstrap_failure_retains_nonsecret_inventory_for_cleanup(
    inventory_path,
) -> None:
    class FailedAPI(SyntheticBootstrapAPI):
        def request(self, method, path, payload=None, *, form=False):
            if path.endswith("/secrets"):
                raise provision.APIError("Fastly PUT secrets returned 403")
            return super().request(method, path, payload, form=form)

    with pytest.raises(provision.APIError, match="403"):
        provision.bootstrap(
            FailedAPI(),
            provision.resource_plan("team-a"),
            inventory_path,
            rate_limiting=True,
            api_token="api-token",
        )
    saved = json.loads(inventory_path.read_text())
    assert saved["services"]["front"]["id"] == "team-a-front-id"
    assert saved["dictionaries"]["credentials"]["id"] == "flp_fos_credentials-id"
    assert saved["activated"] is False
    assert "configured" not in saved


def test_existing_install_refuses_adoption_before_any_write(inventory_path) -> None:
    class Existing(SyntheticBootstrapAPI):
        def request(self, method, path, payload=None, *, form=False):
            if path.startswith("/service?"):
                self.calls.append((method, path, payload, form))
                return [{"name": "team-a-worker"}]
            return super().request(method, path, payload, form=form)

    api = Existing()
    with pytest.raises(ValueError, match="refusing adoption"):
        provision.bootstrap(
            api,
            provision.resource_plan("team-a"),
            inventory_path,
            rate_limiting=False,
            api_token="api-token",
        )
    assert all(method == "GET" for method, *_ in api.calls)
    assert not inventory_path.exists()


def test_bootstrap_refuses_existing_inventory_without_api_calls(
    inventory_path,
) -> None:
    original = '{"install_name":"team-a","configured":true}\n'
    inventory_path.write_text(original)
    api = SyntheticBootstrapAPI()
    with pytest.raises(ValueError, match="Inventory already exists"):
        provision.bootstrap(
            api,
            provision.resource_plan("team-a"),
            inventory_path,
            rate_limiting=False,
            api_token="api-token",
        )
    assert api.calls == []
    assert inventory_path.read_text() == original


def test_bootstrap_requires_api_token_before_writes(inventory_path) -> None:
    api = SyntheticBootstrapAPI()
    with pytest.raises(ValueError, match="Fastly API token"):
        provision.bootstrap(
            api,
            provision.resource_plan("team-a"),
            inventory_path,
            rate_limiting=False,
            api_token="",
        )
    assert api.calls == []
    assert not inventory_path.exists()


def test_recorded_get_contracts_drive_rate_limiting_detection() -> None:
    fixtures = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "fastly-api"
    versions = json.loads((fixtures / "test-service-versions.json").read_text())
    vcl = json.loads((fixtures / "test-service-generated-vcl.json").read_text())
    missing = json.loads((fixtures / "missing-service.json").read_text())
    assert versions["status"] == vcl["status"] == 200
    assert missing["status"] == 404

    class RecordedAPI:
        def request(self, method, path, payload=None, *, form=False):
            assert method == "GET"
            if path.endswith("/generated_vcl"):
                return vcl["response"]
            return versions["response"]

    expected = bool(provision.re.search(r"ratelimit_opt_in\s+true\b", vcl["response"]["content"]))
    assert provision.account_has_rate_limiting(RecordedAPI(), "TEST_SERVICE") is expected


def test_falco_executes_no_erl_variant_and_auth_failure_stripping() -> None:
    path = provision.VCL / "tests-no-erl" / f".main-{uuid.uuid4()}.vcl"
    try:
        path.write_text(
            'table flp_front_auth {\n  "front_key": "test-front-key-not-a-live-secret",\n}\n'
            "table flp_fos_buckets {}\ntable flp_fos_credentials {}\n" + provision.load_vcl(False)
        )
        result = subprocess.run(
            ["falco", "test", "-f", "*.no-erl.vcl", str(path)],
            text=True,
            capture_output=True,
            check=True,
        )
        assert "2 passed, 0 failed" in result.stdout + result.stderr
    finally:
        path.unlink(missing_ok=True)


class SyntheticFrontAPI:
    def __init__(self, *, valid: bool = True, name: str = "team-a-front"):
        self.valid = valid
        self.name = name
        self.calls = []

    def request(self, method, path, payload=None, *, form=False):
        self.calls.append((method, path, payload, form))
        if path == "/service/front-id":
            return {"name": self.name, "type": "vcl"}
        if path == "/service/front-id/version":
            return [{"number": 2, "active": True, "locked": True}]
        if path.endswith("/clone"):
            return {"number": 3}
        if path.endswith("/validate"):
            return {"status": "ok" if self.valid else "error"}
        if path.endswith("/vcl/main"):
            return {}
        if path.endswith("/condition/flp_app") and method == "PUT":
            return {}
        if path.endswith("/condition/flp_tick") and method == "PUT":
            return {}
        if path.endswith("/healthcheck") and method == "GET":
            return [{"name": "flp_clock", "host": "team-a.global.ssl.fastly.net"}]
        if path.endswith("/backend") and method == "GET":
            return [{"name": name} for name in ("flp", "flp_tick", "flp_clock")]
        if method == "POST" and path.endswith(("/healthcheck", "/backend")):
            return {}
        raise AssertionError("Unexpected synthetic front request")


def _front_inventory(path):
    value = {
        "services": {"front": {"id": "front-id", "name": "team-a-front", "version": 2}},
        "rate_limiting": True,
        "activated": True,
        "configured": True,
    }
    path.write_text(json.dumps(value))
    return value


def test_front_update_validates_new_draft_without_activation(inventory_path) -> None:
    _front_inventory(inventory_path)
    api = SyntheticFrontAPI()
    result = provision.prepare_front(api, inventory_path, rate_limiting=False)
    assert result["services"]["front"]["version"] == 3
    assert result["activated"] is False
    assert result["configured"] is True
    uploaded = next(fields for _, path, fields, _ in api.calls if path.endswith("/vcl/main"))
    assert 'set req.http.x-flp-front-rl = "0";' in uploaded["content"]
    assert not any("/activate" in path for _, path, *_ in api.calls)
    assert json.loads(inventory_path.read_text()) == result
    posted = {
        fields["name"]: fields
        for method, path, fields, _ in api.calls
        if method == "POST" and path.startswith("/service/front-id/version/3/")
    }
    assert posted["flp_flush"]["path"] == "/_flush"
    assert posted["flp_flush"]["host"] == "team-a.global.ssl.fastly.net"
    assert posted["flp_flush_clock"]["healthcheck"] == "flp_flush"
    tick = next(
        fields for method, path, fields, _ in api.calls if method == "PUT" and path.endswith("/condition/flp_tick")
    )
    assert tick["statement"] == provision.TICK_CONDITION


def test_front_update_keeps_an_existing_flush_clock(inventory_path) -> None:
    _front_inventory(inventory_path)
    api = SyntheticFrontAPI()
    original = api.request

    def request(method, path, payload=None, *, form=False):
        if method == "GET" and path.endswith("/healthcheck"):
            api.calls.append((method, path, payload, form))
            return [
                {"name": "flp_clock", "host": "team-a.global.ssl.fastly.net"},
                {"name": "flp_flush", "host": "team-a.global.ssl.fastly.net"},
            ]
        if method == "GET" and path.endswith("/backend"):
            api.calls.append((method, path, payload, form))
            return [{"name": "flp_clock"}, {"name": "flp_flush_clock"}]
        return original(method, path, payload, form=form)

    api.request = request
    provision.prepare_front(api, inventory_path, rate_limiting=False)
    assert not any(method == "POST" and path.endswith(("/healthcheck", "/backend")) for method, path, *_ in api.calls)


def test_front_validation_failure_retains_unactivated_draft_for_recovery(
    inventory_path,
) -> None:
    _front_inventory(inventory_path)
    with pytest.raises(provision.APIError, match="validation failed"):
        provision.prepare_front(SyntheticFrontAPI(valid=False), inventory_path, rate_limiting=False)
    saved = json.loads(inventory_path.read_text())
    assert saved["services"]["front"]["version"] == 3
    assert saved["configured"] is False
    assert saved["activated"] is False


def test_front_update_refuses_mismatched_inventory_before_writes(
    inventory_path,
) -> None:
    original = _front_inventory(inventory_path)
    api = SyntheticFrontAPI(name="someone-else")
    with pytest.raises(ValueError, match="identity"):
        provision.prepare_front(api, inventory_path, rate_limiting=False)
    assert all(method == "GET" for method, *_ in api.calls)
    assert json.loads(inventory_path.read_text()) == original


@pytest.fixture
def package(monkeypatch):
    directory = Path(__file__).parent / f".package-{uuid.uuid4()}"
    directory.mkdir()
    path = directory / "flp.tar.gz"
    sha = "a" * 40
    wasm = b"\x00asm\x01\x00\x00\x00" + sha.encode()
    with tarfile.open(path, "w:gz") as archive:
        entry = tarfile.TarInfo("bin/flp.wasm")
        entry.size = len(wasm)
        archive.addfile(entry, io.BytesIO(wasm))
    (directory / "provenance.json").write_text(
        json.dumps(
            {
                "build_sha": sha,
                "package_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    )

    def git(root, *args):
        if args[0] == "status":
            return ""
        if args[0] == "rev-parse":
            return "same-app-tree"
        if args[0] == "show":
            return (provision.VCL / "main.vcl").read_text() + "\n# frozen-test-source\n"
        raise AssertionError("Unexpected Git boundary request")

    monkeypatch.setattr(provision, "_git", git, raising=False)

    def public_front(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://front.example/version"
        assert request.headers.get("Fastly-Key") is None
        return httpx.Response(200, json={"build_sha": sha})

    monkeypatch.setattr(provision, "HTTP_TRANSPORT", httpx.MockTransport(public_front))
    try:
        yield path
    finally:
        shutil.rmtree(directory)


def test_deploy_provenance_rejects_modified_artifact_before_api_calls(package) -> None:
    package.write_bytes(package.read_bytes() + b"tampered")
    with pytest.raises(ValueError, match="Package"):
        provision.verify_provenance(Path.cwd(), package)


def test_deploy_provenance_rejects_changed_runtime_source(package, monkeypatch) -> None:
    def changed(root, *args):
        if args[0] == "status":
            return ""
        return "new-tree" if args[-1] == "HEAD:app" else "built-tree"

    monkeypatch.setattr(provision, "_git", changed)
    with pytest.raises(ValueError, match="source changed"):
        provision.verify_provenance(Path.cwd(), package)


def test_deploy_provenance_requires_embedded_built_sha_not_current_head(
    package,
) -> None:
    wasm = b"\x00asm\x01\x00\x00\x00" + b"b" * 40
    with tarfile.open(package, "w:gz") as archive:
        entry = tarfile.TarInfo("bin/flp.wasm")
        entry.size = len(wasm)
        archive.addfile(entry, io.BytesIO(wasm))
    metadata = json.loads((package.parent / "provenance.json").read_text())
    metadata["package_sha256"] = hashlib.sha256(package.read_bytes()).hexdigest()
    (package.parent / "provenance.json").write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match="built SHA"):
        provision.verify_provenance(Path.cwd(), package)


class SyntheticDeployAPI(SyntheticFrontAPI):
    def __init__(self, *, fail_worker: bool = False, drop_activations: int = 0):
        super().__init__()
        self.fail_worker = fail_worker
        self.drop_activations = drop_activations
        self.active = set()
        self.uploads = []

    def request(self, method, path, payload=None, *, form=False):
        if path in {"/service/compute-id", "/service/worker-id"}:
            self.calls.append((method, path, payload, form))
            return {
                "name": "team-a" if path.endswith("compute-id") else "team-a-worker",
                "type": "wasm",
            }
        if path.endswith("/version"):
            self.calls.append((method, path, payload, form))
            return [{"number": 2, "active": True, "locked": True}]
        if path.endswith("/package") and method == "GET":
            self.calls.append((method, path, payload, form))
            return {"metadata": {"files_hash": "matching-files-hash"}}
        if path.endswith("/backend"):
            self.calls.append((method, path, payload, form))
            return [] if method == "GET" else {}
        if path.endswith("/activate"):
            self.calls.append((method, path, payload, form))
            if self.drop_activations:
                self.drop_activations -= 1
            else:
                self.active.add(path.removesuffix("/activate"))
            return {"status": "ok"}
        if method == "GET" and "/version/" in path and path.count("/") == 4:
            self.calls.append((method, path, payload, form))
            return {"active": path in self.active, "locked": path in self.active}
        if path.endswith(("/purge/flp-html", "/secrets")):
            self.calls.append((method, path, payload, form))
            return {"status": "ok"}
        if path.endswith("/generated_vcl"):
            self.calls.append((method, path, payload, form))
            return {"content": "pragma optional_param ratelimit_opt_in true;"}
        return super().request(method, path, payload, form=form)

    def upload_package(self, service_id, version, path):
        if self.fail_worker and service_id == "worker-id":
            raise provision.APIError("Synthetic package upload failure")
        self.uploads.append((service_id, version, hashlib.sha256(path.read_bytes()).hexdigest()))
        return {}


def _deploy_inventory(path):
    value = _front_inventory(path)
    value["services"]["front"]["domain"] = "front.example"
    value["install_name"] = "team-a"
    value["stores"] = {"secret": {"id": "secret-id"}}
    value["services"].update(
        {
            "compute": {"id": "compute-id", "name": "team-a", "version": 2},
            "worker": {"id": "worker-id", "name": "team-a-worker", "version": 2},
        }
    )
    path.write_text(json.dumps(value))


def test_deploy_uploads_identical_artifact_before_paired_activation_and_html_purge(package, inventory_path) -> None:
    _deploy_inventory(inventory_path)
    api = SyntheticDeployAPI()
    result = provision.deploy(api, inventory_path, package, "team-a")
    assert api.uploads == [
        ("compute-id", 3, hashlib.sha256(package.read_bytes()).hexdigest()),
        ("worker-id", 3, hashlib.sha256(package.read_bytes()).hexdigest()),
    ]
    assert [path for _, path, *_ in api.calls if path.endswith("/activate")] == [
        "/service/worker-id/version/3/activate",
        "/service/compute-id/version/3/activate",
        "/service/front-id/version/3/activate",
    ]
    assert api.calls[-1][:2] == ("POST", "/service/front-id/purge/flp-html")
    assert result["build_sha"] == "a" * 40
    assert result["activated"] is True
    front_vcl = next(fields["content"] for _, path, fields, _ in api.calls if path.endswith("/vcl/main"))
    assert "# frozen-test-source" in front_vcl


def test_deploy_retries_an_activation_that_returned_200_but_did_not_take(package, inventory_path) -> None:
    _deploy_inventory(inventory_path)
    api = SyntheticDeployAPI(drop_activations=1)
    result = provision.deploy(api, inventory_path, package, "team-a")
    activations = [path for _, path, *_ in api.calls if path.endswith("/activate")]
    assert activations[:2] == ["/service/worker-id/version/3/activate"] * 2
    assert "/service/worker-id/version/3" in api.active
    assert result["activated"] is True


def test_deploy_stops_when_activation_never_takes(package, inventory_path, monkeypatch) -> None:
    monkeypatch.setattr(provision.time, "sleep", lambda _: None)
    _deploy_inventory(inventory_path)
    api = SyntheticDeployAPI(drop_activations=99)
    with pytest.raises(provision.APIError, match="worker-id version 3 is not active"):
        provision.deploy(api, inventory_path, package, "team-a")
    assert json.loads(inventory_path.read_text())["activated"] is False
    assert not any("/compute-id/version/3/activate" in p for _, p, *_ in api.calls)


def test_first_deploy_issues_and_announces_the_only_initial_setup_code(package, inventory_path) -> None:
    _deploy_inventory(inventory_path)
    api = SyntheticDeployAPI()
    announced = []
    provision.deploy(api, inventory_path, package, "team-a", announce=announced.append)
    writes = [
        (index, payload)
        for index, (method, path, payload, _) in enumerate(api.calls)
        if method == "PUT" and path == "/resources/stores/secret/secret-id/secrets"
    ]
    assert len(writes) == 1 and len(announced) == 1
    stored = json.loads(base64.b64decode(writes[0][1]["secret"]))
    assert stored["sha256"] == hashlib.sha256(announced[0].encode()).hexdigest()
    last_activation = max(index for index, (_, path, *_) in enumerate(api.calls) if path.endswith("/activate"))
    assert writes[0][0] > last_activation
    assert json.loads(inventory_path.read_text())["admin_code_issued"] is True

    again = SyntheticDeployAPI()
    provision.deploy(again, inventory_path, package, "team-a", announce=announced.append)
    assert len(announced) == 1
    assert not any(path.endswith("/secrets") for _, path, *_ in again.calls)


def test_failed_second_upload_never_activates_or_purges(package, inventory_path) -> None:
    _deploy_inventory(inventory_path)
    api = SyntheticDeployAPI(fail_worker=True)
    with pytest.raises(provision.APIError, match="upload failure"):
        provision.deploy(api, inventory_path, package, "team-a")
    assert not any("/activate" in path or "/purge/" in path for _, path, *_ in api.calls)
    assert json.loads(inventory_path.read_text())["activated"] is False


def test_remote_package_mismatch_stops_before_activation(package, inventory_path) -> None:
    class DifferentPackage(SyntheticDeployAPI):
        def request(self, method, path, payload=None, *, form=False):
            if path == "/service/worker-id/version/3/package":
                return {"metadata": {"files_hash": "different-package-hash"}}
            return super().request(method, path, payload, form=form)

    _deploy_inventory(inventory_path)
    api = DifferentPackage()
    with pytest.raises(provision.APIError, match="package hashes differ"):
        provision.deploy(api, inventory_path, package, "team-a")
    assert not any("/activate" in path for _, path, *_ in api.calls)


def test_package_upload_uses_multipart_binary_without_token_arguments(package, monkeypatch) -> None:
    captured: list[httpx.Request] = []

    def accepted(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, content=b'{"metadata":{"files_hash":"synthetic-files-hash"}}')

    monkeypatch.setattr(provision, "HTTP_TRANSPORT", httpx.MockTransport(accepted))
    api = provision.FastlyAPI("deploy-token")
    api.upload_package("compute-id", 3, package)
    request = captured[0]
    assert request.method == "PUT"
    assert str(request.url).endswith("/service/compute-id/version/3/package")
    assert request.headers.get("Expect") == "100-continue"
    assert request.headers.get("Content-Type", "").startswith("multipart/form-data; boundary=")
    assert package.read_bytes() in request.content
    assert b"deploy-token" not in request.content
    assert b'name="package"; filename="flp.tar.gz"' in request.content


def test_deploy_waits_for_matching_front_sha_before_purging(package, inventory_path, monkeypatch) -> None:
    probe_calls: list[str] = []
    clock = [0.0]
    monkeypatch.setattr(provision.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(provision.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay))

    def propagating(request: httpx.Request) -> httpx.Response:
        probe_calls.append(str(request.url))
        assert request.headers.get("Fastly-Key") is None
        if len(probe_calls) == 1:
            return httpx.Response(503, text="not propagated")
        return httpx.Response(200, json={"build_sha": ("b" if len(probe_calls) == 2 else "a") * 40})

    class GuardedPurge(SyntheticDeployAPI):
        def request(self, method, path, payload=None, *, form=False):
            if path.endswith("/purge/flp-html"):
                assert len(probe_calls) == 3, "Purge raced first-activation propagation"
            return super().request(method, path, payload, form=form)

    monkeypatch.setattr(provision, "HTTP_TRANSPORT", httpx.MockTransport(propagating))
    _deploy_inventory(inventory_path)
    result = provision.deploy(GuardedPurge(), inventory_path, package, "team-a")
    assert result["html_purged"] is True
    assert probe_calls == ["https://front.example/version"] * 3


def test_front_readiness_timeout_does_not_purge_or_hide_activated_state(package, inventory_path, monkeypatch) -> None:
    clock = [0.0]
    monkeypatch.setattr(provision.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(provision.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay))

    def unavailable(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="not propagated")

    monkeypatch.setattr(provision, "HTTP_TRANSPORT", httpx.MockTransport(unavailable))
    _deploy_inventory(inventory_path)
    api = SyntheticDeployAPI()
    with pytest.raises(provision.APIError, match="readiness timed out"):
        provision.deploy(api, inventory_path, package, "team-a")
    assert clock[0] == 180
    assert not any("/purge/" in path for _, path, *_ in api.calls)
    saved = json.loads(inventory_path.read_text())
    assert saved["activated"] is True
    assert saved["html_purged"] is False


def test_purge_only_never_uploads_or_activates_services(package, inventory_path) -> None:
    _deploy_inventory(inventory_path)
    inventory = json.loads(inventory_path.read_text())
    inventory.update(activated=True, build_sha="a" * 40, html_purged=False)
    inventory_path.write_text(json.dumps(inventory))
    api = SyntheticDeployAPI()
    result = provision.purge_html(api, inventory_path)
    assert [(method, path) for method, path, *_ in api.calls] == [
        ("GET", "/service/front-id"),
        ("POST", "/service/front-id/purge/flp-html"),
    ]
    assert api.uploads == []
    assert result["html_purged"] is True
    assert json.loads(inventory_path.read_text()) == result


def test_readiness_forbidden_fails_without_retrying_or_purging(package, inventory_path, monkeypatch) -> None:
    def forbidden(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="forbidden")

    def unexpected_sleep(delay):
        pytest.fail("A permanent front authorization error must not be retried")

    monkeypatch.setattr(provision, "HTTP_TRANSPORT", httpx.MockTransport(forbidden))
    monkeypatch.setattr(provision.time, "sleep", unexpected_sleep)
    _deploy_inventory(inventory_path)
    api = SyntheticDeployAPI()
    with pytest.raises(provision.APIError, match="readiness returned 403"):
        provision.deploy(api, inventory_path, package, "team-a")
    assert not any("/purge/" in path for _, path, *_ in api.calls)


def test_render_terraform_delegates_to_shared_terraform_gen() -> None:
    files = provision.render_terraform({"install_name": "team-a", "fos_region": "us-east-1"})
    assert "cloud_compute.tf.json" in files
    assert "team-a" in files["cloud_compute.tf.json"]
    assert "fastly_service_vcl" in files["cloud_compute.tf.json"]
