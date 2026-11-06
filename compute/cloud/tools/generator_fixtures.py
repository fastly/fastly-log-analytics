"""Rebuild generator fixtures from the read-only, pinned v3 checkout.

Usage: python3 app/tools/generator_fixtures.py /path/to/rc1-reference
Append --lint to check every generated wrapper with falco.

Only pure definitions are loaded through the AST; dependency-heavy HTTP, CLI,
RUM and scoring imports are not executed. Generator function bodies are executed
unchanged. No network access, credentials, or installed Python dependencies.

The routed v3 adapter discards field_limits. Fixtures separately record that
projection and the full-input pure generators; they do not claim those routes
are equivalent. Endpoint-body bytes use sorted-key, compact UTF-8 JSON, not the
ordering of form parameters on Fastly's HTTP API.

Intentional divergence: the group J WAF fields (waf, waf_resp, waf_ms, waf_sig,
waf_req_id) read the vcl_log-only waf.inspection.* variables, not v3's
waf.executed/x-sigsci-* headers, which the current NGWAF edge never sets.
waf_resp falls back to resp.status on a block verdict and 200 on allow, because
the edge integration leaves waf.inspection.agentresponse at 0 (live probe 2026-10-31).
catalog.json and the fixtures carry the new VCL; reapply it after a rebuild.
The core group also logs an integer `ts_ms` (`%{time.start.msec}V`) right after
`timestamp`; ingest routes and sorts by it. It shifts every format, hash,
enabled set and byte estimate, and the exhausted case's URL limit; reapply it too.
The group M deliver capture also reads ifmt from Fastly-Io-Info, because
Fastly-Io-Transform-Stats omits it. Its req-header emptiness checks are
NULL-safe (`!h || h == ""`), because an unset header never equals "" in VCL.
"""

from __future__ import annotations

import ast
import dataclasses
import hashlib
import importlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
PIN = "2c75f058"
sys.dont_write_bytecode = True


def definitions(path: Path, names: set[str], namespace: dict[str, Any]) -> None:
    tree = ast.parse(path.read_text())
    nodes = []
    for node in tree.body:
        identifiers = {getattr(node, "name", "")}
        if isinstance(node, ast.Assign):
            identifiers.update(t.id for t in node.targets if isinstance(t, ast.Name))
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            identifiers.add(node.target.id)
        if identifiers & names:
            nodes.append(node)
    module = ast.Module(
        body=[
            ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
            *nodes,
        ],
        type_ignores=[],
    )
    exec(compile(ast.fix_missing_locations(module), str(path), "exec"), namespace)  # noqa: S102 -- verified pinned local definitions


def write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def main(reference: Path, lint: bool = False) -> None:
    revision = subprocess.check_output(["git", "-C", str(reference), "rev-parse", "HEAD"], text=True).strip()
    expected = subprocess.check_output(["git", "-C", str(reference), "rev-parse", PIN], text=True).strip()
    if revision != expected:
        raise SystemExit(f"Reference must be pinned at {PIN}, got {revision}")
    subprocess.run(
        [
            "git",
            "-C",
            str(reference),
            "diff",
            "--exit-code",
            PIN,
            "--",
            "backend/core/catalog.py",
            "backend/core/insight_definitions.py",
            "backend/core/presets.py",
            "backend/core/vcl_generator.py",
            "backend/provision/cli.py",
            "backend/provision/fastly_api.py",
            "backend/provision/cmcd_fields.py",
            "backend/provision/declarative/vcl_builder.py",
            "backend/provision/declarative/fastly_ops.py",
            "backend/provision/declarative/models.py",
        ],
        check=True,
    )
    sys.path.insert(0, str(reference))
    catalog = importlib.import_module("backend.core.catalog")
    # This project deliberately excludes the opt-in session-cookie field.
    catalog.LOG_FIELD_CATALOG = [f for f in catalog.LOG_FIELD_CATALOG if f["id"] != "cookie_session"]
    sys.modules["backend.core.log_fields"] = catalog
    namespace = dict(vars(catalog))
    namespace.update(re=re, hashlib=hashlib)
    definitions(
        reference / "backend/core/vcl_generator.py",
        {"generate_log_format", "format_hash"},
        namespace,
    )
    raw_format = namespace["generate_log_format"]
    catalog.generate_log_format = raw_format
    definitions(reference / "backend/provision/cli.py", {"VclBuilder"}, namespace)
    namespace.update(fc=catalog, lf=catalog)
    definitions(
        reference / "backend/provision/fastly_api.py",
        {
            "SESSION_COOKIE_ALLOWLIST",
            "_session_cookie_hash_vcl",
            "EDGE_DATA_MAPPING",
            "_SCRUB_HEADERS",
            "_resolve_enabled_custom_fields",
            "get_scrub_vcl_statements",
            "get_capture_vcl_statements",
            "generate_capture_vcl",
        },
        namespace,
    )
    capture = namespace["generate_capture_vcl"]
    pure_api = ModuleType("backend.provision.fastly_api")
    pure_api.__dict__.update(namespace)
    sys.modules["backend.provision.fastly_api"] = pure_api
    provision = ModuleType("backend.provision")
    provision.__path__ = []
    provision.fastly_api = pure_api
    sys.modules["backend.provision"] = provision
    cmcd_namespace = {}
    definitions(
        reference / "backend/provision/cmcd_fields.py",
        {
            "_CMCD_CUSTOM_FIELDS",
            "CMCD_SNIPPET_NAME",
            "_bool_v1",
            "_bool_v2",
            "_cmcd_field_assignments_vcl",
            "_header_mode_vcl",
            "_query_string_mode_vcl",
            "generate_cmcd_vcl",
        },
        cmcd_namespace,
    )
    cmcd_module = ModuleType("backend.provision.cmcd_fields")
    cmcd_module.__dict__.update(cmcd_namespace)
    sys.modules["backend.provision.cmcd_fields"] = cmcd_module
    orchestrator = ModuleType("backend.provision.orchestrator")
    orchestrator.generate_cmcd_vcl = cmcd_namespace["generate_cmcd_vcl"]
    sys.modules["backend.provision.orchestrator"] = orchestrator
    namespace.update(dataclass=dataclasses.dataclass, asdict=dataclasses.asdict)
    namespace["__name__"] = "generator_reference_models"
    sys.modules["generator_reference_models"] = SimpleNamespace(**namespace)
    # Recompile after registering the module for dataclasses' annotation lookup.
    definitions(
        reference / "backend/provision/declarative/models.py",
        {"VCLSnippet", "LoggingEndpoint"},
        namespace,
    )
    definitions(
        reference / "backend/provision/declarative/vcl_builder.py",
        {
            "_state_log_fields_dict",
            "logging_service_snippets",
            "desired_snippets",
            "generate_consolidated_snippet",
            "_legacy_generate_consolidated_snippet",
        },
        namespace,
    )

    fields = catalog.LOG_FIELD_CATALOG
    write(
        ROOT / "src/logging/catalog.json",
        {
            "fields": fields,
            "api_fields": catalog.get_catalog_for_api(),
            "groups": catalog.get_groups_for_api(),
            "insights": catalog.INSIGHT_DEFINITIONS,
            "presets": catalog.PRESETS,
            "reserved": sorted(catalog._DUCKDB_RESERVED),
        },
    )
    base = capture({})
    group_l = capture({"groups": ["L"]})
    group_m = capture({"groups": ["M"]})
    write(
        ROOT / "vcl/capture/templates.json",
        {
            "base": {k: base[k] for k in ("miss", "pass", "recv_reset")},
            "L": {k: group_l[k] for k in ("miss", "pass", "fetch", "error", "deliver")},
            "M": {"deliver": group_m["deliver"]},
            "edge_mapping": {k: v for k, v in namespace["EDGE_DATA_MAPPING"].items() if k != "cookie_session"},
            "scrub_headers": namespace["_SCRUB_HEADERS"],
        },
    )
    custom = [
        {
            "name": "z_origin",
            "label": "Origin",
            "description": "",
            "vcl_log_expression": "beresp.http.X-Origin",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "bytes_estimate": 40,
            "collection_stage": "origin",
            "origin_log_frequency": "miss_pass",
        },
        {
            "name": "a_edge",
            "label": "Edge",
            "vcl_log_expression": "req.http.X-Count",
            "duckdb_type": "BIGINT",
            "value_type": "numeric",
            "bytes_estimate": 30,
        },
        {
            "name": "deliver_num",
            "label": "Deliver",
            "vcl_log_expression": "resp.status",
            "duckdb_type": "INTEGER",
            "value_type": "numeric",
            "bytes_estimate": 20,
            "collection_stage": "deliver",
            "byte_limit": 8,
        },
        {
            "name": "deliver_text",
            "label": "Deliver text",
            "vcl_log_expression": "resp.http.X-Text",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "bytes_estimate": 80,
            "collection_stage": "deliver",
        },
        {"name": "disabled", "enabled": False, "vcl_log_expression": "req.http.X-No"},
    ]
    cases = [{"name": "minimal", "config": {}}]
    cases += [{"name": f"group_{g}", "config": {"groups": [g]}} for g in "ABCDEFGHIJKLM"]
    cases += [
        {"name": "standard", "config": {"groups": ["A", "C", "D", "I"]}},
        {"name": "all", "config": {"groups": list("ABCDEFGHIJKLM")}},
        {
            "name": "custom",
            "config": {"groups": ["L", "M"], "custom_fields": custom},
            "falco_expected_error": "Type mismatch between INTEGER and STRING",
        },
        {
            "name": "limits",
            "config": {
                "groups": ["A"],
                "field_limits": {"url": 13, "ua": 7, "referer": 0},
            },
        },
        {
            "name": "exhausted",
            "config": {
                "groups": list("ABCDEFGHIJKLM"),
                "field_limits": {"url": 40000, "ua": 40000},
            },
        },
        {
            "name": "overrides",
            "config": {
                "groups": ["H", "E"],
                "field_overrides": {
                    "tls_ciphers_sha": True,
                    "country": False,
                    "status": False,
                },
            },
        },
        {
            "name": "custom_types",
            "config": {
                "custom_fields": [
                    {
                        "name": "bool_edge",
                        "value_type": "boolean",
                        "vcl_log_expression": 'if(req.http.X-Flag, "1", "0")',
                        "duckdb_type": "BOOLEAN",
                        "bytes_estimate": 40,
                        "byte_limit": 40,
                    },
                    {
                        "name": "num_origin",
                        "value_type": "numeric",
                        "collection_stage": "origin",
                        "vcl_log_expression": "beresp.status",
                        "bytes_estimate": 40,
                        "byte_limit": 5,
                    },
                    {
                        "name": "origin_all",
                        "collection_stage": "origin",
                        "vcl_log_expression": "beresp.http.X-All",
                        "bytes_estimate": 50,
                    },
                    {
                        "name": "edge_text",
                        "vcl_log_expression": "req.http.X-Text",
                        "bytes_estimate": 3,
                    },
                ],
                "field_limits": {"edge_text": 18, "num_origin": 2},
            },
        },
        {
            "name": "custom_zero_limits",
            "config": {
                "custom_fields": [
                    {
                        "name": "string_zero",
                        "vcl_log_expression": "req.http.X-Empty",
                        "byte_limit": "0",
                    },
                    {
                        "name": "numeric_zero",
                        "vcl_log_expression": "req.http.X-Fallback",
                        "byte_limit": 0,
                    },
                    {
                        "name": "negative_limit",
                        "vcl_log_expression": "req.http.X-Negative",
                        "byte_limit": -1,
                    },
                ],
                "field_limits": {"numeric_zero": 12},
            },
        },
    ]
    endpoint_namespace = dict(namespace)
    definitions(
        reference / "backend/provision/fastly_api.py",
        {"load_log_format"},
        endpoint_namespace,
    )
    pure_api.load_log_format = endpoint_namespace["load_log_format"]
    rum = ModuleType("backend.provision.rum_orchestrator_v2")
    rum._RUM_FIELD_NAMES = frozenset()
    sys.modules[rum.__name__] = rum
    definitions(
        reference / "backend/provision/cli.py",
        {"ANALYTICS_LOG_LAYOUT", "analytics_log_path"},
        endpoint_namespace,
    )
    endpoint_namespace["log_paths"] = SimpleNamespace(analytics_log_path=endpoint_namespace["analytics_log_path"])
    definitions(
        reference / "backend/provision/declarative/vcl_builder.py",
        {"generate_log_format", "desired_logging_endpoints"},
        endpoint_namespace,
    )
    captured_bodies = []
    endpoint_namespace["_upsert_version_resource"] = lambda *args: captured_bodies.append(args[4])
    definitions(
        reference / "backend/provision/declarative/fastly_ops.py",
        {"create_or_update_logging_endpoint"},
        endpoint_namespace,
    )
    condition_tree = ast.parse((reference / "backend/provision/declarative/fastly_ops.py").read_text())
    condition_block = next(
        node
        for node in ast.walk(condition_tree)
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Call)
        and isinstance(node.test.func, ast.Name)
        and node.test.func.id == "any"
        and "log_analytics_condition" in ast.unparse(node.test)
    )
    condition_code = compile(
        ast.fix_missing_locations(ast.Module(body=[condition_block], type_ignores=[])),
        "pinned-logging-condition",
        "exec",
    )
    pure_api._log_sampling_edge_clause = lambda scoring_enabled: "fastly.ff.visits_this_service == 0"
    for case in cases:
        cfg = case["config"]
        case["format"] = raw_format(cfg)
        case["loaded_format"] = endpoint_namespace["load_log_format"](cfg)
        case["hash"] = namespace["format_hash"](cfg)
        case["capture"] = capture(cfg)
        case["enabled"] = sorted(catalog.resolve_enabled_fields(cfg))
        case["estimate_bytes"] = catalog.estimate_log_line_bytes(cfg)
        # v3's declarative adapter drops field_limits. Keep this explicit.
        state = SimpleNamespace(
            logging_enabled=True,
            rum_enabled=False,
            scoring=SimpleNamespace(enabled=False, exclude_url_regex=None),
            cmcd=SimpleNamespace(enabled=False, mode="query_string", version=1),
            log_fields=SimpleNamespace(
                groups=cfg.get("groups", []),
                custom_fields=cfg.get("custom_fields", []),
                field_overrides=cfg.get("field_overrides", {}),
            ),
            logging_endpoint_name="Fastly Log Analytics",
            log_period=60,
            sample_rate=37,
            edge_only=True,
            custom_condition='  req.url.path !~ "^/health"  ',
        )
        case["snippets"] = [s.to_dict() for s in namespace["desired_snippets"](state)]
        endpoints = endpoint_namespace["desired_logging_endpoints"](state)
        case["endpoints"] = [endpoint.to_dict() for endpoint in endpoints]
        for endpoint in endpoints:
            endpoint_namespace["create_or_update_logging_endpoint"](
                "fixture-service",
                1,
                endpoint,
                "fixture-token",
                "fixture-bucket",
                "us-east-1.object.fastlystorage.app",
                "fixture-access",
                "fixture-secret",
            )
        case["endpoint_bodies"] = captured_bodies[:]
        case["endpoint_bodies_canonical"] = [canonical_json(body) for body in captured_bodies]
        captured_bodies.clear()
        direct_endpoints = [
            dataclasses.replace(endpoint, format_string=case["loaded_format"]) for endpoint in endpoints
        ]
        case["direct_endpoints"] = [endpoint.to_dict() for endpoint in direct_endpoints]
        for endpoint in direct_endpoints:
            endpoint_namespace["create_or_update_logging_endpoint"](
                "fixture-service",
                1,
                endpoint,
                "fixture-token",
                "fixture-bucket",
                "us-east-1.object.fastlystorage.app",
                "fixture-access",
                "fixture-secret",
            )
        case["direct_endpoint_bodies"] = captured_bodies[:]
        case["direct_endpoint_bodies_canonical"] = [canonical_json(body) for body in captured_bodies]
        captured_bodies.clear()
        conditions = []
        exec(  # noqa: S102 -- original condition block from the verified pinned checkout
            condition_code,
            {
                "desired_endpoints": endpoints,
                "desired_state": state,
                "status_cb": None,
                "_ensure_cond_safe": lambda name, statement, output=conditions: output.append(
                    {"name": name, "statement": statement, "type": "RESPONSE"}
                ),
            },
        )
        case["conditions"] = conditions
    endpoint_settings_cases = [
        {
            "name": "planned_name_nondefault_period_nonempty_prefix",
            "state": {
                "logging_endpoint_name": "Fastly Object Storage Logs",
                "log_period": 300,
                "fos_prefix": "tenant/logs/",
                "sample_rate": 100,
                "edge_only": False,
                "custom_condition": "",
            },
        },
        {
            "name": "custom_name_max_period_empty_prefix",
            "state": {
                "logging_endpoint_name": "Regional logs / 2",
                "log_period": 3600,
                "fos_prefix": "",
                "sample_rate": 1,
                "edge_only": True,
                "custom_condition": '  req.method == "GET"  ',
            },
        },
    ]
    for settings_case in endpoint_settings_cases:
        settings_case["state"]["log_fields"] = {
            "groups": ["A", "C", "D", "I"],
            "custom_fields": [],
            "field_overrides": {},
        }
        inputs = settings_case["state"]
        settings_state = SimpleNamespace(
            **(vars(state) | inputs | {"log_fields": SimpleNamespace(**inputs["log_fields"])})
        )
        endpoints = endpoint_namespace["desired_logging_endpoints"](settings_state)
        settings_case["endpoints"] = [endpoint.to_dict() for endpoint in endpoints]
        for endpoint in endpoints:
            endpoint_namespace["create_or_update_logging_endpoint"](
                "fixture-service",
                1,
                endpoint,
                "fixture-token",
                "fixture-bucket",
                "us-east-1.object.fastlystorage.app",
                "fixture-access",
                "fixture-secret",
            )
        settings_case["endpoint_bodies"] = captured_bodies[:]
        settings_case["endpoint_bodies_canonical"] = [canonical_json(body) for body in captured_bodies]
        captured_bodies.clear()
        conditions = []
        exec(  # noqa: S102 -- original condition block from the verified pinned checkout
            condition_code,
            {
                "desired_endpoints": endpoints,
                "desired_state": settings_state,
                "status_cb": None,
                "_ensure_cond_safe": lambda name, statement, output=conditions: output.append(
                    {"name": name, "statement": statement, "type": "RESPONSE"}
                ),
            },
        )
        settings_case["conditions"] = conditions
    cmcd_cases = []
    for mode in ("query_string", "headers"):
        for version in (1, 2):
            cmcd_config = {
                "groups": ["A", "C", "D", "I"],
                "custom_fields": cmcd_namespace["_CMCD_CUSTOM_FIELDS"],
                "field_overrides": {},
            }
            cmcd_state = SimpleNamespace(
                **(
                    vars(state)
                    | {
                        "log_fields": SimpleNamespace(**cmcd_config),
                        "cmcd": SimpleNamespace(enabled=True, mode=mode, version=version),
                    }
                )
            )
            cmcd_cases.append(
                {
                    "name": f"cmcd_{mode}_v{version}",
                    "mode": mode,
                    "version": version,
                    "config": cmcd_config,
                    "vcl": cmcd_namespace["generate_cmcd_vcl"](mode=mode, version=version),
                    "format": raw_format(cmcd_config),
                    "capture": capture(
                        cmcd_config,
                        cmcd_enabled=True,
                        cmcd_mode=mode,
                        cmcd_version=version,
                    ),
                    "snippets": [snippet.to_dict() for snippet in namespace["desired_snippets"](cmcd_state)],
                    "sample_json": {
                        "cmcd_sid": "session-01",
                        "cmcd_cid": "content-01",
                        "cmcd_br": 4000,
                        "cmcd_bl": 15000,
                        "cmcd_bs": 1,
                        "cmcd_d": 2000,
                        "cmcd_dl": 5000,
                        "cmcd_mtp": 5500,
                        "cmcd_ot": "v",
                        "cmcd_sf": "d",
                        "cmcd_st": "v",
                        "cmcd_su": 1,
                        "cmcd_tb": 6000,
                        "cmcd_rtp": 7000,
                    },
                }
            )
            endpoints = endpoint_namespace["desired_logging_endpoints"](cmcd_state)
            cmcd_cases[-1]["endpoints"] = [endpoint.to_dict() for endpoint in endpoints]
            for endpoint in endpoints:
                endpoint_namespace["create_or_update_logging_endpoint"](
                    "fixture-service",
                    1,
                    endpoint,
                    "fixture-token",
                    "fixture-bucket",
                    "us-east-1.object.fastlystorage.app",
                    "fixture-access",
                    "fixture-secret",
                )
            cmcd_cases[-1]["endpoint_bodies"] = captured_bodies[:]
            cmcd_cases[-1]["endpoint_bodies_canonical"] = [canonical_json(body) for body in captured_bodies]
            captured_bodies.clear()
    write(
        ROOT / "src/logging/cmcd.json",
        {
            "fields": cmcd_namespace["_CMCD_CUSTOM_FIELDS"],
            "variants": [{key: case[key] for key in ("mode", "version", "vcl")} for case in cmcd_cases],
        },
    )
    validations = []
    valid = dict(custom[1])
    for change in [
        {},
        {"name": "select"},
        {"name": "url"},
        {"name": "é_field"},
        {"label": "é" * 80},
        {"label": "é" * 81},
        {"description": "🙂" * 500},
        {"description": "🙂" * 501},
        {"vcl_log_expression": "x;\nunset req.url;"},
        {"vcl_log_expression": "x" * 512},
        {"vcl_log_expression": "x" * 513},
        {"duckdb_type": "BOOLEAN"},
        {"collection_stage": "deliver"},
        {"bytes_estimate": "oops"},
        {"bytes_estimate": 0},
        {"bytes_estimate": 1},
        {"bytes_estimate": 1024},
        {"bytes_estimate": 1025},
    ]:
        value = valid | change
        validations.append({"field": value, "errors": catalog.validate_custom_field(value, [])})
    validations.append({"field": {}, "errors": catalog.validate_custom_field({}, [])})
    write(
        ROOT / "tests/generator_fixtures.json",
        {
            "reference": revision,
            "contracts": {
                "pure_generators": "Original full input, including field_limits",
                "routed_adapter": "Original v3 FeatureState projection: groups/custom_fields/field_overrides only",
                "direct_endpoints": "Original v3 pure load_log_format(full input) inserted into the same original endpoint spec",
                "body_bytes": "Canonical compact sorted-key UTF-8 JSON; not HTTP form parameter order",
                "prefix_boundary": "Pinned desired_logging_endpoints ignores fos_prefix; original provisioning forces empty prefix",
            },
            "cases": cases,
            "endpoint_settings_cases": endpoint_settings_cases,
            "cmcd_cases": cmcd_cases,
            "validations": validations,
        },
    )
    print(
        f"Generated {len(fields)} catalog entries, {len(cases)} Python parity cases, "
        f"{len(endpoint_settings_cases)} endpoint settings cases, "
        f"{len(cmcd_cases)} CMCD cases, "
        f"{len(validations)} validation cases from {revision[:8]}"
    )
    if lint:
        lint_cases(reference, namespace, cases + cmcd_cases)
        test_cmcd(namespace, cmcd_cases)


def test_cmcd(namespace: dict[str, Any], cases: list[dict[str, Any]]) -> None:
    directory = ROOT / "target/cmcd-vcl"
    directory.mkdir(parents=True, exist_ok=True)
    try:
        for case in cases:
            parts = re.findall(r'"cmcd_[a-z]+":(?:"%\{.*?\}V"|%\{.*?\}V)', case["format"])
            if len(parts) != 14:
                raise SystemExit("CMCD format must contain exactly 14 managed fields")
            expression = namespace["log_format_to_vcl_log"]("{" + ",".join(parts) + "}")
            recv = case["snippets"][0]["body"].replace(
                "  # Section 3: CMCD Extraction",
                "  unset req.http.x-cmcd;\n  # Section 3: CMCD Extraction",
            )
            main_vcl = directory / "main.vcl"
            main_vcl.write_text(
                "sub vcl_recv {\n#FASTLY recv\n"
                + recv
                + "\n}\nsub cmcd_render {\nset req.http.x-cmcd-json = "
                + expression
                + ";\n}\n"
            )
            tests = []
            for sample in (
                "populated",
                "absent",
                "bounded_ids",
                "false_booleans",
                "shield",
                "restart",
            ):
                expected = dict(case["sample_json"])
                if sample == "bounded_ids":
                    expected.update(cmcd_sid="s" * 48, cmcd_cid="c" * 48)
                if sample in ("absent", "false_booleans"):
                    expected.update(cmcd_su=None, cmcd_bs=None)
                if sample == "absent":
                    expected = {key: "" if isinstance(value, str) else None for key, value in expected.items()}
                boolean = "" if case["version"] == 1 else "=?1"
                if sample == "false_booleans":
                    boolean = "=?0"
                sid = "s" * 90 if sample == "bounded_ids" else "session-01"
                cid = "c" * 90 if sample == "bounded_ids" else "content-01"
                headers = {
                    "CMCD-Object": "br=4000,d=2000,ot=v,tb=6000",
                    "CMCD-Request": f"bl=15000,dl=5000,mtp=5500,su{boolean}",
                    "CMCD-Session": f'sid="{sid}",cid="{cid}",sf=d,st=v',
                    "CMCD-Status": f"bs{boolean},rtp=7000",
                }
                setup = [
                    'set req.http.x-cmcd = "sid=forged,br=9999";',
                    'set req.url = "/video.m4s?keep=1";',
                ]
                if sample == "shield":
                    setup.append('testing.inject_variable("fastly.ff.visits_this_service", 1);')
                if sample == "restart":
                    setup.append('testing.inject_variable("req.restarts", 1);')
                if sample in ("shield", "restart"):
                    for key, value in expected.items():
                        setup.append(f"set req.http.x-fos-edge-data:{key} = {json.dumps(str(value))};")
                if case["mode"] == "headers":
                    if sample != "absent":
                        setup.extend(
                            f"set req.http.{key} = {namespace['log_format_to_vcl_log'](value)};"
                            for key, value in headers.items()
                        )
                elif sample != "absent":
                    setup.append(
                        'set req.url = "/video.m4s?keep=1&CMCD=' + quote(",".join(headers.values()), safe="") + '";'
                    )
                assertions = [
                    'testing.call_subroutine("vcl_recv");',
                    'testing.call_subroutine("cmcd_render");',
                    "assert.equal(req.http.x-cmcd-json, "
                    + namespace["log_format_to_vcl_log"](canonical_json(expected))
                    + ");",
                ]
                if sample not in ("shield", "restart"):
                    assertions.append('assert.equal(req.url, "/video.m4s?keep=1");')
                    assertions.extend(f"assert.is_notset(req.http.{key});" for key in headers)
                tests.append("// @scope: recv\nsub test_" + sample + " {\n" + "\n".join(setup + assertions) + "\n}\n")
            (directory / "cmcd.test.vcl").write_text("\n".join(tests))
            result = subprocess.run(
                ["falco", "test", str(main_vcl)],
                cwd=directory,
                text=True,
                capture_output=True,
                check=False,
            )
            if result.returncode:
                raise SystemExit(f"CMCD execution {case['name']} failed:\n{result.stdout}{result.stderr}")
            print(f"falco execution {case['name']}: {len(tests)} cases PASS")
    finally:
        shutil.rmtree(directory)


def lint_cases(reference: Path, namespace: dict[str, Any], cases: list[dict[str, Any]]) -> None:
    definitions(
        reference / "backend/core/vcl_generator.py",
        {
            "VclSafetyError",
            "validate_log_format_macro",
            "log_format_to_vcl_log",
        },
        namespace,
    )
    directory = ROOT / "target/generator-vcl"
    directory.mkdir(parents=True, exist_ok=True)
    failures = []
    try:
        for case in cases:
            snippets = case["capture"]
            parts = []
            for routine in (
                "recv",
                "hash",
                "hit",
                "miss",
                "pass",
                "fetch",
                "error",
                "deliver",
            ):
                body = snippets.get(routine, "")
                if routine == "recv":
                    body = snippets.get("recv_reset", "") + "\n" + body
                parts.append(f"sub vcl_{routine} {{\n#FASTLY {routine}\n{body}\n}}")
            parts.append(
                "sub vcl_log {\n#FASTLY log\nlog " + namespace["log_format_to_vcl_log"](case["format"]) + ";\n}"
            )
            path = directory / f"{case['name']}.vcl"
            path.write_text("\n".join(parts) + "\n")
            result = subprocess.run(["falco", str(path)], text=True, capture_output=True, check=False)
            expected_error = case.get("falco_expected_error")
            output = result.stdout + result.stderr
            if expected_error and result.returncode and expected_error in output:
                print(f"falco {case['name']}: REJECTED (expected upstream limitation: {expected_error})")
            elif result.returncode or expected_error:
                failures.append(case["name"])
                print(f"falco {case['name']}: FAIL\n{output}")
            else:
                print(f"falco {case['name']}: PASS")
    finally:
        shutil.rmtree(directory)
    if failures:
        raise SystemExit(f"Pinned upstream VCL failed falco: {', '.join(failures)}")


def sync_catalog_and_templates(
    catalog_path: Path | None = None,
    templates_path: Path | None = None,
) -> None:
    """Export catalog.json and templates.json directly from the unified repo's backend/ modules."""
    import argparse  # noqa: F401

    repo_root = ROOT.parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    catalog = importlib.import_module("backend.core.catalog")
    insights_mod = importlib.import_module("backend.core.insight_definitions")
    presets_mod = importlib.import_module("backend.core.presets")
    faro_mod = importlib.import_module("backend.core.faro_versions")

    # Load pure VCL/capture definitions from backend/ without executing network/SDK imports
    sys.modules["backend.core.log_fields"] = catalog
    namespace = dict(vars(catalog))
    namespace.update(
        re=re,
        hashlib=hashlib,
        INSIGHT_DEFINITIONS=insights_mod.INSIGHT_DEFINITIONS,
        PRESETS=presets_mod.PRESETS,
    )
    definitions(
        repo_root / "backend/core/vcl_generator.py",
        {"generate_log_format", "format_hash"},
        namespace,
    )
    catalog.generate_log_format = namespace["generate_log_format"]
    definitions(repo_root / "backend/provision/cli.py", {"VclBuilder"}, namespace)
    namespace.update(fc=catalog, lf=catalog)
    definitions(
        repo_root / "backend/provision/fastly_api.py",
        {
            "SESSION_COOKIE_ALLOWLIST",
            "_session_cookie_hash_vcl",
            "EDGE_DATA_MAPPING",
            "_SCRUB_HEADERS",
            "_resolve_enabled_custom_fields",
            "get_scrub_vcl_statements",
            "get_capture_vcl_statements",
            "generate_capture_vcl",
        },
        namespace,
    )
    fastly_client_ns: dict[str, Any] = {"re": re, "Any": Any}
    definitions(
        repo_root / "backend/core/fastly/client.py",
        {
            "SHIELD_MAP",
            "render_vcl_sigv4_signing_block",
            "RUM_RECV_NAME",
            "RUM_DELIVER_SET_COOKIE_NAME",
            "RUM_ASSET_FETCH_NAME",
            "RUM_SIGV4_SIGN_NAME",
            "RUM_FARO_FETCH_NAME",
            "_FARO_VERSION_RE",
            "_assert_faro_version_safe",
            "_generate_recv_vcl",
            "_generate_beacon_field_extraction_vcl",
            "_generate_beacon_response_vcl",
            "_generate_deliver_set_cookie_vcl",
            "_generate_vcl_error_handler",
            "generate_rum_vcl",
            "_generate_asset_fetch_vcl",
            "_generate_sigv4_sign_vcl",
            "_generate_faro_fetch_vcl",
            "generate_rum_asset_fetch_vcl",
        },
        fastly_client_ns,
    )

    fields_out: list[dict[str, Any]] = []
    for raw_field in catalog.LOG_FIELD_CATALOG:
        if raw_field["id"] == "cookie_session":
            continue
        item: dict[str, Any] = {
            "id": raw_field["id"],
            "name": raw_field["id"],
            "group": raw_field["group"],
            "label": raw_field["label"],
            "description": raw_field["description"],
        }
        for opt_key in ("formatter", "precision", "unit", "note"):
            if opt_key in raw_field:
                item[opt_key] = raw_field[opt_key]
        item["vcl"] = raw_field["vcl"]
        item["vcl_format"] = raw_field["vcl_format"]
        item["duckdb_type"] = raw_field["sql_type"]
        item["typical_bytes"] = raw_field["typical_bytes"]
        for flag_key in ("individually_toggleable", "pii", "default_off"):
            if flag_key in raw_field:
                item[flag_key] = raw_field[flag_key]
        item["required_by"] = list(raw_field.get("required_by", []))
        fields_out.append(item)

    api_fields_out: list[dict[str, Any]] = []
    for entry in catalog.get_catalog_for_api():
        if entry["id"] == "cookie_session":
            continue
        api_entry: dict[str, Any] = {}
        for k, v in entry.items():
            if k == "sql_type":
                api_entry["duckdb_type"] = v
            else:
                api_entry[k] = v
        api_fields_out.append(api_entry)

    groups_out: list[dict[str, Any]] = []
    for grp in catalog.get_groups_for_api():
        gid = grp["id"]
        grp_fields = [f for f in fields_out if f["group"] == gid]
        grp_copy = dict(grp)
        grp_copy["total_bytes"] = sum(f["typical_bytes"] for f in grp_fields)
        grp_copy["fields"] = [f["id"] for f in grp_fields]
        groups_out.append(grp_copy)

    enum_encodings = {
        "CONN_SPEED_ENCODE": dict(catalog.CONN_SPEED_ENCODE),
        "PROXY_TYPE_ENCODE": dict(catalog.PROXY_TYPE_ENCODE),
        "PROXY_DESC_ENCODE": dict(catalog.PROXY_DESC_ENCODE),
    }
    out_catalog_path = catalog_path or (ROOT / "src/logging/catalog.json")
    write(
        out_catalog_path,
        {
            "fields": fields_out,
            "api_fields": api_fields_out,
            "groups": groups_out,
            "insights": insights_mod.INSIGHT_DEFINITIONS,
            "presets": presets_mod.PRESETS,
            "reserved": sorted(catalog._DUCKDB_RESERVED),
            "enum_encodings": enum_encodings,
            "CONN_SPEED_ENCODE": dict(catalog.CONN_SPEED_ENCODE),
            "PROXY_TYPE_ENCODE": dict(catalog.PROXY_TYPE_ENCODE),
            "PROXY_DESC_ENCODE": dict(catalog.PROXY_DESC_ENCODE),
            "c_speed_encode": dict(catalog.CONN_SPEED_ENCODE),
            "p_type_encode": dict(catalog.PROXY_TYPE_ENCODE),
            "p_desc_encode": dict(catalog.PROXY_DESC_ENCODE),
            "fos_regions": dict(fastly_client_ns["SHIELD_MAP"]),
        },
    )

    capture = namespace["generate_capture_vcl"]
    base = capture({})
    group_l = capture({"groups": ["L"]})
    group_m = capture({"groups": ["M"]})
    rum_vcl = fastly_client_ns["generate_rum_vcl"]("__SERVICE_ID__", faro_mod.DEFAULT_FARO_VERSION)
    rum_asset_vcl = fastly_client_ns["generate_rum_asset_fetch_vcl"]("iad-va-us", faro_mod.DEFAULT_FARO_VERSION)
    rum_beacon_extraction = (
        "# Extract the RUM request ID; the beacon URL carries everything else.\n"
        'set req.http.x-fos-edge-data:fastly_req_id = querystring.get(req.url, "req");\n'
        'if (req.http.x-fos-edge-data:fastly_req_id == "") {\n'
        "    set req.http.x-fos-edge-data:fastly_req_id = req.http.Fastly-Request-ID;\n"
        "}\n"
        'set req.url = querystring.filter(req.url, "service_id" + querystring.filtersep() + "req");'
    )

    out_templates_path = templates_path or (ROOT / "vcl/capture/templates.json")
    write(
        out_templates_path,
        {
            "base": {k: base[k] for k in ("miss", "pass", "recv_reset")},
            "L": {k: group_l[k] for k in ("miss", "pass", "fetch", "error", "deliver")},
            "M": {"deliver": group_m["deliver"]},
            "group_capture_deliver": {
                "L": group_l["deliver"],
                "M": group_m["deliver"],
            },
            "edge_mapping": {k: v for k, v in namespace["EDGE_DATA_MAPPING"].items() if k != "cookie_session"},
            "scrub_headers": list(namespace["_SCRUB_HEADERS"]),
            "rum_recv": rum_vcl[fastly_client_ns["RUM_RECV_NAME"]],
            "rum_deliver": rum_vcl[fastly_client_ns["RUM_DELIVER_SET_COOKIE_NAME"]],
            "rum_error": fastly_client_ns["_generate_vcl_error_handler"](),
            "rum_asset_fetch": rum_asset_vcl[fastly_client_ns["RUM_ASSET_FETCH_NAME"]],
            "rum_sigv4_sign": rum_asset_vcl[fastly_client_ns["RUM_SIGV4_SIGN_NAME"]],
            "rum_faro_fetch": rum_asset_vcl[fastly_client_ns["RUM_FARO_FETCH_NAME"]],
            "rum_beacon_extraction": rum_beacon_extraction,
            "rum_beacon_response": fastly_client_ns["_generate_beacon_response_vcl"](),
        },
    )

    fixtures_path = ROOT / "tests/generator_fixtures.json"
    if fixtures_path.exists():
        fixtures_data = json.loads(fixtures_path.read_text(encoding="utf-8"))
        for case in fixtures_data.get("cases", []):
            if "M" in (case.get("config") or {}).get("groups", []):
                updated_capture = capture(case["config"])
                if "deliver" in updated_capture:
                    case["capture"]["deliver"] = updated_capture["deliver"]
                    for snippet in case.get("snippets", []):
                        if snippet.get("subroutine") == "vcl_deliver":
                            snippet["body"] = (
                                f"# Section 6: Log Field Capture (vcl_deliver)\n{updated_capture['deliver']}"
                            )
        fixtures_data["golden_vectors"] = _build_golden_vectors(repo_root)
        write(fixtures_path, fixtures_data)


def _fnv1a32_py(data: bytes) -> int:
    h = 0x811C9DC5
    for b in data:
        h = ((h ^ b) * 0x01000193) & 0xFFFFFFFF
    return h


def _fnv1a64_py(data: bytes) -> int:
    h = 0xCBF29CE484222325
    for b in data:
        h = ((h ^ b) * 0x00000100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h


def _build_golden_vectors(repo_root: Path) -> dict[str, Any]:
    """Compute cross-language golden vectors from Python backend modules for Rust parity tests."""
    hll_mod = importlib.import_module("backend.utils.hll")
    rum_ns: dict[str, Any] = {
        "Any": Any,
        "urlparse": importlib.import_module("urllib.parse").urlparse,
        "log_once_per": lambda *a, **kw: False,
        "logger": SimpleNamespace(debug=lambda *a, **kw: None),
    }
    definitions(
        repo_root / "backend/core/rum_ingest.py",
        {"extract_metrics_from_faro_payload"},
        rum_ns,
    )
    extract_faro = rum_ns["extract_metrics_from_faro_payload"]

    faro_payload = {
        "meta": {
            "browser": {"name": "Firefox", "mobile": True},
            "os": {"name": "Android"},
            "page": {"url": "https://example.com/checkout?step=2"},
        },
        "measurements": [
            {
                "type": "web-vitals",
                "values": {"cls": 0.08, "delta": 12.0, "lcp": 2450.5},
                "context": {"rating": "good"},
            }
        ],
        "events": [
            {
                "name": "faro.performance.navigation",
                "attributes": {"ttfb": 120.0},
            },
            {"name": "cta_click"},
        ],
        "exceptions": [
            {
                "value": "TypeError: x is null",
                "stacktrace": {"frames": [{"filename": "https://example.com/app.js", "lineno": 42, "colno": 7}]},
            }
        ],
    }
    faro_log_line = {
        "ts_ms": 1700000005123,
        "service_id": "svc-1",
        "fastly_req_id": "req-abc",
        "rum_cid": "cid-99",
        "city": "Seattle",
        "region": "WA",
        "country": "US",
        "pop": "SEA",
        "tls": "1.3",
        "ttfb": 45.5,
        "rum_body": json.dumps(faro_payload),
    }
    expected_faro = extract_faro(faro_payload, faro_log_line)

    hll_cases = []
    for name, items in [
        ("empty", []),
        ("single_ipv4", ["203.0.113.1"]),
        ("multi_mixed", ["203.0.113.1", "203.0.113.2", "198.51.100.42", "2001:db8::1"]),
    ]:
        sketch = hll_mod.HyperLogLog()
        item_hashes = []
        for item in items:
            sketch.add(item)
            item_hashes.append({"item": item, "hash_hex": f"{hll_mod._hash64(item):016x}"})
        hll_cases.append(
            {
                "name": name,
                "items": items,
                "item_hashes": item_hashes,
                "registers": list(sketch._buckets),
                "to_bytes_hex": sketch.to_bytes().hex(),
                "count": sketch.count(),
            }
        )

    bundle_names = [
        "summary=1700000000-1700000060-r1-n10.parquet",
        "summary=1700000060-1700000120-r2-n20.parquet",
    ]
    bundle_joined = "".join(f"{n}\n" for n in bundle_names).encode("utf-8")

    return {
        "rum_ingest": {
            "service_id": "svc-1",
            "window_s": 60,
            "object_time_s": 1700000000,
            "faro_line": faro_log_line,
            "expected_extracted": expected_faro,
        },
        "rum_validators": {
            "faro_versions": [
                {"version": "2.13.0", "valid": True},
                {"version": "2.10.0", "valid": True},
                {"version": "2.13", "valid": False},
                {"version": "2.13.0-beta", "valid": False},
                {"version": "v2.13.0", "valid": False},
            ],
            "custom_conditions": [
                {
                    "condition": "",
                    "valid": True,
                    "statement": 'req.url.path == "/rum-beacon" && req.method == "POST" && fastly.ff.visits_this_service == 0 && req.restarts == 0',
                },
                {
                    "condition": 'req.http.Host == "example.com"',
                    "valid": True,
                    "statement": 'req.url.path == "/rum-beacon" && req.method == "POST" && fastly.ff.visits_this_service == 0 && req.restarts == 0 && (req.http.Host == "example.com")',
                },
                {"condition": 'req.http.Host == "a"; unset req.url', "valid": False},
                {"condition": "req.http.Host == 'example.com'", "valid": False},
                {"condition": 'req.url.path == "/checkout"', "valid": False},
                {"condition": '(req.http.Host == "example.com"', "valid": False},
            ],
        },
        "hll": hll_cases,
        "fingerprint": {
            "fnv1a": [
                {
                    "input": "",
                    "fnv1a32_hex": f"{_fnv1a32_py(b''):08x}",
                    "fnv1a64_hex": f"{_fnv1a64_py(b''):016x}",
                },
                {
                    "input": "foobar",
                    "fnv1a32_hex": f"{_fnv1a32_py(b'foobar'):08x}",
                    "fnv1a64_hex": f"{_fnv1a64_py(b'foobar'):016x}",
                },
                {
                    "input": "service_id=svc-1/dt=2026-11-05/hour=00",
                    "fnv1a32_hex": f"{_fnv1a32_py(b'service_id=svc-1/dt=2026-11-05/hour=00'):08x}",
                    "fnv1a64_hex": f"{_fnv1a64_py(b'service_id=svc-1/dt=2026-11-05/hour=00'):016x}",
                },
            ],
            "bundle_names": bundle_names,
            "bundle_fingerprint_hex": f"{_fnv1a64_py(bundle_joined):016x}",
        },
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sync catalog.json and templates.json from backend/")
    parser.add_argument("reference", nargs="?", help="Optional rc1 reference path for full fixture rebuild")
    parser.add_argument("--catalog", type=Path, default=None, help="Output path for catalog.json")
    parser.add_argument("--templates", type=Path, default=None, help="Output path for templates.json")
    parser.add_argument("--write", action="store_true", help="Write catalog.json and templates.json")
    parser.add_argument("--sync-in-repo", action="store_true", help="Sync catalog, templates, and fixtures in-repo")
    parser.add_argument("--lint", action="store_true", help="Run falco lint on generated fixtures")
    parser.add_argument(
        "--rebuild-fixtures", action="store_true", help="Rebuild generator_fixtures.json from reference"
    )
    parsed = parser.parse_args()
    if parsed.rebuild_fixtures and parsed.reference:
        main(Path(parsed.reference).resolve(), lint=parsed.lint)
    else:
        sync_catalog_and_templates(catalog_path=parsed.catalog, templates_path=parsed.templates)
