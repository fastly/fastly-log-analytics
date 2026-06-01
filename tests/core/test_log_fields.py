from backend.core.log_fields import PRESETS, generate_log_format, validate_group_deps
from backend.provision import generate_capture_vcl


def test_generate_log_format():
    """Verify that VCL log format strings are generated correctly."""
    # Test minimal preset
    cfg_minimal = {"groups": PRESETS["minimal"]["groups"], "field_overrides": {}}
    fmt_minimal = generate_log_format(cfg_minimal)

    assert isinstance(fmt_minimal, str)
    assert fmt_minimal.startswith("{")
    assert fmt_minimal.endswith("}")
    assert '"timestamp":' in fmt_minimal
    assert '"ip":' in fmt_minimal
    assert '"url":' not in fmt_minimal  # URL is Group A

    # Test standard preset
    cfg_standard = {"groups": PRESETS["standard"]["groups"], "field_overrides": {}}
    fmt_standard = generate_log_format(cfg_standard)

    assert '"url":' in fmt_standard
    assert '"pop":' in fmt_standard  # Group C
    assert '"country":' in fmt_standard  # Group D


def test_field_overrides():
    """Verify that specific fields can be overridden regardless of group."""
    cfg = {
        "groups": [],  # No groups
        "field_overrides": {"url": True, "country": True},
    }
    fmt = generate_log_format(cfg)

    assert '"url":' in fmt
    assert '"country":' in fmt
    assert '"pop":' not in fmt  # Pop is Group C, shouldn't be included


def test_custom_origin_field_propagated_to_cluster():
    """Regression: origin custom fields must not be stripped when serving to cluster nodes.

    When a shield node serves to an edge node (x-is-cluster-fetch=1), it must keep
    resp.http.x-fos-origin-data:{name} in the response so the edge can cache and log it.
    Previously the deliver snippet always ran `unset resp.http.x-fos-origin-data:{name}`,
    causing the edge to log null for every origin custom field.
    """
    cfg = {
        "groups": ["L"],
        "custom_fields": [
            {
                "name": "bereq_drew",
                "vcl_log_expression": "beresp.http.x-drew",
                "collection_stage": "origin",
                "origin_log_frequency": "all",
                "enabled": True,
            }
        ],
    }
    snippets = generate_capture_vcl(cfg)
    deliver = snippets["deliver"]

    # The unset must live inside the cluster-fetch guard block (indented)
    expected_guard_block = (
        'if (req.http.x-is-cluster-fetch != "1") {\n  unset resp.http.x-fos-origin-data:bereq_drew;\n}'
    )
    assert expected_guard_block in deliver, "unset must be inside the x-is-cluster-fetch guard"

    # The unconditional bare unset (old bug) must NOT appear at column 0
    assert "\nunset resp.http.x-fos-origin-data:bereq_drew;" not in deliver, (
        "unset must not appear at the top level (unindented) — it must be inside the guard"
    )


def test_custom_edge_field_not_in_deliver():
    """Edge custom fields should not appear in the deliver snippet at all."""
    cfg = {
        "groups": [],
        "custom_fields": [
            {
                "name": "edge_randomint",
                "vcl_log_expression": "randomint(1, 100)",
                "collection_stage": "edge",
                "enabled": True,
            }
        ],
    }
    snippets = generate_capture_vcl(cfg)
    assert "edge_randomint" in snippets["recv"]
    assert "deliver" not in snippets or "edge_randomint" not in snippets.get("deliver", "")


def test_validate_group_deps():
    """Verify that dependent groups throw validation errors if not met."""
    # E requires D
    assert len(validate_group_deps(["E"])) == 1
    assert "requires Group D" in validate_group_deps(["E"])[0]

    # If D is present, it should pass
    assert len(validate_group_deps(["D", "E"])) == 0


# ── validate_custom_field: 14-rule validation gauntlet ─────────────────────


def _valid_field(**overrides) -> dict:
    """Minimal valid custom-field dict for validation tests."""
    base = {
        "name": "my_field",
        "label": "My Field",
        "vcl_log_expression": "req.url",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
    }
    base.update(overrides)
    return base


def test_validate_custom_field_returns_empty_for_minimum_valid_dict():
    """A minimal valid dict produces no errors. Pinned because the
    14-rule validator is order-sensitive and any new rule must not
    block this golden-path baseline."""
    from backend.core.log_fields import validate_custom_field

    assert validate_custom_field(_valid_field(), []) == []


def test_validate_custom_field_flags_missing_required_keys_and_short_circuits():
    """Missing keys yield "Missing required field: 'X'" errors AND
    short-circuit further validation (other errors would be
    confusing while keys are absent). Pinned because losing the
    short-circuit cascades into N follow-on errors per missing key."""
    from backend.core.log_fields import validate_custom_field

    field = {"name": "x"}  # only one required key present
    errors = validate_custom_field(field, [])

    # Errors include the missing required keys
    assert any("Missing required field" in e for e in errors)
    # But NOT subsequent rule errors (e.g. label-length, since label is absent → required-key error caught it)
    assert not any("label" in e and "characters" in e for e in errors)


def test_validate_custom_field_rejects_invalid_name_regex():
    """Names must match `^[a-z][a-z0-9_]{0,47}$`. Pinned because
    losing this would allow SQL identifiers with dashes/uppercase
    that need quoting everywhere downstream."""
    from backend.core.log_fields import validate_custom_field

    # Starts with a digit
    errors = validate_custom_field(_valid_field(name="1bad"), [])
    assert any("lowercase alphanumeric" in e for e in errors)

    # Contains a hyphen
    errors = validate_custom_field(_valid_field(name="bad-name"), [])
    assert any("lowercase alphanumeric" in e for e in errors)


def test_validate_custom_field_rejects_duckdb_reserved_word():
    """DuckDB reserved words can't be column names. Pinned because
    losing this would error at CREATE TABLE time with an opaque
    parser error."""
    from backend.core.log_fields import validate_custom_field

    # 'select' is a SQL reserved word
    errors = validate_custom_field(_valid_field(name="select"), [])
    assert any("reserved word" in e for e in errors)


def test_validate_custom_field_rejects_builtin_field_collision():
    """Custom field names can't shadow built-in field IDs (timestamp,
    ip, etc). Pinned because losing this would cause downstream
    code that reads the built-in column to get the custom value."""
    from backend.core.log_fields import validate_custom_field

    # 'timestamp' is a built-in
    errors = validate_custom_field(_valid_field(name="timestamp"), [])
    assert any("already a built-in" in e for e in errors)


def test_validate_custom_field_rejects_duplicate_custom_name():
    """Two custom fields can't share a name within the same service.
    Pinned because losing this would let the wizard overwrite
    existing fields silently."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(name="my_field"), ["my_field"])
    assert any("already exists" in e for e in errors)


def test_validate_custom_field_enforces_label_length_1_to_80():
    """`label` must be 1–80 chars. Pinned because the FE allocates
    a fixed-width slot in the column header and longer labels
    would break the layout."""
    from backend.core.log_fields import validate_custom_field

    # Empty label
    assert any("label" in e for e in validate_custom_field(_valid_field(label=""), []))
    # Too long label
    assert any("label" in e for e in validate_custom_field(_valid_field(label="x" * 81), []))


def test_validate_custom_field_enforces_description_max_500_chars():
    """`description` is optional but capped at 500 chars. Pinned
    because the field-list panel renders the description inline
    and longer would overflow."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(description="x" * 501), [])
    assert any("description" in e for e in errors)


def test_validate_custom_field_vcl_expression_must_be_non_empty():
    """`vcl_log_expression` blank → error. Pinned because deploying
    an empty VCL field would fail at Fastly upload."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(vcl_log_expression=""), [])
    assert any("vcl_log_expression" in e and "empty" in e for e in errors)


def test_validate_custom_field_vcl_expression_max_512_chars():
    """`vcl_log_expression` capped at 512 chars. Pinned because
    longer expressions blow past Fastly's per-snippet character
    budget."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(vcl_log_expression="x" * 513), [])
    assert any("vcl_log_expression" in e and "512" in e for e in errors)


def test_validate_custom_field_vcl_rejects_newlines_semicolons_comments():
    """VCL injection protection: no raw newlines, semicolons, or
    `//` / `/*` / `#` comments. Pinned because losing these would
    let user input alter the surrounding VCL snippet structure."""
    from backend.core.log_fields import validate_custom_field

    # Newline
    errors = validate_custom_field(_valid_field(vcl_log_expression="req.url\n"), [])
    assert any("newlines" in e for e in errors)

    # Semicolon
    errors = validate_custom_field(_valid_field(vcl_log_expression="req.url; set req.x = 1"), [])
    assert any("semicolons" in e for e in errors)

    # // comment
    errors = validate_custom_field(_valid_field(vcl_log_expression="req.url // hack"), [])
    assert any("comments" in e for e in errors)

    # # comment
    errors = validate_custom_field(_valid_field(vcl_log_expression="req.url # hack"), [])
    assert any("comments" in e for e in errors)


def test_validate_custom_field_rejects_unknown_duckdb_type():
    """`duckdb_type` must be a known type. Pinned because losing
    this would error at CREATE TABLE with an opaque parser error."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(duckdb_type="QUANTUM"), [])
    assert any("duckdb_type" in e for e in errors)


def test_validate_custom_field_rejects_unknown_value_type():
    """`value_type` enum: string/numeric/boolean/ip/url. Pinned
    because the FE renders different input widgets per type."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(value_type="unknown"), [])
    assert any("value_type" in e for e in errors)


def test_validate_custom_field_rejects_incompatible_duckdb_value_type_pair():
    """E.g. duckdb_type=INTEGER with value_type=ip → incompatible.
    Pinned because losing the pair check would let users create
    schemas that error at first INSERT."""
    from backend.core.log_fields import validate_custom_field

    # INTEGER + 'ip' is not compatible
    errors = validate_custom_field(
        _valid_field(duckdb_type="INTEGER", value_type="ip"),
        [],
    )
    assert any("not compatible" in e for e in errors)


def test_validate_custom_field_bytes_estimate_must_be_int_in_range_1_to_1024():
    """`bytes_estimate` must be int between 1 and 1024. Pinned
    because the wizard uses this to project log_format size — a
    bogus value would silently mislead the byte-budget UI."""
    from backend.core.log_fields import validate_custom_field

    # Zero
    errors = validate_custom_field(_valid_field(bytes_estimate=0), [])
    assert any("bytes_estimate" in e for e in errors)
    # Over the cap
    errors = validate_custom_field(_valid_field(bytes_estimate=2000), [])
    assert any("bytes_estimate" in e for e in errors)
    # Non-int
    errors = validate_custom_field(_valid_field(bytes_estimate="not-a-number"), [])
    assert any("bytes_estimate" in e for e in errors)


def test_validate_custom_field_collection_stage_must_be_edge_or_origin():
    """`collection_stage` enum: edge|origin. Pinned because the VCL
    generator routes the field into different subroutines based on
    this — an unknown value would silently drop the field."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(collection_stage="fetch"), [])
    assert any("collection_stage" in e for e in errors)


def test_validate_custom_field_warns_on_suspiciously_low_bytes_estimate():
    """`bytes_estimate` below the key+quotes+colon overhead emits
    a WARN: prefix (not a hard error). Pinned because the routes
    split WARN: lines into warnings (yellow) vs errors (red)."""
    from backend.core.log_fields import validate_custom_field

    # name is "my_field" = 8 chars; min_bytes = 8 + 5 = 13
    errors = validate_custom_field(_valid_field(bytes_estimate=5), [])
    # Should yield a WARN: line (not a hard error blocking save)
    warnings = [e for e in errors if e.startswith("WARN:")]
    assert len(warnings) >= 1
