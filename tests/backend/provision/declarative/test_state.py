"""Unit tests for FeatureState: validation, auto-injection, immutability."""

import pytest

from backend.provision.declarative.state import FeatureState


class TestFeatureStateValidation:
    """Test FeatureState validation constraints."""

    def test_featurestate_validates_log_period_range(self):
        """Verify log_period is within valid range [30, 3600]."""
        cfg = {
            "service_id": "srv_test",
            "log_period": 5,  # Too small
            "sample_rate": 100,
        }
        with pytest.raises(ValueError, match="log_period.*30.*3600"):
            FeatureState.from_config(cfg)

    def test_featurestate_validates_sample_rate_range(self):
        """Verify sample_rate is within valid range [1, 100]."""
        cfg = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 150,  # Too high
        }
        with pytest.raises(ValueError, match="sample_rate.*1.*100"):
            FeatureState.from_config(cfg)

    def test_featurestate_rejects_scoring_without_domain(self):
        """Verify scoring_enabled=True without scoring_domain raises ValueError."""
        cfg = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "scoring_enabled": True,
            "scoring_domain": "",  # Missing
        }
        with pytest.raises(ValueError, match="scoring.enabled.*scoring.domain"):
            FeatureState.from_config(cfg)

    def test_featurestate_validates_custom_field_names(self):
        """Verify custom field names must be valid identifiers."""
        cfg = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "log_fields": {
                "custom_fields": [
                    {"name": "invalid-field", "expression": "..."}  # Invalid chars
                ]
            },
        }
        with pytest.raises(ValueError, match="Custom field name"):
            FeatureState.from_config(cfg)


class TestFeatureStateDependencyInjection:
    """Test auto-injection of mandatory custom fields."""

    def test_featurestate_from_config_injects_rum_cid(self):
        """Verify rum_cid is auto-injected when rum_enabled=True."""
        cfg = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "rum_enabled": True,
            "log_fields": {"custom_fields": [{"name": "custom_field_1", "expression": "..."}]},
        }
        state = FeatureState.from_config(cfg)
        assert any(f["name"] == "rum_cid" for f in state.log_fields.custom_fields), "rum_cid should be auto-injected"
        assert state.log_fields.custom_fields[0]["name"] == "custom_field_1", "User fields should come first"

    def test_featurestate_from_config_injects_edge_score_when_scoring_enabled(self):
        """Verify edge_score is auto-injected when scoring_enabled=True."""
        cfg = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "scoring_enabled": True,
            "scoring_domain": "scorer.example.com",
            "log_fields": {"custom_fields": []},
        }
        state = FeatureState.from_config(cfg)
        assert any(f["name"] == "edge_score" for f in state.log_fields.custom_fields), (
            "edge_score should be auto-injected"
        )
        assert any(f["name"] == "edge_score_l1" for f in state.log_fields.custom_fields)
        assert any(f["name"] == "edge_score_l2" for f in state.log_fields.custom_fields)

    def test_featurestate_from_config_injects_cmcd_fields(self):
        """Verify CMCD fields are auto-injected when cmcd_enabled=True."""
        cfg = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "cmcd_enabled": True,
            "log_fields": {"custom_fields": []},
        }
        state = FeatureState.from_config(cfg)
        cmcd_fields = [f["name"] for f in state.log_fields.custom_fields if "cmcd" in f["name"]]
        assert len(cmcd_fields) >= 10, f"Should inject at least 10 CMCD fields, got {cmcd_fields}"

    def test_featurestate_from_config_does_not_duplicate_injected_fields(self):
        """Verify fields are not duplicated if already present."""
        cfg = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "rum_enabled": True,
            "log_fields": {
                "custom_fields": [
                    {"name": "rum_cid", "expression": "req.http.x-rum-cid"}  # User provided
                ]
            },
        }
        state = FeatureState.from_config(cfg)
        rum_cid_count = sum(1 for f in state.log_fields.custom_fields if f["name"] == "rum_cid")
        assert rum_cid_count == 1, "rum_cid should not be duplicated"


class TestFeatureStateImmutability:
    """Test that FeatureState is frozen (immutable)."""

    def test_featurestate_frozen_prevents_mutation(self):
        """Verify FeatureState is immutable (frozen=True)."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
            }
        )
        with pytest.raises(Exception):  # FrozenInstanceError
            state.rum_enabled = False


class TestFeatureStateSerialization:
    """Test FeatureState.to_dict() serialization."""

    def test_featurestate_to_dict_includes_all_fields(self):
        """Verify to_dict() includes all fields."""
        cfg = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "rum_enabled": True,
            "scoring_enabled": True,
            "scoring_domain": "scorer.example.com",
            "logging_enabled": False,
        }
        state = FeatureState.from_config(cfg)
        d = state.to_dict()
        assert d["service_id"] == "srv_test"
        assert d["log_period"] == 60
        assert d["logging_enabled"] is False
        assert d["rum_enabled"] is True
        assert d["scoring_enabled"] is True

    def test_featurestate_to_dict_defaults_logging_enabled(self):
        """Verify logging_enabled defaults to True when omitted from config."""
        cfg = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
        }
        state = FeatureState.from_config(cfg)
        d = state.to_dict()
        assert d["logging_enabled"] is True


class TestFeatureStateDualFormatBackwardCompat:
    """Test backward compatibility between flat and nested config formats."""

    def test_featurestate_accepts_flat_cmcd_format(self):
        """Verify flat cmcd_* fields are accepted and normalized to nested."""
        cfg_flat = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "cmcd_enabled": True,
            "cmcd_mode": "headers",
            "cmcd_version": 2,
            "log_fields": {"custom_fields": []},
        }
        state = FeatureState.from_config(cfg_flat)
        assert state.cmcd.enabled is True
        assert state.cmcd.mode == "headers"
        assert state.cmcd.version == 2

    def test_featurestate_accepts_nested_cmcd_format(self):
        """Verify nested cmcd object is accepted."""
        cfg_nested = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "cmcd": {
                "enabled": True,
                "mode": "headers",
                "version": 2,
            },
            "log_fields": {"custom_fields": []},
        }
        state = FeatureState.from_config(cfg_nested)
        assert state.cmcd.enabled is True
        assert state.cmcd.mode == "headers"
        assert state.cmcd.version == 2

    def test_featurestate_flat_and_nested_cmcd_produce_same_result(self):
        """Verify flat and nested CMCD formats produce identical FeatureState."""
        cfg_flat = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "cmcd_enabled": True,
            "cmcd_mode": "headers",
            "log_fields": {"custom_fields": []},
        }
        cfg_nested = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "cmcd": {"enabled": True, "mode": "headers"},
            "log_fields": {"custom_fields": []},
        }
        state_flat = FeatureState.from_config(cfg_flat)
        state_nested = FeatureState.from_config(cfg_nested)
        assert state_flat.cmcd.enabled == state_nested.cmcd.enabled
        assert state_flat.cmcd.mode == state_nested.cmcd.mode
        assert state_flat.cmcd.version == state_nested.cmcd.version

    def test_featurestate_accepts_flat_scoring_format(self):
        """Verify flat scoring_* fields are accepted and normalized to nested."""
        cfg_flat = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "scoring_enabled": True,
            "scoring_domain": "scorer.example.com",
            "scoring_request_secret": "secret123",
            "scoring_enforce_status_code": 429,
            "log_fields": {"custom_fields": []},
        }
        state = FeatureState.from_config(cfg_flat)
        assert state.scoring.enabled is True
        assert state.scoring.domain == "scorer.example.com"
        assert state.scoring.request_secret == "secret123"
        assert state.scoring.enforce_status_code == 429

    def test_featurestate_accepts_nested_scoring_format(self):
        """Verify nested scoring object is accepted."""
        cfg_nested = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "scoring": {
                "enabled": True,
                "domain": "scorer.example.com",
                "request_secret": "secret123",
                "enforce_status_code": 429,
            },
            "log_fields": {"custom_fields": []},
        }
        state = FeatureState.from_config(cfg_nested)
        assert state.scoring.enabled is True
        assert state.scoring.domain == "scorer.example.com"
        assert state.scoring.request_secret == "secret123"
        assert state.scoring.enforce_status_code == 429

    def test_featurestate_flat_and_nested_scoring_produce_same_result(self):
        """Verify flat and nested Scoring formats produce identical FeatureState."""
        cfg_flat = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "scoring_enabled": True,
            "scoring_domain": "scorer.example.com",
            "log_fields": {"custom_fields": []},
        }
        cfg_nested = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "scoring": {
                "enabled": True,
                "domain": "scorer.example.com",
            },
            "log_fields": {"custom_fields": []},
        }
        state_flat = FeatureState.from_config(cfg_flat)
        state_nested = FeatureState.from_config(cfg_nested)
        assert state_flat.scoring.enabled == state_nested.scoring.enabled
        assert state_flat.scoring.domain == state_nested.scoring.domain

    def test_featurestate_to_dict_outputs_flat_format(self):
        """Verify to_dict() serializes to flat format for wire compatibility."""
        cfg = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "cmcd_enabled": True,
            "cmcd_mode": "headers",
            "scoring_enabled": True,
            "scoring_domain": "scorer.example.com",
            "log_fields": {
                "groups": ["test_group"],
                "custom_fields": [{"name": "custom_1", "expression": "..."}],
            },
        }
        state = FeatureState.from_config(cfg)
        d = state.to_dict()
        # Verify flat format in to_dict() output
        assert d["cmcd_enabled"] is True
        assert d["cmcd_mode"] == "headers"
        assert d["scoring_enabled"] is True
        assert d["scoring_domain"] == "scorer.example.com"
        assert d["log_groups"] == ["test_group"]
        assert len(d["custom_fields"]) >= 1  # At least user field + injected fields

    def test_featurestate_flat_override_beats_nested(self):
        """Verify flat fields override nested when both are present (migration edge case)."""
        cfg = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "cmcd_enabled": True,
            "cmcd": {"enabled": False},  # Flat overrides nested
            "log_fields": {"custom_fields": []},
        }
        state = FeatureState.from_config(cfg)
        # Flat format should win per line 175 in state.py
        assert state.cmcd.enabled is True
