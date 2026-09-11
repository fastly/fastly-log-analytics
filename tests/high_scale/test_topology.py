import pytest

from backend.high_scale.topology import build_topology_profile


def test_local_profile_does_not_claim_production_ha() -> None:
    profile = build_topology_profile("local")
    assert profile.clickhouse_replicas_per_shard == 1
    assert profile.keeper_members == 1
    assert profile.production_ha is False


def test_portable_production_profile_has_replicas_and_keeper() -> None:
    profile = build_topology_profile("portable-production")
    assert profile.clickhouse_replicas_per_shard == 2
    assert profile.keeper_members == 3
    assert profile.production_ha is True


def test_unknown_profile_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown"):
        build_topology_profile("elevation-production")
