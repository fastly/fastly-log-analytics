"""Portable topology profiles; infrastructure ownership remains external."""

from __future__ import annotations

from backend.high_scale.models import TopologyProfile


def build_topology_profile(profile_name: str) -> TopologyProfile:
    if profile_name == "local":
        return TopologyProfile("local", 1, 1, True, False)
    if profile_name == "portable-production":
        return TopologyProfile("portable-production", 2, 3, True, True)
    raise ValueError("unknown high-scale topology profile")
