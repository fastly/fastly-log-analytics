"""Known Compose tags receive narrow syntax validation, not blanket exclusions."""

import re
from pathlib import Path

import yaml


def test_custom_compose_tags_use_narrow_syntax_hook():
    root = Path(__file__).resolve().parents[2]
    config = yaml.safe_load((root / ".pre-commit-config.yaml").read_text())
    hooks = [hook for repo in config["repos"] for hook in repo["hooks"] if hook["id"] == "check-yaml"]
    generic = next(hook for hook in hooks if "alias" not in hook)
    tagged = next(hook for hook in hooks if hook.get("alias") == "check-compose-tagged-yaml")
    assert tagged["args"] == ["--unsafe"]
    for filename in (
        "docker-compose.prod.yml",
        "docker-compose.high-scale-local.yml",
        "docker-compose.admin-mtls.prod.yml",
    ):
        assert re.search(generic["exclude"], filename)
        assert re.search(tagged["files"], filename)
    for filename in ("docker-compose.yml", "docker-compose.admin-mtls.yml", "config.yml", ".github/workflows/ci.yml"):
        assert not re.search(generic["exclude"], filename)
        assert not re.search(tagged["files"], filename)
