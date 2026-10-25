import sys

from backend.core.fastly import client

service = client
mock_fixtures = client
sys.modules[f"{__name__}.service"] = client
sys.modules[f"{__name__}.mock_fixtures"] = client
