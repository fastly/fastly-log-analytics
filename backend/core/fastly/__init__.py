import sys

from backend.core.fastly import client

service = client
sys.modules[f"{__name__}.service"] = client
