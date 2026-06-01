import json
import os
import sys

# Ensure project root is in sys.path (scripts/ lives one level below root)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend.main import app


def generate_openapi(output_path: str | None = None) -> None:
    schema = json.dumps(app.openapi(), indent=2)
    if output_path:
        with open(output_path, "w") as f:
            f.write(schema)
    else:
        sys.stdout.write(schema)


if __name__ == "__main__":
    # Accept an optional output path so callers can avoid shell redirection.
    # This prevents uv's stdout warnings from contaminating the output file.
    generate_openapi(sys.argv[1] if len(sys.argv) > 1 else None)
