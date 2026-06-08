import json
import os
import sys

# Ensure project root is in sys.path (scripts/ lives one level below root)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend.main import app


def generate_openapi(output_path: str | None = None) -> None:
    schema = json.dumps(app.openapi(), indent=2)
    if output_path:
        # Skip the write (and preserve mtime) when the schema is byte-identical
        # to what's already on disk. Downstream tooling (openapi-typescript)
        # can then mtime-compare against its generated artifact and skip its
        # own regen. The freshness guarantee is unchanged: we still introspect
        # the live FastAPI app on every invocation; we just don't churn the
        # file when there's nothing to write.
        try:
            with open(output_path) as f:
                if f.read() == schema:
                    return
        except FileNotFoundError:
            pass
        with open(output_path, "w") as f:
            f.write(schema)
    else:
        sys.stdout.write(schema)


if __name__ == "__main__":
    # Accept an optional output path so callers can avoid shell redirection.
    # This prevents uv's stdout warnings from contaminating the output file.
    generate_openapi(sys.argv[1] if len(sys.argv) > 1 else None)
