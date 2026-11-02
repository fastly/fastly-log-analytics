import json
import os
import shutil
import subprocess
import sys

# Ensure project root is in sys.path (scripts/ lives one level below root)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend.main import app


def refresh_api_types(
    schema_path: str = "openapi.json",
    types_path: str = "types/api.generated.ts",
) -> None:
    def _mtime(path: str) -> float:
        try:
            return os.stat(path).st_mtime
        except OSError:
            return 0.0

    if _mtime(schema_path) <= _mtime(types_path):
        print(f"{types_path} already up-to-date with {schema_path}, skipping openapi-typescript")
        return
    cmd = (
        ["openapi-typescript", schema_path, "-o", types_path]
        if shutil.which("openapi-typescript")
        else ["npx", "openapi-typescript", schema_path, "-o", types_path]
    )
    subprocess.run(cmd, check=True)


def generate_openapi(output_path: str | None = None, *, refresh_ts: bool = False) -> None:
    schema = json.dumps(app.openapi(), indent=2)
    if output_path:
        # Skip the write (and preserve mtime) when the schema is byte-identical
        # to what's already on disk so downstream openapi-typescript can skip regen.
        try:
            with open(output_path) as f:
                if f.read() == schema:
                    if refresh_ts:
                        refresh_api_types(output_path)
                    return
        except FileNotFoundError:
            pass
        with open(output_path, "w") as f:
            f.write(schema)
        if refresh_ts:
            refresh_api_types(output_path)
    else:
        sys.stdout.write(schema)


if __name__ == "__main__":
    refresh_ts_flag = "--refresh-ts" in sys.argv[1:]
    pos_args = [a for a in sys.argv[1:] if a != "--refresh-ts"]
    out_arg = pos_args[0] if pos_args else ("openapi.json" if refresh_ts_flag else None)
    generate_openapi(out_arg, refresh_ts=refresh_ts_flag)
