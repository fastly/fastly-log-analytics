import json
import os
import re
import subprocess
from pathlib import Path

SENSITIVE = re.compile(r"token|secret|password|(?:^|_)key(?:$|_)|(?:^|_)ak$|(?:^|_)sk$", re.IGNORECASE)


def credential_values(environment: Path, state: Path) -> set[bytes]:
    values: set[bytes] = set()

    def collect(value: object, key: str = "") -> None:
        if isinstance(value, dict):
            for name, child in value.items():
                collect(child, str(name))
        elif isinstance(value, list):
            for child in value:
                collect(child, key)
        elif isinstance(value, str) and SENSITIVE.search(key):
            values.add(value.encode())

    collect(dict(os.environ))
    paths = [environment]
    if state.is_dir():
        paths.extend(path for path in state.rglob("*") if path.is_file())
    for path in paths:
        if not path.is_file():
            continue
        if path.suffix == ".json":
            collect(json.loads(path.read_text()))
        elif path.suffix == ".env" or path == environment:
            for line in path.read_text().splitlines():
                name, separator, value = line.removeprefix("export ").partition("=")
                if separator and not name.lstrip().startswith("#"):
                    collect(value.strip().strip("\"'"), name)
        elif SENSITIVE.search(path.name):
            value = path.read_bytes().strip()
            values.add(value)
            if b":" in value:
                values.add(value.split(b":", 1)[1].strip())
    return {value for value in values if len(value) >= 12}


def find_exposures(paths: list[Path], credentials: set[bytes]) -> list[Path]:
    return [path for path in paths if path.is_file() and any(value in path.read_bytes() for value in credentials)]


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    files = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "app"],
        cwd=root,
    )
    paths = [root / os.fsdecode(name) for name in files.split(b"\0") if name]
    exposed = find_exposures(paths, credential_values(root / ".env", Path.home() / ".flp-state"))
    for path in exposed:
        print(f"credential-value exposure: {path.relative_to(root)}")
    print(f"Credential scan: {len(paths)} files, {len(exposed)} exposures")
    return int(bool(exposed))


if __name__ == "__main__":
    raise SystemExit(main())
