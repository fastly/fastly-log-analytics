import os
import re
import sys


def update_values(frontend_tag, backend_tag):
    frontend_repo = os.environ.get("FLA_FRONTEND_IMAGE")
    backend_repo = os.environ.get("FLA_BACKEND_IMAGE")
    if not frontend_repo or not backend_repo:
        raise SystemExit("FLA_FRONTEND_IMAGE and FLA_BACKEND_IMAGE are required")

    with open("/tmp/current-values.yaml") as f:
        content = f.read()

    # Replace tag: <tag> after repo: <frontend_repo>
    content = re.sub(
        rf"(repo:\s*{re.escape(frontend_repo)}\s*\n\s*tag:\s*)[^\s]+",
        r"\g<1>" + frontend_tag,
        content,
    )
    # Replace inline frontend image: <tag>
    content = re.sub(
        rf'({re.escape(frontend_repo)}:)[^\s"\'\n]+',
        r"\g<1>" + frontend_tag,
        content,
    )

    # Replace tag: <tag> after repo: <backend_repo>
    content = re.sub(
        rf"(repo:\s*{re.escape(backend_repo)}\s*\n\s*tag:\s*)[^\s]+",
        r"\g<1>" + backend_tag,
        content,
    )
    # Replace inline backend image: <tag>
    content = re.sub(
        rf'({re.escape(backend_repo)}:)[^\s"\'\n]+',
        r"\g<1>" + backend_tag,
        content,
    )

    with open("/tmp/current-values-updated.yaml", "w") as f:
        f.write(content)
    print("Values updated successfully.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python update_helm_values.py <frontend_tag> <backend_tag>")
        sys.exit(1)
    update_values(sys.argv[1], sys.argv[2])
