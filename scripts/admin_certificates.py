#!/usr/bin/env python3
"""Operator-initiated admin PKI lifecycle. Uses OpenSSL; never modifies trust stores.

All output lives outside this checkout, with directories 0700 and files 0600.
Client CSRs can be signed without moving the client's private key to the CA host.
"""

import argparse
import ipaddress
import os
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
HOST = re.compile(
    r"(?=.{1,253}$)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
)


def run(*args: str) -> str:
    result = subprocess.run(["openssl", *args], stdin=subprocess.DEVNULL, capture_output=True, text=True, check=False)
    if result.returncode:
        raise ValueError(f"OpenSSL {args[0]} failed: {result.stderr.strip()}")
    return result.stdout


def private_file(path: Path) -> Path:
    path = path.resolve(strict=True)
    if not path.is_file() or path.stat().st_mode & 0o077:
        raise ValueError(f"{path}: private input must be a file accessible only to its owner (chmod 600)")
    return path


def password_file(path: Path) -> Path:
    path = private_file(path)
    lines = path.read_bytes().splitlines()
    if not lines or not lines[0].strip():
        raise ValueError("CA/export password file must not be empty")
    return path


def output_dir(path: Path) -> Path:
    path = path.resolve()
    if path == REPO or REPO in path.parents:
        raise ValueError("Certificate/key output must be outside the repository")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.stat().st_mode & 0o077:
        raise ValueError(f"{path}: output directory must be owner-only (chmod 700)")
    return path


def reserve(path: Path) -> None:
    # Refuse overwrites, including symlinks. Rotation uses a new directory.
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)


def key(path: Path, password: Path | None = None) -> None:
    reserve(path)
    args = ["genpkey", "-algorithm", "EC", "-pkeyopt", "ec_paramgen_curve:P-256", "-out", str(path)]
    if password:
        args += ["-aes-256-cbc", "-pass", f"file:{password}"]
    run(*args)


def ca(directory: Path, name: str, password: Path, days: int) -> None:
    key_path, cert_path = directory / f"{name}.key", directory / f"{name}.crt"
    key(key_path, password)
    reserve(cert_path)
    run(
        "req",
        "-new",
        "-x509",
        "-sha256",
        "-key",
        str(key_path),
        "-passin",
        f"file:{password}",
        "-out",
        str(cert_path),
        "-days",
        str(days),
        "-subj",
        f"/CN=Log Analytics {name}",
        "-addext",
        "basicConstraints=critical,CA:TRUE,pathlen:0",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
    )


def sign(csr: Path, cert: Path, state: Path, password: Path, purpose: str, days: int, host: str = "") -> None:
    run("req", "-in", str(csr), "-verify", "-noout")
    ca_name = "client-ca" if purpose == "clientAuth" else "server-ca"
    # Refuse signing leaves beyond the remaining CA lifetime.
    run("x509", "-in", str(state / f"{ca_name}.crt"), "-checkend", str(days * 86400), "-noout")
    ext = f"basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage={purpose}\n"
    if host:
        try:
            ipaddress.ip_address(host)
            ext += f"subjectAltName=IP:{host}\n"
        except ValueError:
            ext += f"subjectAltName=DNS:{host}\n"
    reserve(cert)
    with tempfile.TemporaryDirectory() as temp:
        extension_file = Path(temp) / "extensions"
        extension_file.write_text(ext)
        run(
            "x509",
            "-req",
            "-sha256",
            "-in",
            str(csr),
            "-CA",
            str(state / f"{ca_name}.crt"),
            "-CAkey",
            str(private_file(state / f"{ca_name}.key")),
            "-passin",
            f"file:{password}",
            "-set_serial",
            str(secrets.randbits(159) or 1),
            "-days",
            str(days),
            "-extfile",
            str(extension_file),
            "-out",
            str(cert),
        )


def execute(args: argparse.Namespace) -> None:
    if args.command == "init":
        directory = output_dir(args.out)
        password = password_file(args.password_file)
        # Fail before creating anything when a prior initialization exists.
        if any(directory.iterdir()):
            raise ValueError("init requires an empty directory; use a new directory for CA rotation")
        ca(directory, "client-ca", password, args.days)
        ca(directory, "server-ca", password, args.days)
        credential = directory / "gateway-secret"
        reserve(credential)
        credential.write_text(secrets.token_urlsafe(48))
    elif args.command in {"client-request", "server"}:
        directory = output_dir(args.out)
        name = args.name if args.command == "client-request" else "server"
        if not NAME.fullmatch(name):
            raise ValueError("name must be 1-64 letters, numbers, dot, dash or underscore")
        if args.command == "server" and not HOST.fullmatch(args.host):
            raise ValueError("host must be one explicit DNS hostname, without wildcard, scheme or port")
        if args.command == "server":
            state = args.state.resolve(strict=True)
            password = password_file(args.password_file)
        else:
            password = password_file(args.password_file) if args.password_file else None
        key_path, csr = directory / f"{name}.key", directory / f"{name}.csr"
        key(key_path, password if args.command == "client-request" else None)
        reserve(csr)
        req = ["req", "-new", "-key", str(key_path), "-out", str(csr), "-subj", f"/CN={name}"]
        if args.command == "client-request" and password:
            req += ["-passin", f"file:{password}"]
        run(*req)
        if args.command == "server":
            sign(csr, directory / "server.crt", state, password, "serverAuth", args.days, args.host)
    elif args.command == "sign-client":
        directory = output_dir(args.out)
        sign(
            args.csr.resolve(strict=True),
            directory / "client.crt",
            args.state.resolve(strict=True),
            password_file(args.password_file),
            "clientAuth",
            args.days,
        )
    elif args.command == "export-client":
        directory = output_dir(args.out)
        reserve(directory / "client.p12")
        pkcs = [
            "pkcs12",
            "-export",
            "-inkey",
            str(private_file(args.key)),
            "-in",
            str(args.cert),
            "-certfile",
            str(args.ca),
            "-out",
            str(directory / "client.p12"),
            "-passout",
            f"file:{password_file(args.password_file)}",
        ]
        if args.key_password_file:
            pkcs += ["-passin", f"file:{password_file(args.key_password_file)}"]
        else:
            # Prevent an accidentally encrypted input key from prompting.
            pkcs += ["-passin", "pass:"]
        run(*pkcs)
    elif args.command == "bundle":
        directory = output_dir(args.out)
        state = args.state.resolve(strict=True)
        credential = private_file(state / "gateway-secret").read_text().strip()
        if len(credential) < 32 or not re.fullmatch(r"[A-Za-z0-9_-]+", credential):
            raise ValueError("gateway-secret must contain at least 32 URL-safe characters")
        public = run("x509", "-in", str(args.server_cert), "-pubkey", "-noout")
        actual = run("pkey", "-in", str(private_file(args.server_key)), "-passin", "pass:", "-pubout")
        if public != actual:
            raise ValueError("server certificate and private key do not match")
        run("x509", "-in", str(args.server_cert), "-checkend", "0", "-noout")
        run("x509", "-in", str(state / "client-ca.crt"), "-checkend", "0", "-noout")
        for name, source in (
            ("server.crt", args.server_cert),
            ("server.key", args.server_key),
            ("client-ca.crt", state / "client-ca.crt"),
            ("ADMIN_GATEWAY_SECRET", state / "gateway-secret"),
        ):
            reserve(directory / name)
            (directory / name).write_bytes(source.read_bytes())
    elif args.command == "check":
        run("verify", "-CAfile", str(args.ca), "-purpose", args.purpose, str(args.cert))
        run("x509", "-in", str(args.cert), "-checkend", str(args.days * 86400), "-noout")
        print(f"Certificate verified; valid for at least {args.days} more days")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("init", "server", "client-request", "sign-client", "export-client", "bundle", "check"):
        sub = commands.add_parser(command)
        if command != "check":
            sub.add_argument("--out", type=Path, required=True)
        if command in {"server", "sign-client", "bundle"}:
            sub.add_argument("--state", type=Path, required=True)
        if command in {"init", "server", "sign-client", "export-client", "client-request"}:
            sub.add_argument("--password-file", type=Path, required=command != "client-request")
        if command in {"init", "server", "sign-client", "check"}:
            sub.add_argument(
                "--days", type=int, default=3650 if command == "init" else 90 if command != "check" else 30
            )
        if command == "server":
            sub.add_argument("--host", required=True)
        if command == "client-request":
            sub.add_argument("--name", required=True)
        if command == "sign-client":
            sub.add_argument("--csr", type=Path, required=True)
        if command in {"export-client", "check"}:
            sub.add_argument("--cert", type=Path, required=True)
            sub.add_argument("--ca", type=Path, required=True)
        if command == "export-client":
            sub.add_argument("--key", type=Path, required=True)
            sub.add_argument("--key-password-file", type=Path)
        if command == "bundle":
            sub.add_argument("--server-cert", type=Path, required=True)
            sub.add_argument("--server-key", type=Path, required=True)
        if command == "check":
            sub.add_argument("--purpose", choices=["sslclient", "sslserver"], default="sslclient")
    args = parser.parse_args()
    if not shutil.which("openssl"):
        parser.error("Missing required tool: openssl; install it explicitly before running this CLI")
    if hasattr(args, "days") and not 1 <= args.days <= 3650:
        parser.error("--days must be between 1 and 3650")
    os.umask(0o077)
    try:
        execute(args)
    except (OSError, ValueError) as exc:
        print(f"admin-certificates: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
