#!/usr/bin/env python3
"""Operator-initiated admin PKI lifecycle. Uses cryptography.x509; never modifies trust stores.

All output lives outside this checkout, with directories 0700 and files 0600.
Client CSRs can be signed without moving the client's private key to the CA host.
"""

import argparse
import ipaddress
import os
import re
import secrets
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

REPO = Path(__file__).resolve().parents[1]
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
HOST = re.compile(
    r"(?=.{1,253}$)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
)


def private_file(path: Path) -> Path:
    path = path.resolve(strict=True)
    if not path.is_file() or path.stat().st_mode & 0o077:
        raise ValueError(f"{path}: private input must be a file accessible only to its owner (chmod 600)")
    return path


def password_file(path: Path) -> bytes:
    path = private_file(path)
    lines = path.read_bytes().splitlines()
    if not lines or not lines[0].strip():
        raise ValueError("CA/export password file must not be empty")
    return lines[0]


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


def key(path: Path, password: bytes | None = None) -> ec.EllipticCurvePrivateKey:
    reserve(path)
    priv = ec.generate_private_key(ec.SECP256R1())
    enc = serialization.BestAvailableEncryption(password) if password else serialization.NoEncryption()
    path.write_bytes(priv.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, enc))
    return priv


def _cert_builder(
    subject: x509.Name,
    issuer: x509.Name,
    pub_key: ec.EllipticCurvePublicKey,
    issuer_pub_key: ec.EllipticCurvePublicKey,
    days: int,
    *,
    is_ca: bool = False,
    start: datetime | None = None,
) -> x509.CertificateBuilder:
    now = start or datetime.now(UTC)
    return (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(pub_key)
        .serial_number(secrets.randbits(159) or 1)
        .not_valid_before(now)
        .not_valid_after(now + timedelta(days=days))
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(pub_key), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(issuer_pub_key), critical=False)
        .add_extension(x509.BasicConstraints(ca=is_ca, path_length=0 if is_ca else None), critical=True)
        .add_extension(
            x509.KeyUsage(not is_ca, False, False, False, False, is_ca, is_ca, False, False),
            critical=True,
        )
    )


def ca(directory: Path, name: str, password: bytes, days: int) -> None:
    key_path, cert_path = directory / f"{name}.key", directory / f"{name}.crt"
    priv = key(key_path, password)
    reserve(cert_path)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, f"Log Analytics {name}")])
    cert = _cert_builder(subject, subject, priv.public_key(), priv.public_key(), days, is_ca=True).sign(
        priv, hashes.SHA256()
    )
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def sign(csr: Path, cert: Path, state: Path, password: bytes, purpose: str, days: int, host: str = "") -> None:
    csr_obj = x509.load_pem_x509_csr(csr.read_bytes())
    if not csr_obj.is_signature_valid:
        raise ValueError("CSR signature verification failed")
    ca_name = "client-ca" if purpose == "clientAuth" else "server-ca"
    ca_cert = x509.load_pem_x509_certificate((state / f"{ca_name}.crt").read_bytes())
    if ca_cert.not_valid_after_utc < datetime.now(UTC) + timedelta(days=days):
        raise ValueError("CA certificate expires before requested leaf lifetime")
    ca_key = serialization.load_pem_private_key(private_file(state / f"{ca_name}.key").read_bytes(), password=password)
    if not isinstance(ca_key, ec.EllipticCurvePrivateKey):
        raise ValueError("CA private key must be an EC key")
    csr_pub = csr_obj.public_key()
    if not isinstance(csr_pub, ec.EllipticCurvePublicKey):
        raise ValueError("CSR public key must be an EC key")
    eku = ExtendedKeyUsageOID.CLIENT_AUTH if purpose == "clientAuth" else ExtendedKeyUsageOID.SERVER_AUTH
    builder = _cert_builder(csr_obj.subject, ca_cert.subject, csr_pub, ca_key.public_key(), days).add_extension(
        x509.ExtendedKeyUsage([eku]), critical=False
    )
    if host:
        try:
            san: x509.GeneralName = x509.IPAddress(ipaddress.ip_address(host))
        except ValueError:
            san = x509.DNSName(host)
        builder = builder.add_extension(x509.SubjectAlternativeName([san]), critical=False)
    reserve(cert)
    cert.write_bytes(builder.sign(ca_key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM))


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
        priv = key(key_path, password if args.command == "client-request" else None)
        reserve(csr)
        csr_obj = (
            x509.CertificateSigningRequestBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
            .sign(priv, hashes.SHA256())
        )
        csr.write_bytes(csr_obj.public_bytes(serialization.Encoding.PEM))
        if args.command == "server":
            assert password is not None
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
        key_pw = password_file(args.key_password_file) if args.key_password_file else None
        priv_key = serialization.load_pem_private_key(private_file(args.key).read_bytes(), password=key_pw)
        if not isinstance(priv_key, ec.EllipticCurvePrivateKey):
            raise ValueError("client private key must be an EC key")
        client_cert = x509.load_pem_x509_certificate(args.cert.read_bytes())
        ca_cert = x509.load_pem_x509_certificate(args.ca.read_bytes())
        p12_bytes = pkcs12.serialize_key_and_certificates(
            name=None,
            key=priv_key,
            cert=client_cert,
            cas=[ca_cert],
            encryption_algorithm=serialization.BestAvailableEncryption(password_file(args.password_file)),
        )
        reserve(directory / "client.p12")
        (directory / "client.p12").write_bytes(p12_bytes)
    elif args.command == "bundle":
        directory = output_dir(args.out)
        state = args.state.resolve(strict=True)
        credential = private_file(state / "gateway-secret").read_text().strip()
        if len(credential) < 32 or not re.fullmatch(r"[A-Za-z0-9_-]+", credential):
            raise ValueError("gateway-secret must contain at least 32 URL-safe characters")
        server_cert = x509.load_pem_x509_certificate(args.server_cert.read_bytes())
        server_key = serialization.load_pem_private_key(private_file(args.server_key).read_bytes(), password=None)
        if server_cert.public_key() != server_key.public_key():
            raise ValueError("server certificate and private key do not match")
        client_ca = x509.load_pem_x509_certificate((state / "client-ca.crt").read_bytes())
        now = datetime.now(UTC)
        if server_cert.not_valid_after_utc <= now or client_ca.not_valid_after_utc <= now:
            raise ValueError("bundle certificate has expired")
        for name, source in (
            ("server.crt", args.server_cert),
            ("server.key", args.server_key),
            ("client-ca.crt", state / "client-ca.crt"),
            ("ADMIN_GATEWAY_SECRET", state / "gateway-secret"),
        ):
            reserve(directory / name)
            (directory / name).write_bytes(source.read_bytes())
    elif args.command == "check":
        ca_cert = x509.load_pem_x509_certificate(args.ca.read_bytes())
        cert = x509.load_pem_x509_certificate(args.cert.read_bytes())
        try:
            cert.verify_directly_issued_by(ca_cert)
            bc = cert.extensions.get_extension_for_class(x509.BasicConstraints).value
            eku = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
        except Exception as exc:
            raise ValueError(f"Certificate verification failed: {exc}") from exc
        expected_oid = (
            ExtendedKeyUsageOID.CLIENT_AUTH if args.purpose == "sslclient" else ExtendedKeyUsageOID.SERVER_AUTH
        )
        if bc.ca or expected_oid not in eku:
            raise ValueError("Certificate verification failed: invalid constraints or extended key usage")
        now = datetime.now(UTC)
        if cert.not_valid_before_utc > now or cert.not_valid_after_utc < now + timedelta(days=args.days):
            raise ValueError("Certificate verification failed: certificate expired or expires too soon")
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
