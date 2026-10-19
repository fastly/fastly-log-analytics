"""Shared argparse flags for the operator audit/health scripts."""

import argparse


def add_common_args(
    parser: argparse.ArgumentParser,
    *,
    timeout_default: float | None = None,
    timeout_label: str = "Socket",
    json_help: str | None = None,
) -> None:
    """Add ``--timeout`` (when ``timeout_default`` is given) and ``--json`` (when ``json_help`` is given)."""
    if timeout_default is not None:
        parser.add_argument(
            "--timeout",
            type=float,
            default=timeout_default,
            help=f"{timeout_label} timeout in seconds (default: {timeout_default})",
        )
    if json_help is not None:
        parser.add_argument("--json", action="store_true", help=json_help)
