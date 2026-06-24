"""Typed-friendly wrappers around pyiceberg expressions.

pyiceberg 0.11's expression constructors (``LessThan``, ``GreaterThanOrEqual``,
``LessThanOrEqual``) accept positional ``(term_str, literal_value)`` at
runtime, but pyiceberg's type stubs require a keyword-only ``value=...``
argument that isn't really required (the runtime ``__init__`` accepts the
literal positionally). Calling the constructors directly from application
code produces a 5-line mypy error storm per call site.

These wrappers normalize the call signature so call sites can use the
Pythonic positional form without a type-ignore comment per call. If
pyiceberg ever ships stubs that match its runtime, delete this module and
inline the constructor calls.
"""

from __future__ import annotations

from typing import Any

from pyiceberg.expressions import (
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
)


def gt(term: str, value: Any) -> GreaterThan:
    return GreaterThan(term, value)  # type: ignore[misc,call-arg,arg-type]


def gte(term: str, value: Any) -> GreaterThanOrEqual:
    return GreaterThanOrEqual(term, value)  # type: ignore[misc,call-arg,arg-type]


def lt(term: str, value: Any) -> LessThan:
    return LessThan(term, value)  # type: ignore[misc,call-arg,arg-type]


def lte(term: str, value: Any) -> LessThanOrEqual:
    return LessThanOrEqual(term, value)  # type: ignore[misc,call-arg,arg-type]
