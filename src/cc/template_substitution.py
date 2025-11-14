import os
import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence, Iterable

# Matches anything inside {...}
TEMPLATE_BRACES = re.compile(r"\{([^{}]+)\}")

# ATTR|ENV|VAR::name[ index? ]  — empty [] means "iterate"
TOKEN = re.compile(
    r"""
    ^(?P<kind>ATTR|ENV|VAR)       # source
    ::
    (?P<name>[A-Za-z_]\w*)        # identifier
    (?:\[
        (?P<index>                # optional index:
            -?\d+                 #   integer
            |'(?:[^']*)'          #   or single-quoted string (can be empty)
        )?
    \])?
    $""",
    re.VERBOSE | re.ASCII,
)


@dataclass(frozen=True)
class Token:
    kind: str  # "ATTR" | "ENV" | "VAR"
    name: str
    has_brackets: bool  # were [] present at all?
    index: int | str | None  # None if no index; int or str if provided


def _parse_token(s: str) -> Token | None:
    m = TOKEN.match(s)
    if not m:
        return None
    kind = m.group("kind")
    name = m.group("name")
    idx = m.group("index")

    has_brackets = "[" in s and s.endswith("]")
    if idx is None:
        return Token(kind, name, has_brackets, None)

    if idx.startswith("'") and idx.endswith("'"):
        return Token(kind, name, True, idx[1:-1])  # strip quotes
    else:
        return Token(kind, name, True, int(idx))


def _resolve_value(tok: Token, attrs: Mapping[str, Any]) -> Any:
    # Dispatch for base value
    if tok.kind == "ATTR":
        base = attrs.get(tok.name, None)
    elif tok.kind == "ENV":
        base = os.environ.get(tok.name, None)
        base = base.split(",")
        if len(base) == 1:
            base = base[0]
    else:
        raise KeyError(f"Unknown source {tok.kind}")

    if base is None:
        raise KeyError(f"No value found for {tok.kind}::{tok.name}")

    # Indexing (if index provided)
    if tok.index is None:
        return base

    if isinstance(tok.index, int):
        if not isinstance(base, Sequence) or isinstance(base, (str, bytes)):
            raise TypeError(f"{tok.kind}::{tok.name} is not indexable by integer")
        try:
            return base[tok.index]
        except IndexError as e:
            raise IndexError(f"Index out of range for {tok.kind}::{tok.name}") from e

    # string key
    if isinstance(base, Mapping):
        if tok.index not in base:
            raise KeyError(f"Key '{tok.index}' not found in {tok.kind}::{tok.name}")
        return base[tok.index]
    # Allow attribute-style lookup for objects
    try:
        return getattr(base, tok.index)
    except AttributeError as e:
        raise TypeError(
            f"{tok.kind}::{tok.name} does not support string indexing '{tok.index}'"
        ) from e


def _substitute_non_iterative(template: str, attrs: Mapping[str, Any]) -> str:
    def repl(m: re.Match[str]) -> str:
        inner = m.group(1)
        tok = _parse_token(inner)
        if not tok:
            return m.group(0)  # leave untouched
        if tok.kind in ["VAR"]:
            return m.group(0)  # ignore known keywords that we want to preserve
        # Only substitute if not an empty bracket iterator
        if tok.has_brackets and tok.index is None:
            # this is the iterable form: leave for the expansion phase
            return m.group(0)
        val = _resolve_value(tok, attrs)
        if isinstance(val, list):
            val = ",".join(val)
        return str(val)

    return TEMPLATE_BRACES.sub(repl, template)


def _expand_iterators_once(
    k: str, template: str, attrs: Mapping[str, Any], allow_expansion: bool
) -> dict[str, str]:
    """
    Left-to-right expansion: find the first {K::name[]} and expand it over its iterable.
    Returns a dict of expanded strings keyed by k-<idx or key>.
    If no iterator is found, returns {k: template}.
    """
    for m in TEMPLATE_BRACES.finditer(template):
        inner = m.group(1)
        tok = _parse_token(inner)
        if not tok or not (tok.has_brackets and tok.index is None):
            continue

        if not allow_expansion:
            raise TypeError(
                f"Expansion not allowed for key: {k}, {tok.kind}::{tok.name}[] attempted"
            )

        base = _resolve_value(tok, attrs)
        if isinstance(base, Mapping):
            items: Iterable[tuple[str, Any]] = base.items()
        elif isinstance(base, Sequence) and not isinstance(base, (str, bytes)):
            items = list(enumerate(base))
        else:
            raise TypeError(f"{tok.kind}::{tok.name} is not iterable for [] expansion")

        out: dict[str, str] = {}
        placeholder = "{" + inner + "}"
        for idx, val in items:
            child_key = f"{k}-{idx}"
            if isinstance(val, list):
                val = ",".join(str(item) for item in val)  # this is stupid syntax
            child_val = template.replace(placeholder, str(val))
            out[child_key] = child_val
        return out

    return {k: template}


def template_substitute(
    name: str, template: str, values: Mapping[str, Any], allow_expansion: bool
) -> dict[str, str]:
    """
    - Resolves {ATTR::foo}, {ENV::BAR}, {ATTR::arr[0]}, {ATTR::obj['key']}
    - Expands iterables for {ATTR::list[]} or {ATTR::dict[]} left-to-right.
    Returns a dict of possibly multiple expanded strings keyed by name and suffixes.
    """
    # 1) resolve everything except [] iterators
    t = _substitute_non_iterative(template, values)

    # 2) iteratively expand one iterator at a time (left-to-right)
    out = {name: t}

    while True:
        next_out: dict[str, str] = {}
        expanded_any = False
        for k, v in out.items():
            expanded = _expand_iterators_once(k, v, values, allow_expansion)
            if len(expanded) == 1 and next(iter(expanded)) == k:
                # nothing expanded
                next_out[k] = v
            else:
                expanded_any = True
                next_out.update(expanded)
        out = next_out
        if not expanded_any:
            break

    return out
