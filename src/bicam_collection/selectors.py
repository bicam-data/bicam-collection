from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class SelectorToken:
    name: str
    iterate: bool = False


def _parse_selector(path: str) -> list[SelectorToken]:
    if not path.startswith("$."):
        raise ValueError(f"Unsupported selector: {path}")
    tokens: list[SelectorToken] = []
    for raw_part in path[2:].split("."):
        if raw_part.endswith("[*]"):
            tokens.append(SelectorToken(raw_part[:-3], iterate=True))
        else:
            tokens.append(SelectorToken(raw_part, iterate=False))
    return tokens


def select(data: Any, path: str) -> list[Any]:
    current: list[Any] = [data]
    for token in _parse_selector(path):
        next_values: list[Any] = []
        for value in current:
            if not isinstance(value, dict):
                continue
            child = value.get(token.name)
            if child is None:
                continue
            if token.iterate:
                if isinstance(child, list):
                    next_values.extend(child)
            else:
                next_values.append(child)
        current = next_values
    return current


def select_one(data: Any, path: str) -> Any | None:
    values = select(data, path)
    return values[0] if values else None

