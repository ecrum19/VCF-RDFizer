"""Public, deliberately narrow protocol for data-linking plug-ins.

Resolvers receive deduplicated keys and return objects; the runner owns subject
identity, predicates, serialization, batching, and network policy.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .session import CachedSession


@dataclass(frozen=True, order=True)
class LinkKey:
    token: str = ""
    chrom: str = ""
    start: int = 0
    end: int = 0


@dataclass(frozen=True)
class Link:
    key: LinkKey
    object: str


@dataclass(frozen=True)
class LinkerContext:
    session: "CachedSession"
