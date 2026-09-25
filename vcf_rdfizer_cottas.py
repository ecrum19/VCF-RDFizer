"""COTTAS index selection and artifact naming shared by host and container."""

from pathlib import Path
from itertools import permutations


COTTAS_INDEXES = ("spo", "sop", "pso", "pos", "osp", "ops")
COTTAS_QUAD_INDEXES = tuple("".join(order) for order in permutations("spog"))
COTTAS_ALL_INDEXES = COTTAS_INDEXES + COTTAS_QUAD_INDEXES


def parse_cottas_indexes(value: str) -> tuple[str, ...]:
    """Normalize an ordered selection, rejecting typos before conversion."""
    if value.strip().lower() == "all":
        return COTTAS_INDEXES
    if value.strip().lower() == "all-quads":
        return COTTAS_QUAD_INDEXES
    indexes = tuple(dict.fromkeys(part.strip().lower() for part in value.split(",")))
    if any(index not in COTTAS_ALL_INDEXES for index in indexes):
        raise ValueError("COTTAS indexes must be permutations of spo or spog, all, or all-quads")
    return indexes


def require_dataset_indexes(indexes: tuple[str, ...]) -> None:
    """Prevent triple-only orders from discarding a dataset's graph column."""
    if any("g" not in index for index in indexes):
        raise ValueError("RDF datasets require indexes containing g (e.g. spog,gspo or all-quads)")


def cottas_index_paths(path: Path, indexes: tuple[str, ...]) -> dict[str, Path]:
    """Keep the primary filename; suffix additional copies with their order."""
    return {
        index: path if number == 0 else path.with_suffix(f".{index}.cottas")
        for number, index in enumerate(indexes)
    }
