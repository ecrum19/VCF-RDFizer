"""COTTAS index selection and artifact naming shared by host and container."""

from pathlib import Path


COTTAS_INDEXES = ("spo", "sop", "pso", "pos", "osp", "ops")


def parse_cottas_indexes(value: str) -> tuple[str, ...]:
    """Normalize an ordered selection, rejecting typos before conversion."""
    if value.strip().lower() == "all":
        return COTTAS_INDEXES
    indexes = tuple(dict.fromkeys(part.strip().lower() for part in value.split(",")))
    if any(index not in COTTAS_INDEXES for index in indexes):
        raise ValueError("COTTAS indexes must be spo,sop,pso,pos,osp,ops or all")
    return indexes


def cottas_index_paths(path: Path, indexes: tuple[str, ...]) -> dict[str, Path]:
    """Keep the primary filename; suffix additional copies with their order."""
    return {
        index: path if number == 0 else path.with_suffix(f".{index}.cottas")
        for number, index in enumerate(indexes)
    }
