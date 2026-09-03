#!/usr/bin/env python3
"""Run the partitioned HDT/COTTAS pipeline inside one Docker container.

The wrapper deliberately keeps this runner independent from ``vcf_rdfizer.py``.
The latter is the host-side CLI, while this file is copied into the Docker
image and owns the ephemeral workspace mounted at ``/work``.  Keeping chunk
and merge files here prevents large intermediate artifacts, including COTTAS's
DuckDB scratch database, from consuming the user's output filesystem.
"""

from __future__ import annotations

import argparse
import errno
import gzip
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


HDT_METHODS = {"hdt", "hdt_gzip", "hdt_brotli"}
COTTAS_METHODS = {"cottas", "cottas_gzip", "cottas_brotli"}
STDERR_TAIL_BYTES = 16 * 1024
DEFAULT_COTTAS_MERGE_MEMORY_LIMIT = "4G"
DEFAULT_COTTAS_MERGE_THREADS = "1"
PROGRESS_HEARTBEAT_BYTES = 64 * 1024 * 1024


def prepare_progress_path(path: Path | None) -> None:
    """Best-effort setup for the optional host-mounted progress sidecar."""
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass


def emit_progress(
    path: Path | None,
    stage: str,
    phase: str,
    *,
    completed: int | float | None = None,
    total: int | float | None = None,
    unit: str | None = None,
    detail: str | None = None,
) -> None:
    """Append one small JSONL event; progress must never break conversion."""
    if path is None:
        return
    payload = {"stage": stage, "phase": phase}
    for key, value in (
        ("completed", completed),
        ("total", total),
        ("unit", unit),
        ("detail", detail),
    ):
        if value is not None:
            payload[key] = value
    try:
        with path.open("a", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"))
            handle.write("\n")
    except OSError:
        pass


def progress_descriptor(name: str) -> tuple[str, str, int | None]:
    """Map internal stage names to a small set of terminal progress tasks."""
    for prefix, stage in (
        ("hdt-build-", "hdt-chunks"),
        ("cottas-build-", "cottas-chunks"),
    ):
        if name.startswith(prefix):
            try:
                return stage, "chunks", int(name[len(prefix) :])
            except ValueError:
                break
    if name.startswith("hdt-merge-"):
        return "hdt-merge", "stage", None
    if name.startswith("cottas-merge"):
        return "cottas-merge", "stage", None
    if name.startswith("hdt-"):
        return "hdt", "stage", None
    if name.startswith("cottas-"):
        return "cottas", "stage", None
    return name, "stage", None


def is_triple_line(line: bytes) -> bool:
    """Identify an N-Triples record without parsing or copying its terms."""
    stripped = line.strip()
    return bool(stripped) and not stripped.startswith(b"#") and stripped.endswith(b".")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="VCF-RDFizer partitioned compression runner")
    parser.add_argument("--source", required=True, help="plain or gzip-compressed N-Triples input")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--output-name", required=True)
    parser.add_argument("--methods", required=True, help="comma-separated internal method names")
    parser.add_argument("--target-chunk-bytes", required=True, type=int)
    parser.add_argument("--min-chunk-bytes", required=True, type=int)
    parser.add_argument("--max-chunk-bytes", required=True, type=int)
    parser.add_argument("--expected-triples", type=int)
    parser.add_argument("--result-path", required=True)
    parser.add_argument(
        "--progress-path",
        help="optional JSONL sidecar for host-side terminal progress",
    )
    parser.add_argument(
        "--allow-index-failures",
        action="store_true",
        help="continue with other representations when HDT/COTTAS indexing fails",
    )
    return parser.parse_args()


def iter_rdf_lines(path: Path):
    """Read plain or gzip RDF incrementally without materializing the source."""
    opener = gzip.open if path.name.endswith(".gz") else Path.open
    with opener(path, "rb") as handle:
        yield from handle


def stream_chunks(
    source: Path,
    chunk_dir: Path,
    *,
    target_bytes: int,
    min_bytes: int,
    max_bytes: int,
    progress_path: Path | None = None,
    progress_total: int | None = None,
) -> tuple[object, dict]:
    """Yield complete-record chunks while building a mutable chunk plan.

    The old implementation returned only after expanding the *entire* source
    into raw ``.nt`` chunks.  A space-optimized aggregate is normally gzip
    compressed, so that made the Docker workspace hold another full,
    uncompressed copy of the aggregate before either converter could reclaim a
    single byte.  The caller now converts and unlinks each yielded chunk before
    asking for the next one.
    """
    if target_bytes <= 0 or min_bytes <= 0 or max_bytes <= 0:
        raise ValueError("RDF chunk sizes must be positive")
    if min_bytes > target_bytes or target_bytes > max_bytes:
        raise ValueError("RDF chunk sizes must satisfy min <= target <= max")

    chunk_dir.mkdir(parents=True, exist_ok=True)
    prepare_progress_path(progress_path)
    plan = {
        "source_file_count": 1,
        "source_paths": [str(source)],
        "chunk_count": 0,
        "chunk_input_bytes": 0,
        "record_count": 0,
        "target_chunk_bytes": target_bytes,
        "min_chunk_bytes": min_bytes,
        "max_chunk_bytes": max_bytes,
        "chunks": [],
    }

    def generate():
        handle = None
        chunk_path = None
        chunk_size = 0
        chunk_start_offset = 0
        chunk_start_record = 0
        logical_offset = 0
        record_count = 0
        chunk_index = 0
        last_progress_offset = 0

        def open_chunk():
            nonlocal handle, chunk_path, chunk_size, chunk_start_offset, chunk_start_record, chunk_index
            chunk_path = chunk_dir / f"chunk-{chunk_index:05d}.nt"
            chunk_index += 1
            handle = chunk_path.open("wb")
            chunk_size = 0
            chunk_start_offset = logical_offset
            chunk_start_record = record_count

        def close_chunk():
            nonlocal handle, chunk_path, chunk_size
            if handle is None or chunk_path is None:
                return None
            handle.close()
            metadata = {
                "chunk_id": len(plan["chunks"]),
                "path": str(chunk_path),
                "start_record": chunk_start_record,
                "end_record": record_count,
                "start_uncompressed_byte": chunk_start_offset,
                "end_uncompressed_byte": logical_offset,
                "record_count": record_count - chunk_start_record,
                "payload_bytes": chunk_size,
            }
            plan["chunks"].append(metadata)
            plan["chunk_count"] = len(plan["chunks"])
            emit_progress(
                progress_path,
                "rdf-scan",
                "chunk",
                completed=record_count,
                total=progress_total,
                unit="triples",
                detail=(
                    f"{plan['chunk_count']:,} chunks · "
                    f"{logical_offset:,} bytes read"
                ),
            )
            completed_path = chunk_path
            handle = None
            chunk_path = None
            chunk_size = 0
            return completed_path, metadata

        try:
            for line in iter_rdf_lines(source):
                if not line.endswith(b"\n"):
                    raise ValueError(f"RDF source contains a non-line-terminated record: {source}")
                line_size = len(line)
                if handle is None:
                    open_chunk()
                elif chunk_size > 0 and (
                    (chunk_size >= target_bytes and chunk_size >= min_bytes)
                    or chunk_size + line_size > max_bytes
                ):
                    completed_chunk = close_chunk()
                    if completed_chunk is not None:
                        yield completed_chunk
                    open_chunk()

                handle.write(line)
                chunk_size += line_size
                logical_offset += line_size
                if is_triple_line(line):
                    record_count += 1
                plan["chunk_input_bytes"] = logical_offset
                plan["record_count"] = record_count
                if (
                    logical_offset - last_progress_offset >= PROGRESS_HEARTBEAT_BYTES
                ):
                    emit_progress(
                        progress_path,
                        "rdf-scan",
                        "heartbeat",
                        completed=record_count,
                        total=progress_total,
                        unit="triples",
                        detail=f"{logical_offset:,} bytes read",
                    )
                    last_progress_offset = logical_offset

            completed_chunk = close_chunk()
            if completed_chunk is not None:
                yield completed_chunk
        finally:
            # A write/decompression error can leave one unyielded, partial
            # chunk. It has no consumer, so remove it before the volume is
            # released.
            if handle is not None:
                try:
                    handle.close()
                finally:
                    if chunk_path is not None:
                        chunk_path.unlink(missing_ok=True)

    return generate(), plan


def plan_chunks(
    source: Path,
    chunk_dir: Path,
    *,
    target_bytes: int,
    min_bytes: int,
    max_bytes: int,
) -> tuple[list[Path], dict]:
    """Materialize chunks for callers that explicitly need every path.

    The production compressor uses :func:`stream_chunks` so it never retains a
    full uncompressed copy of a gzip aggregate. This compatibility helper is
    deliberately kept for diagnostics and standalone callers.
    """
    stream, plan = stream_chunks(
        source,
        chunk_dir,
        target_bytes=target_bytes,
        min_bytes=min_bytes,
        max_bytes=max_bytes,
    )
    chunk_paths = [path for path, _metadata in stream]
    return chunk_paths, plan


def resolve_executable(candidates: tuple[str, ...], label: str) -> str:
    for candidate in candidates:
        resolved = shutil.which(candidate) if "/" not in candidate else candidate
        if resolved and Path(resolved).is_file() and os.access(resolved, os.X_OK):
            return resolved
    raise RuntimeError(f"Missing {label} in container")


def find_hdt_index_sidecar(hdt_path: Path) -> Path | None:
    """Locate the non-empty canonical HDT versioned index sidecar."""
    for candidate in sorted(hdt_path.parent.glob(f"{hdt_path.name}.index.*")):
        if candidate.is_file() and candidate.stat().st_size > 0:
            return candidate
    return None


def hdtc_merge_temp_dir(work_dir: Path, merged_path: Path) -> Path:
    """Return the isolated disk workspace for one hdtc merge stage."""
    return work_dir / f".{merged_path.stem}.hdtc-work"


def hdtc_merge_command(
    hdtc_bin: str,
    left: Path,
    right: Path,
    merged: Path,
    *,
    work_dir: Path,
    memory_limit: str,
) -> list[str]:
    """Build a bounded-memory native merge command for two HDT chunks.

    ``hdtc create`` accepts existing ``.hdt`` inputs, so it performs the same
    logical merge as hdt-java's ``hdtCat`` without constructing Java HashMaps.
    Its temporary files are kept in a per-stage directory so they can be
    removed immediately after each pairwise merge.
    """
    return [
        hdtc_bin,
        "--quiet",
        "create",
        str(left),
        str(right),
        "--output",
        str(merged),
        "--memory-limit",
        memory_limit,
        "--temp-dir",
        str(hdtc_merge_temp_dir(work_dir, merged)),
    ]


def cottas_merge_many_command(
    python_bin: str,
    inputs: list[Path],
    merged: Path,
) -> list[str]:
    """Build one disk-backed, multi-input COTTAS merge command.

    The adapter performs the global distinct/order operation through a
    dedicated DuckDB database with a bounded memory budget and `/work` spill
    directory. Unlike ``pycottas.cat``, it never routes the large merge through
    DuckDB's process-global in-memory connection.
    """
    return [
        python_bin,
        "/opt/vcf-rdfizer/cottas_tool.py",
        "merge-many",
        "--input-cottas-files",
        *(str(path) for path in inputs),
        "--output-cottas-file",
        str(merged),
        "--index",
        "spo",
    ]


def cottas_merge_command(
    python_bin: str,
    left: Path,
    right: Path,
    merged: Path,
) -> list[str]:
    """Build a compatible two-input disk-backed COTTAS merge command.

    The normal partitioned workflow uses ``merge-many`` because its DuckDB
    implementation is spill-capable. This command remains available for
    explicit callers that need a two-input merge.
    """
    return [
        python_bin,
        "/opt/vcf-rdfizer/cottas_tool.py",
        "merge",
        str(left),
        str(right),
        str(merged),
        "spo",
    ]


def parse_time_log(path: Path) -> dict:
    if not path.exists():
        return {"user_seconds": None, "sys_seconds": None, "max_rss_kb": None}
    text = path.read_text(encoding="utf-8", errors="replace")

    def number(pattern: str, integer: bool = False):
        import re

        match = re.search(pattern, text, flags=re.MULTILINE)
        if not match:
            return None
        try:
            return int(float(match.group(1))) if integer else float(match.group(1))
        except ValueError:
            return None

    return {
        "user_seconds": number(r"User time \(seconds\):\s*([0-9]+(?:\.[0-9]+)?)"),
        "sys_seconds": number(r"System time \(seconds\):\s*([0-9]+(?:\.[0-9]+)?)"),
        "max_rss_kb": number(r"Maximum resident set size.*:\s*([0-9]+)", integer=True),
    }


class StageRunner:
    """Execute stages and accumulate both detailed and method-level metrics."""

    def __init__(self, work_dir: Path, progress_path: Path | None = None):
        self.work_dir = work_dir
        self.progress_path = progress_path
        prepare_progress_path(progress_path)
        self.stages: list[dict] = []

    def run(
        self,
        name: str,
        command: list[str],
        output_path: Path | None = None,
        stdout_path: Path | None = None,
    ) -> dict:
        time_path = self.work_dir / f".{name}.time"
        stderr_path = self.work_dir / f".{name}.stderr"
        progress_stage, progress_unit, progress_ordinal = progress_descriptor(name)
        emit_progress(
            self.progress_path,
            progress_stage,
            "started",
            completed=progress_ordinal,
            unit=progress_unit,
            detail=name,
        )
        if time_path.exists():
            time_path.unlink()
        if stderr_path.exists():
            stderr_path.unlink()
        started = time.perf_counter()
        workspace_free_before = None
        workspace_total = None
        try:
            workspace_usage = shutil.disk_usage(self.work_dir)
            workspace_free_before = workspace_usage.free
            workspace_total = workspace_usage.total
        except OSError:
            pass
        time_bin = "/usr/bin/time" if Path("/usr/bin/time").exists() else None
        if time_bin:
            # macOS ships a BSD ``time`` at this path; only GNU time supports
            # the ``-v`` metrics format used by ``parse_time_log``.
            probe = subprocess.run(
                [time_bin, "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            if probe.returncode != 0 or b"GNU time" not in probe.stdout + probe.stderr:
                time_bin = None
        if time_bin:
            timed_command = [time_bin, "-v", "-o", str(time_path), *command]
        else:
            timed_command = command
        stdout_handle = None
        stderr_handle = None
        try:
            if stdout_path is not None:
                stdout_path.parent.mkdir(parents=True, exist_ok=True)
                stdout_handle = stdout_path.open("wb")
            stderr_handle = stderr_path.open("wb")
            completed = subprocess.run(
                timed_command,
                stdout=stdout_handle,
                stderr=stderr_handle,
                check=False,
            )
        finally:
            if stdout_handle is not None:
                stdout_handle.close()
            if stderr_handle is not None:
                stderr_handle.close()
        emit_progress(
            self.progress_path,
            progress_stage,
            "complete" if completed.returncode == 0 else "failed",
            completed=(
                progress_ordinal + 1
                if progress_ordinal is not None and completed.returncode == 0
                else progress_ordinal
            ),
            unit=progress_unit,
            detail=name,
        )
        stderr_tail = ""
        if stderr_path.exists():
            try:
                with stderr_path.open("rb") as handle:
                    handle.seek(0, os.SEEK_END)
                    handle.seek(max(0, handle.tell() - STDERR_TAIL_BYTES))
                    stderr_tail = handle.read().decode("utf-8", errors="replace").strip()
            finally:
                stderr_path.unlink(missing_ok=True)
        result = {
            "stage_name": name,
            "exit_code": completed.returncode,
            "wall_seconds": time.perf_counter() - started,
            "output_path": "" if output_path is None else str(output_path),
            "output_size_bytes": output_path.stat().st_size
            if output_path is not None and output_path.is_file()
            else 0,
        }
        try:
            workspace_usage = shutil.disk_usage(self.work_dir)
            result["workspace_free_bytes_after"] = workspace_usage.free
            result["workspace_total_bytes"] = workspace_usage.total
        except OSError:
            pass
        if workspace_free_before is not None:
            result["workspace_free_bytes_before"] = workspace_free_before
        if workspace_total is not None:
            result.setdefault("workspace_total_bytes", workspace_total)
        if stderr_tail:
            result["stderr_tail"] = stderr_tail
        result.update(parse_time_log(time_path))
        self.stages.append({"name": name, **result})
        time_path.unlink(missing_ok=True)
        return result


def add_totals(total: dict, stage: dict):
    total["exit_code"] = max(total["exit_code"], int(stage.get("exit_code") or 0))
    total["wall_seconds"] += float(stage.get("wall_seconds") or 0.0)
    if stage.get("user_seconds") is not None:
        total["user_seconds"] += float(stage["user_seconds"])
        total["has_user"] = True
    if stage.get("sys_seconds") is not None:
        total["sys_seconds"] += float(stage["sys_seconds"])
        total["has_sys"] = True
    if stage.get("max_rss_kb") is not None:
        total["max_rss_kb"] = max(total["max_rss_kb"], int(stage["max_rss_kb"]))
        total["has_rss"] = True


def finalize_totals(total: dict) -> dict:
    return {
        "exit_code": total["exit_code"],
        "wall_seconds": total["wall_seconds"],
        "user_seconds": total["user_seconds"] if total["has_user"] else None,
        "sys_seconds": total["sys_seconds"] if total["has_sys"] else None,
        "max_rss_kb": total["max_rss_kb"] if total["has_rss"] else None,
    }


def failure_message(stage_result: dict | None, fallback: str) -> str:
    """Turn a failed subprocess result into an actionable pipeline error.

    ``StageRunner`` intentionally stores a bounded stderr tail in the result
    payload.  The outer CLI has no access to the ephemeral Docker volume once
    the container exits, so omitting that text here turns a concrete DuckDB
    error (for example an unwritable spill directory) into a useless
    ``exit_code=1`` report.
    """
    if not stage_result:
        return fallback
    exit_code = stage_result.get("exit_code")
    diagnostics = []
    if exit_code is not None:
        diagnostics.append(f"exit_code={exit_code}")
        try:
            numeric_exit_code = int(exit_code)
        except (TypeError, ValueError):
            numeric_exit_code = None
        if numeric_exit_code == -9:
            diagnostics.append(
                "the process was killed by SIGKILL (usually the kernel/Docker OOM killer)"
            )
        elif numeric_exit_code == 137:
            diagnostics.append("the process was killed (often Docker memory/OOM pressure)")
        elif numeric_exit_code == 143:
            diagnostics.append("the process was terminated (SIGTERM)")
    stderr_tail = " ".join(str(stage_result.get("stderr_tail") or "").split())
    if stderr_tail:
        # Preserve the end of a traceback, which contains the concrete
        # exception, without making top-level CLI output unbounded.
        diagnostics.append(f"stderr={stderr_tail[-2048:]}")
    return f"{fallback} ({'; '.join(diagnostics)})" if diagnostics else fallback


def merge_pairwise(
    paths: list[Path],
    *,
    prefix: str,
    runner: StageRunner,
    merge_command,
    total: dict,
    cleanup_merged_workspace=None,
) -> tuple[Path | None, int]:
    rounds = 0
    current = list(paths)
    while len(current) > 1:
        rounds += 1
        next_paths: list[Path] = []
        for pair_index in range(0, len(current), 2):
            left = current[pair_index]
            if pair_index + 1 >= len(current):
                next_paths.append(left)
                continue
            right = current[pair_index + 1]
            merged = left.parent / f"{prefix}-merge-r{rounds:02d}-{pair_index // 2:05d}{left.suffix}"
            try:
                stage = runner.run(
                    f"{prefix}-merge-r{rounds:02d}-{pair_index // 2:05d}",
                    merge_command(left, right, merged),
                    merged,
                )
            finally:
                if cleanup_merged_workspace is not None:
                    cleanup_merged_workspace(merged)
            add_totals(total, stage)
            if stage["exit_code"] != 0:
                return None, rounds
            left.unlink(missing_ok=True)
            right.unlink(missing_ok=True)
            next_paths.append(merged)
        current = next_paths
    return (current[0] if current else None), rounds


def main() -> int:
    args = parse_args()
    source = Path(args.source)
    output_dir = Path(args.output_dir)
    result_path = Path(args.result_path)
    methods = [method.strip() for method in args.methods.split(",") if method.strip()]
    work_dir = Path("/work")
    chunk_dir = work_dir / "rdf_chunks"
    progress_path = Path(args.progress_path) if args.progress_path else None
    runner = StageRunner(work_dir, progress_path=progress_path)
    results: dict[str, dict] = {}
    index_warnings: list[dict] = []

    hdt_total = {"exit_code": 0, "wall_seconds": 0.0, "user_seconds": 0.0, "sys_seconds": 0.0, "max_rss_kb": 0, "has_user": False, "has_sys": False, "has_rss": False}
    cottas_total = {"exit_code": 0, "wall_seconds": 0.0, "user_seconds": 0.0, "sys_seconds": 0.0, "max_rss_kb": 0, "has_user": False, "has_sys": False, "has_rss": False}
    output_hdt = output_dir / f"{args.output_name}.hdt"
    output_cottas = output_dir / f"{args.output_name}.cottas"
    cottas_failed = False
    cottas_warning = None

    def write_result(payload: dict):
        """Best-effort result handoff that never hides the original failure."""
        try:
            result_path.parent.mkdir(parents=True, exist_ok=True)
            result_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        except OSError as write_error:
            print(
                f"Warning: unable to write partitioned-compression result to {result_path}: {write_error}",
                file=sys.stderr,
            )

    def record_index_warning(
        index_format: str,
        stage: str,
        artifact: Path,
        message: str,
        stage_result: dict | None = None,
    ) -> dict:
        warning = {
            "format": index_format,
            "stage": stage,
            "status": "index_unavailable",
            "artifact_path": str(artifact),
            "message": " ".join(str(message).split()),
        }
        if stage_result:
            warning["stage_name"] = stage_result.get("stage_name", stage)
            warning["exit_code"] = stage_result.get("exit_code")
            for key in (
                "workspace_free_bytes_before",
                "workspace_free_bytes_after",
                "workspace_total_bytes",
                "max_rss_kb",
                "cottas_merge_memory_limit",
                "cottas_merge_threads",
            ):
                if key in stage_result:
                    warning[key] = stage_result[key]
            stderr_tail = str(stage_result.get("stderr_tail") or "").strip()
            if stderr_tail:
                warning["stderr_tail"] = stderr_tail
        index_warnings.append(warning)
        print(
            f"Warning: {index_format.upper()} index generation failed for '{artifact}'; "
            f"continuing with the remaining pipeline. {warning['message']}",
            file=sys.stderr,
        )
        return warning

    def skipped_cottas_result(method: str) -> dict:
        artifact = {
            "cottas": output_cottas,
            "cottas_gzip": output_dir / f"{args.output_name}.cottas.gz",
            "cottas_brotli": output_dir / f"{args.output_name}.cottas.br",
        }[method]
        return {
            "exit_code": 1,
            "wall_seconds": 0.0,
            "user_seconds": 0.0,
            "sys_seconds": 0.0,
            "max_rss_kb": 0,
            "output_path": str(artifact),
            "output_size_bytes": 0,
            "source": "index_unavailable",
            "details": {"index_status": "failed", "index_warning": cottas_warning},
        }

    try:
        if not source.is_file():
            raise FileNotFoundError(f"RDF source not found: {source}")
        if not methods or not all(method in HDT_METHODS | COTTAS_METHODS for method in methods):
            raise ValueError(f"Unsupported partitioned method list: {methods}")

        hdt_paths: list[Path] = []
        cottas_paths: list[Path] = []

        def cleanup_cottas_intermediates() -> None:
            """Release COTTAS chunks/merge outputs after a failed attempt."""
            for path in list(cottas_paths):
                path.unlink(missing_ok=True)
            cottas_paths.clear()
            for path in work_dir.glob("cottas-merge-*.cottas"):
                path.unlink(missing_ok=True)

        needs_hdt = any(method in HDT_METHODS for method in methods)
        hdt_bin = (
            resolve_executable(
                (
                    os.environ.get("RDF2HDT_BIN", ""),
                    "/usr/local/bin/rdf2hdt",
                    "/opt/hdt-cpp/bin/rdf2hdt",
                ),
                "rdf2hdt",
            )
            if needs_hdt
            else None
        )
        hdtc_bin = (
            resolve_executable(
                (
                    os.environ.get("HDTC_BIN", ""),
                    "/usr/local/bin/hdtc",
                    "hdtc",
                ),
                "hdtc HDT merger",
            )
            if needs_hdt
            else None
        )
        hdt_merge_memory_limit = os.environ.get("HDT_MERGE_MEMORY_LIMIT", "512M").strip()
        if needs_hdt and not hdt_merge_memory_limit:
            raise ValueError("HDT_MERGE_MEMORY_LIMIT must be a non-empty hdtc memory size")
        cottas_python = os.environ.get("COTTAS_PYTHON_BIN") or shutil.which("python3")
        if any(method in COTTAS_METHODS for method in methods) and not cottas_python:
            raise RuntimeError("Missing Python runtime for COTTAS")
        emit_progress(
            progress_path,
            "rdf-scan",
            "started",
            completed=0,
            total=args.expected_triples,
            unit="triples",
        )
        chunk_stream, plan = stream_chunks(
            source,
            chunk_dir,
            target_bytes=args.target_chunk_bytes,
            min_bytes=args.min_chunk_bytes,
            max_bytes=args.max_chunk_bytes,
            progress_path=progress_path,
            progress_total=args.expected_triples,
        )

        def validate_artifact(
            *,
            name: str,
            artifact: Path,
            artifact_format: str,
            python_bin: str,
            skip_index_check: bool = False,
        ) -> dict:
            """Decode/count one final artifact and return its validation report."""
            validation_path = work_dir / f".{name}.validation.json"
            command = [
                python_bin,
                "/opt/vcf-rdfizer/validate_compression.py",
                "--source",
                str(source),
                "--artifact",
                str(artifact),
                "--format",
                artifact_format,
                "--source-triples",
                str(source_triples),
                "--result-path",
                str(validation_path),
            ]
            if args.expected_triples is not None:
                command.extend(["--expected-triples", str(args.expected_triples)])
            if skip_index_check:
                command.append("--skip-index-check")
            stage = runner.run(name, command, validation_path)
            try:
                report = json.loads(validation_path.read_text(encoding="utf-8"))
            except (FileNotFoundError, json.JSONDecodeError) as exc:
                report = {
                    "valid": False,
                    "count_match": False,
                    "error": f"validator did not produce a valid report: {exc}",
                }
            finally:
                validation_path.unlink(missing_ok=True)
            report.setdefault(
                "timing",
                {
                    "wall_seconds": stage.get("wall_seconds"),
                    "user_seconds": stage.get("user_seconds"),
                    "sys_seconds": stage.get("sys_seconds"),
                    "max_rss_kb": stage.get("max_rss_kb"),
                },
            )
            if (
                stage["exit_code"] != 0
                or not report.get("valid")
                or not report.get("count_match")
            ):
                raise RuntimeError(
                    f"{artifact_format.upper()} validation failed for {artifact.name}: "
                    f"{report.get('error', 'decoded triple count mismatch')}"
                )
            return report

        # Convert each uncompressed chunk before requesting the next one from
        # the gzip reader. This bounds raw-RDF workspace use to one chunk,
        # rather than the entire decompressed aggregate.
        for index, (chunk, _chunk_metadata) in enumerate(chunk_stream):
            try:
                if needs_hdt:
                    chunk_hdt = work_dir / f"chunk-{index:05d}.hdt"
                    stage = runner.run(
                        f"hdt-build-{index:05d}",
                        [hdt_bin, str(chunk), str(chunk_hdt)],
                        chunk_hdt,
                    )
                    add_totals(hdt_total, stage)
                    if stage["exit_code"] != 0:
                        raise RuntimeError("partitioned HDT chunk conversion failed")
                    hdt_paths.append(chunk_hdt)
                if any(method in COTTAS_METHODS for method in methods) and not cottas_failed:
                    chunk_cottas = work_dir / f"chunk-{index:05d}.cottas"
                    stage = runner.run(
                        f"cottas-build-{index:05d}",
                        [cottas_python, "/opt/vcf-rdfizer/cottas_tool.py", "convert", str(chunk), str(chunk_cottas), "spo"],
                        chunk_cottas,
                    )
                    add_totals(cottas_total, stage)
                    if stage["exit_code"] != 0:
                        if not args.allow_index_failures:
                            raise RuntimeError("partitioned COTTAS chunk conversion failed")
                        cottas_failed = True
                        cottas_total["exit_code"] = 0
                        cottas_warning = record_index_warning(
                            "cottas",
                            "cottas-index",
                            output_cottas,
                            failure_message(
                                stage,
                                "partitioned COTTAS conversion/index creation failed for a chunk",
                            ),
                            stage_result=stage,
                        )
                        chunk_cottas.unlink(missing_ok=True)
                        cleanup_cottas_intermediates()
                    else:
                        cottas_paths.append(chunk_cottas)
            finally:
                chunk.unlink(missing_ok=True)

        emit_progress(
            progress_path,
            "rdf-scan",
            "complete",
            completed=plan["record_count"],
            total=args.expected_triples,
            unit="triples",
            detail=f"{plan['chunk_count']:,} chunks",
        )
        if needs_hdt:
            emit_progress(
                progress_path,
                "hdt-chunks",
                "complete",
                completed=len(hdt_paths),
                total=plan["chunk_count"],
                unit="chunks",
            )
        if any(method in COTTAS_METHODS for method in methods):
            emit_progress(
                progress_path,
                "cottas-chunks",
                "failed" if cottas_failed else "complete",
                completed=len(cottas_paths),
                total=plan["chunk_count"],
                unit="chunks",
            )
        if not plan["chunks"]:
            raise ValueError("RDF source contains no complete records")
        source_triples = int(plan["record_count"])
        if args.expected_triples is not None and source_triples != args.expected_triples:
            raise ValueError(
                "source triple count does not match the upstream conversion count: "
                f"source={source_triples}, expected={args.expected_triples}"
            )

        if hdt_paths:
            final_hdt, hdt_rounds = merge_pairwise(
                hdt_paths,
                prefix="hdt",
                runner=runner,
                merge_command=lambda left, right, merged: hdtc_merge_command(
                    hdtc_bin,
                    left,
                    right,
                    merged,
                    work_dir=work_dir,
                    memory_limit=hdt_merge_memory_limit,
                ),
                total=hdt_total,
                cleanup_merged_workspace=lambda merged: shutil.rmtree(
                    hdtc_merge_temp_dir(work_dir, merged), ignore_errors=True
                ),
            )
            if final_hdt is None:
                raise RuntimeError("hdtc HDT merge failed")
            shutil.copyfile(final_hdt, output_hdt)
            final_hdt.unlink(missing_ok=True)
            index_stage = runner.run(
                "hdt-index",
                ["/opt/vcf-rdfizer/ensure_hdt_index.sh", str(output_hdt)],
            )
            add_totals(hdt_total, index_stage)
            output_index = find_hdt_index_sidecar(output_hdt)
            if output_index is not None:
                # The canonical filename is versioned, so record the actual
                # sidecar after the helper completes instead of assuming .index.
                index_stage["output_path"] = str(output_index)
                index_stage["output_size_bytes"] = output_index.stat().st_size
                runner.stages[-1].update(index_stage)
            if index_stage["exit_code"] != 0 or output_index is None:
                if not args.allow_index_failures:
                    raise RuntimeError("final HDT index initialization failed")
                hdt_total["exit_code"] = 0
                hdt_index_warning = record_index_warning(
                    "hdt",
                    "hdt-index",
                    output_hdt,
                    "the HDT artifact remains available, but its query index was not created",
                )
            else:
                hdt_index_warning = None
            hdt_validation = validate_artifact(
                name="hdt-validate",
                artifact=output_hdt,
                artifact_format="hdt",
                python_bin=sys.executable,
                skip_index_check=True,
            )
            results["hdt"] = {
                **finalize_totals(hdt_total),
                "output_path": str(output_hdt),
                "output_size_bytes": output_hdt.stat().st_size,
                "source": "partitioned_generated",
                "details": {
                    **plan,
                    "merge_rounds": hdt_rounds,
                    "index_path": str(output_index),
                    "index_size_bytes": output_index.stat().st_size if output_index else 0,
                    "index_status": "failed" if hdt_index_warning else "ready",
                    "index_warning": hdt_index_warning,
                    "validation": hdt_validation,
                },
            }

        if cottas_paths and not cottas_failed:
            # pycottas.cat performs its global DISTINCT/ORDER BY through an
            # unbounded process-global in-memory DuckDB connection.  That is
            # what made both the one-shot and the final pairwise COTTAS merge
            # susceptible to SIGKILL on large condensed cohorts. The adapter's
            # merge-many command instead opens a dedicated disk-backed DuckDB
            # database with a bounded memory limit and /work spill files, so a
            # single global indexed merge is safe and avoids duplicate work.
            cottas_stage = None
            cottas_rounds = 0
            if len(cottas_paths) == 1:
                final_cottas = cottas_paths[0]
            else:
                cottas_merged_path = work_dir / "cottas-merge-final.cottas"
                cottas_stage = runner.run(
                    "cottas-merge-disk",
                    cottas_merge_many_command(
                        cottas_python,
                        cottas_paths,
                        cottas_merged_path,
                    ),
                    cottas_merged_path,
                )
                cottas_stage["cottas_merge_memory_limit"] = os.environ.get(
                    "COTTAS_MERGE_MEMORY_LIMIT", DEFAULT_COTTAS_MERGE_MEMORY_LIMIT
                )
                cottas_stage["cottas_merge_threads"] = os.environ.get(
                    "COTTAS_MERGE_THREADS", DEFAULT_COTTAS_MERGE_THREADS
                )
                runner.stages[-1].update(cottas_stage)
                add_totals(cottas_total, cottas_stage)
                cottas_rounds = 1
                final_cottas = (
                    cottas_merged_path
                    if cottas_stage["exit_code"] == 0 and cottas_merged_path.is_file()
                    else None
                )
            if final_cottas is None:
                if not args.allow_index_failures:
                    raise RuntimeError(
                        failure_message(cottas_stage, "COTTAS merge/index creation failed")
                    )
                cottas_failed = True
                cottas_total["exit_code"] = 0
                cleanup_cottas_intermediates()
                cottas_warning = record_index_warning(
                    "cottas",
                    "cottas-index",
                    output_cottas,
                    failure_message(cottas_stage, "COTTAS merge/index creation failed"),
                    stage_result=cottas_stage,
                )
            else:
                shutil.copyfile(final_cottas, output_cottas)
                final_cottas.unlink(missing_ok=True)
                cleanup_cottas_intermediates()
                try:
                    cottas_validation = validate_artifact(
                        name="cottas-validate",
                        artifact=output_cottas,
                        artifact_format="cottas",
                        python_bin=cottas_python,
                    )
                except RuntimeError as exc:
                    if not args.allow_index_failures:
                        raise
                    cottas_failed = True
                    cottas_total["exit_code"] = 0
                    output_cottas.unlink(missing_ok=True)
                    cottas_warning = record_index_warning(
                        "cottas",
                        "cottas-index",
                        output_cottas,
                        str(exc),
                    )
                if not cottas_failed:
                    results["cottas"] = {
                        **finalize_totals(cottas_total),
                        "output_path": str(output_cottas),
                        "output_size_bytes": output_cottas.stat().st_size,
                        "source": "partitioned_generated",
                        "details": {
                            **plan,
                            "merge_rounds": cottas_rounds,
                            "merge_strategy": "duckdb_disk_backed",
                            "merge_memory_limit": os.environ.get(
                                "COTTAS_MERGE_MEMORY_LIMIT",
                                DEFAULT_COTTAS_MERGE_MEMORY_LIMIT,
                            ),
                            "merge_threads": os.environ.get(
                                "COTTAS_MERGE_THREADS", DEFAULT_COTTAS_MERGE_THREADS
                            ),
                            "index": "spo",
                            "validation": cottas_validation,
                        },
                    }

        if any(method in COTTAS_METHODS for method in methods) and cottas_failed:
            results["cottas"] = skipped_cottas_result("cottas")

        for method in methods:
            if method == "hdt_gzip":
                artifact = output_dir / f"{args.output_name}.hdt.gz"
                stage = runner.run("hdt-gzip", ["gzip", "-c", str(output_hdt)], artifact, artifact)
            elif method == "hdt_brotli":
                artifact = output_dir / f"{args.output_name}.hdt.br"
                stage = runner.run("hdt-brotli", ["brotli", "-q", "7", "-c", str(output_hdt)], artifact, artifact)
            elif method == "cottas_gzip":
                if cottas_failed:
                    results[method] = skipped_cottas_result(method)
                    continue
                artifact = output_dir / f"{args.output_name}.cottas.gz"
                stage = runner.run("cottas-gzip", ["gzip", "-c", str(output_cottas)], artifact, artifact)
            elif method == "cottas_brotli":
                if cottas_failed:
                    results[method] = skipped_cottas_result(method)
                    continue
                artifact = output_dir / f"{args.output_name}.cottas.br"
                stage = runner.run("cottas-brotli", ["brotli", "-q", "7", "-c", str(output_cottas)], artifact, artifact)
            else:
                continue
            if stage["exit_code"] != 0:
                raise RuntimeError(f"{method} packaging failed")
            results[method] = stage

        write_result(
            {
                "exit_code": 0,
                "methods": results,
                "stages": runner.stages,
                "index_warnings": index_warnings,
            }
        )
        return 0
    except Exception as exc:
        error = str(exc)
        if isinstance(exc, OSError) and exc.errno == errno.ENOSPC:
            error = (
                "temporary partitioned-compression workspace (/work) ran out of storage. "
                "Chunks are streamed one at a time, but the workspace must still hold "
                "one raw chunk plus the in-progress HDT/COTTAS artifacts. Reduce "
                "--chunk-target-bytes and --chunk-max-bytes, or increase Docker's disk limit."
            )
        write_result({"exit_code": 1, "methods": results, "stages": runner.stages, "error": error})
        print(f"partitioned compression failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
