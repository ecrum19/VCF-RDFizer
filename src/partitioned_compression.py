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
HDT_CAT_CANDIDATES = (
    "hdtCat",
    "hdtCat.sh",
    "/opt/hdt-java/bin/hdtCat.sh",
    "/opt/hdt-java/bin/hdtCat",
)


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


def plan_chunks(
    source: Path,
    chunk_dir: Path,
    *,
    target_bytes: int,
    min_bytes: int,
    max_bytes: int,
) -> tuple[list[Path], dict]:
    """Create chunks on complete N-Triples records in one sequential pass."""
    if target_bytes <= 0 or min_bytes <= 0 or max_bytes <= 0:
        raise ValueError("RDF chunk sizes must be positive")
    if min_bytes > target_bytes or target_bytes > max_bytes:
        raise ValueError("RDF chunk sizes must satisfy min <= target <= max")

    chunk_dir.mkdir(parents=True, exist_ok=True)
    chunk_paths: list[Path] = []
    chunk_metadata: list[dict] = []
    handle = None
    chunk_path = None
    chunk_size = 0
    chunk_start_offset = 0
    chunk_start_record = 0
    logical_offset = 0
    record_count = 0
    chunk_index = 0

    def close_chunk():
        nonlocal handle, chunk_path, chunk_size
        if handle is None or chunk_path is None:
            return
        handle.close()
        chunk_paths.append(chunk_path)
        chunk_metadata.append(
            {
                "chunk_id": len(chunk_metadata),
                "path": str(chunk_path),
                "start_record": chunk_start_record,
                "end_record": record_count,
                "start_uncompressed_byte": chunk_start_offset,
                "end_uncompressed_byte": logical_offset,
                "record_count": record_count - chunk_start_record,
                "payload_bytes": chunk_size,
            }
        )
        handle = None
        chunk_path = None
        chunk_size = 0

    try:
        for line in iter_rdf_lines(source):
            if not line.endswith(b"\n"):
                raise ValueError(f"RDF source contains a non-line-terminated record: {source}")
            line_size = len(line)
            if handle is None:
                chunk_path = chunk_dir / f"chunk-{chunk_index:05d}.nt"
                chunk_index += 1
                handle = chunk_path.open("wb")
                chunk_start_offset = logical_offset
                chunk_start_record = record_count
            elif chunk_size > 0 and (
                (chunk_size >= target_bytes and chunk_size >= min_bytes)
                or chunk_size + line_size > max_bytes
            ):
                close_chunk()
                chunk_path = chunk_dir / f"chunk-{chunk_index:05d}.nt"
                chunk_index += 1
                handle = chunk_path.open("wb")
                chunk_start_offset = logical_offset
                chunk_start_record = record_count

            handle.write(line)
            chunk_size += line_size
            logical_offset += line_size
            if is_triple_line(line):
                record_count += 1
    finally:
        close_chunk()

    return chunk_paths, {
        "source_file_count": 1,
        "source_paths": [str(source)],
        "chunk_count": len(chunk_paths),
        "chunk_input_bytes": logical_offset,
        "record_count": record_count,
        "target_chunk_bytes": target_bytes,
        "min_chunk_bytes": min_bytes,
        "max_chunk_bytes": max_bytes,
        "chunks": chunk_metadata,
    }


def resolve_executable(candidates: tuple[str, ...], label: str) -> str:
    for candidate in candidates:
        resolved = shutil.which(candidate) if "/" not in candidate else candidate
        if resolved and Path(resolved).is_file() and os.access(resolved, os.X_OK):
            return resolved
    raise RuntimeError(f"Missing {label} in container")


def find_hdt_index_sidecar(hdt_path: Path) -> Path | None:
    """Locate HDT Java's non-empty versioned index sidecar."""
    for candidate in sorted(hdt_path.parent.glob(f"{hdt_path.name}.index.*")):
        if candidate.is_file() and candidate.stat().st_size > 0:
            return candidate
    return None


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

    def __init__(self, work_dir: Path):
        self.work_dir = work_dir
        self.stages: list[dict] = []

    def run(
        self,
        name: str,
        command: list[str],
        output_path: Path | None = None,
        stdout_path: Path | None = None,
    ) -> dict:
        time_path = self.work_dir / f".{name}.time"
        if time_path.exists():
            time_path.unlink()
        started = time.perf_counter()
        time_bin = "/usr/bin/time" if Path("/usr/bin/time").exists() else None
        if time_bin:
            timed_command = [time_bin, "-v", "-o", str(time_path), *command]
        else:
            timed_command = command
        stdout_handle = None
        try:
            if stdout_path is not None:
                stdout_path.parent.mkdir(parents=True, exist_ok=True)
                stdout_handle = stdout_path.open("wb")
            completed = subprocess.run(timed_command, stdout=stdout_handle, check=False)
        finally:
            if stdout_handle is not None:
                stdout_handle.close()
        result = {
            "exit_code": completed.returncode,
            "wall_seconds": time.perf_counter() - started,
            "output_path": "" if output_path is None else str(output_path),
            "output_size_bytes": output_path.stat().st_size
            if output_path is not None and output_path.is_file()
            else 0,
        }
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


def merge_pairwise(
    paths: list[Path],
    *,
    prefix: str,
    runner: StageRunner,
    merge_command,
    total: dict,
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
            stage = runner.run(
                f"{prefix}-merge-r{rounds:02d}-{pair_index // 2:05d}",
                merge_command(left, right, merged),
                merged,
            )
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
    runner = StageRunner(work_dir)
    results: dict[str, dict] = {}
    index_warnings: list[dict] = []

    hdt_total = {"exit_code": 0, "wall_seconds": 0.0, "user_seconds": 0.0, "sys_seconds": 0.0, "max_rss_kb": 0, "has_user": False, "has_sys": False, "has_rss": False}
    cottas_total = {"exit_code": 0, "wall_seconds": 0.0, "user_seconds": 0.0, "sys_seconds": 0.0, "max_rss_kb": 0, "has_user": False, "has_sys": False, "has_rss": False}
    output_hdt = output_dir / f"{args.output_name}.hdt"
    output_cottas = output_dir / f"{args.output_name}.cottas"
    cottas_failed = False
    cottas_warning = None

    def record_index_warning(index_format: str, stage: str, artifact: Path, message: str) -> dict:
        warning = {
            "format": index_format,
            "stage": stage,
            "status": "index_unavailable",
            "artifact_path": str(artifact),
            "message": " ".join(str(message).split()),
        }
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

        chunks, plan = plan_chunks(
            source,
            chunk_dir,
            target_bytes=args.target_chunk_bytes,
            min_bytes=args.min_chunk_bytes,
            max_bytes=args.max_chunk_bytes,
        )
        if not chunks:
            raise ValueError("RDF source contains no complete records")
        source_triples = int(plan["record_count"])
        if args.expected_triples is not None and source_triples != args.expected_triples:
            raise ValueError(
                "source triple count does not match the upstream conversion count: "
                f"source={source_triples}, expected={args.expected_triples}"
            )
        hdt_paths: list[Path] = []
        cottas_paths: list[Path] = []
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
        hdt_cat = resolve_executable(HDT_CAT_CANDIDATES, "HDTCat") if needs_hdt else None
        cottas_python = os.environ.get("COTTAS_PYTHON_BIN") or shutil.which("python3")
        if any(method in COTTAS_METHODS for method in methods) and not cottas_python:
            raise RuntimeError("Missing Python runtime for COTTAS")

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

        for index, chunk in enumerate(chunks):
            if needs_hdt:
                chunk_hdt = work_dir / f"chunk-{index:05d}.hdt"
                stage = runner.run(f"hdt-build-{index:05d}", [hdt_bin, str(chunk), str(chunk_hdt)], chunk_hdt)
                add_totals(hdt_total, stage)
                if stage["exit_code"] != 0:
                    raise RuntimeError("partitioned HDT chunk conversion failed")
                hdt_paths.append(chunk_hdt)
            if any(method in COTTAS_METHODS for method in methods):
                if cottas_failed:
                    chunk.unlink(missing_ok=True)
                    continue
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
                        "partitioned COTTAS conversion/index creation failed for a chunk",
                    )
                    chunk_cottas.unlink(missing_ok=True)
                    chunk.unlink(missing_ok=True)
                    continue
                cottas_paths.append(chunk_cottas)
            chunk.unlink(missing_ok=True)

        if hdt_paths:
            final_hdt, hdt_rounds = merge_pairwise(
                hdt_paths,
                prefix="hdt",
                runner=runner,
                merge_command=lambda left, right, merged: [hdt_cat, str(left), str(right), str(merged)],
                total=hdt_total,
            )
            if final_hdt is None:
                raise RuntimeError("HDTCat merge failed")
            shutil.copyfile(final_hdt, output_hdt)
            final_hdt.unlink(missing_ok=True)
            index_stage = runner.run(
                "hdt-index",
                ["/opt/vcf-rdfizer/ensure_hdt_index.sh", str(output_hdt)],
            )
            add_totals(hdt_total, index_stage)
            output_index = find_hdt_index_sidecar(output_hdt)
            if output_index is not None:
                # The filename is versioned by HDT Java, so record the actual
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
            final_cottas, cottas_rounds = merge_pairwise(
                cottas_paths,
                prefix="cottas",
                runner=runner,
                merge_command=lambda left, right, merged: [cottas_python, "/opt/vcf-rdfizer/cottas_tool.py", "merge", str(left), str(right), str(merged), "spo"],
                total=cottas_total,
            )
            if final_cottas is None:
                if not args.allow_index_failures:
                    raise RuntimeError("COTTAS merge failed")
                cottas_failed = True
                cottas_total["exit_code"] = 0
                cottas_warning = record_index_warning(
                    "cottas",
                    "cottas-index",
                    output_cottas,
                    "COTTAS merge/index creation failed",
                )
            else:
                shutil.copyfile(final_cottas, output_cottas)
                final_cottas.unlink(missing_ok=True)
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

        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(json.dumps({"exit_code": 0, "methods": results, "stages": runner.stages, "index_warnings": index_warnings}, indent=2) + "\n", encoding="utf-8")
        return 0
    except Exception as exc:
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(json.dumps({"exit_code": 1, "methods": results, "stages": runner.stages, "error": str(exc)}, indent=2) + "\n", encoding="utf-8")
        print(f"partitioned compression failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
