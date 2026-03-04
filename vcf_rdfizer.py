#!/usr/bin/env python3
"""VCF-RDFizer wrapper.

This module orchestrates the end-to-end Dockerized pipeline:
1) validate CLI/input state
2) convert VCF -> TSV
3) run RMLStreamer conversion
4) run selected compression/decompression operations
5) persist run and compression metrics

The implementation is intentionally split into small helpers so failures can be
diagnosed at a specific stage and future workflow changes stay localized.
"""

import argparse
import csv
import importlib.resources as importlib_resources
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


RMLSTREAMER_JAR_CONTAINER = "/opt/rmlstreamer/RMLStreamer-v2.5.0-standalone.jar"
_COMMAND_LOGGER = None
_DOCKER_USE_SUDO = False

COMPRESSED_VCF_EXPANSION_FACTOR = 5.0
TSV_OVERHEAD_FACTOR = 1.10
# Calibrated from observed runs:
# - small fixtures: ~42x-45x VCF->RDF inflation
# - larger real dataset: ~66x VCF->RDF inflation
RDF_EXPANSION_LOW_FACTOR = 42.0
RDF_EXPANSION_HIGH_FACTOR = 67.0
# Conversion metrics columns are always written by run_conversion.sh.
CONVERSION_METRICS_HEADER = [
    "run_id",
    "timestamp",
    "output_name",
    "output_dir",
    "exit_code_java",
    "wall_seconds_java",
    "user_seconds_java",
    "sys_seconds_java",
    "max_rss_kb_java",
    "input_mapping_size_bytes",
    "input_vcf_size_bytes",
    "output_dir_size_bytes",
    "output_triples",
    "jar",
    "mapping_file",
    "output_path",
]

COMPRESSION_COMMON_COLUMNS = ["combined_rdf_size_bytes", "compression_methods"]

COMPRESSION_METHOD_COLUMNS = {
    "gzip": [
        "gzip_size_bytes",
        "exit_code_gzip",
        "wall_seconds_gzip",
        "user_seconds_gzip",
        "sys_seconds_gzip",
        "max_rss_kb_gzip",
    ],
    "brotli": [
        "brotli_size_bytes",
        "exit_code_brotli",
        "wall_seconds_brotli",
        "user_seconds_brotli",
        "sys_seconds_brotli",
        "max_rss_kb_brotli",
    ],
    "hdt": [
        "hdt_size_bytes",
        "exit_code_hdt",
        "wall_seconds_hdt",
        "user_seconds_hdt",
        "sys_seconds_hdt",
        "max_rss_kb_hdt",
    ],
    "hdt_gzip": [
        "gzip_on_hdt_size_bytes",
        "exit_code_gzip_on_hdt",
        "wall_seconds_gzip_on_hdt",
        "user_seconds_gzip_on_hdt",
        "sys_seconds_gzip_on_hdt",
        "max_rss_kb_gzip_on_hdt",
    ],
    "hdt_brotli": [
        "brotli_on_hdt_size_bytes",
        "exit_code_brotli_on_hdt",
        "wall_seconds_brotli_on_hdt",
        "user_seconds_brotli_on_hdt",
        "sys_seconds_brotli_on_hdt",
        "max_rss_kb_brotli_on_hdt",
    ],
}

HDT_SOURCE_COLUMN = "hdt_source"
VALID_COMPRESSION_METHODS = {"gzip", "brotli", "hdt", "hdt_gzip", "hdt_brotli"}
HDT_COMPRESSION_METHODS = {"hdt", "hdt_gzip", "hdt_brotli"}


# ---------------------------------------------------------------------------
# Command execution and Docker environment helpers
# ---------------------------------------------------------------------------
class CommandLogger:
    """Write executed commands and their stdout/stderr to a wrapper log file."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a", encoding="utf-8")

    def run(self, cmd, cwd=None, env=None):
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        rendered = " ".join(shlex.quote(str(part)) for part in cmd)
        self._handle.write(f"\n[{timestamp}] $ {rendered}\n")
        if cwd is not None:
            self._handle.write(f"cwd={cwd}\n")
        self._handle.flush()

        result = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            stdout=self._handle,
            stderr=self._handle,
            text=True,
        )
        self._handle.write(f"[exit {result.returncode}]\n")
        self._handle.flush()
        return result.returncode

    def close(self):
        if not self._handle.closed:
            self._handle.close()


def eprint(*args):
    """Print to stderr."""
    print(*args, file=sys.stderr)


def ui_symbol(symbol: str, fallback: str) -> str:
    """Return a console symbol or ASCII fallback when stdout encoding can't represent it."""
    stream = getattr(sys, "stdout", None)
    encoding = getattr(stream, "encoding", None) or "utf-8"
    try:
        symbol.encode(encoding)
        return symbol
    except (UnicodeEncodeError, LookupError):
        return fallback


def success_symbol() -> str:
    """Unicode checkmark with ASCII fallback for Windows cp1252 consoles."""
    return ui_symbol("✅", "[ok]")


class RunTracker:
    """Track run progress and intermediate artifacts for safe interruption cleanup."""

    def __init__(self, log_path: Path):
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.log_path.open("a", encoding="utf-8")
        self.intermediate_paths: set[Path] = set()
        self.raw_rdf_paths: set[Path] = set()

    def mark(self, message: str):
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        self._handle.write(f"[{timestamp}] {message}\n")
        self._handle.flush()

    def track_intermediate(self, path: Path):
        self.intermediate_paths.add(path)

    def track_raw_rdf(self, path: Path):
        self.raw_rdf_paths.add(path)

    def close(self):
        if not self._handle.closed:
            self._handle.close()


def elapsed_to_seconds(value: str):
    """Parse elapsed clock strings from `time` output into seconds."""
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        if ":" not in text:
            return float(text)
        parts = text.split(":")
        if len(parts) == 2:
            minutes, seconds = parts
            return float(minutes) * 60.0 + float(seconds)
        if len(parts) == 3:
            hours, minutes, seconds = parts
            return float(hours) * 3600.0 + float(minutes) * 60.0 + float(seconds)
    except ValueError:
        return None
    return None


def parse_time_log_metrics(time_log: Path):
    """Parse GNU `/usr/bin/time -v` or POSIX `time -p` logs into numeric metrics."""
    if not time_log.exists():
        return {
            "wall_seconds": None,
            "user_seconds": None,
            "sys_seconds": None,
            "max_rss_kb": None,
        }

    text = time_log.read_text(encoding="utf-8", errors="replace")

    def first_float(pattern: str):
        match = re.search(pattern, text, flags=re.MULTILINE)
        if not match:
            return None
        try:
            return float(match.group(1))
        except (TypeError, ValueError):
            return None

    def first_int(pattern: str):
        match = re.search(pattern, text, flags=re.MULTILINE)
        if not match:
            return None
        try:
            return int(float(match.group(1)))
        except (TypeError, ValueError):
            return None

    wall_seconds = None
    elapsed_match = re.search(r"Elapsed \(wall clock\) time.*:\s*([^\n]+)", text)
    if elapsed_match:
        wall_seconds = elapsed_to_seconds(elapsed_match.group(1).strip())
    if wall_seconds is None:
        wall_seconds = first_float(r"^real\s+([0-9]+(?:\.[0-9]+)?)$")

    user_seconds = first_float(r"User time \(seconds\):\s*([0-9]+(?:\.[0-9]+)?)")
    if user_seconds is None:
        user_seconds = first_float(r"^user\s+([0-9]+(?:\.[0-9]+)?)$")

    sys_seconds = first_float(r"System time \(seconds\):\s*([0-9]+(?:\.[0-9]+)?)")
    if sys_seconds is None:
        sys_seconds = first_float(r"^sys\s+([0-9]+(?:\.[0-9]+)?)$")

    max_rss_kb = first_int(r"Maximum resident set size.*:\s*([0-9]+)")

    return {
        "wall_seconds": wall_seconds,
        "user_seconds": user_seconds,
        "sys_seconds": sys_seconds,
        "max_rss_kb": max_rss_kb,
    }


def run(cmd, cwd=None, env=None):
    """Run a command and return only its exit code.

    If command logging is enabled, stream output to the wrapper log file.
    """
    if _COMMAND_LOGGER is not None:
        return _COMMAND_LOGGER.run(cmd, cwd=cwd, env=env)
    return subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True).returncode


def docker_cmd_prefix(*, use_sudo: bool | None = None):
    """Return the docker executable prefix, optionally with sudo."""
    if use_sudo is None:
        use_sudo = _DOCKER_USE_SUDO
    return ["sudo", "docker"] if use_sudo else ["docker"]


def docker_run_base(*, as_user: bool = True):
    """Return base args for `docker run`, optionally mapped to host UID/GID."""
    base = [*docker_cmd_prefix(), "run", "--rm"]
    if not as_user:
        return base
    as_user = os.environ.get("VCF_RDFIZER_DOCKER_AS_USER", "1").strip().lower()
    if as_user in {"0", "false", "no"}:
        return base
    getuid = getattr(os, "getuid", None)
    getgid = getattr(os, "getgid", None)
    if callable(getuid) and callable(getgid):
        base.extend(["--user", f"{getuid()}:{getgid()}"])
    return base


def _can_write_dir(path: Path) -> bool:
    """Best-effort write probe for directories."""
    try:
        ensure_dir(path)
        probe = path / f".vcf_rdfizer_permcheck_{os.getpid()}"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return True
    except OSError:
        return False


def _can_write_file(path: Path) -> bool:
    """Best-effort write probe for files."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8"):
            pass
        return True
    except OSError:
        return False


def auto_fix_path_permissions(
    *,
    target_path: Path,
    is_dir: bool,
    image_ref: str,
    wrapper_log_path: Path,
) -> bool:
    """Try to recover ownership/permissions using an in-container chown/chmod pass."""
    getuid = getattr(os, "getuid", None)
    getgid = getattr(os, "getgid", None)
    if not callable(getuid) or not callable(getgid):
        return False
    uid_gid = f"{getuid()}:{getgid()}"

    if is_dir:
        mount_host = target_path
        mount_container = "/fix"
        cmd_body = (
            f"chown -R {uid_gid} {shlex.quote(mount_container)} || true; "
            f"chmod -R u+rwX {shlex.quote(mount_container)} || true"
        )
    else:
        mount_host = target_path.parent
        mount_container = "/fix"
        file_name = target_path.name
        cmd_body = (
            f"chown {uid_gid} {shlex.quote(mount_container + '/' + file_name)} || true; "
            f"chmod u+rw {shlex.quote(mount_container + '/' + file_name)} || true"
        )

    if not mount_host.exists():
        return False

    fix_cmd = [
        *docker_run_base(as_user=False),
        "-v",
        f"{str(mount_host)}:{mount_container}",
        image_ref,
        "bash",
        "-lc",
        cmd_body,
    ]
    if run(fix_cmd) != 0:
        eprint(
            f"Error: failed automatic permission recovery for '{target_path}'. "
            f"See log: {wrapper_log_path}"
        )
        return False

    return _can_write_dir(target_path) if is_dir else _can_write_file(target_path)


def ensure_writable_path_or_fix(
    *,
    target_path: Path,
    is_dir: bool,
    image_ref: str,
    wrapper_log_path: Path,
) -> bool:
    """Return whether a path is writable, attempting one automatic permission fix."""
    writable = _can_write_dir(target_path) if is_dir else _can_write_file(target_path)
    if writable:
        return True
    return auto_fix_path_permissions(
        target_path=target_path,
        is_dir=is_dir,
        image_ref=image_ref,
        wrapper_log_path=wrapper_log_path,
    )


def check_docker():
    """Validate Docker access.

    Probes `docker version` first; if that fails and sudo is available, retries
    with sudo and persists that mode for later Docker commands.
    """
    global _DOCKER_USE_SUDO

    if shutil.which("docker") is None:
        eprint("Error: Docker is not installed or not on PATH.")
        return False

    use_sudo_order = [False]
    if shutil.which("sudo") is not None:
        use_sudo_order.append(True)

    for use_sudo in use_sudo_order:
        code = run([*docker_cmd_prefix(use_sudo=use_sudo), "version"])
        if code == 0:
            _DOCKER_USE_SUDO = use_sudo
            if use_sudo:
                print("  Docker access requires sudo; using sudo for Docker commands.")
            return True

    eprint("Error: Docker is not available. Is the daemon running?")
    return False


# ---------------------------------------------------------------------------
# Input discovery and naming helpers
# ---------------------------------------------------------------------------
def is_vcf_file(path: Path) -> bool:
    """Return True for .vcf and .vcf.gz files."""
    name = path.name
    return name.endswith(".vcf") or name.endswith(".vcf.gz")


def list_vcfs_in_dir(path: Path):
    """List VCF inputs in a stable order for deterministic processing."""
    files = []
    for item in sorted(path.iterdir()):
        if item.is_file() and is_vcf_file(item):
            files.append(item)
    return files


def resolve_input(input_path: Path):
    """Legacy input resolver (single mount + container input path)."""
    if not input_path.exists():
        raise ValueError(f"Input path not found: {input_path}")

    if input_path.is_file():
        if not is_vcf_file(input_path):
            raise ValueError("Input file must end with .vcf or .vcf.gz")
        input_dir = input_path.parent
        container_input = f"/data/in/{input_path.name}"
        return input_dir, container_input

    if input_path.is_dir():
        vcfs = list_vcfs_in_dir(input_path)
        if not vcfs:
            raise ValueError("No .vcf or .vcf.gz files found in the input directory")
        container_input = "/data/in"
        return input_path, container_input

    raise ValueError("Input path must be a file or a directory")


def vcf_output_prefix(path: Path) -> str:
    """Derive stable sample prefix from VCF filename."""
    name = path.name
    if name.endswith(".vcf.gz"):
        return name[: -len(".vcf.gz")]
    if name.endswith(".vcf"):
        return name[: -len(".vcf")]
    return path.stem


def unique_in_order(items):
    """Deduplicate while preserving original order."""
    seen = set()
    ordered = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def resolve_input_snapshot(input_path: Path):
    """Capture VCF inputs at start of run.

    This prevents accidental inclusion of files that appear after pipeline
    execution begins and ensures full-mode processing is deterministic.
    """
    if not input_path.exists():
        raise ValueError(f"Input path not found: {input_path}")

    if input_path.is_file():
        if not is_vcf_file(input_path):
            raise ValueError("Input file must end with .vcf or .vcf.gz")
        mount_dir = input_path.parent
        container_inputs = [f"/data/in/{input_path.name}"]
        input_metrics_target = container_inputs[0]
        prefixes = [vcf_output_prefix(input_path)]
        return mount_dir, container_inputs, input_metrics_target, prefixes

    if input_path.is_dir():
        snapshot_files = list_vcfs_in_dir(input_path)
        if not snapshot_files:
            raise ValueError("No .vcf or .vcf.gz files found in the input directory")
        mount_dir = input_path
        container_inputs = [f"/data/in/{p.name}" for p in snapshot_files]
        input_metrics_target = "/data/in"
        prefixes = [vcf_output_prefix(p) for p in snapshot_files]
        return mount_dir, container_inputs, input_metrics_target, prefixes

    raise ValueError("Input path must be a file or a directory")


# ---------------------------------------------------------------------------
# General formatting and file-system utility helpers
# ---------------------------------------------------------------------------
def ensure_dir(path: Path):
    """Create a directory tree if missing."""
    path.mkdir(parents=True, exist_ok=True)


def format_bytes(num_bytes: int) -> str:
    """Human-friendly byte formatter for console output."""
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{num_bytes} B"


def format_duration(seconds: float) -> str:
    """Human-friendly duration formatter used in end-of-run summaries."""
    total_seconds = max(0.0, float(seconds))
    if total_seconds < 60:
        return f"{total_seconds:.2f}s"
    minutes, secs = divmod(total_seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {secs:.1f}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {secs:.1f}s"


def file_size_bytes(path: Path):
    """File size helper that returns None when path does not exist/is not a file."""
    if not path.exists() or not path.is_file():
        return None
    return path.stat().st_size


def _as_int(value):
    """Loss-tolerant integer coercion for metrics values."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        try:
            return int(float(stripped))
        except ValueError:
            return None
    return None


def read_conversion_total_triples(metrics_dir: Path, output_name: str, run_id: str):
    """Read TOTAL triple count for one conversion output from conversion metrics JSON."""
    safe_name = safe_metrics_name(output_name)
    candidates = [
        metrics_dir / "conversion_metrics" / safe_name / f"{run_id}.json",
        metrics_dir / "conversion_metrics" / safe_name / run_id,
        # Backward compatibility with older artifact names:
        metrics_dir / f"conversion-metrics-{safe_name}-{run_id}.json",
    ]
    metrics_json = next((path for path in candidates if path.exists()), None)
    if metrics_json is None:
        return None
    try:
        payload = json.loads(metrics_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    artifacts = payload.get("artifacts", {})
    triples = artifacts.get("output_triples")
    if isinstance(triples, dict):
        return _as_int(triples.get("TOTAL"))
    return _as_int(triples)


def collect_full_mode_total_triples(metrics_dir: Path, run_id: str):
    """Aggregate TOTAL triple counts across all conversion metrics files for a run."""
    total = 0
    found = False
    candidate_files = []
    candidate_files.extend(sorted(metrics_dir.glob("conversion_metrics/*/*")))
    # Backward compatibility with older artifact names:
    candidate_files.extend(sorted(metrics_dir.glob(f"conversion-metrics-*-{run_id}.json")))

    for metrics_json in candidate_files:
        if (
            metrics_json.name != run_id
            and metrics_json.name != f"{run_id}.json"
            and not metrics_json.name.endswith(f"-{run_id}.json")
        ):
            continue
        try:
            payload = json.loads(metrics_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        artifacts = payload.get("artifacts", {})
        triples = artifacts.get("output_triples")
        if isinstance(triples, dict):
            value = _as_int(triples.get("TOTAL"))
        else:
            value = _as_int(triples)
        if value is None:
            continue
        total += value
        found = True
    return total if found else None


def append_wrapper_timing_log(
    *,
    metrics_dir: Path,
    run_id: str,
    timestamp: str,
    mode: str,
    exit_code: int,
    elapsed_seconds: float,
    total_triples: int | None = None,
):
    """Append one wrapper-level execution summary row."""
    ensure_dir(metrics_dir)
    timings_csv = metrics_dir / "wrapper_execution_times.csv"
    header = [
        "run_id",
        "timestamp",
        "mode",
        "exit_code",
        "status",
        "elapsed_seconds",
        "elapsed_human",
        "total_triples",
    ]
    write_header = not timings_csv.exists()
    with timings_csv.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        if write_header:
            writer.writeheader()
        writer.writerow(
            {
                "run_id": run_id,
                "timestamp": timestamp,
                "mode": mode,
                "exit_code": int(exit_code),
                "status": "success" if int(exit_code) == 0 else "failure",
                "elapsed_seconds": f"{float(elapsed_seconds):.6f}",
                "elapsed_human": format_duration(elapsed_seconds),
                "total_triples": "" if total_triples is None else str(int(total_triples)),
            }
        )


def write_failed_inputs_report(*, metrics_dir: Path, failures: list[dict]):
    """Write per-input failure summary for full-mode partial runs."""
    ensure_dir(metrics_dir)
    report_path = metrics_dir / "failed_inputs.csv"
    header = [
        "input_index",
        "input_vcf",
        "expected_prefix",
        "stage",
        "error",
    ]
    with report_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for entry in failures:
            writer.writerow(
                {
                    "input_index": entry.get("input_index", ""),
                    "input_vcf": entry.get("input_vcf", ""),
                    "expected_prefix": entry.get("expected_prefix", ""),
                    "stage": entry.get("stage", ""),
                    "error": entry.get("error", ""),
                }
            )
    return report_path


def print_nt_hdt_summary(
    *,
    output_root: Path,
    nt_path: Path,
    hdt_path: Path,
    indent: str = "",
    nt_note: str | None = None,
    nt_size_override: int | None = None,
    selected_methods: list[str] | None = None,
    method_results: dict[str, dict] | None = None,
):
    """Print per-output size summary for RDF and selected compression artifacts."""
    print(f"{indent}* Output directory: {output_root}")

    nt_size = nt_size_override if nt_size_override is not None else file_size_bytes(nt_path)

    if nt_size is None:
        nt_text = f"not found at {nt_path}"
    else:
        nt_text = f"{format_bytes(nt_size)} ({nt_path})"
    if nt_note:
        nt_text = f"{nt_text} ({nt_note})"
    print(f"{indent}  - {rdf_label_for_path(nt_path)}: {nt_text}")

    # Backward-compatible fallback summary when no explicit compression method
    # set is provided to this printer.
    if selected_methods is None:
        hdt_size = file_size_bytes(hdt_path)
        if hdt_size is None:
            print(f"{indent}  - HDT (.hdt): not generated at {hdt_path}")
        else:
            print(f"{indent}  - HDT (.hdt): {format_bytes(hdt_size)} ({hdt_path})")
        return

    if not selected_methods:
        print(f"{indent}  - Compression: none selected")
        return

    results = method_results or {}
    for method in selected_methods:
        artifact_name = compression_artifact_name_for_method(nt_path, method)
        artifact_path = output_root / artifact_name
        result = results.get(method, {})
        size = result.get("output_size_bytes")
        if size is None:
            size = file_size_bytes(artifact_path)
        if size is None:
            artifact_text = f"not generated at {artifact_path}"
        else:
            artifact_text = f"{format_bytes(int(size))} ({artifact_path})"

        if method in {"hdt", "hdt_gzip", "hdt_brotli"}:
            source = str(result.get("source", "")).strip()
            if not source and "hdt" in results:
                source = str(results.get("hdt", {}).get("source", "")).strip()
            if source == "existing":
                artifact_text = f"{artifact_text} (reused existing HDT)"

        print(f"{indent}  - {compression_method_label_for_path(nt_path, method)}: {artifact_text}")


def rdf_label_for_path(path: Path) -> str:
    """Return human-readable RDF format label for a path."""
    if path.suffix == ".nt":
        return "N-Triples (.nt)"
    if path.suffix:
        return f"RDF ({path.suffix})"
    return "RDF"


def compression_artifact_name_for_method(path: Path, method: str) -> str:
    """Compute expected compressed artifact filename for a method."""
    stem = path.stem
    ext = path.suffix.lstrip(".") or "nt"
    if method == "gzip":
        return f"{stem}.{ext}.gz"
    if method == "brotli":
        return f"{stem}.{ext}.br"
    if method == "hdt":
        return f"{stem}.hdt"
    if method == "hdt_gzip":
        return f"{stem}.hdt.gz"
    if method == "hdt_brotli":
        return f"{stem}.hdt.br"
    return f"{stem}.{method}"


def compression_method_label_for_path(path: Path, method: str) -> str:
    """Return human-readable compression method label for a path."""
    ext = path.suffix.lstrip(".") or "nt"
    labels = {
        "gzip": f"gzip (.{ext}.gz)",
        "brotli": f"brotli (.{ext}.br)",
        "hdt": "HDT (.hdt)",
        "hdt_gzip": "gzip-on-HDT (.hdt.gz)",
        "hdt_brotli": "brotli-on-HDT (.hdt.br)",
    }
    return labels.get(method, method)


def remove_file_with_docker_fallback(
    *,
    path: Path,
    mount_root: Path,
    mount_point: str,
    image_ref: str,
    wrapper_log_path: Path,
) -> bool:
    """Delete a file directly, falling back to an in-container `rm` on permission errors."""
    if not path.exists():
        return True

    try:
        path.unlink()
        return True
    except PermissionError:
        pass

    try:
        rel = path.relative_to(mount_root)
    except ValueError:
        eprint(f"Error: cannot remove file outside mounted root: {path}")
        eprint(f"See log for details: {wrapper_log_path}")
        return False

    container_path = f"{mount_point}/{rel.as_posix()}"
    rm_cmd = [
        *docker_run_base(),
        "-v",
        f"{str(mount_root)}:{mount_point}",
        image_ref,
        "bash",
        "-lc",
        f"rm -f {shlex.quote(container_path)}",
    ]
    if run(rm_cmd) != 0:
        eprint(f"Error: failed to remove file with Docker fallback: {path}")
        eprint(f"See log for details: {wrapper_log_path}")
        return False
    return True


def remove_path_with_docker_fallback(
    *,
    path: Path,
    mount_root: Path,
    mount_point: str,
    image_ref: str | None,
    wrapper_log_path: Path,
) -> bool:
    """Delete a file/dir directly, then fall back to in-container `rm -rf` if needed."""
    if not path.exists():
        return True

    try:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        return True
    except OSError:
        pass

    if not image_ref:
        eprint(f"Error: cannot remove '{path}' after interruption (Docker image unresolved).")
        eprint(f"See log for details: {wrapper_log_path}")
        return False

    try:
        rel = path.resolve().relative_to(mount_root.resolve())
    except ValueError:
        eprint(f"Error: cannot remove path outside mounted root: {path}")
        eprint(f"See log for details: {wrapper_log_path}")
        return False

    if rel.as_posix() in {".", ""}:
        eprint(f"Error: refusing to remove mounted root path via fallback: {path}")
        eprint(f"See log for details: {wrapper_log_path}")
        return False

    container_path = f"{mount_point.rstrip('/')}/{rel.as_posix()}"
    rm_cmd = [
        *docker_run_base(as_user=False),
        "-v",
        f"{str(mount_root)}:{mount_point}",
        image_ref,
        "bash",
        "-lc",
        f"rm -rf {shlex.quote(container_path)}",
    ]
    if run(rm_cmd) != 0:
        eprint(f"Error: failed to remove path with Docker fallback: {path}")
        eprint(f"See log for details: {wrapper_log_path}")
        return False
    return True


def cleanup_interrupted_full_run(
    *,
    run_tracker: RunTracker,
    out_root: Path,
    image_ref: str | None,
    keep_rdf: bool,
    wrapper_log_path: Path,
):
    """Best-effort cleanup for full-mode interruption.

    Removes tracked intermediates (and raw RDF artifacts when `keep_rdf` is not set),
    then records a compact cleanup summary in the run progress log.
    """
    targets: set[Path] = set(run_tracker.intermediate_paths)
    if not keep_rdf:
        targets.update(run_tracker.raw_rdf_paths)

    removed = 0
    failed = 0
    for path in sorted(targets, key=lambda p: len(p.parts), reverse=True):
        if not path.exists():
            continue
        if remove_path_with_docker_fallback(
            path=path,
            mount_root=out_root,
            mount_point="/data/out",
            image_ref=image_ref,
            wrapper_log_path=wrapper_log_path,
        ):
            removed += 1
        else:
            failed += 1

    run_tracker.mark(
        f"Interrupt cleanup finished: removed={removed}, failed={failed}, keep_rdf={str(keep_rdf).lower()}"
    )
    return removed, failed


def existing_parent(path: Path) -> Path:
    """Return the closest existing parent path (used for disk free-space anchor)."""
    cur = path
    while not cur.exists():
        if cur.parent == cur:
            break
        cur = cur.parent
    return cur


def is_within_path(path: Path, root: Path) -> bool:
    """Return whether `path` is located under `root` after resolution."""
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def collect_input_vcfs(input_path: Path):
    """Return VCF input list from either single-file or directory mode."""
    if input_path.is_file():
        return [input_path]
    if input_path.is_dir():
        return list_vcfs_in_dir(input_path)
    return []


def estimate_pipeline_sizes(vcf_files, out_dir: Path):
    """Estimate rough TSV/RDF footprint for preflight disk-space warnings."""
    input_bytes = 0
    est_tsv_bytes = 0
    est_rdf_low_bytes = 0
    est_rdf_high_bytes = 0

    for vcf in vcf_files:
        size = vcf.stat().st_size
        input_bytes += size
        if vcf.name.endswith(".vcf.gz"):
            expanded_vcf = size * COMPRESSED_VCF_EXPANSION_FACTOR
        else:
            expanded_vcf = float(size)

        est_tsv_bytes += expanded_vcf * TSV_OVERHEAD_FACTOR
        est_rdf_low_bytes += expanded_vcf * RDF_EXPANSION_LOW_FACTOR
        est_rdf_high_bytes += expanded_vcf * RDF_EXPANSION_HIGH_FACTOR

    out_anchor = existing_parent(out_dir)
    free_disk_bytes = shutil.disk_usage(out_anchor).free

    return {
        "input_bytes": int(input_bytes),
        "tsv_bytes": int(est_tsv_bytes),
        "rdf_low_bytes": int(est_rdf_low_bytes),
        "rdf_high_bytes": int(est_rdf_high_bytes),
        "free_disk_bytes": int(free_disk_bytes),
        "disk_anchor": out_anchor,
    }


# ---------------------------------------------------------------------------
# Mapping/rules and Docker image management helpers
# ---------------------------------------------------------------------------
def slugify(value: str) -> str:
    """Normalize a value for safe filesystem naming."""
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "vcf"


def discover_tsv_triplets(tsv_dir: Path):
    """Discover per-VCF TSV triplets: records/header/metadata."""
    triplets = []
    for records_path in sorted(tsv_dir.glob("*.records.tsv")):
        prefix = records_path.name[: -len(".records.tsv")]
        header_path = tsv_dir / f"{prefix}.header_lines.tsv"
        metadata_path = tsv_dir / f"{prefix}.file_metadata.tsv"
        if not header_path.exists():
            raise ValueError(f"Missing header TSV for '{prefix}': {header_path}")
        if not metadata_path.exists():
            raise ValueError(f"Missing metadata TSV for '{prefix}': {metadata_path}")
        triplets.append(
            {
                "prefix": prefix,
                "records": records_path,
                "headers": header_path,
                "metadata": metadata_path,
            }
        )

    if triplets:
        return triplets

    tsv_files = sorted(p.name for p in tsv_dir.glob("*.tsv"))
    tsv_preview = ", ".join(tsv_files) if tsv_files else "(none)"
    raise ValueError(
        f"No per-VCF records TSV files found in {tsv_dir}. "
        f"Expected '*.records.tsv'. Found: {tsv_preview}"
    )


def render_rules_for_triplet(template_rules: Path, output_rules: Path, records_name: str, headers_name: str, metadata_name: str):
    """Render per-input mapping rules by substituting TSV placeholders."""
    text = template_rules.read_text()
    text = text.replace('/data/tsv/records.tsv', f'/data/tsv/{records_name}')
    text = text.replace('/data/tsv/header_lines.tsv', f'/data/tsv/{headers_name}')
    text = text.replace('/data/tsv/file_metadata.tsv', f'/data/tsv/{metadata_name}')
    output_rules.write_text(text)


def resolve_default_rules_path(repo_root: Path) -> Path:
    """Resolve default rules path for both source checkout and installed package.

    Resolution order:
    1) `<repo_root>/rules/default_rules.ttl` (editable/local checkout)
    2) packaged data file in `vcf_rdfizer_data/rules/default_rules.ttl` (wheel/sdist install)
    """
    local_default = (repo_root / "rules" / "default_rules.ttl").resolve()
    if local_default.exists() and local_default.is_file():
        return local_default

    try:
        packaged = importlib_resources.files("vcf_rdfizer_data").joinpath(
            "rules/default_rules.ttl"
        )
        with importlib_resources.as_file(packaged) as packaged_path:
            packaged_resolved = packaged_path.resolve()
            if packaged_resolved.exists() and packaged_resolved.is_file():
                return packaged_resolved
    except (ModuleNotFoundError, FileNotFoundError):
        pass

    raise ValueError(
        "default rules file not found. Provide --rules explicitly or reinstall package."
    )


def docker_image_exists(image: str) -> bool:
    """Return True when Docker image reference exists locally."""
    return run([*docker_cmd_prefix(), "image", "inspect", image]) == 0


def docker_build_image(image: str, repo_root: Path):
    """Build Docker image from repository Dockerfile."""
    return run([*docker_cmd_prefix(), "build", "-t", image, "."], cwd=str(repo_root))


def docker_pull_image(image: str):
    """Pull Docker image from registry."""
    return run([*docker_cmd_prefix(), "pull", image])


def resolve_image_ref(image: str, image_version: str | None):
    """Resolve image + optional tag into a concrete Docker reference."""
    if ":" in image:
        if image_version is not None:
            raise ValueError("Do not include a tag in --image when using --image-version.")
        return image, False
    if image_version is None:
        return f"{image}:latest", False
    return f"{image}:{image_version}", True


def parse_compression_methods(raw: str):
    """Parse and validate compression method selection from CLI."""
    value = (raw or "").strip()
    if value == "" or value == "none":
        return []

    methods = []
    for token in value.split(","):
        method = token.strip()
        if not method:
            continue
        if method not in VALID_COMPRESSION_METHODS:
            raise ValueError(
                "Unsupported compression method "
                f"'{method}'. Use gzip,brotli,hdt,hdt_gzip,hdt_brotli, or none."
            )
        if method not in methods:
            methods.append(method)
    return methods


def safe_metrics_name(value: str) -> str:
    """Sanitize names used in metrics artifact filenames."""
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")
    return safe or "rdf"


def metrics_header_for_methods(selected_methods: list[str]) -> list[str]:
    """Build a run-specific metrics.csv header with only relevant columns."""
    methods = list(selected_methods or [])
    header = list(CONVERSION_METRICS_HEADER)
    if not methods:
        return header

    header.extend(COMPRESSION_COMMON_COLUMNS)
    if "gzip" in methods:
        header.extend(COMPRESSION_METHOD_COLUMNS["gzip"])
    if "brotli" in methods:
        header.extend(COMPRESSION_METHOD_COLUMNS["brotli"])

    uses_hdt = any(method in HDT_COMPRESSION_METHODS for method in methods)
    if uses_hdt:
        header.extend(COMPRESSION_METHOD_COLUMNS["hdt"])
        header.append(HDT_SOURCE_COLUMN)
    if "hdt_gzip" in methods:
        header.extend(COMPRESSION_METHOD_COLUMNS["hdt_gzip"])
    if "hdt_brotli" in methods:
        header.extend(COMPRESSION_METHOD_COLUMNS["hdt_brotli"])

    return unique_in_order(header)


# ---------------------------------------------------------------------------
# Metrics serialization helpers
# ---------------------------------------------------------------------------
def update_metrics_csv_with_compression(
    *,
    metrics_csv: Path,
    run_id: str,
    timestamp: str,
    output_name: str,
    output_dir: Path,
    combined_size_bytes: int,
    selected_methods: list[str],
    method_results: dict[str, dict],
):
    """Upsert compression-related columns in `metrics.csv` for one output artifact.

    This function keeps raw-RDF compression metrics distinct from compound
    HDT-first metrics (gzip_on_hdt / brotli_on_hdt) to avoid ambiguity.
    """
    metrics_csv.parent.mkdir(parents=True, exist_ok=True)
    target_header = metrics_header_for_methods(selected_methods)
    rows = []
    existing_header = []

    if metrics_csv.exists():
        with metrics_csv.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            existing_header = list(reader.fieldnames or [])
            rows = list(reader)

    # Keep only current run-relevant columns, while preserving conversion rows.
    if existing_header:
        header_mismatch = existing_header != target_header
        rows = [{name: row.get(name, "") for name in target_header} for row in rows]
        if header_mismatch:
            backup = metrics_csv.with_name(f"metrics_csv_bak_{run_id}.csv")
            shutil.copyfile(metrics_csv, backup)
    else:
        rows = []

    row = None
    for existing in rows:
        if existing.get("run_id") == run_id and existing.get("output_name") == output_name:
            row = existing
            break

    if row is None:
        row = {name: "" for name in target_header}
        row["run_id"] = run_id
        row["timestamp"] = timestamp
        row["output_name"] = output_name
        row["output_dir"] = str(output_dir)
        rows.append(row)

    if "combined_rdf_size_bytes" in row:
        row["combined_rdf_size_bytes"] = str(int(combined_size_bytes))
    if "compression_methods" in row:
        row["compression_methods"] = "|".join(selected_methods) if selected_methods else "none"

    defaults = {
        "gzip_size_bytes": "0",
        "brotli_size_bytes": "0",
        "hdt_size_bytes": "0",
        "exit_code_gzip": "0",
        "exit_code_brotli": "0",
        "exit_code_hdt": "0",
        "wall_seconds_gzip": "null",
        "wall_seconds_brotli": "null",
        "wall_seconds_hdt": "null",
        "user_seconds_gzip": "null",
        "user_seconds_brotli": "null",
        "user_seconds_hdt": "null",
        "sys_seconds_gzip": "null",
        "sys_seconds_brotli": "null",
        "sys_seconds_hdt": "null",
        "max_rss_kb_gzip": "null",
        "max_rss_kb_brotli": "null",
        "max_rss_kb_hdt": "null",
        "hdt_source": "not_used",
        "gzip_on_hdt_size_bytes": "0",
        "brotli_on_hdt_size_bytes": "0",
        "exit_code_gzip_on_hdt": "0",
        "exit_code_brotli_on_hdt": "0",
        "wall_seconds_gzip_on_hdt": "null",
        "user_seconds_gzip_on_hdt": "null",
        "sys_seconds_gzip_on_hdt": "null",
        "max_rss_kb_gzip_on_hdt": "null",
        "wall_seconds_brotli_on_hdt": "null",
        "user_seconds_brotli_on_hdt": "null",
        "sys_seconds_brotli_on_hdt": "null",
        "max_rss_kb_brotli_on_hdt": "null",
    }
    for key, value in defaults.items():
        if key in row:
            row[key] = value

    def assign_timing(prefix: str, result: dict):
        wall_val = result.get("wall_seconds")
        user_val = result.get("user_seconds")
        sys_val = result.get("sys_seconds")
        rss_val = result.get("max_rss_kb")
        wall_col = f"wall_seconds_{prefix}"
        user_col = f"user_seconds_{prefix}"
        sys_col = f"sys_seconds_{prefix}"
        rss_col = f"max_rss_kb_{prefix}"

        if wall_col in row:
            row[wall_col] = "null" if wall_val is None else f"{float(wall_val):.6f}"
        if user_col in row:
            row[user_col] = "null" if user_val is None else f"{float(user_val):.6f}"
        if sys_col in row:
            row[sys_col] = "null" if sys_val is None else f"{float(sys_val):.6f}"
        if rss_col in row:
            row[rss_col] = "null" if rss_val is None else str(int(rss_val))

    for method in ("gzip", "brotli", "hdt"):
        result = method_results.get(method)
        if result is None:
            continue
        size_key = f"{method}_size_bytes"
        exit_key = f"exit_code_{method}"
        if size_key in row:
            row[size_key] = str(int(result.get("output_size_bytes") or 0))
        if exit_key in row:
            row[exit_key] = str(int(result.get("exit_code") or 0))
        assign_timing(method, result)

    hdt_result = method_results.get("hdt")
    if hdt_result is not None and "hdt_source" in row:
        row["hdt_source"] = str(hdt_result.get("source") or "generated")

    hdt_gzip_result = method_results.get("hdt_gzip")
    if hdt_gzip_result is not None:
        if "gzip_on_hdt_size_bytes" in row:
            row["gzip_on_hdt_size_bytes"] = str(int(hdt_gzip_result.get("output_size_bytes") or 0))
        if "exit_code_gzip_on_hdt" in row:
            row["exit_code_gzip_on_hdt"] = str(int(hdt_gzip_result.get("exit_code") or 0))
        assign_timing("gzip_on_hdt", hdt_gzip_result)

    hdt_brotli_result = method_results.get("hdt_brotli")
    if hdt_brotli_result is not None:
        if "brotli_on_hdt_size_bytes" in row:
            row["brotli_on_hdt_size_bytes"] = str(int(hdt_brotli_result.get("output_size_bytes") or 0))
        if "exit_code_brotli_on_hdt" in row:
            row["exit_code_brotli_on_hdt"] = str(int(hdt_brotli_result.get("exit_code") or 0))
        assign_timing("brotli_on_hdt", hdt_brotli_result)

    with metrics_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=target_header)
        writer.writeheader()
        writer.writerows({name: row.get(name, "") for name in target_header} for row in rows)


def write_compression_metrics_artifacts(
    *,
    metrics_dir: Path,
    run_id: str,
    timestamp: str,
    output_name: str,
    source_rdf_path: Path,
    combined_size_bytes: int,
    selected_methods: list[str],
    method_results: dict[str, dict],
):
    """Write per-output compression artifacts (time files + structured JSON)."""
    metrics_dir.mkdir(parents=True, exist_ok=True)
    safe_name = safe_metrics_name(output_name)

    for method, result in method_results.items():
        time_log_dir = metrics_dir / "compression_time" / method / safe_name
        time_log_dir.mkdir(parents=True, exist_ok=True)
        time_log = time_log_dir / f"{run_id}.txt"
        lines = [
            f"method={method}",
            f"exit_code={result.get('exit_code', 1)}",
            f"wall_seconds={result.get('wall_seconds', 'null')}",
            f"user_seconds={result.get('user_seconds', 'null')}",
            f"sys_seconds={result.get('sys_seconds', 'null')}",
            f"max_rss_kb={result.get('max_rss_kb', 'null')}",
            f"output_path={result.get('output_path', '')}",
            f"output_size_bytes={result.get('output_size_bytes', 0)}",
        ]
        time_log.write_text("\n".join(lines) + "\n", encoding="utf-8")

    gzip_result = method_results.get("gzip", {})
    brotli_result = method_results.get("brotli", {})
    hdt_result = method_results.get("hdt", {})
    hdt_gzip_result = method_results.get("hdt_gzip", {})
    hdt_brotli_result = method_results.get("hdt_brotli", {})

    def timing_payload(result: dict):
        return {
            "wall_seconds": result.get("wall_seconds"),
            "user_seconds": result.get("user_seconds"),
            "sys_seconds": result.get("sys_seconds"),
            "max_rss_kb": result.get("max_rss_kb"),
        }

    payload = {
        "run_id": run_id,
        "timestamp": timestamp,
        "output_dir": str(source_rdf_path.parent),
        "output_name": output_name,
        "compression_methods": ",".join(selected_methods) if selected_methods else "none",
        "combined_rdf_path": str(source_rdf_path),
        "combined_rdf_size_bytes": int(combined_size_bytes),
        "hdt_source": str(hdt_result.get("source") or "not_used"),
        "gzip_raw_rdf": {
            "output_gz_path": gzip_result.get("output_path", ""),
            "output_gz_size_bytes": int(gzip_result.get("output_size_bytes") or 0),
            "exit_code": int(gzip_result.get("exit_code") or 0),
            "timing": timing_payload(gzip_result),
        },
        "brotli_raw_rdf": {
            "output_brotli_path": brotli_result.get("output_path", ""),
            "output_brotli_size_bytes": int(brotli_result.get("output_size_bytes") or 0),
            "exit_code": int(brotli_result.get("exit_code") or 0),
            "timing": timing_payload(brotli_result),
        },
        "hdt_conversion": {
            "output_hdt_path": hdt_result.get("output_path", ""),
            "output_hdt_size_bytes": int(hdt_result.get("output_size_bytes") or 0),
            "exit_code": int(hdt_result.get("exit_code") or 0),
            "timing": timing_payload(hdt_result),
        },
        "gzip_on_hdt": {
            "output_hdt_gz_path": hdt_gzip_result.get("output_path", ""),
            "output_hdt_gz_size_bytes": int(hdt_gzip_result.get("output_size_bytes") or 0),
            "exit_code": int(hdt_gzip_result.get("exit_code") or 0),
            "timing": timing_payload(hdt_gzip_result),
        },
        "brotli_on_hdt": {
            "output_hdt_br_path": hdt_brotli_result.get("output_path", ""),
            "output_hdt_br_size_bytes": int(hdt_brotli_result.get("output_size_bytes") or 0),
            "exit_code": int(hdt_brotli_result.get("exit_code") or 0),
            "timing": timing_payload(hdt_brotli_result),
        },
    }

    metrics_json_dir = metrics_dir / "compression_metrics" / safe_name
    metrics_json_dir.mkdir(parents=True, exist_ok=True)
    metrics_json = metrics_json_dir / f"{run_id}.json"
    metrics_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def aggregate_method_results_across_files(method_results_by_file: dict[str, dict[str, dict]]):
    """Aggregate per-file compression results into one per-method summary.

    This is used for batch RDF layout where one VCF can produce many raw RDF
    part files. Aggregation keeps metrics comparable with aggregate layout by
    storing one row per sample (not one row per part file).
    """
    aggregated: dict[str, dict] = {}
    for file_results in method_results_by_file.values():
        for method, result in file_results.items():
            current = aggregated.setdefault(
                method,
                {
                    "exit_code": 0,
                    "wall_seconds": 0.0,
                    "user_seconds": 0.0,
                    "sys_seconds": 0.0,
                    "max_rss_kb": 0,
                    "output_size_bytes": 0,
                    "_seen_wall": False,
                    "_seen_user": False,
                    "_seen_sys": False,
                    "_seen_rss": False,
                },
            )
            current["exit_code"] = max(
                int(current.get("exit_code", 0)),
                int(result.get("exit_code") or 0),
            )
            wall = result.get("wall_seconds")
            if wall is not None:
                current["wall_seconds"] = float(current.get("wall_seconds", 0.0)) + float(wall)
                current["_seen_wall"] = True
            user = result.get("user_seconds")
            if user is not None:
                current["user_seconds"] = float(current.get("user_seconds", 0.0)) + float(user)
                current["_seen_user"] = True
            sys_seconds = result.get("sys_seconds")
            if sys_seconds is not None:
                current["sys_seconds"] = float(current.get("sys_seconds", 0.0)) + float(sys_seconds)
                current["_seen_sys"] = True
            max_rss = result.get("max_rss_kb")
            if max_rss is not None:
                current["max_rss_kb"] = max(int(current.get("max_rss_kb", 0)), int(max_rss))
                current["_seen_rss"] = True
            current["output_size_bytes"] = int(current.get("output_size_bytes", 0)) + int(
                result.get("output_size_bytes") or 0
            )

            source = result.get("source")
            if source is not None:
                source = str(source)
                prior = current.get("source")
                if prior is None:
                    current["source"] = source
                elif prior != source:
                    current["source"] = "mixed"

    for current in aggregated.values():
        if not current.pop("_seen_wall", False):
            current["wall_seconds"] = None
        if not current.pop("_seen_user", False):
            current["user_seconds"] = None
        if not current.pop("_seen_sys", False):
            current["sys_seconds"] = None
        if not current.pop("_seen_rss", False):
            current["max_rss_kb"] = None

    return aggregated


def validate_mode_dirs(paths):
    """Validate that expected directory arguments are not file paths."""
    for p in paths:
        if p.exists() and not p.is_dir():
            raise ValueError(f"expected a directory path but found a file: {p}")


# ---------------------------------------------------------------------------
# Mode runners (full/compress/decompress)
# ---------------------------------------------------------------------------
def ensure_image_available(
    image_ref: str,
    *,
    step_label: str,
    version_requested: bool,
    build: bool,
    no_build: bool,
    repo_root: Path,
    wrapper_log_path: Path,
):
    """Resolve image availability policy (build/pull/reuse) with clear status codes."""
    if build:
        print(f"{step_label}: Ensuring Docker image is available")
        print("  - Building Docker image")
        if docker_build_image(image_ref, repo_root) != 0:
            eprint(f"Error: docker build failed. See log: {wrapper_log_path}")
            return 1
        print(f"{step_label}: Ensuring Docker image is available {success_symbol()}")
        return 0

    if docker_image_exists(image_ref):
        print(f"{step_label}: Ensuring Docker image is available {success_symbol()}")
        return 0

    if version_requested:
        print(f"{step_label}: Ensuring Docker image is available")
        print(f"  - Pulling image: {image_ref}")
        if docker_pull_image(image_ref) != 0:
            eprint(f"Error: image version '{image_ref}' not found. See log: {wrapper_log_path}")
            return 2
        print(f"{step_label}: Ensuring Docker image is available {success_symbol()}")
        return 0

    if no_build:
        eprint(f"Error: image '{image_ref}' not found and --no-build set.")
        return 2

    print(f"{step_label}: Ensuring Docker image is available")
    print("  - Image missing locally, building")
    if docker_build_image(image_ref, repo_root) != 0:
        eprint(f"Error: docker build failed. See log: {wrapper_log_path}")
        return 1
    print(f"{step_label}: Ensuring Docker image is available {success_symbol()}")
    return 0


def run_compression_methods_for_rdf(
    *,
    rdf_path: Path,
    out_dir: Path,
    target_out_dir: Path | None = None,
    image_ref: str,
    methods: list[str],
    wrapper_log_path: Path,
    status_indent: str | None,
):
    """Run selected compression methods for a single RDF file.

    Supports compound HDT-first methods by reusing an existing `.hdt` when
    present, or generating it once and reusing it for subsequent steps.
    Returns `(ok, method_results)`.
    """
    in_dir = rdf_path.parent
    input_container = f"/data/in/{rdf_path.name}"
    input_stem = rdf_path.stem
    input_ext = rdf_path.suffix.lstrip(".") or "nt"
    if target_out_dir is None:
        target_out_dir = out_dir / input_stem
    ensure_dir(target_out_dir)
    if not ensure_writable_path_or_fix(
        target_path=target_out_dir,
        is_dir=True,
        image_ref=image_ref,
        wrapper_log_path=wrapper_log_path,
    ):
        eprint(f"Error: cannot write compression outputs in '{target_out_dir}'.")
        return False, {}
    try:
        relative_out = target_out_dir.resolve().relative_to(out_dir.resolve())
    except ValueError:
        eprint(
            f"Error: target output directory '{target_out_dir}' is outside mounted root '{out_dir}'."
        )
        return False, {}
    target_out_container = "/data/out"
    if str(relative_out) not in {".", ""}:
        target_out_container = f"/data/out/{relative_out.as_posix()}"

    method_results: dict[str, dict] = {}
    hdt_name = f"{input_stem}.hdt"
    hdt_path = target_out_dir / hdt_name
    hdt_container = f"{target_out_container}/{hdt_name}"
    hdt_is_ready = False
    hdt_source = "generated"

    def run_container_command(*, method: str, output_name: str, command: str):
        """Execute one compression command in Docker and capture timing/size."""
        timing_name = f".{input_stem}.{method}.time"
        timing_container = f"{target_out_container}/{timing_name}"
        timing_host = target_out_dir / timing_name
        wrapped_command = (
            "set -euo pipefail; "
            f"rm -f {shlex.quote(timing_container)}; "
            'if [[ -x /usr/bin/time ]] && /usr/bin/time --version >/dev/null 2>&1; then '
            f"/usr/bin/time -v -o {shlex.quote(timing_container)} -- bash -lc {shlex.quote(command)}; "
            "else "
            f"{{ time -p bash -lc {shlex.quote(command)}; }} > {shlex.quote(timing_container)} 2>&1; "
            "fi"
        )
        cmd = [
            *docker_run_base(),
            "-v",
            f"{str(in_dir)}:/data/in:ro",
            "-v",
            f"{str(out_dir)}:/data/out",
            image_ref,
            "bash",
            "-lc",
            wrapped_command,
        ]
        started = time.perf_counter()
        exit_code = run(cmd)
        elapsed = time.perf_counter() - started
        timing = parse_time_log_metrics(timing_host)
        output_path = target_out_dir / output_name
        method_results[method] = {
            "exit_code": exit_code,
            "wall_seconds": timing.get("wall_seconds")
            if timing.get("wall_seconds") is not None
            else elapsed,
            "user_seconds": timing.get("user_seconds"),
            "sys_seconds": timing.get("sys_seconds"),
            "max_rss_kb": timing.get("max_rss_kb"),
            "output_path": str(output_path),
            "output_size_bytes": int(file_size_bytes(output_path) or 0),
        }
        if timing_host.exists():
            try:
                timing_host.unlink()
            except OSError:
                pass
        if method == "hdt":
            method_results[method]["source"] = "generated"
        if exit_code != 0:
            eprint(f"Error: {method} compression failed. See log: {wrapper_log_path}")
            return False
        return True

    def ensure_hdt_available():
        """Ensure `.hdt` exists for HDT-based compound methods."""
        nonlocal hdt_is_ready, hdt_source
        if hdt_is_ready:
            return True
        if hdt_path.exists():
            hdt_is_ready = True
            hdt_source = "existing"
            method_results.setdefault(
                "hdt",
                {
                    "exit_code": 0,
                    "wall_seconds": 0.0,
                    "user_seconds": 0.0,
                    "sys_seconds": 0.0,
                    "max_rss_kb": 0,
                    "output_path": str(hdt_path),
                    "output_size_bytes": int(file_size_bytes(hdt_path) or 0),
                },
            )
            return True
        hdt_command = (
            "set -euo pipefail; "
            f"rm -f {shlex.quote(hdt_container)}; "
            'HDT_BIN="${RDF2HDT_BIN:-$(command -v rdf2hdt || true)}"; '
            'if [[ -z "$HDT_BIN" ]]; then '
            'for candidate in /usr/local/bin/rdf2hdt /opt/hdt-cpp/bin/rdf2hdt; do '
            '[[ -x "$candidate" ]] && HDT_BIN="$candidate" && break; '
            "done; "
            "fi; "
            'if [[ -z "$HDT_BIN" || ! -x "$HDT_BIN" ]]; then '
            'echo "Missing rdf2hdt binary in container" >&2; exit 127; '
            "fi; "
            '"$HDT_BIN" '
            f"{shlex.quote(input_container)} {shlex.quote(hdt_container)}"
        )
        if not run_container_command(method="hdt", output_name=hdt_name, command=hdt_command):
            return False
        hdt_is_ready = True
        hdt_source = "generated"
        return True

    for method in methods:
        if method == "gzip":
            output_name = f"{input_stem}.{input_ext}.gz"
            out_container = f"{target_out_container}/{output_name}"
            command = (
                "set -euo pipefail; "
                f"rm -f {shlex.quote(out_container)}; "
                f"gzip -c {shlex.quote(input_container)} > {shlex.quote(out_container)}"
            )
            if not run_container_command(method=method, output_name=output_name, command=command):
                return False, method_results
            if status_indent is not None:
                print(f"{status_indent}- {method}: {output_name} {success_symbol()}")
            continue

        if method == "brotli":
            output_name = f"{input_stem}.{input_ext}.br"
            out_container = f"{target_out_container}/{output_name}"
            command = (
                "set -euo pipefail; "
                f"rm -f {shlex.quote(out_container)}; "
                f"brotli -q 7 -c {shlex.quote(input_container)} > {shlex.quote(out_container)}"
            )
            if not run_container_command(method=method, output_name=output_name, command=command):
                return False, method_results
            if status_indent is not None:
                print(f"{status_indent}- {method}: {output_name} {success_symbol()}")
            continue

        if method == "hdt":
            if not ensure_hdt_available():
                return False, method_results
            if status_indent is not None:
                suffix = " (reused existing HDT)" if hdt_source == "existing" else ""
                print(f"{status_indent}- hdt: {hdt_name} {success_symbol()}{suffix}")
            continue

        if method == "hdt_gzip":
            if not ensure_hdt_available():
                return False, method_results
            output_name = f"{input_stem}.hdt.gz"
            out_container = f"{target_out_container}/{output_name}"
            command = (
                "set -euo pipefail; "
                f"rm -f {shlex.quote(out_container)}; "
                f"gzip -c {shlex.quote(hdt_container)} > {shlex.quote(out_container)}"
            )
            if not run_container_command(method=method, output_name=output_name, command=command):
                return False, method_results
            if status_indent is not None:
                suffix = " (using existing HDT)" if hdt_source == "existing" else ""
                print(f"{status_indent}- {method}: {output_name} {success_symbol()}{suffix}")
            continue

        if method == "hdt_brotli":
            if not ensure_hdt_available():
                return False, method_results
            output_name = f"{input_stem}.hdt.br"
            out_container = f"{target_out_container}/{output_name}"
            command = (
                "set -euo pipefail; "
                f"rm -f {shlex.quote(out_container)}; "
                f"brotli -q 7 -c {shlex.quote(hdt_container)} > {shlex.quote(out_container)}"
            )
            if not run_container_command(method=method, output_name=output_name, command=command):
                return False, method_results
            if status_indent is not None:
                suffix = " (using existing HDT)" if hdt_source == "existing" else ""
                print(f"{status_indent}- {method}: {output_name} {success_symbol()}{suffix}")
            continue

    return True, method_results


def run_full_mode(
    *,
    input_mount_dir: Path,
    container_inputs: list[str],
    input_metrics_target: str,
    expected_prefixes: list[str],
    rules_path: Path,
    out_dir: Path,
    tsv_dir: Path,
    metrics_dir: Path,
    image_ref: str,
    out_name: str,
    rdf_layout: str,
    compression: str,
    keep_tsv: bool,
    keep_rdf: bool,
    run_id: str,
    timestamp: str,
    wrapper_log_path: Path,
    run_tracker: RunTracker | None = None,
):
    """Execute full pipeline: per-input TSV -> RDF -> compression -> metrics."""
    print("Step 3/5: Processing per-input pipeline (TSV -> RDF -> compression)")
    intermediate_dir = tsv_dir.parent
    ensure_dir(tsv_dir)
    ensure_dir(out_dir)
    ensure_dir(metrics_dir)

    selected_methods = parse_compression_methods(compression)

    generated_rules_dir = metrics_dir / "_generated_rules"
    if generated_rules_dir.exists():
        shutil.rmtree(generated_rules_dir, ignore_errors=True)
    ensure_dir(generated_rules_dir)
    if run_tracker is not None:
        run_tracker.track_intermediate(intermediate_dir)
        run_tracker.track_intermediate(tsv_dir)
        run_tracker.track_intermediate(generated_rules_dir)
        run_tracker.mark("Full pipeline started")

    total_triples_produced = 0
    saw_triple_counts = False
    input_failures: list[dict] = []

    total_inputs = len(container_inputs)
    for idx, (container_input, expected_prefix) in enumerate(
        zip(container_inputs, expected_prefixes),
        start=1,
    ):
        input_name = Path(container_input).name
        try:
            container_rel = Path(container_input).relative_to("/data/in")
            input_vcf = str((input_mount_dir / container_rel).resolve())
        except ValueError:
            input_vcf = container_input
        input_failed = False

        def fail_current(stage: str, message: str):
            nonlocal input_failed
            input_failed = True
            compact = " ".join(str(message).split())
            eprint(f"    ! Input {idx}/{total_inputs} ({input_name}) failed at {stage}: {compact}")
            input_failures.append(
                {
                    "input_index": idx,
                    "input_vcf": input_vcf,
                    "expected_prefix": expected_prefix,
                    "stage": stage,
                    "error": compact,
                }
            )
            if run_tracker is not None:
                run_tracker.mark(
                    f"Input {idx}/{total_inputs} failed at {stage} for {expected_prefix}: {compact}"
                )

        print(f"  - Input {idx}/{total_inputs}: {input_name}")
        if run_tracker is not None:
            run_tracker.mark(f"Input {idx}/{total_inputs} started: {expected_prefix}")

        # Pre-flight write checks for expected TSV outputs to fail fast on
        # permission/mount problems before starting container work.
        for suffix in ("records.tsv", "header_lines.tsv", "file_metadata.tsv"):
            expected_tsv_output = tsv_dir / f"{expected_prefix}.{suffix}"
            if not ensure_writable_path_or_fix(
                target_path=expected_tsv_output,
                is_dir=False,
                image_ref=image_ref,
                wrapper_log_path=wrapper_log_path,
            ):
                fail_current(
                    "preflight-write-check",
                    f"cannot write expected TSV output '{expected_tsv_output}'. See log: {wrapper_log_path}",
                )
                break
        if input_failed:
            continue

        tsv_cmd = [
            *docker_run_base(),
            "-v",
            f"{str(input_mount_dir)}:/data/in:ro",
            "-v",
            f"{str(tsv_dir)}:/data/tsv",
            image_ref,
            "bash",
            "/opt/vcf-rdfizer/vcf_as_tsv.sh",
            container_input,
            "/data/tsv",
        ]
        if run(tsv_cmd) != 0:
            fail_current("tsv-conversion", f"TSV conversion failed. See log: {wrapper_log_path}")
            continue
        print(f"    * TSV conversion {success_symbol()}")
        if run_tracker is not None:
            run_tracker.mark(f"Input {idx}: TSV conversion completed for {expected_prefix}")

        # Discover and lock the exact triplet generated for this input; this
        # guards against stale TSV files from previous runs.
        try:
            tsv_triplets = discover_tsv_triplets(tsv_dir)
        except ValueError as exc:
            fail_current("tsv-discovery", f"{exc}. See log: {wrapper_log_path}")
            continue

        triplets_by_prefix = {triplet["prefix"]: triplet for triplet in tsv_triplets}
        if expected_prefix not in triplets_by_prefix:
            fail_current(
                "tsv-validation",
                f"TSV conversion did not produce the expected triplet for '{expected_prefix}'. "
                f"See log: {wrapper_log_path}",
            )
            continue

        triplet = triplets_by_prefix[expected_prefix]
        prefix = triplet["prefix"]
        safe_prefix = slugify(prefix)
        generated_rules = generated_rules_dir / f"{safe_prefix}.rules.ttl"
        render_rules_for_triplet(
            rules_path,
            generated_rules,
            triplet["records"].name,
            triplet["headers"].name,
            triplet["metadata"].name,
        )

        output_name = safe_prefix or slugify(out_name)
        output_sample_dir = out_dir / output_name
        if not ensure_writable_path_or_fix(
            target_path=output_sample_dir,
            is_dir=True,
            image_ref=image_ref,
            wrapper_log_path=wrapper_log_path,
        ):
            fail_current(
                "output-write-check",
                f"cannot write output directory '{output_sample_dir}'. See log: {wrapper_log_path}",
            )
            continue
        container_generated_rules = f"/data/rules/{generated_rules.name}"

        run_cmd = [
            *docker_run_base(),
            "-v",
            f"{str(generated_rules_dir)}:/data/rules:ro",
            "-v",
            f"{str(tsv_dir)}:/data/tsv:ro",
            "-v",
            f"{str(out_dir)}:/data/out",
            "-v",
            f"{str(metrics_dir)}:/data/metrics",
            "-w",
            "/data/rules",
            "-e",
            f"JAR={RMLSTREAMER_JAR_CONTAINER}",
            "-e",
            f"IN={container_generated_rules}",
            "-e",
            "OUT_DIR=/data/out",
            "-e",
            f"OUT_NAME={output_name}",
            "-e",
            f"AGGREGATE_RDF={'1' if rdf_layout == 'aggregate' else '0'}",
            "-e",
            f"RUN_ID={run_id}",
            "-e",
            f"TIMESTAMP={timestamp}",
            "-e",
            f"IN_VCF={input_metrics_target}",
            "-e",
            "LOGDIR=/data/metrics",
            image_ref,
            "bash",
            "/opt/vcf-rdfizer/run_conversion.sh",
        ]
        if run(run_cmd) != 0:
            fail_current(
                "rdf-conversion",
                f"RMLStreamer step failed for '{prefix}'. See log: {wrapper_log_path}",
            )
            continue
        print(f"    * RDF conversion {success_symbol()}")
        if run_tracker is not None:
            run_tracker.mark(f"Input {idx}: RDF conversion completed for {prefix}")

        triples_produced = read_conversion_total_triples(metrics_dir, output_name, run_id)
        if triples_produced is not None:
            saw_triple_counts = True
            total_triples_produced += triples_produced
            print(f"    * Triples produced: {triples_produced:,}")

        if rdf_layout == "aggregate":
            # Aggregate mode yields one merged RDF artifact per sample.
            nt_path = out_dir / output_name / f"{output_name}.nt"
            if nt_path.exists():
                raw_rdf_files = [nt_path]
            else:
                raw_rdf_files = [nt_path]
        else:
            # Batch mode keeps each RMLStreamer part as its own RDF artifact.
            raw_rdf_files = sorted((out_dir / output_name).glob("*.nt"))
            if not raw_rdf_files:
                fail_current(
                    "rdf-discovery",
                    f"no RDF part files produced in batch mode for '{output_name}'. "
                    f"Expected .nt files in {out_dir / output_name}. See log: {wrapper_log_path}",
                )
                continue

        if run_tracker is not None:
            for raw_rdf_path in raw_rdf_files:
                run_tracker.track_raw_rdf(raw_rdf_path)

        method_results_by_file: dict[str, dict[str, dict]] = {}
        if selected_methods:
            # Compress each produced RDF artifact independently.
            for raw_rdf_path in raw_rdf_files:
                ok, method_results = run_compression_methods_for_rdf(
                    rdf_path=raw_rdf_path,
                    out_dir=out_dir / output_name,
                    target_out_dir=out_dir / output_name,
                    image_ref=image_ref,
                    methods=selected_methods,
                    wrapper_log_path=wrapper_log_path,
                    status_indent=None,
                )
                if not ok:
                    fail_current(
                        "compression",
                        f"compression failed for '{raw_rdf_path.name}'. See log: {wrapper_log_path}",
                    )
                    break
                method_results_by_file[raw_rdf_path.name] = method_results
        if input_failed:
            continue
        print(f"    * Compression {success_symbol()}")
        if run_tracker is not None:
            run_tracker.mark(f"Input {idx}: compression completed for {output_name}")

        raw_size_before_cleanup_by_file = {
            raw_rdf_path.name: int(file_size_bytes(raw_rdf_path) or 0) for raw_rdf_path in raw_rdf_files
        }
        try:
            # Persist machine-readable metrics after compression succeeds.
            if rdf_layout == "batch":
                # Batch mode produces many RDF parts for a single sample. Keep
                # one metrics row per sample by aggregating per-part compression
                # outputs and timings.
                aggregated_results = aggregate_method_results_across_files(method_results_by_file)
                combined_size_before_cleanup = sum(raw_size_before_cleanup_by_file.values())
                write_compression_metrics_artifacts(
                    metrics_dir=metrics_dir,
                    run_id=run_id,
                    timestamp=timestamp,
                    output_name=output_name,
                    source_rdf_path=out_dir / output_name,
                    combined_size_bytes=combined_size_before_cleanup,
                    selected_methods=selected_methods,
                    method_results=aggregated_results,
                )
                update_metrics_csv_with_compression(
                    metrics_csv=metrics_dir / "metrics.csv",
                    run_id=run_id,
                    timestamp=timestamp,
                    output_name=output_name,
                    output_dir=out_dir / output_name,
                    combined_size_bytes=combined_size_before_cleanup,
                    selected_methods=selected_methods,
                    method_results=aggregated_results,
                )
            else:
                for raw_rdf_path in raw_rdf_files:
                    method_results = method_results_by_file.get(raw_rdf_path.name, {})
                    source_size_before_cleanup = raw_size_before_cleanup_by_file[raw_rdf_path.name]
                    write_compression_metrics_artifacts(
                        metrics_dir=metrics_dir,
                        run_id=run_id,
                        timestamp=timestamp,
                        output_name=raw_rdf_path.stem,
                        source_rdf_path=raw_rdf_path,
                        combined_size_bytes=source_size_before_cleanup,
                        selected_methods=selected_methods,
                        method_results=method_results,
                    )
                    update_metrics_csv_with_compression(
                        metrics_csv=metrics_dir / "metrics.csv",
                        run_id=run_id,
                        timestamp=timestamp,
                        output_name=raw_rdf_path.stem,
                        output_dir=out_dir / output_name,
                        combined_size_bytes=source_size_before_cleanup,
                        selected_methods=selected_methods,
                        method_results=method_results,
                    )
        except PermissionError as exc:
            blocked_path = exc.filename or str(metrics_dir)
            eprint("Error: unable to write compression metrics due to file permissions.")
            eprint(f"Blocked path: {blocked_path}")
            eprint(
                "Fix ownership, then rerun: "
                f"sudo chown -R $USER:$USER {shlex.quote(str(metrics_dir))}"
            )
            return 1

        if not keep_rdf and selected_methods:
            # Cleanup raw RDF only after every selected compression method has
            # completed successfully for that specific RDF artifact.
            cleanup_failed = False
            for raw_rdf_path in raw_rdf_files:
                method_results = method_results_by_file.get(raw_rdf_path.name, {})
                missing_or_failed = []
                for method in selected_methods:
                    result = method_results.get(method)
                    if result is None or int(result.get("exit_code", 1)) != 0:
                        missing_or_failed.append(method)
                if missing_or_failed:
                    fail_current(
                        "rdf-cleanup-validation",
                        "refusing to remove raw RDF before all selected compression methods "
                        f"completed successfully for '{raw_rdf_path.name}'. "
                        f"Pending/failed: {', '.join(missing_or_failed)}. "
                        f"See log: {wrapper_log_path}",
                    )
                    cleanup_failed = True
                    break

                if raw_rdf_path.exists():
                    if not remove_file_with_docker_fallback(
                        path=raw_rdf_path,
                        mount_root=out_dir,
                        mount_point="/data/out",
                        image_ref=image_ref,
                        wrapper_log_path=wrapper_log_path,
                    ):
                        fail_current(
                            "rdf-cleanup",
                            f"failed to remove raw RDF '{raw_rdf_path.name}'. See log: {wrapper_log_path}",
                        )
                        cleanup_failed = True
                        break
            if cleanup_failed:
                continue

        if rdf_layout == "batch" and raw_rdf_files:
            output_root = out_dir / output_name
            part_count = len(raw_rdf_files)
            raw_total_size = sum(raw_size_before_cleanup_by_file.values())

            if keep_rdf:
                raw_note = "retained via --keep-rdf"
            elif selected_methods:
                raw_note = "removed, set --keep-rdf to retain"
            else:
                raw_note = "kept (compression methods set to none)"

            first_path = raw_rdf_files[0]
            print(f"    * Output directory: {output_root}")
            print(f"      - RDF part files: {part_count}")
            raw_text = f"{format_bytes(raw_total_size)} across {part_count} files"
            print(
                f"      - {rdf_label_for_path(first_path)} total: {raw_text} "
                f"({raw_note})"
            )

            if selected_methods:
                for method in selected_methods:
                    method_total = 0
                    method_count = 0
                    for raw_rdf_path in raw_rdf_files:
                        result = method_results_by_file.get(raw_rdf_path.name, {}).get(method)
                        if not result or int(result.get("exit_code", 1)) != 0:
                            continue
                        method_total += int(result.get("output_size_bytes") or 0)
                        method_count += 1

                    label = compression_method_label_for_path(first_path, method)
                    if method_count == 0:
                        print(f"      - {label}: not generated")
                    else:
                        print(
                            f"      - {label}: {format_bytes(method_total)} "
                            f"across {method_count} files"
                        )
            else:
                print("      - Compression: none selected")
                print(f"      - Final RDF size (no compression): {format_bytes(raw_total_size)}")
        else:
            for raw_rdf_path in raw_rdf_files:
                hdt_path = (out_dir / output_name) / f"{raw_rdf_path.stem}.hdt"
                rdf_size = file_size_bytes(raw_rdf_path)
                nt_note = None
                method_results = method_results_by_file.get(raw_rdf_path.name, {})
                if raw_rdf_path.exists():
                    nt_note = "retained via --keep-rdf" if keep_rdf else "retained"
                elif not keep_rdf and selected_methods:
                    nt_note = "removed, set --keep-rdf to retain"
                elif not keep_rdf and not selected_methods:
                    nt_note = "kept (compression methods set to none)"
                print_nt_hdt_summary(
                    output_root=out_dir / output_name,
                    nt_path=raw_rdf_path,
                    hdt_path=hdt_path,
                    indent="    ",
                    nt_note=nt_note,
                    nt_size_override=rdf_size,
                    selected_methods=selected_methods,
                    method_results=method_results,
                )
            if not selected_methods:
                total_raw_size = sum(raw_size_before_cleanup_by_file.values())
                print(f"    * Final RDF size (no compression): {format_bytes(total_raw_size)}")

        if not keep_tsv:
            # Cleanup only the triplet generated for this input iteration.
            tsv_cleanup_failed = False
            for tsv_path in (triplet["records"], triplet["headers"], triplet["metadata"]):
                if tsv_path.exists():
                    if not remove_file_with_docker_fallback(
                        path=tsv_path,
                        mount_root=tsv_dir,
                        mount_point="/data/tsv",
                        image_ref=image_ref,
                        wrapper_log_path=wrapper_log_path,
                    ):
                        fail_current(
                            "tsv-cleanup",
                            f"failed to remove intermediate TSV '{tsv_path.name}'. See log: {wrapper_log_path}",
                        )
                        tsv_cleanup_failed = True
                        break
            if tsv_cleanup_failed:
                continue

        if run_tracker is not None:
            run_tracker.mark(f"Input {idx}/{total_inputs} completed: {output_name}")

    if not keep_tsv and intermediate_dir.exists():
        if not remove_path_with_docker_fallback(
            path=intermediate_dir,
            mount_root=out_dir,
            mount_point="/data/out",
            image_ref=image_ref,
            wrapper_log_path=wrapper_log_path,
        ):
            eprint(
                f"Warning: failed to remove intermediate directory '{intermediate_dir}'. "
                f"See log: {wrapper_log_path}"
            )
            if run_tracker is not None:
                run_tracker.mark(f"Intermediate cleanup failed for {intermediate_dir}")

    if saw_triple_counts:
        print(f"Total triples produced (full run): {total_triples_produced:,}")
    elif not selected_methods:
        print("Total triples produced (full run): unavailable")

    if input_failures:
        report_path = write_failed_inputs_report(metrics_dir=metrics_dir, failures=input_failures)
        eprint(
            f"Completed with failures for {len(input_failures)}/{total_inputs} input(s). "
            f"Failure report: {report_path}"
        )
        print("Conversion process completed with failures.")
        if run_tracker is not None:
            run_tracker.mark(
                f"Full pipeline completed with failures ({len(input_failures)}/{total_inputs}). "
                f"Report: {report_path}"
            )
        return 1

    print("Conversion process finished.")
    if run_tracker is not None:
        run_tracker.mark("Full pipeline finished successfully")
    return 0


def run_compress_mode(
    *,
    rdf_path: Path,
    out_dir: Path,
    image_ref: str,
    methods: list[str],
    wrapper_log_path: Path,
):
    """Execute compression-only mode for a designated RDF file."""
    print("Step 3/3: Compressing RDF input")
    if not methods:
        print("No compression methods selected (`none`). Nothing to do.")
        return 0

    if any(method in HDT_COMPRESSION_METHODS for method in methods):
        file_size = file_size_bytes(rdf_path) or 0
        if file_size > 5 * 1024 * 1024 * 1024:
            eprint(
                "Warning: selected HDT compression for an .nt file larger than 5 GB. "
                "This may fail due to memory limits depending on environment."
            )

    ensure_dir(out_dir)
    ok, method_results = run_compression_methods_for_rdf(
        rdf_path=rdf_path,
        out_dir=out_dir,
        image_ref=image_ref,
        methods=methods,
        wrapper_log_path=wrapper_log_path,
        status_indent="  ",
    )
    if not ok:
        return 1

    input_stem = rdf_path.stem
    target_out_dir = out_dir / input_stem
    hdt_path = target_out_dir / f"{input_stem}.hdt"
    print_nt_hdt_summary(
        output_root=target_out_dir,
        nt_path=rdf_path,
        hdt_path=hdt_path,
        indent="  ",
        selected_methods=methods,
        method_results=method_results,
    )
    print("Conversion process finished.")
    return 0


def detect_compressed_format(path: Path):
    """Infer compressed RDF format from filename/extension."""
    if path.name.endswith(".nt.gz") or path.suffix == ".gz":
        return "gzip"
    if path.name.endswith(".nt.br") or path.suffix == ".br":
        return "brotli"
    if path.suffix == ".hdt":
        return "hdt"
    raise ValueError("Compressed input must end with .gz, .br, or .hdt")


def default_decompressed_name(path: Path, fmt: str):
    """Compute default output filename for decompression mode."""
    if fmt == "gzip":
        if path.name.endswith(".nt.gz"):
            return path.name[: -len(".gz")]
        return f"{path.stem}.nt"
    if fmt == "brotli":
        if path.name.endswith(".nt.br"):
            return path.name[: -len(".br")]
        return f"{path.stem}.nt"
    return f"{path.stem}.nt"


def run_decompress_mode(
    *,
    compressed_path: Path,
    decompressed_out: Path,
    image_ref: str,
    wrapper_log_path: Path,
):
    """Execute decompression-only mode (.gz/.br/.hdt -> RDF)."""
    print("Step 3/3: Decompressing RDF input")
    fmt = detect_compressed_format(compressed_path)
    ensure_dir(decompressed_out.parent)

    source_container = f"/data/in/{compressed_path.name}"
    output_container = f"/data/out/{decompressed_out.name}"

    if fmt == "gzip":
        command = (
            "set -euo pipefail; "
            f"rm -f {shlex.quote(output_container)}; "
            f"gzip -dc {shlex.quote(source_container)} > {shlex.quote(output_container)}"
        )
    elif fmt == "brotli":
        command = (
            "set -euo pipefail; "
            f"rm -f {shlex.quote(output_container)}; "
            f"brotli -d -c {shlex.quote(source_container)} > {shlex.quote(output_container)}"
        )
    else:
        command = (
            "set -euo pipefail; "
            f"rm -f {shlex.quote(output_container)}; "
            'HDT2RDF_BIN="${HDT2RDF_BIN:-$(command -v hdt2rdf || true)}"; '
            'if [[ -z "$HDT2RDF_BIN" ]]; then '
            'for candidate in /usr/local/bin/hdt2rdf /opt/hdt-cpp/bin/hdt2rdf; do '
            '[[ -x "$candidate" ]] && HDT2RDF_BIN="$candidate" && break; '
            "done; "
            "fi; "
            'if [[ -z "$HDT2RDF_BIN" || ! -x "$HDT2RDF_BIN" ]]; then '
            'echo "Missing hdt2rdf binary in container" >&2; exit 127; '
            "fi; "
            '"$HDT2RDF_BIN" '
            f"{shlex.quote(source_container)} {shlex.quote(output_container)}"
        )

    cmd = [
        *docker_run_base(),
        "-v",
        f"{str(compressed_path.parent)}:/data/in:ro",
        "-v",
        f"{str(decompressed_out.parent)}:/data/out",
        image_ref,
        "bash",
        "-lc",
        command,
    ]
    if run(cmd) != 0:
        eprint(f"Error: decompression failed. See log: {wrapper_log_path}")
        return 1

    print(f"Done. Decompressed file: {decompressed_out}")
    return 0


def main():
    """CLI entrypoint.

    Handles argument validation, Docker/image preflight, mode dispatch, and
    wrapper-level runtime logging.
    """
    parser = argparse.ArgumentParser(
        description="VCF-RDFizer Docker wrapper",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  Full pipeline:\n"
            "    vcf_rdfizer.py -m full -i ./vcf_files --rdf-layout aggregate -o ./results\n"
            "  Full pipeline (batch RDF outputs, compress each part):\n"
            "    vcf_rdfizer.py -m full -i ./vcf_files --rdf-layout batch -c hdt -o ./results\n"
            "  Compression-only:\n"
            "    vcf_rdfizer.py -m compress -q ./results/out/sample/sample.nt -c gzip,hdt_gzip -o ./results\n"
            "  Decompression-only:\n"
            "    vcf_rdfizer.py -m decompress -C ./results/out/sample/sample.nt.gz -o ./results\n"
        ),
    )
    parser.add_argument(
        "-m",
        "--mode",
        choices=["full", "compress", "decompress"],
        default="full",
        help="Run mode: full VCF->RDF pipeline, compression-only, or decompression-only",
    )
    parser.add_argument(
        "-i",
        "--input",
        default=None,
        help="VCF file or directory (required for --mode full)",
    )
    parser.add_argument(
        "-q",
        "--rdf",
        "--nt",
        dest="rdf",
        default=None,
        help="Input RDF file (.nt) for --mode compress",
    )
    parser.add_argument(
        "-C",
        "--compressed-input",
        default=None,
        help="Compressed RDF input (.gz/.br/.hdt) for --mode decompress",
    )
    parser.add_argument(
        "-d",
        "--decompress-out",
        default=None,
        help="Output RDF file path for --mode decompress (default: <out>/decompressed/<name>.nt)",
    )
    parser.add_argument(
        "-r",
        "--rules",
        default=None,
        help="RML mapping rules .ttl (default: <repo>/rules/default_rules.ttl)",
    )
    parser.add_argument(
        "-l",
        "--rdf-layout",
        choices=["aggregate", "batch"],
        default=None,
        help="Full mode required: aggregate merges RML output parts into one .nt; batch keeps part files separate",
    )
    parser.add_argument(
        "-o",
        "--out",
        required=True,
        help="Required output root directory for this run (stores outputs, metrics, and hidden intermediates)",
    )
    parser.add_argument(
        "-I",
        "--image",
        default="ecrum19/vcf-rdfizer",
        help="Docker image repo (no tag) or full image reference",
    )
    parser.add_argument(
        "-v",
        "--image-version",
        default=None,
        help="Image tag/version to use (e.g. 1.2.3). Defaults to 'latest' if omitted and --image has no tag.",
    )
    parser.add_argument("-b", "--build", action="store_true", help="Force docker build")
    parser.add_argument("-B", "--no-build", action="store_true", help="Fail if image missing")
    parser.add_argument(
        "-n",
        "--out-name",
        default="rdf",
        help="Fallback output directory/file basename when a TSV basename cannot be inferred",
    )
    parser.add_argument(
        "-c",
        "--compression",
        default="gzip,brotli,hdt",
        help="Compression methods (gzip,brotli,hdt,hdt_gzip,hdt_brotli,none)",
    )
    parser.add_argument("-k", "--keep-tsv", action="store_true", help="Keep TSV intermediates")
    parser.add_argument(
        "-e",
        "--estimate-size",
        action="store_true",
        help="Print a rough storage estimate before running conversion",
    )
    parser.add_argument(
        "-R",
        "--keep-rdf",
        "--keep_rdf",
        action="store_true",
        help="Keep raw N-Triples outputs after compression in full mode",
    )
    args = parser.parse_args()

    if args.build and args.no_build:
        eprint("Error: --build and --no-build are mutually exclusive.")
        return 2

    repo_root = Path(__file__).resolve().parent
    out_root = Path(args.out).expanduser().resolve()
    out_dir = out_root
    tsv_dir = out_root / ".intermediate" / "tsv"
    metrics_root = out_root / "run_metrics"
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    metrics_dir = metrics_root / run_id
    mode = args.mode

    step1_label = "Step 1/5" if mode == "full" else "Step 1/3"

    try:
        # Mode-specific argument validation and canonical path resolution.
        if mode == "full":
            if args.input is None:
                raise ValueError("--input is required in --mode full")
            if args.rdf_layout is None:
                raise ValueError("--rdf-layout is required in --mode full (aggregate|batch)")
            input_path = Path(args.input).expanduser().resolve()
            (
                input_mount_dir,
                container_inputs,
                input_metrics_target,
                expected_prefixes,
            ) = resolve_input_snapshot(input_path)
            if args.rules is None:
                rules_path = resolve_default_rules_path(repo_root)
            else:
                rules_path = Path(args.rules).expanduser().resolve()
            if not rules_path.exists() or not rules_path.is_file():
                raise ValueError(f"rules file not found: {rules_path}")
            validate_mode_dirs([out_root, out_dir, tsv_dir, metrics_root])
            parse_compression_methods(args.compression)
        elif mode == "compress":
            if not args.rdf:
                raise ValueError("--rdf is required in --mode compress")
            rdf_path = Path(args.rdf).expanduser().resolve()
            if not rdf_path.exists() or not rdf_path.is_file():
                raise ValueError(f"RDF input file not found: {rdf_path}")
            if rdf_path.suffix != ".nt":
                raise ValueError("Compression input must be a .nt file")
            methods = parse_compression_methods(args.compression)
            validate_mode_dirs([out_root, out_dir, metrics_root])
        else:
            if not args.compressed_input:
                raise ValueError("--compressed-input is required in --mode decompress")
            compressed_path = Path(args.compressed_input).expanduser().resolve()
            if not compressed_path.exists() or not compressed_path.is_file():
                raise ValueError(f"Compressed input file not found: {compressed_path}")
            fmt = detect_compressed_format(compressed_path)
            validate_mode_dirs([out_root, out_dir, metrics_root])
            if args.decompress_out is None:
                default_name = default_decompressed_name(compressed_path, fmt)
                decompressed_out = out_dir / Path(default_name).stem / default_name
            else:
                decompressed_out = Path(args.decompress_out).expanduser().resolve()
                if not is_within_path(decompressed_out, out_root):
                    raise ValueError(
                        f"--decompress-out must be inside output directory: {out_root}"
                    )
            if decompressed_out.exists() and decompressed_out.is_dir():
                raise ValueError(f"decompression output path is a directory: {decompressed_out}")
            if decompressed_out.parent.exists() and not decompressed_out.parent.is_dir():
                raise ValueError(
                    f"decompression output parent is not a directory: {decompressed_out.parent}"
                )
    except ValueError as exc:
        eprint(f"Error: {exc}")
        return 2

    print(f"{step1_label}: Validating inputs {success_symbol()}")

    if mode == "full" and args.estimate_size:
        # Optional coarse sizing estimate for disk-risk visibility.
        vcf_files = collect_input_vcfs(input_path)
        estimate = estimate_pipeline_sizes(vcf_files, out_dir)
        print("  Preflight size estimate (rough):")
        print(f"    - Input VCF size: {format_bytes(estimate['input_bytes'])}")
        print(f"    - Estimated TSV intermediate size: {format_bytes(estimate['tsv_bytes'])}")
        print(
            "    - Estimated RDF N-Triples size: "
            f"{format_bytes(estimate['rdf_low_bytes'])} to {format_bytes(estimate['rdf_high_bytes'])}"
        )
        print(
            f"    - Free disk space at {estimate['disk_anchor']}: {format_bytes(estimate['free_disk_bytes'])}"
        )
        if estimate["rdf_high_bytes"] > estimate["free_disk_bytes"]:
            eprint(
                "Warning: Estimated upper-bound RDF size exceeds currently free disk. "
                "You may run out of space."
            )

    wrapper_log_path = metrics_dir / "wrapper_logs" / f"{run_id}.log"
    progress_log_path = metrics_dir / "progress.log"
    execution_started = time.perf_counter()
    global _COMMAND_LOGGER
    _COMMAND_LOGGER = CommandLogger(wrapper_log_path)
    run_tracker = RunTracker(progress_log_path)
    run_tracker.mark(f"Run started (mode={mode})")
    print(f"  Detailed logs: {wrapper_log_path}")
    print(f"  Progress log: {progress_log_path}")

    result_code = 1
    total_triples = None
    resolved_image_ref = None

    def execute_mode():
        nonlocal resolved_image_ref
        # Shared preflight for all modes: Docker availability + image strategy.
        run_tracker.mark("Checking Docker availability")
        if not check_docker():
            run_tracker.mark("Docker availability check failed")
            eprint(f"See log for details: {wrapper_log_path}")
            return 2
        run_tracker.mark("Docker availability check passed")

        try:
            image_ref, version_requested = resolve_image_ref(args.image, args.image_version)
            resolved_image_ref = image_ref
        except ValueError as exc:
            run_tracker.mark(f"Image resolution failed: {exc}")
            eprint(f"Error: {exc}")
            return 2

        run_tracker.mark(f"Ensuring image available: {image_ref}")
        image_code = ensure_image_available(
            image_ref,
            step_label="Step 2/5" if mode == "full" else "Step 2/3",
            version_requested=version_requested,
            build=args.build,
            no_build=args.no_build,
            repo_root=repo_root,
            wrapper_log_path=wrapper_log_path,
        )
        if image_code != 0:
            run_tracker.mark(f"Image availability failed (code={image_code})")
            return image_code
        run_tracker.mark("Image ready")

        if mode == "full":
            tsv_write_target = tsv_dir if tsv_dir.exists() else tsv_dir.parent
            out_write_target = out_dir if out_dir.exists() else out_dir.parent
            metrics_write_target = metrics_dir if metrics_dir.exists() else metrics_dir.parent
            writable_targets = [
                (tsv_write_target, True),
                (out_write_target, True),
                (metrics_write_target, True),
                (metrics_dir / "metrics.csv", False),
            ]
        elif mode == "compress":
            out_write_target = out_dir if out_dir.exists() else out_dir.parent
            metrics_write_target = metrics_dir if metrics_dir.exists() else metrics_dir.parent
            writable_targets = [
                (out_write_target, True),
                (metrics_write_target, True),
            ]
        else:
            decompress_parent = (
                decompressed_out.parent
                if decompressed_out.parent.exists()
                else decompressed_out.parent.parent
            )
            metrics_write_target = metrics_dir if metrics_dir.exists() else metrics_dir.parent
            writable_targets = [
                (decompress_parent, True),
                (metrics_write_target, True),
            ]

        for target, is_dir in writable_targets:
            # Proactively resolve write-permission issues on mounted paths.
            if not ensure_writable_path_or_fix(
                target_path=target,
                is_dir=is_dir,
                image_ref=image_ref,
                wrapper_log_path=wrapper_log_path,
            ):
                run_tracker.mark(f"Writeability check failed for {target}")
                eprint(f"Error: cannot write to '{target}'.")
                eprint(
                    "Try fixing ownership once with: "
                    f"sudo chown -R $USER:$USER {shlex.quote(str(target if is_dir else target.parent))}"
                )
                return 1
        run_tracker.mark("Writeability checks passed")

        if mode == "full":
            # Full-mode orchestrates conversion + compression pipeline.
            return run_full_mode(
                input_mount_dir=input_mount_dir,
                container_inputs=container_inputs,
                input_metrics_target=input_metrics_target,
                expected_prefixes=expected_prefixes,
                rules_path=rules_path,
                out_dir=out_dir,
                tsv_dir=tsv_dir,
                metrics_dir=metrics_dir,
                image_ref=image_ref,
                out_name=args.out_name,
                rdf_layout=args.rdf_layout,
                compression=args.compression,
                keep_tsv=args.keep_tsv,
                keep_rdf=args.keep_rdf,
                run_id=run_id,
                timestamp=timestamp,
                wrapper_log_path=wrapper_log_path,
                run_tracker=run_tracker,
            )
        if mode == "compress":
            # Compression-only mode.
            return run_compress_mode(
                rdf_path=rdf_path,
                out_dir=out_dir,
                image_ref=image_ref,
                methods=methods,
                wrapper_log_path=wrapper_log_path,
            )
        # Decompression-only mode.
        return run_decompress_mode(
            compressed_path=compressed_path,
            decompressed_out=decompressed_out,
            image_ref=image_ref,
            wrapper_log_path=wrapper_log_path,
        )

    def _interrupt_handler(_signum, _frame):
        raise KeyboardInterrupt()

    original_sigterm = None
    if hasattr(signal, "SIGTERM"):
        original_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, _interrupt_handler)

    try:
        result_code = execute_mode()
    except KeyboardInterrupt:
        result_code = 130
        eprint("Run interrupted by user signal; starting best-effort cleanup.")
        run_tracker.mark("Run interrupted by user signal")
        if mode == "full":
            removed, failed = cleanup_interrupted_full_run(
                run_tracker=run_tracker,
                out_root=out_root,
                image_ref=resolved_image_ref,
                keep_rdf=args.keep_rdf,
                wrapper_log_path=wrapper_log_path,
            )
            eprint(
                "Interrupt cleanup summary: "
                f"removed={removed}, failed={failed}, keep_rdf={str(args.keep_rdf).lower()}"
            )
        eprint(f"Progress log: {progress_log_path}")
    finally:
        if hasattr(signal, "SIGTERM") and original_sigterm is not None:
            signal.signal(signal.SIGTERM, original_sigterm)
        # Always report/record wrapper runtime, even on failure paths.
        elapsed_seconds = time.perf_counter() - execution_started
        if mode == "full" and result_code == 0:
            total_triples = collect_full_mode_total_triples(metrics_dir, run_id)

        print(f"Run time ({mode} mode): {format_duration(elapsed_seconds)}")
        try:
            append_wrapper_timing_log(
                metrics_dir=metrics_dir,
                run_id=run_id,
                timestamp=timestamp,
                mode=mode,
                exit_code=result_code,
                elapsed_seconds=elapsed_seconds,
                total_triples=total_triples,
            )
            print(f"Timing log: {metrics_dir / 'wrapper_execution_times.csv'}")
        except OSError as exc:
            eprint(f"Warning: failed to write wrapper timing log: {exc}")

        if _COMMAND_LOGGER is not None:
            _COMMAND_LOGGER.close()
            _COMMAND_LOGGER = None
        if run_tracker is not None:
            run_tracker.mark(f"Run finished (exit_code={result_code})")
            run_tracker.close()

    return result_code


if __name__ == "__main__":
    raise SystemExit(main())
