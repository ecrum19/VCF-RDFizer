#!/usr/bin/env python3
"""VCF-RDFizer wrapper.

This module orchestrates the end-to-end Dockerized pipeline:
1) validate CLI/input state
2) convert VCF -> TSV
3) run RMLStreamer conversion
4) run selected compression/decompression operations
5) optionally regenerate the query index for an existing HDT or COTTAS file
6) persist run and compression metrics

The implementation is intentionally split into small helpers so failures can be
diagnosed at a specific stage and future workflow changes stay localized.
"""

import argparse
import csv
import gzip
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
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

try:
    from rich.console import Console
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TaskProgressColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
    )
except ImportError:  # pragma: no cover - package metadata installs Rich
    Console = None
    Progress = None
    BarColumn = None
    SpinnerColumn = None
    TaskProgressColumn = None
    TextColumn = None
    TimeElapsedColumn = None
    TimeRemainingColumn = None


RMLSTREAMER_JAR_CONTAINER = "/opt/rmlstreamer/RMLStreamer-v2.5.0-standalone.jar"
_COMMAND_LOGGER = None
_DOCKER_USE_SUDO = False
_ACTIVE_PROGRESS = None
_PROGRESS_ALLOWED = True
PROGRESS_POLL_INTERVAL_SECONDS = 0.25

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
    "exit_code_tsv",
    "wall_seconds_tsv",
    "user_seconds_tsv",
    "sys_seconds_tsv",
    "max_rss_kb_tsv",
    "tsv_output_size_bytes",
    "tsv_output_path",
]

TSV_BENCHMARK_HEADER = [
    "run_id",
    "timestamp",
    "input_vcf",
    "prefix",
    "exit_code_tsv",
    "wall_seconds_tsv",
    "user_seconds_tsv",
    "sys_seconds_tsv",
    "max_rss_kb_tsv",
    "tsv_output_size_bytes",
    "tsv_output_path",
    "tsv_time_log_path",
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
        "source_triples_hdt",
        "decoded_triples_hdt",
        "validation_hdt",
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
    "cottas_gzip": [
        "gzip_on_cottas_size_bytes",
        "exit_code_gzip_on_cottas",
        "wall_seconds_gzip_on_cottas",
        "user_seconds_gzip_on_cottas",
        "sys_seconds_gzip_on_cottas",
        "max_rss_kb_gzip_on_cottas",
    ],
    "cottas_brotli": [
        "brotli_on_cottas_size_bytes",
        "exit_code_brotli_on_cottas",
        "wall_seconds_brotli_on_cottas",
        "user_seconds_brotli_on_cottas",
        "sys_seconds_brotli_on_cottas",
        "max_rss_kb_brotli_on_cottas",
    ],
    "cottas": [
        "cottas_size_bytes",
        "exit_code_cottas",
        "wall_seconds_cottas",
        "user_seconds_cottas",
        "sys_seconds_cottas",
        "max_rss_kb_cottas",
        "source_triples_cottas",
        "decoded_triples_cottas",
        "validation_cottas",
    ],
}

HDT_SOURCE_COLUMN = "hdt_source"
VALID_COMPRESSION_METHODS = {
    "gzip",
    "brotli",
    "hdt",
    "hdt_gzip",
    "hdt_brotli",
    "cottas",
    "cottas_gzip",
    "cottas_brotli",
}
HDT_COMPRESSION_METHODS = {"hdt", "hdt_gzip", "hdt_brotli"}
COTTAS_COMPRESSION_METHODS = {"cottas", "cottas_gzip", "cottas_brotli"}
PARTITIONED_COMPRESSION_METHODS = HDT_COMPRESSION_METHODS | COTTAS_COMPRESSION_METHODS
RDF_COMPRESSION_CHOICES = {"gzip", "brotli"}
REPRESENTATION_CHOICES = {"hdt", "cottas"}
ARTIFACT_COMPRESSION_CHOICES = {"gzip", "brotli"}
DEFAULT_RDF_COMPRESSION = "gzip,brotli"
DEFAULT_REPRESENTATIONS = "hdt"
DEFAULT_ARTIFACT_COMPRESSION = "none"
HDT_STRATEGY_CHOICES = {"auto", "single", "partitioned"}
DEFAULT_HDT_STRATEGY = "auto"
RDF_STORAGE_MODES = {"space-optimized", "plain"}
DEFAULT_CHUNK_TARGET_BYTES = 512 * 1024 * 1024
DEFAULT_CHUNK_MIN_BYTES = 128 * 1024 * 1024
DEFAULT_CHUNK_MAX_BYTES = 1024 * 1024 * 1024
HDT_INDEX_HELPER_CONTAINER = "/opt/vcf-rdfizer/ensure_hdt_index.sh"
COTTAS_TOOL_CONTAINER = "/opt/vcf-rdfizer/cottas_tool.py"
PARTITIONED_COMPRESSION_RUNNER_CONTAINER = "/opt/vcf-rdfizer/partitioned_compression.py"
SAMPLE_CALLS_HEADER = [
    "SOURCE_FILE",
    "ROW_ID",
    "SAMPLE_INDEX",
    "SAMPLE_ID",
    "SAMPLE_URI_ID",
    "SAMPLE_PAYLOAD",
]
SAMPLE_FORMAT_HEADER = [
    "SOURCE_FILE",
    "ROW_ID",
    "SAMPLE_INDEX",
    "SAMPLE_ID",
    "SAMPLE_URI_ID",
    "FORMAT_INDEX",
    "FORMAT_KEY",
    "FORMAT_VALUE",
]
CANONICAL_SAMPLE_RULE_MARKERS = (
    "<#VariantCallToSampleLinkMap>",
    "<#SampleCallMap>",
    "<#SampleCallToFormatValueLinkMap>",
    "<#FormatFieldValueMap>",
)
CANONICAL_SAMPLE_RULE_FRAGMENTS = (
    'rr:template "file://{SOURCE_FILE}#call/{ROW_ID}"',
    "rr:predicate vcfr:hasSampleCall",
    'rr:template "file://{SOURCE_FILE}#sample/{ROW_ID}/{SAMPLE_URI_ID}"',
    "rr:class vcfr:SampleCall",
    "rr:predicate vcfr:sampleId",
    'rml:reference "SAMPLE_ID"',
    "rr:predicate vcfr:hasFormatValue",
    'rr:template "file://{SOURCE_FILE}#sample/{ROW_ID}/{SAMPLE_URI_ID}/fmt/{FORMAT_KEY}"',
    "rr:class vcfr:FormatFieldValue",
    "rr:predicate vcfr:fieldValue",
    'rml:reference "FORMAT_VALUE"',
)
VCFR_NAMESPACE = "https://w3id.org/vcf-rdfizer/vocab#"
RDF_TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
XSD_POSITIVE_INTEGER_URI = "http://www.w3.org/2001/XMLSchema#positiveInteger"
SAMPLE_RDF_BUFFER_BYTES = 8 * 1024 * 1024
SAMPLE_REPRESENTATION_CHOICES = {"expanded", "condensed"}
# This is an internal rules-compatibility value, not a third public
# representation. It means that custom helper TSV rows must be materialized.
SAMPLE_HELPER_STRATEGY_MATERIALIZED = "expanded"
METRICS_LAYOUT_VERSION = 2


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

        if _ACTIVE_PROGRESS is not None and _ACTIVE_PROGRESS.enabled:
            process = subprocess.Popen(
                cmd,
                cwd=cwd,
                env=env,
                stdout=self._handle,
                stderr=self._handle,
                text=True,
            )
            exit_code = _ACTIVE_PROGRESS.wait_for_process(process)
        else:
            result = subprocess.run(
                cmd,
                cwd=cwd,
                env=env,
                stdout=self._handle,
                stderr=self._handle,
                text=True,
            )
            exit_code = result.returncode
        self._handle.write(f"[exit {exit_code}]\n")
        self._handle.flush()
        return exit_code

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


def progress_ui_enabled() -> bool:
    """Return whether transient Rich progress output should be displayed."""
    if not _PROGRESS_ALLOWED or Progress is None or Console is None:
        return False
    if os.environ.get("VCF_RDFIZER_NO_PROGRESS"):
        return False
    if os.environ.get("CI"):
        return False
    stream = getattr(sys, "stderr", None)
    isatty = getattr(stream, "isatty", None)
    return bool(callable(isatty) and isatty())


def progress_events_enabled() -> bool:
    """Return whether progress sidecars should be consumed at all.

    Rich needs a TTY to redraw a spinner, but compression is often launched
    through ``tee``, a scheduler, or a remote shell with redirected stderr.
    Keep collecting the same low-volume events in those cases so
    ``ProgressSession`` can render readable line-based status instead.
    """
    if not _PROGRESS_ALLOWED:
        return False
    if os.environ.get("VCF_RDFIZER_NO_PROGRESS"):
        return False
    return not os.environ.get("CI")


class ProgressSession:
    """Render low-volume progress events from one Docker operation with Rich.

    The container writes newline-delimited JSON to ``path``. The host polls the
    small sidecar while the Docker process runs, keeping command logs and
    binary subprocess stdout separate from terminal UI output.
    """

    def __init__(self, path: Path | None, label: str):
        self.path = path
        self.label = label
        self.enabled = progress_events_enabled()
        self.rich_enabled = self.enabled and progress_ui_enabled()
        self._offset = 0
        self._progress = None
        self._starter_task = None
        self._tasks: dict[str, int] = {}
        self._previous = None
        self._plain_event_states: dict[str, tuple[str, object, object]] = {}

    def __enter__(self):
        global _ACTIVE_PROGRESS
        self._previous = _ACTIVE_PROGRESS
        _ACTIVE_PROGRESS = self
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.unlink(missing_ok=True)

        if not self.enabled:
            return self
        if not self.rich_enabled:
            eprint(f"{self.label}: started")
            return self

        console = Console(stderr=True, highlight=False)
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}", style="progress.description", markup=False),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("{task.fields[detail]}", markup=False),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            refresh_per_second=8,
            transient=True,
            auto_refresh=False,
        )
        self._starter_task = self._progress.add_task(
            self.label,
            total=None,
            detail="starting",
        )
        self._progress.start()
        return self

    @staticmethod
    def _number(value):
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return None
        return None

    @staticmethod
    def _detail(event: dict) -> str:
        completed = ProgressSession._number(event.get("completed"))
        unit = event.get("unit")
        if completed is None:
            detail = ""
        elif unit == "bytes":
            detail = f"{format_bytes(int(completed))}"
        elif unit == "triples":
            detail = f"{int(completed):,} triples"
        elif unit == "chunks":
            detail = f"{int(completed):,} chunks"
        elif unit == "parts":
            detail = f"{int(completed):,} parts"
        else:
            detail = f"{int(completed):,}"

        parts = ProgressSession._number(event.get("parts"))
        if parts is not None and unit != "parts":
            detail = f"{detail} · {int(parts):,} parts" if detail else f"{int(parts):,} parts"
        extra = event.get("detail")
        if extra:
            detail = f"{detail} · {extra}" if detail else str(extra)
        return detail

    def _update_event(self, event: dict):
        stage = str(event.get("stage") or "work")
        phase = str(event.get("phase") or "working")
        if self._progress is None:
            # A redirected terminal cannot host a redrawable Rich spinner.
            # Emit compact, state-changing lines instead so lengthy
            # compression still visibly advances in a terminal or log.
            completed = event.get("completed")
            total = event.get("total")
            state = (phase, completed, total)
            if self._plain_event_states.get(stage) != state:
                self._plain_event_states[stage] = state
                detail = self._detail(event)
                suffix = f" — {detail}" if detail else ""
                eprint(f"  {self.label}: {stage} {phase}{suffix}")
            return
        task_id = self._tasks.get(stage)
        if task_id is None:
            if self._starter_task is not None:
                self._progress.remove_task(self._starter_task)
                self._starter_task = None
            total = self._number(event.get("total"))
            task_id = self._progress.add_task(
                f"{stage}: {phase}",
                total=total if total is not None else None,
                detail=self._detail(event),
            )
            self._tasks[stage] = task_id
            return

        update = {
            "description": f"{stage}: {phase}",
            "detail": self._detail(event),
        }
        if "total" in event:
            total = self._number(event.get("total"))
            update["total"] = total if total is not None else None
        completed = self._number(event.get("completed"))
        if completed is not None:
            update["completed"] = completed
        self._progress.update(task_id, **update)

    def poll_events(self):
        """Consume complete JSONL events without retaining the event stream."""
        if not self.enabled or self.path is None or not self.path.exists():
            return
        try:
            with self.path.open("rb") as handle:
                handle.seek(self._offset)
                data = handle.read()
        except OSError:
            return

        consumed = 0
        for raw_line in data.splitlines(keepends=True):
            if not raw_line.endswith(b"\n"):
                break
            consumed += len(raw_line)
            try:
                event = json.loads(raw_line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(event, dict):
                self._update_event(event)
        self._offset += consumed

    def wait_for_process(self, process):
        """Wait while polling progress, without a second monitor thread."""
        while process.poll() is None:
            self.poll_events()
            if self._progress is not None:
                self._progress.refresh()
            time.sleep(PROGRESS_POLL_INTERVAL_SECONDS)
        self.poll_events()
        if self._progress is not None:
            self._progress.refresh()
        return process.returncode

    def __exit__(self, exc_type, exc_value, traceback):
        global _ACTIVE_PROGRESS
        self.poll_events()
        if self._progress is not None:
            self._progress.stop()
        elif self.enabled:
            eprint(f"{self.label}: finished")
        if self.path is not None:
            try:
                self.path.unlink()
            except OSError:
                pass
        _ACTIVE_PROGRESS = self._previous
        return False


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
    if _ACTIVE_PROGRESS is not None and _ACTIVE_PROGRESS.enabled:
        process = subprocess.Popen(
            cmd,
            cwd=cwd,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return _ACTIVE_PROGRESS.wait_for_process(process)
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


def docker_hdt_index_env_args() -> list[str]:
    """Forward an optional host-side hdtc memory budget into Docker."""
    memory_limit = os.environ.get("HDT_INDEX_MEMORY_LIMIT", "").strip()
    if not memory_limit:
        return []
    return ["-e", f"HDT_INDEX_MEMORY_LIMIT={memory_limit}"]


def docker_hdt_merge_env_args() -> list[str]:
    """Forward an optional host-side hdtc merge memory budget into Docker."""
    memory_limit = os.environ.get("HDT_MERGE_MEMORY_LIMIT", "").strip()
    if not memory_limit:
        return []
    return ["-e", f"HDT_MERGE_MEMORY_LIMIT={memory_limit}"]


def docker_cottas_merge_env_args() -> list[str]:
    """Forward an optional bounded COTTAS streaming-merge batch size."""
    batch_rows = os.environ.get("COTTAS_MERGE_BATCH_ROWS", "").strip()
    return ["-e", f"COTTAS_MERGE_BATCH_ROWS={batch_rows}"] if batch_rows else []


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


def find_hdt_index_sidecar(hdt_path: Path) -> Path | None:
    """Return the non-empty canonical HDT versioned index sidecar."""
    for candidate in sorted(hdt_path.parent.glob(f"{hdt_path.name}.index.*")):
        size = file_size_bytes(candidate)
        if size is not None and size > 0:
            return candidate
    return None


def write_nt_chunk(chunk_path: Path, source_paths: list[Path]) -> int:
    """Concatenate one or more RDF files into a chunk-local `.nt` input."""
    ensure_dir(chunk_path.parent)
    total_bytes = 0
    with chunk_path.open("w", encoding="utf-8") as out_handle:
        for source_path in source_paths:
            with source_path.open("r", encoding="utf-8", errors="replace") as in_handle:
                for line in in_handle:
                    out_handle.write(line)
                    total_bytes += len(line.encode("utf-8"))
    return total_bytes


def split_nt_file_for_hdt(
    source_path: Path,
    chunk_dir: Path,
    *,
    target_bytes: int,
    max_bytes: int,
) -> list[Path]:
    """Split an oversized RDF file into line-preserving chunk files for HDT conversion."""
    ensure_dir(chunk_dir)
    chunk_paths: list[Path] = []
    chunk_handle = None
    chunk_path = None
    chunk_size = 0

    def open_chunk(index: int):
        path = chunk_dir / f"{source_path.stem}.split-{index:05d}.nt"
        return path, path.open("w", encoding="utf-8")

    try:
        with source_path.open("r", encoding="utf-8", errors="replace") as in_handle:
            chunk_index = 0
            for line in in_handle:
                line_size = len(line.encode("utf-8"))
                if chunk_handle is None:
                    chunk_path, chunk_handle = open_chunk(chunk_index)
                    chunk_paths.append(chunk_path)
                    chunk_size = 0
                    chunk_index += 1
                elif chunk_size > 0 and (
                    chunk_size >= target_bytes or chunk_size + line_size > max_bytes
                ):
                    chunk_handle.close()
                    chunk_path, chunk_handle = open_chunk(chunk_index)
                    chunk_paths.append(chunk_path)
                    chunk_size = 0
                    chunk_index += 1

                chunk_handle.write(line)
                chunk_size += line_size
    finally:
        if chunk_handle is not None and not chunk_handle.closed:
            chunk_handle.close()

    return chunk_paths or [source_path]


def iter_rdf_binary_lines(path: Path):
    """Yield RDF records from plain or gzip-compressed line-oriented RDF."""
    opener = gzip.open if path.name.endswith(".gz") else Path.open
    with opener(path, "rb") as handle:
        for line in handle:
            yield line


def plan_record_safe_rdf_chunks(
    source_paths: list[Path],
    chunk_dir: Path,
    *,
    target_bytes: int,
    min_bytes: int,
    max_bytes: int,
    guide_path: Path | None = None,
) -> tuple[list[Path], dict]:
    """Create bounded RDF chunks without splitting a line-level statement.

    The guide is written as boundaries are discovered during this single
    sequential pass. A separate pre-scan would read/decompress the complete
    aggregate twice, so the guide and chunk files are produced together.
    Logical offsets are uncompressed offsets and therefore work for both plain
    and gzip-backed aggregate sources.
    """
    if not source_paths:
        return [], {"source_file_count": 0, "chunk_count": 0, "chunk_input_bytes": 0}
    if target_bytes <= 0 or min_bytes <= 0 or max_bytes <= 0:
        raise ValueError("RDF chunk sizes must be positive.")
    if min_bytes > target_bytes or target_bytes > max_bytes:
        raise ValueError("RDF chunk sizes must satisfy min <= target <= max.")

    ensure_dir(chunk_dir)
    chunk_paths: list[Path] = []
    guide_chunks: list[dict] = []
    chunk_handle = None
    chunk_path = None
    chunk_size = 0
    chunk_start_offset = 0
    chunk_start_record = 0
    logical_offset = 0
    record_count = 0
    total_bytes = 0
    chunk_index = 0

    def close_chunk():
        nonlocal chunk_handle, chunk_path, chunk_size
        if chunk_handle is None or chunk_path is None:
            return
        chunk_handle.close()
        chunk_paths.append(chunk_path)
        guide_chunks.append(
            {
                "chunk_id": len(guide_chunks),
                "path": str(chunk_path),
                "start_record": chunk_start_record,
                "end_record": record_count,
                "start_uncompressed_byte": chunk_start_offset,
                "end_uncompressed_byte": logical_offset,
                "record_count": record_count - chunk_start_record,
                "payload_bytes": chunk_size,
            }
        )
        chunk_handle = None
        chunk_path = None
        chunk_size = 0

    try:
        for source_path in source_paths:
            for line in iter_rdf_binary_lines(source_path):
                if not line.endswith(b"\n"):
                    raise ValueError(
                        f"RDF source contains a non-line-terminated record: {source_path}"
                    )
                line_size = len(line)
                if chunk_handle is None:
                    chunk_path = chunk_dir / f"chunk-{chunk_index:05d}.nt"
                    chunk_index += 1
                    chunk_handle = chunk_path.open("wb")
                    chunk_start_offset = logical_offset
                    chunk_start_record = record_count
                elif chunk_size > 0 and (
                    (chunk_size >= target_bytes and chunk_size >= min_bytes)
                    or chunk_size + line_size > max_bytes
                ):
                    close_chunk()
                    chunk_path = chunk_dir / f"chunk-{chunk_index:05d}.nt"
                    chunk_index += 1
                    chunk_handle = chunk_path.open("wb")
                    chunk_start_offset = logical_offset
                    chunk_start_record = record_count

                chunk_handle.write(line)
                chunk_size += line_size
                logical_offset += line_size
                total_bytes += line_size
                record_count += 1
    finally:
        close_chunk()

    plan = {
        "source_file_count": len(source_paths),
        "source_paths": [str(path) for path in source_paths],
        "chunk_count": len(chunk_paths),
        "chunk_input_bytes": total_bytes,
        "record_count": record_count,
        "target_chunk_bytes": target_bytes,
        "min_chunk_bytes": min_bytes,
        "max_chunk_bytes": max_bytes,
        "chunks": guide_chunks,
    }
    if guide_path is not None:
        ensure_dir(guide_path.parent)
        plan["guide_path"] = str(guide_path)
        guide_path.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    return chunk_paths, plan


def plan_partitioned_hdt_chunks(
    rdf_paths: list[Path],
    chunk_dir: Path,
    *,
    target_bytes: int,
    min_bytes: int,
    max_bytes: int,
) -> tuple[list[Path], dict]:
    """Plan chunk-local `.nt` inputs for partitioned HDT generation.

    The goal is to keep HDT conversion work units small enough to be fast,
    while also avoiding the pathological "many tiny HDTs" case. Existing RDF
    part files are treated as the first split boundary, and only oversized
    parts are re-split on line boundaries.
    """
    ensure_dir(chunk_dir)

    prepared_inputs: list[tuple[Path, int]] = []
    for rdf_path in rdf_paths:
        size = int(file_size_bytes(rdf_path) or 0)
        if size <= max_bytes:
            prepared_inputs.append((rdf_path, size))
            continue
        for split_path in split_nt_file_for_hdt(
            rdf_path,
            chunk_dir / "_split_inputs",
            target_bytes=target_bytes,
            max_bytes=max_bytes,
        ):
            prepared_inputs.append((split_path, int(file_size_bytes(split_path) or 0)))

    chunk_groups: list[list[tuple[Path, int]]] = []
    current_group: list[tuple[Path, int]] = []
    current_size = 0
    for path, size in prepared_inputs:
        if not current_group:
            current_group = [(path, size)]
            current_size = size
            continue
        if current_size < min_bytes or current_size + size <= target_bytes:
            current_group.append((path, size))
            current_size += size
            continue
        chunk_groups.append(current_group)
        current_group = [(path, size)]
        current_size = size
    if current_group:
        chunk_groups.append(current_group)

    chunk_inputs: list[Path] = []
    chunk_input_bytes = 0
    for chunk_index, group in enumerate(chunk_groups):
        chunk_path = chunk_dir / f"chunk-{chunk_index:05d}.nt"
        chunk_input_bytes += write_nt_chunk(chunk_path, [path for path, _size in group])
        chunk_inputs.append(chunk_path)

    plan = {
        "source_file_count": len(rdf_paths),
        "prepared_input_count": len(prepared_inputs),
        "chunk_count": len(chunk_inputs),
        "chunk_input_bytes": chunk_input_bytes,
    }
    return chunk_inputs, plan


def count_triples_in_nt_files(paths: list[Path]) -> int | None:
    """Count triples in RDF line-oriented files as a fallback when metrics are missing."""
    total = 0
    matched_any = False
    pattern = re.compile(r"^\s*[^#].*\.\s*$")
    for path in paths:
        if not path.exists() or not path.is_file():
            continue
        try:
            opener = gzip.open if path.name.endswith(".gz") else Path.open
            with opener(path, "rt", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    if pattern.match(line):
                        total += 1
                        matched_any = True
        except OSError:
            return None
    return total if matched_any else 0


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
        metrics_dir / "stages" / "conversion" / f"{safe_name}.json",
        metrics_dir / "conversion_metrics" / safe_name / f"{run_id}.json",
        metrics_dir / "conversion_metrics" / safe_name / run_id,
        # Backward compatibility with older artifact names:
        metrics_dir / f"conversion-metrics-{safe_name}-{run_id}.json",
    ]
    metrics_json = next((path for path in candidates if path.exists()), None)
    if metrics_json is not None:
        try:
            payload = json.loads(metrics_json.read_text(encoding="utf-8"))
            artifacts = payload.get("artifacts", {})
            triples = artifacts.get("output_triples")
            if isinstance(triples, dict):
                value = _as_int(triples.get("TOTAL"))
            else:
                value = _as_int(triples)
            if value is not None:
                return value
        except (OSError, json.JSONDecodeError):
            pass

    # Fallback to conversion CSV if JSON metric is unavailable.
    metrics_csv = metrics_dir / "metrics.csv"
    if not metrics_csv.exists():
        return None
    try:
        with metrics_csv.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if row.get("run_id") == run_id and row.get("output_name") == output_name:
                    return _as_int(row.get("output_triples"))
    except OSError:
        return None
    return None


def collect_full_mode_total_triples(metrics_dir: Path, run_id: str):
    """Aggregate TOTAL triple counts across all conversion metrics files for a run."""
    total = 0
    found = False
    candidate_files = []
    candidate_files.extend(sorted((metrics_dir / "stages" / "conversion").glob("*.json")))
    candidate_files.extend(sorted(metrics_dir.glob("conversion_metrics/*/*")))
    # Backward compatibility with older artifact names:
    candidate_files.extend(sorted(metrics_dir.glob(f"conversion-metrics-*-{run_id}.json")))

    for metrics_json in candidate_files:
        if metrics_json.parent.name != "conversion" and (
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
    """Write per-input failure summary for multi-input modes."""
    report_dir = metrics_dir / "reports"
    ensure_dir(report_dir)
    report_path = report_dir / "failed_inputs.csv"
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


def write_index_warnings_report(*, metrics_dir: Path, run_id: str, warnings: list[dict]):
    """Write non-fatal full-run HDT/COTTAS index warnings as JSON."""
    report_dir = metrics_dir / "reports"
    ensure_dir(report_dir)
    report_path = report_dir / "index_warnings.json"
    payload = {
        "run_id": run_id,
        "warning_count": len(warnings),
        "warnings": warnings,
    }
    report_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
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

        if method in HDT_COMPRESSION_METHODS | COTTAS_COMPRESSION_METHODS:
            source = str(result.get("source", "")).strip()
            if not source and method in HDT_COMPRESSION_METHODS and "hdt" in results:
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


def rdf_output_basename(path: Path) -> str:
    """Return the common output basename for ``.nt`` and ``.nt.gz`` RDF."""
    if path.name.endswith(".nt.gz"):
        return path.name[: -len(".nt.gz")]
    if path.name.endswith(".nt"):
        return path.name[: -len(".nt")]
    return path.stem


def compression_artifact_name_for_method(path: Path, method: str) -> str:
    """Compute expected compressed artifact filename for a method."""
    if path.name.endswith(".nt.gz"):
        stem = rdf_output_basename(path)
        ext = "nt"
    else:
        stem = rdf_output_basename(path)
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
    if method == "cottas":
        return f"{stem}.cottas"
    if method == "cottas_gzip":
        return f"{stem}.cottas.gz"
    if method == "cottas_brotli":
        return f"{stem}.cottas.br"
    return f"{stem}.{method}"


def planned_output_paths(
    *,
    out_dir: Path,
    output_name: str,
    rdf_name: str | None,
    methods: list[str],
    partitioned: bool,
) -> set[Path]:
    """List final output paths that a compression plan would create."""
    target_dir = out_dir / output_name
    planned: set[Path] = set()
    if rdf_name is not None:
        planned.add(target_dir / rdf_name)

    rdf_path = Path(rdf_name or f"{output_name}.nt")
    planned.update(
        target_dir / compression_artifact_name_for_method(rdf_path, method)
        for method in methods
    )
    if any(method in HDT_COMPRESSION_METHODS for method in methods):
        planned.add(target_dir / f"{output_name}.hdt")
        # The pinned Java-free indexer generates this canonical sidecar.
        planned.add(target_dir / f"{output_name}.hdt.index.v1-1")
    if any(method in COTTAS_COMPRESSION_METHODS for method in methods):
        planned.add(target_dir / f"{output_name}.cottas")
    if partitioned:
        planned.add(target_dir / f".{safe_metrics_name(output_name)}.partitioned-results.json")
    return planned


def validate_no_output_collisions(plans: dict[str, set[Path]]):
    """Fail before execution rather than overwriting planned output artifacts."""
    claimed_by: dict[Path, list[str]] = {}
    existing: set[Path] = set()
    for owner, paths in plans.items():
        for path in paths:
            claimed_by.setdefault(path, []).append(owner)
            if path.exists():
                existing.add(path)
            if path.parent.exists() and not path.parent.is_dir():
                existing.add(path.parent)
            if ".hdt.index." in path.name:
                hdt_name = path.name.split(".index.", 1)[0]
                existing.update(path.parent.glob(f"{hdt_name}.index.*"))

    duplicate_plans = [path for path, owners in claimed_by.items() if len(owners) > 1]
    if not existing and not duplicate_plans:
        return

    conflicts = sorted({*existing, *duplicate_plans}, key=lambda path: str(path))
    listed = ", ".join(str(path) for path in conflicts)
    raise ValueError(
        "Refusing to overwrite existing output file(s): "
        f"{listed}. VCF-RDFizer does not overwrite outputs; choose a different "
        "--out directory or rename/remove the conflicting file(s) and try again."
    )


def compression_method_label_for_path(path: Path, method: str) -> str:
    """Return human-readable compression method label for a path."""
    ext = path.suffix.lstrip(".") or "nt"
    if path.name.endswith(".nt.gz"):
        ext = "nt"
    labels = {
        "gzip": f"gzip (.{ext}.gz)",
        "brotli": f"brotli (.{ext}.br)",
        "hdt": "HDT (.hdt)",
        "hdt_gzip": "gzip-on-HDT (.hdt.gz)",
        "hdt_brotli": "brotli-on-HDT (.hdt.br)",
        "cottas": "COTTAS (.cottas)",
        "cottas_gzip": "gzip-on-COTTAS (.cottas.gz)",
        "cottas_brotli": "brotli-on-COTTAS (.cottas.br)",
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
    keep_rmlstreamer_rdf_output: bool,
    wrapper_log_path: Path,
):
    """Best-effort cleanup for full-mode interruption.

    Removes tracked intermediates (and raw RDF artifacts when
    `keep_rmlstreamer_rdf_output` is not set),
    then records a compact cleanup summary in the run progress log.
    """
    targets: set[Path] = set(run_tracker.intermediate_paths)
    if not keep_rmlstreamer_rdf_output:
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
        "Interrupt cleanup finished: "
        f"removed={removed}, failed={failed}, "
        "keep_rmlstreamer_rdf_output="
        f"{str(keep_rmlstreamer_rdf_output).lower()}"
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


def write_sample_support_headers(sample_calls_tsv: Path, sample_format_tsv: Path):
    """Create empty sample helper tables with their canonical TSV headers."""
    sample_calls_tsv.parent.mkdir(parents=True, exist_ok=True)
    sample_format_tsv.parent.mkdir(parents=True, exist_ok=True)
    with sample_calls_tsv.open("w", newline="", encoding="utf-8") as sample_calls_handle, \
        sample_format_tsv.open("w", newline="", encoding="utf-8") as sample_format_handle:
        csv.writer(sample_calls_handle, delimiter="\t").writerow(SAMPLE_CALLS_HEADER)
        csv.writer(sample_format_handle, delimiter="\t").writerow(SAMPLE_FORMAT_HEADER)


def sample_support_strategy(rules_path: Path) -> str:
    """Choose no, streamed, or materialized sample handling for one mapping file.

    The built-in four sample maps can be emitted directly as N-Triples without
    writing their enormous Cartesian helper TSVs. A custom mapping with extra
    helper-table consumers retains the materialized TSV behavior.
    """
    text = rules_path.read_text(encoding="utf-8")
    calls_refs = text.count('/data/tsv/sample_calls.tsv')
    format_refs = text.count('/data/tsv/sample_format_values.tsv')
    if calls_refs == 0 and format_refs == 0:
        return "none"
    if (
        calls_refs == 2
        and format_refs == 2
        and all(marker in text for marker in CANONICAL_SAMPLE_RULE_MARKERS)
        and all(fragment in text for fragment in CANONICAL_SAMPLE_RULE_FRAGMENTS)
    ):
        return "stream"
    return SAMPLE_HELPER_STRATEGY_MATERIALIZED


@dataclass(frozen=True)
class SampleWorkflow:
    """One mutually exclusive sample-representation execution plan."""

    representation: str
    helper_strategy: str
    emitter: str | None


def resolve_sample_workflow(representation: str, rules_path: Path) -> SampleWorkflow:
    """Resolve rules compatibility into exactly one sample workflow.

    Expanded mode preserves custom helper-table mappings. Condensed mode emits its
    RDF directly from records.tsv; it cannot safely coexist with custom rules
    that consume materialized helper tables because that would execute both
    representations and reintroduce semantic inflation.
    """
    if representation not in SAMPLE_REPRESENTATION_CHOICES:
        choices = ", ".join(sorted(SAMPLE_REPRESENTATION_CHOICES))
        raise ValueError(
            f"unsupported sample representation '{representation}'; choose {choices}"
        )

    rules_strategy = sample_support_strategy(rules_path)
    if representation == "expanded":
        if rules_strategy == "stream":
            return SampleWorkflow("expanded", "header-only", "expanded")
        if rules_strategy == SAMPLE_HELPER_STRATEGY_MATERIALIZED:
            return SampleWorkflow("expanded", SAMPLE_HELPER_STRATEGY_MATERIALIZED, None)
        return SampleWorkflow("expanded", "none", None)

    if rules_strategy == SAMPLE_HELPER_STRATEGY_MATERIALIZED:
        raise ValueError(
            "--sample-representation condensed cannot be combined with custom rules "
            "that consume materialized sample_calls.tsv or sample_format_values.tsv tables. "
            "Remove those materialized helper-table consumers or use expanded mode."
        )
    helper_strategy = "header-only" if rules_strategy == "stream" else "none"
    return SampleWorkflow("condensed", helper_strategy, "condensed")


def _set_max_csv_field_size():
    """Allow chromosome-scale multi-sample payload columns in Python's CSV reader."""
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10


def _sample_id_to_uri_id(sample_id: str, fallback_index: int) -> str:
    candidate = re.sub(r"[^A-Za-z0-9._~-]+", "_", sample_id).strip("_")
    return candidate or f"sample_{fallback_index}"


def _rml_uri_component(value: str) -> str:
    """Match RMLStreamer's Java URLEncoder-based template substitution."""
    encoded = quote_plus(value, safe="*-._", encoding="utf-8", errors="strict")
    # urllib follows current RFC rules and always leaves '~' unescaped, whereas
    # java.net.URLEncoder (used by RMLStreamer 2.5.0) encodes it.
    return encoded.replace("+", "%20").replace("~", "%7E")


def _ntriples_string_literal(value: str) -> str:
    """Serialize an RDF 1.1 plain/xsd:string literal for N-Triples."""
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


def _ntriples_literal(value: str) -> str:
    literal = _ntriples_string_literal(value)
    if value == ".":
        literal += f"^^<{VCFR_NAMESPACE}Null>"
    return literal


@dataclass(frozen=True)
class SampleColumn:
    """One reusable VCF sample column."""

    index: int
    sample_id: str
    uri_id: str


@dataclass(frozen=True)
class ParsedSampleRecord:
    """One VCF record with FORMAT keys aligned to all sample columns."""

    source_file: str
    row_id: str
    format_keys: tuple[str, ...]
    sample_payloads: tuple[str, ...]
    sample_values: tuple[tuple[str, ...], ...]


class SampleRecordStream:
    """Read a records.tsv sample block once and expose a stable sample schema."""

    def __init__(self, records_tsv: Path):
        self.records_tsv = records_tsv
        self.columns: tuple[SampleColumn, ...] = ()
        self.source_file = ""
        self._handle = None
        self._reader = None
        self._header: list[str] = []
        self._pending_row: list[str] | None = None

    def __enter__(self):
        _set_max_csv_field_size()
        self._handle = self.records_tsv.open(newline="", encoding="utf-8")
        self._reader = csv.reader(self._handle, delimiter="\t")
        self._header = next(self._reader, None) or []
        self._pending_row = self._next_nonempty_row()
        if self._pending_row:
            self.source_file = self._pending_row[0] if self._pending_row else ""

        sample_header = self._header[-1].strip() if len(self._header) >= 12 else ""
        declared_ids = (
            []
            if sample_header == "SAMPLES"
            else [token for token in sample_header.split() if token]
        )
        if not declared_ids and self._pending_row is not None and len(self._header) >= 12:
            samples_raw = self._pending_row[-1] if self._pending_row else ""
            payload_count = len(samples_raw.split()) if samples_raw else 0
            declared_ids = [f"SAMPLE_{index}" for index in range(1, payload_count + 1)]

        uri_id_counts: dict[str, int] = {}
        columns: list[SampleColumn] = []
        for index, sample_id in enumerate(declared_ids, start=1):
            uri_id_base = _sample_id_to_uri_id(sample_id, index)
            uri_id_counts[uri_id_base] = uri_id_counts.get(uri_id_base, 0) + 1
            occurrence = uri_id_counts[uri_id_base]
            uri_id = f"{uri_id_base}_{occurrence}" if occurrence > 1 else uri_id_base
            columns.append(SampleColumn(index, sample_id, uri_id))
        self.columns = tuple(columns)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._handle is not None:
            self._handle.close()
        self._handle = None
        self._reader = None

    def _next_nonempty_row(self) -> list[str] | None:
        if self._reader is None:
            return None
        for row in self._reader:
            if row:
                return row
        return None

    def __iter__(self):
        if self._reader is None:
            raise RuntimeError("SampleRecordStream must be used as a context manager")
        pending = self._pending_row
        self._pending_row = None
        if pending is not None:
            yield self._parse_row(pending)
        for row in self._reader:
            if row:
                yield self._parse_row(row)

    def _parse_row(self, row: list[str]) -> ParsedSampleRecord:
        if len(row) < len(self._header):
            row = row + [""] * (len(self._header) - len(row))

        source_file = row[0] if len(row) > 0 else ""
        if self.source_file and source_file != self.source_file:
            raise ValueError(
                f"records TSV mixes SOURCE_FILE values '{self.source_file}' and "
                f"'{source_file}'"
            )
        row_id = row[1] if len(row) > 1 else ""
        format_raw = row[10] if len(row) > 10 else ""
        samples_raw = row[-1] if len(row) >= 12 else ""
        declared_format_keys = format_raw.split(":") if format_raw else []
        sample_payloads = samples_raw.split() if samples_raw else []
        if len(sample_payloads) > len(self.columns):
            raise ValueError(
                f"record {row_id or '(unknown)'} contains {len(sample_payloads)} sample "
                f"payloads but the TSV header declares {len(self.columns)} sample columns"
            )
        sample_payloads.extend([""] * (len(self.columns) - len(sample_payloads)))

        raw_sample_values = [
            payload.split(":") if payload else [] for payload in sample_payloads
        ]
        total_fields = max(
            [len(declared_format_keys), *(len(values) for values in raw_sample_values)],
            default=0,
        )
        format_keys = tuple(
            declared_format_keys[index]
            if index < len(declared_format_keys) and declared_format_keys[index]
            else f"FIELD_{index + 1}"
            for index in range(total_fields)
        )
        if len(set(format_keys)) != len(format_keys):
            raise ValueError(
                f"record {row_id or '(unknown)'} contains duplicate FORMAT keys: "
                + ":".join(format_keys)
            )

        sample_values = tuple(
            tuple(values[index] if index < len(values) else "" for index in range(total_fields))
            for values in raw_sample_values
        )
        return ParsedSampleRecord(
            source_file=source_file,
            row_id=row_id,
            format_keys=format_keys,
            sample_payloads=tuple(sample_payloads),
            sample_values=sample_values,
        )


def _append_rdf_atomically(rdf_path: Path, stats: dict, producer):
    """Append generated N-Triples and restore the original artifact on failure."""
    original_size = rdf_path.stat().st_size
    opener = gzip.open if rdf_path.name.endswith(".gz") else Path.open
    output_handle = None
    buffer = bytearray()

    def emit(line: str):
        nonlocal buffer
        buffer.extend(line.encode("utf-8"))
        stats["triples"] += 1
        if len(buffer) >= SAMPLE_RDF_BUFFER_BYTES:
            output_handle.write(buffer)
            buffer = bytearray()

    try:
        output_handle = opener(rdf_path, "ab")
        producer(emit)
        if buffer:
            output_handle.write(buffer)
        output_handle.close()
        output_handle = None
        stats["appended_bytes"] = rdf_path.stat().st_size - original_size
        return stats
    except BaseException:
        if output_handle is not None:
            try:
                output_handle.close()
            except OSError:
                pass
        with rdf_path.open("r+b") as rollback_handle:
            rollback_handle.truncate(original_size)
        raise


def append_expanded_sample_rdf(
    records_tsv: Path,
    rdf_path: Path,
    *,
    progress_interval_records: int = 10_000,
) -> dict:
    """Append the expanded SampleCall/FormatFieldValue representation.

    This produces the same canonical SampleCall and FormatFieldValue triples as
    the default RML maps without materializing V*S and V*S*F helper TSV rows.
    """
    stats = {
        "representation": "expanded",
        "records": 0,
        "sample_calls": 0,
        "format_values": 0,
        "triples": 0,
        "appended_bytes": 0,
    }
    if not records_tsv.is_file():
        return stats
    if not rdf_path.is_file():
        raise FileNotFoundError(f"RDF aggregate not found for sample streaming: {rdf_path}")

    with SampleRecordStream(records_tsv) as record_stream:
        if not record_stream.columns or not record_stream.source_file:
            return stats

        def produce(emit):
            source_component = _rml_uri_component(record_stream.source_file)
            file_uri = f"file://{source_component}"
            emit(
                f"<{file_uri}> <{VCFR_NAMESPACE}representationProfile> "
                f"<{VCFR_NAMESPACE}ExpandedRepresentation> .\n"
            )
            for record in record_stream:
                row_component = _rml_uri_component(record.row_id)
                call_uri = f"{file_uri}#call/{row_component}"

                for sample_index, sample_column in enumerate(record_stream.columns):
                    sample_component = _rml_uri_component(sample_column.uri_id)
                    sample_uri = f"file://{source_component}#sample/{row_component}/{sample_component}"

                    emit(f"<{call_uri}> <{VCFR_NAMESPACE}hasSampleCall> <{sample_uri}> .\n")
                    emit(f"<{sample_uri}> <{RDF_TYPE_URI}> <{VCFR_NAMESPACE}SampleCall> .\n")
                    emit(
                        f"<{sample_uri}> <{VCFR_NAMESPACE}sampleId> "
                        f"{_ntriples_literal(sample_column.sample_id)} .\n"
                    )
                    stats["sample_calls"] += 1

                    for format_index, format_key in enumerate(record.format_keys):
                        format_value = record.sample_values[sample_index][format_index]
                        format_component = _rml_uri_component(format_key)
                        format_uri = f"{sample_uri}/fmt/{format_component}"
                        emit(f"<{sample_uri}> <{VCFR_NAMESPACE}hasFormatValue> <{format_uri}> .\n")
                        emit(f"<{format_uri}> <{RDF_TYPE_URI}> <{VCFR_NAMESPACE}FormatFieldValue> .\n")
                        if format_value:
                            emit(
                                f"<{format_uri}> <{VCFR_NAMESPACE}fieldValue> "
                                f"{_ntriples_literal(format_value)} .\n"
                            )
                        stats["format_values"] += 1

                stats["records"] += 1
                if progress_interval_records > 0 and stats["records"] % progress_interval_records == 0:
                    print(
                        "    * Sample RDF streaming: "
                        f"{stats['records']:,} variants, {stats['sample_calls']:,} calls",
                        flush=True,
                    )

        return _append_rdf_atomically(rdf_path, stats, produce)


def append_canonical_sample_rdf(
    records_tsv: Path,
    rdf_path: Path,
    *,
    progress_interval_records: int = 10_000,
) -> dict:
    """Backward-compatible name for the expanded sample RDF emitter."""
    return append_expanded_sample_rdf(
        records_tsv,
        rdf_path,
        progress_interval_records=progress_interval_records,
    )


@dataclass(frozen=True)
class FormatDefinition:
    """Structured attributes and RDF identity for one FORMAT declaration."""

    uri: str
    field_number: str
    description: str


def _parse_structured_header_fields(value: str) -> dict[str, str]:
    """Parse comma-delimited VCF header attributes while respecting quotes."""
    inner = value.strip()
    if inner.startswith("<") and inner.endswith(">"):
        inner = inner[1:-1]

    tokens: list[str] = []
    token: list[str] = []
    in_quotes = False
    escaped = False
    for character in inner:
        if escaped:
            token.append(character)
            escaped = False
        elif character == "\\" and in_quotes:
            token.append(character)
            escaped = True
        elif character == '"':
            token.append(character)
            in_quotes = not in_quotes
        elif character == "," and not in_quotes:
            tokens.append("".join(token))
            token = []
        else:
            token.append(character)
    tokens.append("".join(token))

    fields: dict[str, str] = {}
    for item in tokens:
        if "=" not in item:
            continue
        key, raw_value = item.split("=", 1)
        parsed_value = raw_value.strip()
        if len(parsed_value) >= 2 and parsed_value[0] == parsed_value[-1] == '"':
            parsed_value = parsed_value[1:-1]
            parsed_value = parsed_value.replace('\\"', '"').replace("\\\\", "\\")
        fields[key.strip()] = parsed_value
    return fields


def _load_format_definitions(header_lines_tsv: Path) -> dict[str, FormatDefinition]:
    """Map FORMAT IDs to structured definitions backed by emitted HeaderLine IRIs."""
    definitions: dict[str, FormatDefinition] = {}
    if not header_lines_tsv.is_file():
        return definitions
    _set_max_csv_field_size()
    with header_lines_tsv.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            if (row.get("HEADER_KEY") or "").upper() != "FORMAT":
                continue
            fields = _parse_structured_header_fields(row.get("HEADER_VALUE") or "")
            format_id = fields.get("ID", "").strip()
            if not format_id:
                continue
            source_component = _rml_uri_component(row.get("SOURCE_FILE") or "")
            index_component = _rml_uri_component(row.get("HEADER_INDEX") or "")
            definitions.setdefault(
                format_id,
                FormatDefinition(
                    uri=f"file://{source_component}#header/line/{index_component}",
                    field_number=fields.get("Number") or ".",
                    description=(
                        fields.get("Description")
                        or f"FORMAT field {format_id} (source declaration has no Description)"
                    ),
                ),
            )
    return definitions


def append_condensed_sample_rdf(
    records_tsv: Path,
    header_lines_tsv: Path,
    rdf_path: Path,
    *,
    progress_interval_records: int = 10_000,
) -> dict:
    """Append sample-ordered cohort matrices and FORMAT value vectors."""
    stats = {
        "representation": "condensed",
        "records": 0,
        "samples": 0,
        "matrices": 0,
        "format_vectors": 0,
        "format_definitions": 0,
        "triples": 0,
        "appended_bytes": 0,
    }
    if not records_tsv.is_file():
        return stats
    if not rdf_path.is_file():
        raise FileNotFoundError(f"RDF aggregate not found for sample streaming: {rdf_path}")

    definitions = _load_format_definitions(header_lines_tsv)
    with SampleRecordStream(records_tsv) as record_stream:
        if not record_stream.columns or not record_stream.source_file:
            return stats

        def produce(emit):
            source_component = _rml_uri_component(record_stream.source_file)
            file_uri = f"file://{source_component}"
            sample_set_uri = f"{file_uri}#samples"
            emitted_definitions: set[str] = set()

            emit(
                f"<{file_uri}> <{VCFR_NAMESPACE}representationProfile> "
                f"<{VCFR_NAMESPACE}CondensedRepresentation> .\n"
            )
            emit(f"<{file_uri}> <{VCFR_NAMESPACE}hasSampleSet> <{sample_set_uri}> .\n")
            emit(f"<{sample_set_uri}> <{RDF_TYPE_URI}> <{VCFR_NAMESPACE}SampleSet> .\n")

            for sample_column in record_stream.columns:
                sample_component = _rml_uri_component(sample_column.uri_id)
                sample_uri = f"{sample_set_uri}/{sample_component}"
                emit(f"<{sample_set_uri}> <{VCFR_NAMESPACE}hasSample> <{sample_uri}> .\n")
                emit(f"<{sample_uri}> <{RDF_TYPE_URI}> <{VCFR_NAMESPACE}VCFSample> .\n")
                emit(
                    f"<{sample_uri}> <{VCFR_NAMESPACE}sampleName> "
                    f"{_ntriples_string_literal(sample_column.sample_id)} .\n"
                )
                emit(
                    f"<{sample_uri}> <{VCFR_NAMESPACE}sampleIndex> "
                    f'"{sample_column.index}"^^<{XSD_POSITIVE_INTEGER_URI}> .\n'
                )
                stats["samples"] += 1

            for record in record_stream:
                if not record.format_keys:
                    continue
                row_component = _rml_uri_component(record.row_id)
                call_uri = f"{file_uri}#call/{row_component}"
                matrix_uri = f"{call_uri}/matrix"
                emit(f"<{call_uri}> <{VCFR_NAMESPACE}hasCallMatrix> <{matrix_uri}> .\n")
                emit(f"<{matrix_uri}> <{RDF_TYPE_URI}> <{VCFR_NAMESPACE}CohortCallMatrix> .\n")
                emit(
                    f"<{matrix_uri}> <{VCFR_NAMESPACE}appliesToSampleSet> "
                    f"<{sample_set_uri}> .\n"
                )
                stats["matrices"] += 1

                for format_index, format_key in enumerate(record.format_keys):
                    format_component = _rml_uri_component(format_key)
                    vector_uri = f"{matrix_uri}/fmt/{format_component}"
                    definition = definitions.get(format_key)
                    if definition is None:
                        definition = FormatDefinition(
                            uri=f"{file_uri}#header/format/{format_component}",
                            field_number=".",
                            description=(
                                f"Synthesized definition for undeclared FORMAT key {format_key}"
                            ),
                        )
                    definition_uri = definition.uri
                    if definition_uri not in emitted_definitions:
                        emit(
                            f"<{definition_uri}> <{RDF_TYPE_URI}> "
                            f"<{VCFR_NAMESPACE}FormatFieldDefinition> .\n"
                        )
                        emit(
                            f"<{definition_uri}> <{VCFR_NAMESPACE}fieldId> "
                            f"{_ntriples_string_literal(format_key)} .\n"
                        )
                        emit(
                            f"<{definition_uri}> <{VCFR_NAMESPACE}fieldNumber> "
                            f"{_ntriples_string_literal(definition.field_number)} .\n"
                        )
                        emit(
                            f"<{definition_uri}> <{VCFR_NAMESPACE}fieldDescription> "
                            f"{_ntriples_string_literal(definition.description)} .\n"
                        )
                        emitted_definitions.add(definition_uri)
                        stats["format_definitions"] += 1

                    encoded_values = "\t".join(
                        values[format_index] or "." for values in record.sample_values
                    )
                    emit(
                        f"<{matrix_uri}> <{VCFR_NAMESPACE}hasFormatValueVector> "
                        f"<{vector_uri}> .\n"
                    )
                    emit(f"<{vector_uri}> <{RDF_TYPE_URI}> <{VCFR_NAMESPACE}FormatValueVector> .\n")
                    emit(
                        f"<{vector_uri}> <{VCFR_NAMESPACE}declaredBy> "
                        f"<{definition_uri}> .\n"
                    )
                    emit(
                        f"<{vector_uri}> <{VCFR_NAMESPACE}valueEncoding> "
                        f"<{VCFR_NAMESPACE}VCFTextVector> .\n"
                    )
                    emit(
                        f"<{vector_uri}> <{VCFR_NAMESPACE}encodedValues> "
                        f"{_ntriples_string_literal(encoded_values)} .\n"
                    )
                    stats["format_vectors"] += 1

                stats["records"] += 1
                if progress_interval_records > 0 and stats["records"] % progress_interval_records == 0:
                    print(
                        "    * Condensed sample RDF streaming: "
                        f"{stats['records']:,} variants, {stats['format_vectors']:,} vectors",
                        flush=True,
                    )

        return _append_rdf_atomically(rdf_path, stats, produce)


def emit_sample_representation(
    workflow: SampleWorkflow,
    *,
    records_tsv: Path,
    header_lines_tsv: Path,
    rdf_path: Path,
) -> dict | None:
    """Execute the workflow's sole direct RDF emitter, if it has one."""
    if workflow.emitter is None:
        return None
    if workflow.emitter == "expanded":
        return append_expanded_sample_rdf(records_tsv, rdf_path)
    if workflow.emitter == "condensed":
        return append_condensed_sample_rdf(records_tsv, header_lines_tsv, rdf_path)
    raise RuntimeError(f"unknown sample RDF emitter: {workflow.emitter}")


def update_conversion_metrics_after_sample_stream(
    *,
    metrics_dir: Path,
    output_name: str,
    run_id: str,
    rdf_path: Path,
    total_triples: int,
    sample_stats: dict,
):
    """Bring conversion JSON/CSV metrics in sync after direct sample emission."""
    safe_name = safe_metrics_name(output_name)
    metrics_json = metrics_dir / "stages" / "conversion" / f"{safe_name}.json"
    output_size = int(rdf_path.stat().st_size)
    if metrics_json.is_file():
        try:
            payload = json.loads(metrics_json.read_text(encoding="utf-8"))
            artifacts = payload.setdefault("artifacts", {})
            prior_triples = artifacts.get("output_triples")
            if isinstance(prior_triples, dict):
                for key in list(prior_triples):
                    prior_triples[key] = int(total_triples)
                prior_triples["TOTAL"] = int(total_triples)
            else:
                artifacts["output_triples"] = int(total_triples)
            artifacts["output_size_bytes"] = output_size
            payload["sample_representation"] = sample_stats
            # Retained for consumers of pre-condensed conversion metrics.
            payload["sample_streaming"] = sample_stats
            metrics_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        except (OSError, json.JSONDecodeError):
            pass

    metrics_csv = metrics_dir / "metrics.csv"
    if not metrics_csv.is_file():
        return
    try:
        with metrics_csv.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = list(reader.fieldnames or [])
            rows = list(reader)
        changed = False
        for row in rows:
            if row.get("run_id") == run_id and row.get("output_name") == output_name:
                row["output_triples"] = str(int(total_triples))
                row["output_dir_size_bytes"] = str(output_size)
                changed = True
        if changed:
            with metrics_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
    except OSError:
        pass


def build_sample_support_tsvs(records_tsv: Path, sample_calls_tsv: Path, sample_format_tsv: Path):
    """Materialize per-sample helper TSVs from records.tsv.

    These helper tables keep records.tsv user-facing and simple while providing
    deterministic row-wise inputs for rules that map:
    - one `SampleCall` per sample/record
    - one `FormatFieldValue` per sample/record/FORMAT key
    """
    write_sample_support_headers(sample_calls_tsv, sample_format_tsv)
    with sample_calls_tsv.open("a", newline="", encoding="utf-8") as sample_calls_handle, \
        sample_format_tsv.open("a", newline="", encoding="utf-8") as sample_format_handle:
        sample_calls_writer = csv.writer(sample_calls_handle, delimiter="\t")
        sample_format_writer = csv.writer(sample_format_handle, delimiter="\t")

        if not records_tsv.exists():
            return

        with SampleRecordStream(records_tsv) as record_stream:
            for record in record_stream:
                for sample_offset, sample_column in enumerate(record_stream.columns):
                    sample_calls_writer.writerow(
                        [
                            record.source_file,
                            record.row_id,
                            str(sample_column.index),
                            sample_column.sample_id,
                            sample_column.uri_id,
                            record.sample_payloads[sample_offset],
                        ]
                    )

                    for format_offset, format_key in enumerate(record.format_keys):
                        sample_format_writer.writerow(
                            [
                                record.source_file,
                                record.row_id,
                                str(sample_column.index),
                                sample_column.sample_id,
                                sample_column.uri_id,
                                str(format_offset + 1),
                                format_key,
                                record.sample_values[sample_offset][format_offset],
                            ]
                        )


def render_rules_for_triplet(
    template_rules: Path,
    output_rules: Path,
    records_name: str,
    headers_name: str,
    metadata_name: str,
    sample_calls_name: str,
    sample_format_name: str,
):
    """Render per-input mapping rules by substituting TSV placeholders."""
    text = template_rules.read_text()
    text = text.replace('/data/tsv/records.tsv', f'/data/tsv/{records_name}')
    text = text.replace('/data/tsv/header_lines.tsv', f'/data/tsv/{headers_name}')
    text = text.replace('/data/tsv/file_metadata.tsv', f'/data/tsv/{metadata_name}')
    text = text.replace('/data/tsv/sample_calls.tsv', f'/data/tsv/{sample_calls_name}')
    text = text.replace('/data/tsv/sample_format_values.tsv', f'/data/tsv/{sample_format_name}')
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


def repo_has_dockerfile(repo_root: Path) -> bool:
    """Return whether a local checkout includes a buildable Dockerfile."""
    dockerfile = repo_root / "Dockerfile"
    return dockerfile.exists() and dockerfile.is_file()


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
    """Parse internal compression stage names used by the execution helpers."""
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
                "Unsupported internal compression stage "
                f"'{method}'."
            )
        if method not in methods:
            methods.append(method)
    return methods


def parse_compression_plan_option(raw: str, *, option_name: str, choices: set[str]):
    """Parse one comma-separated public compression-plan option."""
    value = (raw or "").strip()
    if value == "" or value == "none":
        return []

    values = []
    for token in value.split(","):
        choice = token.strip()
        if not choice:
            continue
        if choice == "none" or choice not in choices:
            allowed = ",".join(sorted(choices))
            raise ValueError(
                f"Unsupported value '{choice}' for {option_name}. "
                f"Use {allowed}, or none."
            )
        if choice not in values:
            values.append(choice)
    return values


def build_compression_methods(
    *,
    rdf_compression: str,
    representations: str,
    artifact_compression: str,
):
    """Translate the public compression plan into internal execution stages.

    Raw RDF codecs are independent from indexed representations. Packaging
    codecs are applied to every selected representation, so users do not need
    compound names such as ``hdt_gzip`` or ``cottas_brotli``.
    """
    raw_methods = parse_compression_plan_option(
        rdf_compression,
        option_name="--rdf-compression",
        choices=RDF_COMPRESSION_CHOICES,
    )
    selected_representations = parse_compression_plan_option(
        representations,
        option_name="--representations",
        choices=REPRESENTATION_CHOICES,
    )
    packaging_methods = parse_compression_plan_option(
        artifact_compression,
        option_name="--artifact-compression",
        choices=ARTIFACT_COMPRESSION_CHOICES,
    )
    if packaging_methods and not selected_representations:
        raise ValueError(
            "--artifact-compression requires at least one value in --representations."
        )

    methods = list(raw_methods)
    for representation in selected_representations:
        if representation not in methods:
            methods.append(representation)
        for packaging_method in packaging_methods:
            compound = f"{representation}_{packaging_method}"
            if compound not in methods:
                methods.append(compound)
    return methods


def parse_positive_int(value: str, *, name: str) -> int:
    """Parse a strictly positive integer CLI value."""
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer.") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be a positive integer.")
    return parsed


def compression_uses_partitioning(methods: list[str]) -> bool:
    """Return whether selected methods need bounded RDF chunks."""
    return any(method in PARTITIONED_COMPRESSION_METHODS for method in methods)


def compression_uses_hdt(methods: list[str]) -> bool:
    """Return whether any selected compression step depends on HDT generation."""
    return any(method in HDT_COMPRESSION_METHODS for method in methods)


def should_use_partitioned_hdt(
    *,
    mode: str,
    methods: list[str],
    hdt_strategy: str,
    rdf_storage_mode: str | None = None,
) -> bool:
    """Resolve whether the HDT pipeline should use chunked generation + hdtc merge."""
    if not compression_uses_hdt(methods):
        return False
    if hdt_strategy == "single":
        return False
    if hdt_strategy == "partitioned":
        return True
    # `auto` uses partitioned conversion for the aggregate storage modes, while
    # ordinary compression-only inputs remain single-pass unless requested.
    return mode == "full" and rdf_storage_mode in RDF_STORAGE_MODES


def safe_metrics_name(value: str) -> str:
    """Sanitize names used in metrics artifact filenames."""
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")
    return safe or "rdf"


def input_artifact_stem(path: Path) -> str:
    """Return a stable, human-readable label for a source artifact.

    Metric directories are intended for people first, so remove only the
    recognized VCF/RDF/representation suffixes rather than repeatedly applying
    :attr:`Path.stem` (which turns ``cohort.vcf.gz`` into ``cohort.vcf``).
    """
    name = path.name
    for suffix in (
        ".vcf.gz",
        ".vcf",
        ".nt.gz",
        ".nt.br",
        ".nt",
        ".cottas.gz",
        ".cottas.br",
        ".cottas",
        ".hdt",
        ".gz",
        ".br",
    ):
        if name.endswith(suffix):
            return name[: -len(suffix)] or "input"
    return path.stem or "input"


def metrics_run_label(source_paths: list[Path], source_root: Path | None = None) -> str:
    """Return the input-identifying label for one metrics-run directory."""
    if len(source_paths) == 1:
        return safe_metrics_name(input_artifact_stem(source_paths[0]))

    root_label = input_artifact_stem(source_root) if source_root is not None else "inputs"
    return safe_metrics_name(f"batch-{root_label}-{len(source_paths)}-inputs")


def metrics_run_directory(metrics_root: Path, source_label: str, run_id: str) -> Path:
    """Build the canonical input-labelled directory for one invocation."""
    return metrics_root / f"{safe_metrics_name(source_label)}__{run_id}"


def read_metrics_csv_rows(path: Path) -> list[dict]:
    """Read a metrics CSV without allowing a damaged optional report to fail a run."""
    if not path.is_file():
        return []
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except OSError:
        return []


def write_run_manifest(
    *,
    metrics_dir: Path,
    run_id: str,
    timestamp: str,
    mode: str,
    source_label: str,
    source_paths: list[Path],
    out_root: Path,
    options: dict,
):
    """Write the static, human-readable identity and configuration of a run."""
    ensure_dir(metrics_dir)
    payload = {
        "metrics_layout_version": METRICS_LAYOUT_VERSION,
        "run_id": run_id,
        "timestamp": timestamp,
        "mode": mode,
        "source_label": source_label,
        "output_root": str(out_root),
        "metrics_directory": str(metrics_dir),
        "inputs": [
            {
                "path": str(path),
                "file_name": path.name,
                "size_bytes": file_size_bytes(path),
            }
            for path in source_paths
        ],
        "options": options,
    }
    path = metrics_dir / "run.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def update_run_manifest(metrics_dir: Path, **updates) -> None:
    """Merge late-bound runtime details (such as the resolved image) into run.json."""
    path = metrics_dir / "run.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
        payload.update(updates)
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    except (OSError, json.JSONDecodeError):
        # Metadata must improve diagnosability, never invalidate a completed run.
        pass


def write_run_summary(
    *,
    metrics_dir: Path,
    run_id: str,
    timestamp: str,
    mode: str,
    exit_code: int,
    elapsed_seconds: float,
    total_triples: int | None,
):
    """Write one discoverable end-of-run summary for all workflow modes.

    Individual stage reports retain their native detail; this file provides the
    compact landing page that links their location and repeats the tabular rows
    most often used for analysis.
    """
    stage_dir = metrics_dir / "stages"
    stage_reports = [
        path.relative_to(metrics_dir).as_posix()
        for path in sorted(stage_dir.rglob("*.json"))
    ] if stage_dir.is_dir() else []
    report_dir = metrics_dir / "reports"
    reports = [
        path.relative_to(metrics_dir).as_posix()
        for path in sorted(report_dir.iterdir())
        if path.is_file()
    ] if report_dir.is_dir() else []
    payload = {
        "metrics_layout_version": METRICS_LAYOUT_VERSION,
        "run_id": run_id,
        "timestamp": timestamp,
        "completed_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "mode": mode,
        "status": "success" if int(exit_code) == 0 else "failure",
        "exit_code": int(exit_code),
        "execution": {
            "wall_seconds": round(float(elapsed_seconds), 6),
            "wall_human": format_duration(elapsed_seconds),
            "total_triples": total_triples,
        },
        "summary_tables": {
            "conversion_and_compression": read_metrics_csv_rows(metrics_dir / "metrics.csv"),
            "tsv": read_metrics_csv_rows(metrics_dir / "tsv_metrics.csv"),
            "wrapper": read_metrics_csv_rows(metrics_dir / "wrapper_execution_times.csv"),
        },
        "stage_reports": stage_reports,
        "reports": reports,
        "logs": [
            path.relative_to(metrics_dir).as_posix()
            for path in sorted((metrics_dir / "logs").rglob("*"))
            if path.is_file()
        ] if (metrics_dir / "logs").is_dir() else [],
    }
    path = metrics_dir / "summary.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def progress_event_path(metrics_dir: Path | None, *components: str) -> Path | None:
    """Return a hidden, per-operation progress sidecar path."""
    if metrics_dir is None:
        return None
    safe_components = [safe_metrics_name(component) for component in components]
    filename = "-".join(safe_components or ["run"]) + ".jsonl"
    return metrics_dir / ".progress" / filename


def container_progress_path(path: Path | None, metrics_dir: Path | None) -> str | None:
    """Translate a host progress sidecar into its mounted container path."""
    if path is None or metrics_dir is None:
        return None
    try:
        relative = path.resolve().relative_to(metrics_dir.resolve())
    except ValueError:
        return None
    return f"/data/metrics/{relative.as_posix()}"


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

    uses_cottas = any(method in COTTAS_COMPRESSION_METHODS for method in methods)
    if uses_cottas:
        header.extend(COMPRESSION_METHOD_COLUMNS["cottas"])

    uses_hdt = any(method in HDT_COMPRESSION_METHODS for method in methods)
    if uses_hdt:
        header.extend(COMPRESSION_METHOD_COLUMNS["hdt"])
        header.append(HDT_SOURCE_COLUMN)
    if "hdt_gzip" in methods:
        header.extend(COMPRESSION_METHOD_COLUMNS["hdt_gzip"])
    if "hdt_brotli" in methods:
        header.extend(COMPRESSION_METHOD_COLUMNS["hdt_brotli"])
    if "cottas_gzip" in methods:
        header.extend(COMPRESSION_METHOD_COLUMNS["cottas_gzip"])
    if "cottas_brotli" in methods:
        header.extend(COMPRESSION_METHOD_COLUMNS["cottas_brotli"])

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
    tsv_metrics: dict | None = None,
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
        "exit_code_tsv": "0",
        "wall_seconds_tsv": "null",
        "user_seconds_tsv": "null",
        "sys_seconds_tsv": "null",
        "max_rss_kb_tsv": "null",
        "tsv_output_size_bytes": "0",
        "tsv_output_path": "",
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
        "source_triples_hdt": "null",
        "decoded_triples_hdt": "null",
        "validation_hdt": "null",
        "cottas_size_bytes": "0",
        "exit_code_cottas": "0",
        "wall_seconds_cottas": "null",
        "user_seconds_cottas": "null",
        "sys_seconds_cottas": "null",
        "max_rss_kb_cottas": "null",
        "source_triples_cottas": "null",
        "decoded_triples_cottas": "null",
        "validation_cottas": "null",
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

    if tsv_metrics is not None:
        if "exit_code_tsv" in row:
            row["exit_code_tsv"] = str(int(tsv_metrics.get("exit_code") or 0))
        if "wall_seconds_tsv" in row:
            wall = tsv_metrics.get("wall_seconds")
            row["wall_seconds_tsv"] = "null" if wall is None else f"{float(wall):.6f}"
        if "user_seconds_tsv" in row:
            user = tsv_metrics.get("user_seconds")
            row["user_seconds_tsv"] = "null" if user is None else f"{float(user):.6f}"
        if "sys_seconds_tsv" in row:
            sys_val = tsv_metrics.get("sys_seconds")
            row["sys_seconds_tsv"] = "null" if sys_val is None else f"{float(sys_val):.6f}"
        if "max_rss_kb_tsv" in row:
            rss = tsv_metrics.get("max_rss_kb")
            row["max_rss_kb_tsv"] = "null" if rss is None else str(int(rss))
        if "tsv_output_size_bytes" in row:
            row["tsv_output_size_bytes"] = str(int(tsv_metrics.get("output_size_bytes") or 0))
        if "tsv_output_path" in row:
            paths = tsv_metrics.get("output_paths") or []
            row["tsv_output_path"] = "|".join(str(path) for path in paths)

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

    def assign_validation(method: str, result: dict):
        validation = result.get("validation") or result.get("details", {}).get("validation") or {}
        if not validation:
            return
        source_key = f"source_triples_{method}"
        decoded_key = f"decoded_triples_{method}"
        valid_key = f"validation_{method}"
        if source_key in row:
            source = validation.get("source_triples")
            row[source_key] = "null" if source is None else str(int(source))
        if decoded_key in row:
            decoded = validation.get("decoded_triples")
            row[decoded_key] = "null" if decoded is None else str(int(decoded))
        if valid_key in row:
            row[valid_key] = "true" if validation.get("valid") and validation.get("count_match") else "false"

    for method in ("gzip", "brotli", "hdt", "cottas"):
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
        if method in {"hdt", "cottas"}:
            assign_validation(method, result)

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

    cottas_gzip_result = method_results.get("cottas_gzip")
    if cottas_gzip_result is not None:
        if "gzip_on_cottas_size_bytes" in row:
            row["gzip_on_cottas_size_bytes"] = str(int(cottas_gzip_result.get("output_size_bytes") or 0))
        if "exit_code_gzip_on_cottas" in row:
            row["exit_code_gzip_on_cottas"] = str(int(cottas_gzip_result.get("exit_code") or 0))
        assign_timing("gzip_on_cottas", cottas_gzip_result)

    cottas_brotli_result = method_results.get("cottas_brotli")
    if cottas_brotli_result is not None:
        if "brotli_on_cottas_size_bytes" in row:
            row["brotli_on_cottas_size_bytes"] = str(int(cottas_brotli_result.get("output_size_bytes") or 0))
        if "exit_code_brotli_on_cottas" in row:
            row["exit_code_brotli_on_cottas"] = str(int(cottas_brotli_result.get("exit_code") or 0))
        assign_timing("brotli_on_cottas", cottas_brotli_result)

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
    index_warnings: list[dict] | None = None,
):
    """Write the final per-output compression summary and method timing files."""
    metrics_dir.mkdir(parents=True, exist_ok=True)
    safe_name = safe_metrics_name(output_name)

    for method, result in method_results.items():
        time_log_dir = metrics_dir / "timings" / "compression" / safe_name
        time_log_dir.mkdir(parents=True, exist_ok=True)
        time_log = time_log_dir / f"{safe_metrics_name(method)}.txt"
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
        validation = result.get("validation") or result.get("details", {}).get("validation")
        if validation:
            lines.extend(
                [
                    f"validation_valid={str(bool(validation.get('valid'))).lower()}",
                    f"validation_count_match={str(bool(validation.get('count_match'))).lower()}",
                    f"source_triples={validation.get('source_triples', 'null')}",
                    f"decoded_triples={validation.get('decoded_triples', 'null')}",
                ]
            )
        time_log.write_text("\n".join(lines) + "\n", encoding="utf-8")

    gzip_result = method_results.get("gzip", {})
    brotli_result = method_results.get("brotli", {})
    hdt_result = method_results.get("hdt", {})
    hdt_gzip_result = method_results.get("hdt_gzip", {})
    hdt_brotli_result = method_results.get("hdt_brotli", {})
    cottas_result = method_results.get("cottas", {})
    cottas_gzip_result = method_results.get("cottas_gzip", {})
    cottas_brotli_result = method_results.get("cottas_brotli", {})

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
        "index_warnings": list(index_warnings or []),
        # This preserves every method-specific detail returned by a container
        # (validation, chunk plan, index information, and workspace metrics),
        # rather than reducing it to the CSV's scalar columns.
        "methods": method_results,
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
        "cottas_conversion": {
            "output_cottas_path": cottas_result.get("output_path", ""),
            "output_cottas_size_bytes": int(cottas_result.get("output_size_bytes") or 0),
            "exit_code": int(cottas_result.get("exit_code") or 0),
            "timing": timing_payload(cottas_result),
            "validation": cottas_result.get("validation")
            or cottas_result.get("details", {}).get("validation"),
        },
        "hdt_conversion": {
            "output_hdt_path": hdt_result.get("output_path", ""),
            "output_hdt_size_bytes": int(hdt_result.get("output_size_bytes") or 0),
            "exit_code": int(hdt_result.get("exit_code") or 0),
            "timing": timing_payload(hdt_result),
            "validation": hdt_result.get("validation")
            or hdt_result.get("details", {}).get("validation"),
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
        "gzip_on_cottas": {
            "output_cottas_gz_path": cottas_gzip_result.get("output_path", ""),
            "output_cottas_gz_size_bytes": int(cottas_gzip_result.get("output_size_bytes") or 0),
            "exit_code": int(cottas_gzip_result.get("exit_code") or 0),
            "timing": timing_payload(cottas_gzip_result),
        },
        "brotli_on_cottas": {
            "output_cottas_br_path": cottas_brotli_result.get("output_path", ""),
            "output_cottas_br_size_bytes": int(cottas_brotli_result.get("output_size_bytes") or 0),
            "exit_code": int(cottas_brotli_result.get("exit_code") or 0),
            "timing": timing_payload(cottas_brotli_result),
        },
    }

    metrics_json_dir = metrics_dir / "stages" / "compression"
    metrics_json_dir.mkdir(parents=True, exist_ok=True)
    metrics_json = metrics_json_dir / f"{safe_name}.json"
    metrics_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_raw_compression_metrics_artifact(
    *,
    metrics_dir: Path,
    run_id: str,
    timestamp: str,
    output_name: str,
    rdf_name: str,
    source_rdf_path: Path,
    selected_methods: list[str],
    method_results: dict[str, dict],
    index_warnings: list[dict] | None = None,
    auxiliary_stages: dict[str, dict] | None = None,
):
    """Persist the operation-level compression detail for one RDF source."""
    safe_output = safe_metrics_name(output_name)
    safe_rdf = safe_metrics_name(rdf_name)
    raw_json_dir = metrics_dir / "stages" / "compression_operations" / safe_output
    raw_json_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "run_id": run_id,
        "timestamp": timestamp,
        "output_name": output_name,
        "rdf_name": rdf_name,
        "source_rdf_path": str(source_rdf_path),
        "compression_methods": ",".join(selected_methods) if selected_methods else "none",
        "index_warnings": list(index_warnings or []),
        "auxiliary_stages": dict(auxiliary_stages or {}),
        "methods": {},
    }

    for method, result in method_results.items():
        details = result.get("details", {})
        payload["methods"][method] = {
            "exit_code": int(result.get("exit_code") or 0),
            "wall_seconds": result.get("wall_seconds"),
            "user_seconds": result.get("user_seconds"),
            "sys_seconds": result.get("sys_seconds"),
            "max_rss_kb": result.get("max_rss_kb"),
            "output_path": result.get("output_path", ""),
            "output_size_bytes": int(result.get("output_size_bytes") or 0),
            "source": result.get("source"),
            "details": details,
            "validation": result.get("validation") or details.get("validation"),
        }

    raw_json = raw_json_dir / f"{safe_rdf}.json"
    raw_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_partitioned_container_stage_report(
    *,
    metrics_dir: Path,
    output_name: str,
    source_rdf_path: Path,
    payload: dict,
):
    """Preserve the detailed stage handoff from the ephemeral Docker volume.

    The partitioned runner records every chunk build, merge, validation, disk
    watermark, and GNU-time resource measurement. Its workspace is deleted at
    the end of the operation, so this report is the durable location for those
    deeper container metrics on both success and failure.
    """
    report_dir = metrics_dir / "stages" / "partitioned"
    ensure_dir(report_dir)
    report = {
        "runtime_environment": "docker-volume",
        "source_rdf_path": str(source_rdf_path),
        "output_name": output_name,
        "container_result": payload,
    }
    report_path = report_dir / f"{safe_metrics_name(output_name)}.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report_path


def validate_mode_dirs(paths):
    """Validate that expected directory arguments are not file paths."""
    for p in paths:
        if p.exists() and not p.is_dir():
            raise ValueError(f"expected a directory path but found a file: {p}")


def tsv_output_paths_for_prefix(tsv_dir: Path, prefix: str) -> list[Path]:
    """Return expected TSV outputs produced by `vcf_as_tsv.sh` for one input prefix."""
    return [
        tsv_dir / f"{prefix}.records.tsv",
        tsv_dir / f"{prefix}.header_lines.tsv",
        tsv_dir / f"{prefix}.file_metadata.tsv",
    ]


def summarize_tsv_outputs(tsv_dir: Path, prefix: str):
    """Summarize TSV output paths and total bytes for one prefix."""
    output_paths = tsv_output_paths_for_prefix(tsv_dir, prefix)
    existing = [path for path in output_paths if path.exists()]
    total_size = sum(int(file_size_bytes(path) or 0) for path in existing)
    return existing, total_size


def write_tsv_metrics_artifacts(
    *,
    metrics_dir: Path,
    run_id: str,
    timestamp: str,
    prefix: str,
    input_path: str,
    exit_code: int,
    timing: dict,
    output_paths: list[Path],
    output_size_bytes: int,
):
    """Persist one TSV container stage report in the canonical stage tree."""
    safe_prefix = safe_metrics_name(prefix)
    json_dir = metrics_dir / "stages" / "tsv"
    json_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": run_id,
        "timestamp": timestamp,
        "prefix": prefix,
        "input_path": input_path,
        "exit_code": int(exit_code),
        "timing": {
            "wall_seconds": timing.get("wall_seconds"),
            "user_seconds": timing.get("user_seconds"),
            "sys_seconds": timing.get("sys_seconds"),
            "max_rss_kb": timing.get("max_rss_kb"),
        },
        "artifacts": {
            "output_paths": [str(path) for path in output_paths],
            "output_size_bytes": int(output_size_bytes),
        },
    }
    (json_dir / f"{safe_prefix}.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def run_tsv_conversion_with_metrics(
    *,
    input_mount_dir: Path,
    container_input: str,
    tsv_dir: Path,
    metrics_dir: Path,
    image_ref: str,
    run_id: str,
    timestamp: str,
    prefix: str,
):
    """Run VCF->TSV conversion and collect per-input timing/resource metrics."""
    safe_prefix = safe_metrics_name(prefix)
    raw_time_dir = metrics_dir / "timings" / "tsv"
    raw_time_dir.mkdir(parents=True, exist_ok=True)
    time_log_host = raw_time_dir / f"{safe_prefix}.txt"
    time_log_container = f"/data/metrics/timings/tsv/{safe_prefix}.txt"

    wrapped_command = (
        "set -euo pipefail; "
        f"rm -f {shlex.quote(time_log_container)}; "
        'if [[ -x /usr/bin/time ]] && /usr/bin/time --version >/dev/null 2>&1; then '
        f"/usr/bin/time -v -o {shlex.quote(time_log_container)} -- "
        f"bash /opt/vcf-rdfizer/vcf_as_tsv.sh {shlex.quote(container_input)} /data/tsv; "
        "else "
        f"{{ time -p bash /opt/vcf-rdfizer/vcf_as_tsv.sh {shlex.quote(container_input)} /data/tsv; }} "
        f"> {shlex.quote(time_log_container)} 2>&1; "
        "fi"
    )
    cmd = [
        *docker_run_base(),
        "-v",
        f"{str(input_mount_dir)}:/data/in:ro",
        "-v",
        f"{str(tsv_dir)}:/data/tsv",
        "-v",
        f"{str(metrics_dir)}:/data/metrics",
        image_ref,
        "bash",
        "-lc",
        wrapped_command,
    ]

    started = time.perf_counter()
    with ProgressSession(None, f"TSV conversion: {prefix}"):
        exit_code = run(cmd)
    elapsed = time.perf_counter() - started

    timing = parse_time_log_metrics(time_log_host)
    # Use the wrapper-observed docker runtime as the authoritative wall clock.
    # GNU `time` remains useful for CPU/RSS, but its elapsed field has proven
    # unreliable for long-running conversion/compression workloads.
    timing["wall_seconds"] = elapsed

    output_paths, output_size_bytes = summarize_tsv_outputs(tsv_dir, prefix)
    write_tsv_metrics_artifacts(
        metrics_dir=metrics_dir,
        run_id=run_id,
        timestamp=timestamp,
        prefix=prefix,
        input_path=container_input,
        exit_code=exit_code,
        timing=timing,
        output_paths=output_paths,
        output_size_bytes=output_size_bytes,
    )

    return {
        "exit_code": exit_code,
        "wall_seconds": timing.get("wall_seconds"),
        "user_seconds": timing.get("user_seconds"),
        "sys_seconds": timing.get("sys_seconds"),
        "max_rss_kb": timing.get("max_rss_kb"),
        "output_size_bytes": int(output_size_bytes),
        "output_paths": [str(path) for path in output_paths],
        "time_log_path": str(time_log_host),
    }


def write_tsv_benchmark_metrics_csv(*, metrics_dir: Path, rows: list[dict]):
    """Write TSV-only benchmark metrics summary."""
    ensure_dir(metrics_dir)
    csv_path = metrics_dir / "tsv_metrics.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=TSV_BENCHMARK_HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in TSV_BENCHMARK_HEADER})
    return csv_path


def run_tsv_mode(
    *,
    input_mount_dir: Path,
    container_inputs: list[str],
    expected_prefixes: list[str],
    tsv_dir: Path,
    metrics_dir: Path,
    image_ref: str,
    run_id: str,
    timestamp: str,
    wrapper_log_path: Path,
    run_tracker: RunTracker | None = None,
):
    """Execute TSV-only benchmarking mode."""
    print("Step 3/3: Processing TSV-only benchmark")
    ensure_dir(tsv_dir)
    ensure_dir(metrics_dir)

    benchmark_rows: list[dict] = []
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
            run_tracker.mark(f"TSV input {idx}/{total_inputs} started: {expected_prefix}")

        for expected_tsv_output in tsv_output_paths_for_prefix(tsv_dir, expected_prefix):
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

        tsv_metrics = run_tsv_conversion_with_metrics(
            input_mount_dir=input_mount_dir,
            container_input=container_input,
            tsv_dir=tsv_dir,
            metrics_dir=metrics_dir,
            image_ref=image_ref,
            run_id=run_id,
            timestamp=timestamp,
            prefix=expected_prefix,
        )
        tsv_exit_code = int(tsv_metrics.get("exit_code") or 0)
        if tsv_exit_code != 0:
            fail_current("tsv-conversion", f"TSV conversion failed. See log: {wrapper_log_path}")
            continue

        expected_outputs = tsv_output_paths_for_prefix(tsv_dir, expected_prefix)
        missing_outputs = [path for path in expected_outputs if not path.exists()]
        if missing_outputs:
            missing_list = ", ".join(path.name for path in missing_outputs)
            fail_current(
                "tsv-validation",
                (
                    f"TSV conversion did not produce expected outputs for '{expected_prefix}': "
                    f"{missing_list}. See log: {wrapper_log_path}"
                ),
            )
            continue

        print(f"    * TSV conversion {success_symbol()}")
        if run_tracker is not None:
            run_tracker.mark(f"TSV input {idx}: conversion completed for {expected_prefix}")

        wall_seconds = tsv_metrics.get("wall_seconds")
        user_seconds = tsv_metrics.get("user_seconds")
        sys_seconds = tsv_metrics.get("sys_seconds")
        max_rss_kb = tsv_metrics.get("max_rss_kb")
        benchmark_rows.append(
            {
                "run_id": run_id,
                "timestamp": timestamp,
                "input_vcf": input_vcf,
                "prefix": expected_prefix,
                "exit_code_tsv": str(tsv_exit_code),
                "wall_seconds_tsv": "null" if wall_seconds is None else f"{float(wall_seconds):.6f}",
                "user_seconds_tsv": "null" if user_seconds is None else f"{float(user_seconds):.6f}",
                "sys_seconds_tsv": "null" if sys_seconds is None else f"{float(sys_seconds):.6f}",
                "max_rss_kb_tsv": "null" if max_rss_kb is None else str(int(max_rss_kb)),
                "tsv_output_size_bytes": str(int(tsv_metrics.get("output_size_bytes") or 0)),
                "tsv_output_path": "|".join(str(path) for path in expected_outputs),
                "tsv_time_log_path": str(tsv_metrics.get("time_log_path") or ""),
            }
        )

    benchmark_csv = write_tsv_benchmark_metrics_csv(metrics_dir=metrics_dir, rows=benchmark_rows)
    print(f"TSV benchmark metrics: {benchmark_csv}")

    if input_failures:
        report_path = write_failed_inputs_report(metrics_dir=metrics_dir, failures=input_failures)
        eprint(
            f"Completed with failures for {len(input_failures)}/{total_inputs} input(s). "
            f"Failure report: {report_path}"
        )
        print("TSV benchmarking completed with failures.")
        if run_tracker is not None:
            run_tracker.mark(
                f"TSV benchmark completed with failures ({len(input_failures)}/{total_inputs}). "
                f"Report: {report_path}"
            )
        return 1

    print("TSV benchmarking finished.")
    if run_tracker is not None:
        run_tracker.mark("TSV benchmark finished successfully")
    return 0


# ---------------------------------------------------------------------------
# Mode runners (full/tsv/compress/decompress)
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
    has_local_dockerfile = repo_has_dockerfile(repo_root)

    if build:
        if not has_local_dockerfile:
            eprint(
                "Error: --build requested but no local Dockerfile is available. "
                "Install from a source checkout or use a published image tag."
            )
            return 2
        print(f"{step_label}: Ensuring Docker image is available")
        print("  - Building Docker image")
        with ProgressSession(None, "Building Docker image"):
            image_exit_code = docker_build_image(image_ref, repo_root)
        if image_exit_code != 0:
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
        with ProgressSession(None, "Pulling Docker image"):
            image_exit_code = docker_pull_image(image_ref)
        if image_exit_code != 0:
            eprint(f"Error: image version '{image_ref}' not found. See log: {wrapper_log_path}")
            return 2
        print(f"{step_label}: Ensuring Docker image is available {success_symbol()}")
        return 0

    if no_build:
        eprint(f"Error: image '{image_ref}' not found and --no-build set.")
        return 2

    print(f"{step_label}: Ensuring Docker image is available")
    if has_local_dockerfile:
        print("  - Image missing locally, building")
        with ProgressSession(None, "Building Docker image"):
            image_exit_code = docker_build_image(image_ref, repo_root)
        if image_exit_code != 0:
            eprint(f"Error: docker build failed. See log: {wrapper_log_path}")
            return 1
    else:
        print(f"  - Image missing locally, pulling: {image_ref}")
        with ProgressSession(None, "Pulling Docker image"):
            image_exit_code = docker_pull_image(image_ref)
        if image_exit_code != 0:
            eprint(f"Error: image '{image_ref}' could not be pulled. See log: {wrapper_log_path}")
            return 2
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
    metrics_dir: Path | None = None,
    run_id: str | None = None,
    timestamp: str | None = None,
    output_name: str | None = None,
    expected_triples: int | None = None,
    index_warnings: list[dict] | None = None,
):
    """Run selected compression stages for a single RDF file.

    Representation packaging stages reuse an existing `.hdt` or `.cottas` when
    present, or generate it once and reuse it for subsequent packaging steps.
    Returns `(ok, method_results)`.
    """
    in_dir = rdf_path.parent
    input_container = f"/data/in/{rdf_path.name}"
    input_stem = rdf_output_basename(rdf_path)
    input_ext = "nt" if rdf_path.name.endswith(".nt.gz") else rdf_path.suffix.lstrip(".") or "nt"
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
    allow_index_failure = index_warnings is not None
    hdt_name = f"{input_stem}.hdt"
    hdt_path = target_out_dir / hdt_name
    hdt_container = f"{target_out_container}/{hdt_name}"
    hdt_is_ready = False
    hdt_source = "generated"
    cottas_name = f"{input_stem}.cottas"
    cottas_path = target_out_dir / cottas_name
    cottas_container = f"{target_out_container}/{cottas_name}"
    cottas_is_ready = False
    cottas_failure_warning: dict | None = None
    metrics_output_name = output_name or target_out_dir.name
    safe_output_name = safe_metrics_name(metrics_output_name)
    safe_rdf_name = safe_metrics_name(rdf_path.name)
    auxiliary_stage_results: dict[str, dict] = {}

    def run_container_command(
        *,
        method: str,
        artifact_name: str,
        command: str,
        record_method: bool = True,
        quiet_failure: bool = False,
    ):
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
            *docker_hdt_index_env_args(),
            "-v",
            f"{str(in_dir)}:/data/in:ro",
            "-v",
            f"{str(out_dir)}:/data/out",
        ]
        cmd.extend([image_ref, "bash", "-lc", wrapped_command])
        started = time.perf_counter()
        progress_label = f"{method.replace('_', ' ').title()}: {input_stem}"
        with ProgressSession(None, progress_label):
            exit_code = run(cmd)
        elapsed = time.perf_counter() - started
        timing = parse_time_log_metrics(timing_host)
        output_path = target_out_dir / artifact_name
        result = {
            "exit_code": exit_code,
            # Prefer the wrapper-observed docker runtime over the inner
            # `/usr/bin/time` elapsed field for long-running jobs.
            "wall_seconds": elapsed,
            "user_seconds": timing.get("user_seconds"),
            "sys_seconds": timing.get("sys_seconds"),
            "max_rss_kb": timing.get("max_rss_kb"),
            "output_path": str(output_path),
            "output_size_bytes": int(file_size_bytes(output_path) or 0),
        }
        if record_method:
            method_results[method] = result
        else:
            # Validation/index checks consume container resources too. Retain
            # their measurements in the operation report rather than dropping
            # them because they do not produce a final representation.
            auxiliary_stage_results[method] = result
        if metrics_dir is not None and timing_host.exists():
            raw_time_dir = (
                metrics_dir
                / "timings"
                / "compression"
                / safe_output_name
            )
            raw_time_dir.mkdir(parents=True, exist_ok=True)
            raw_time_path = raw_time_dir / f"{safe_metrics_name(method)}.txt"
            try:
                shutil.copyfile(timing_host, raw_time_path)
            except OSError:
                pass
        if timing_host.exists():
            try:
                timing_host.unlink()
            except OSError:
                pass
        if record_method and method == "hdt":
            method_results[method]["source"] = "generated"
        if exit_code != 0:
            if record_method and not quiet_failure:
                eprint(f"Error: {method} compression failed. See log: {wrapper_log_path}")
            return False
        return True

    def record_index_warning(
        *,
        index_format: str,
        artifact_path: Path,
        message: str,
        stage: str,
    ) -> dict:
        """Record one recoverable representation/index problem for a full run."""
        compact = " ".join(str(message).split())
        warning = {
            "format": index_format,
            "stage": stage,
            "status": "index_unavailable",
            "artifact_path": str(artifact_path),
            "message": compact,
        }
        if warning not in index_warnings:
            index_warnings.append(warning)
            eprint(
                f"Warning: {index_format.upper()} index generation failed for "
                f"'{artifact_path}'; continuing with the remaining pipeline. {compact}"
            )
        return warning

    def validate_container_artifact(
        *,
        method: str,
        artifact_name: str,
        artifact_format: str,
        artifact_container: str,
    ) -> tuple[bool, dict]:
        """Validate a base representation before any packaging or cleanup."""
        def perform_validation(*, skip_index_check: bool) -> tuple[bool, dict]:
            report_name = f".{input_stem}.{artifact_format}.validation.json"
            report_path = target_out_dir / report_name
            report_container = f"{target_out_container}/{report_name}"
            command_parts = [
                "set -euo pipefail;",
                f"rm -f {shlex.quote(report_container)};",
                'PYTHON_BIN="${COTTAS_PYTHON_BIN:-$(command -v python3 || true)}";',
                'if [[ -z "$PYTHON_BIN" || ! -x "$PYTHON_BIN" ]]; then ',
                'echo "Missing Python executable in container" >&2; exit 127; fi;',
                'VALIDATOR="/opt/vcf-rdfizer/validate_compression.py";',
                'if [[ ! -f "$VALIDATOR" ]]; then ',
                'echo "Missing compression validator in container" >&2; exit 127; fi;',
                '"$PYTHON_BIN" "$VALIDATOR"',
                f"--source {shlex.quote(input_container)}",
                f"--artifact {shlex.quote(artifact_container)}",
                f"--format {shlex.quote(artifact_format)}",
                f"--result-path {shlex.quote(report_container)}",
            ]
            if expected_triples is not None:
                command_parts.append(f"--expected-triples {int(expected_triples)}")
            if skip_index_check:
                command_parts.append("--skip-index-check")
            command = " ".join(command_parts)
            command_ok = run_container_command(
                method=f"{method}-validation",
                artifact_name=report_name,
                command=command,
                record_method=False,
                quiet_failure=skip_index_check,
            )
            try:
                report = json.loads(report_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                report = {
                    "valid": False,
                    "count_match": False,
                    "error": (
                        f"{artifact_format.upper()} validation command failed"
                        if not command_ok
                        else f"validator did not produce a valid report: {exc}"
                    ),
                }
            report_path.unlink(missing_ok=True)
            execution = auxiliary_stage_results.get(f"{method}-validation")
            if execution is not None:
                report["execution"] = execution
            return (
                bool(report.get("valid")) and bool(report.get("count_match")),
                report,
            )

        valid, report = perform_validation(skip_index_check=False)
        if valid:
            return True, report

        if allow_index_failure and artifact_format == "hdt":
            readable, readable_report = perform_validation(skip_index_check=True)
            if readable:
                warning = record_index_warning(
                    index_format="hdt",
                    artifact_path=target_out_dir / artifact_name,
                    stage="hdt-index",
                    message=report.get(
                        "error",
                        "HDT validation succeeded when the index check was skipped",
                    ),
                )
                readable_report["index_status"] = "failed"
                readable_report["index_warning"] = warning
                return True, readable_report

        eprint(
            f"Error: {artifact_format.upper()} validation failed for {artifact_name}: "
            f"{report.get('error', 'decoded triple count mismatch')}. "
            f"See log: {wrapper_log_path}"
        )
        return False, report

    def ensure_hdt_available():
        """Ensure `.hdt` exists for HDT-based compound methods."""
        nonlocal hdt_is_ready, hdt_source
        if hdt_is_ready:
            return True
        if hdt_path.exists():
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
            valid, report = validate_container_artifact(
                method="hdt",
                artifact_name=hdt_name,
                artifact_format="hdt",
                artifact_container=hdt_container,
            )
            if not valid:
                if allow_index_failure:
                    cottas_failure_warning = record_index_warning(
                        index_format="cottas",
                        artifact_path=cottas_path,
                        stage="cottas-index",
                        message=report.get(
                            "error",
                            "existing COTTAS validation/index check failed",
                        ),
                    )
                return False
            method_results["hdt"]["validation"] = report
            if report.get("index_status"):
                method_results["hdt"]["index_status"] = report["index_status"]
                method_results["hdt"]["index_warning"] = report.get("index_warning")
            hdt_is_ready = True
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
        if not run_container_command(method="hdt", artifact_name=hdt_name, command=hdt_command):
            return False
        valid, report = validate_container_artifact(
            method="hdt",
            artifact_name=hdt_name,
            artifact_format="hdt",
            artifact_container=hdt_container,
        )
        if not valid:
            return False
        method_results["hdt"]["validation"] = report
        if report.get("index_status"):
            method_results["hdt"]["index_status"] = report["index_status"]
            method_results["hdt"]["index_warning"] = report.get("index_warning")
        hdt_is_ready = True
        hdt_source = "generated"
        return True

    def ensure_cottas_available():
        """Ensure `.cottas` exists for COTTAS packaging stages."""
        nonlocal cottas_is_ready, cottas_failure_warning
        if cottas_is_ready:
            return True
        if cottas_failure_warning is not None:
            return False
        if cottas_path.exists():
            method_results.setdefault(
                "cottas",
                {
                    "exit_code": 0,
                    "wall_seconds": 0.0,
                    "user_seconds": 0.0,
                    "sys_seconds": 0.0,
                    "max_rss_kb": 0,
                    "output_path": str(cottas_path),
                    "output_size_bytes": int(file_size_bytes(cottas_path) or 0),
                    "source": "existing",
                },
            )
            valid, report = validate_container_artifact(
                method="cottas",
                artifact_name=cottas_name,
                artifact_format="cottas",
                artifact_container=cottas_container,
            )
            if not valid:
                return False
            method_results["cottas"]["validation"] = report
            cottas_is_ready = True
            return True
        cottas_command = (
            "set -euo pipefail; "
            f"rm -f {shlex.quote(cottas_container)}; "
            'PYTHON_BIN="${COTTAS_PYTHON_BIN:-$(command -v python3 || true)}"; '
            'if [[ -z "$PYTHON_BIN" || ! -x "$PYTHON_BIN" ]]; then '
            'echo "Missing pycottas Python executable in container" >&2; exit 127; fi; '
            f'"$PYTHON_BIN" /opt/vcf-rdfizer/cottas_tool.py convert '
            f"{shlex.quote(input_container)} {shlex.quote(cottas_container)} spo"
        )
        if not run_container_command(
            method="cottas",
            artifact_name=cottas_name,
            command=cottas_command,
            quiet_failure=allow_index_failure,
        ):
            if allow_index_failure:
                cottas_failure_warning = record_index_warning(
                    index_format="cottas",
                    artifact_path=cottas_path,
                    stage="cottas-index",
                    message="COTTAS conversion/index creation did not produce a usable artifact",
                )
            return False
        valid, report = validate_container_artifact(
            method="cottas",
            artifact_name=cottas_name,
            artifact_format="cottas",
            artifact_container=cottas_container,
        )
        if not valid:
            if allow_index_failure:
                cottas_failure_warning = record_index_warning(
                    index_format="cottas",
                    artifact_path=cottas_path,
                    stage="cottas-index",
                    message=report.get(
                        "error",
                        "COTTAS validation failed after conversion/index creation",
                    ),
                )
            return False
        method_results["cottas"]["validation"] = report
        cottas_is_ready = True
        return True

    def mark_cottas_method_unavailable(method: str):
        """Mark COTTAS and dependent packaging methods as skipped after a failure."""
        warning = cottas_failure_warning
        if warning is None:
            return
        result = method_results.setdefault(
            method,
            {
                "exit_code": 1,
                "wall_seconds": 0.0,
                "user_seconds": 0.0,
                "sys_seconds": 0.0,
                "max_rss_kb": 0,
                "output_path": str(
                    {
                        "cottas": cottas_path,
                        "cottas_gzip": target_out_dir / f"{input_stem}.cottas.gz",
                        "cottas_brotli": target_out_dir / f"{input_stem}.cottas.br",
                    }.get(method, cottas_path)
                ),
                "output_size_bytes": 0,
            },
        )
        result["exit_code"] = 1
        result["index_status"] = "failed"
        result["index_warning"] = warning

    for method in methods:
        if method == "gzip":
            artifact_name = f"{input_stem}.{input_ext}.gz"
            out_container = f"{target_out_container}/{artifact_name}"
            if rdf_path.name.endswith(".gz"):
                # The space-optimized aggregate is already gzip-compressed.
                # Keep it as the selected gzip artifact rather than producing
                # a redundant second gzip layer.
                if rdf_path.resolve() != (target_out_dir / artifact_name).resolve():
                    shutil.copyfile(rdf_path, target_out_dir / artifact_name)
                method_results[method] = {
                    "exit_code": 0,
                    "wall_seconds": 0.0,
                    "user_seconds": 0.0,
                    "sys_seconds": 0.0,
                    "max_rss_kb": 0,
                    "output_path": str(target_out_dir / artifact_name),
                    "output_size_bytes": int(file_size_bytes(target_out_dir / artifact_name) or 0),
                    "source": "already_compressed",
                }
                if status_indent is not None:
                    print(f"{status_indent}- {method}: {artifact_name} {success_symbol()} (already compressed)")
                continue
            command = (
                "set -euo pipefail; "
                f"rm -f {shlex.quote(out_container)}; "
                f"gzip -c {shlex.quote(input_container)} > {shlex.quote(out_container)}"
            )
            if not run_container_command(method=method, artifact_name=artifact_name, command=command):
                return False, method_results
            if status_indent is not None:
                print(f"{status_indent}- {method}: {artifact_name} {success_symbol()}")
            continue

        if method == "brotli":
            artifact_name = f"{input_stem}.{input_ext}.br"
            out_container = f"{target_out_container}/{artifact_name}"
            input_command = (
                f"gzip -dc {shlex.quote(input_container)}"
                if rdf_path.name.endswith(".gz")
                else f"cat {shlex.quote(input_container)}"
            )
            command = (
                "set -euo pipefail; "
                f"rm -f {shlex.quote(out_container)}; "
                f"{input_command} | brotli -q 7 -c > {shlex.quote(out_container)}"
            )
            if not run_container_command(method=method, artifact_name=artifact_name, command=command):
                return False, method_results
            if status_indent is not None:
                print(f"{status_indent}- {method}: {artifact_name} {success_symbol()}")
            continue

        if method == "cottas":
            if not ensure_cottas_available():
                if not allow_index_failure:
                    return False, method_results
                mark_cottas_method_unavailable(method)
                continue
            if status_indent is not None:
                suffix = " (reused existing COTTAS)" if method_results[method].get("source") == "existing" else ""
                print(f"{status_indent}- {method}: {cottas_name} {success_symbol()}{suffix}")
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
            artifact_name = f"{input_stem}.hdt.gz"
            out_container = f"{target_out_container}/{artifact_name}"
            command = (
                "set -euo pipefail; "
                f"rm -f {shlex.quote(out_container)}; "
                f"gzip -c {shlex.quote(hdt_container)} > {shlex.quote(out_container)}"
            )
            if not run_container_command(method=method, artifact_name=artifact_name, command=command):
                return False, method_results
            if status_indent is not None:
                suffix = " (using existing HDT)" if hdt_source == "existing" else ""
                print(f"{status_indent}- {method}: {artifact_name} {success_symbol()}{suffix}")
            continue

        if method == "hdt_brotli":
            if not ensure_hdt_available():
                return False, method_results
            artifact_name = f"{input_stem}.hdt.br"
            out_container = f"{target_out_container}/{artifact_name}"
            command = (
                "set -euo pipefail; "
                f"rm -f {shlex.quote(out_container)}; "
                f"brotli -q 7 -c {shlex.quote(hdt_container)} > {shlex.quote(out_container)}"
            )
            if not run_container_command(method=method, artifact_name=artifact_name, command=command):
                return False, method_results
            if status_indent is not None:
                suffix = " (using existing HDT)" if hdt_source == "existing" else ""
                print(f"{status_indent}- {method}: {artifact_name} {success_symbol()}{suffix}")
            continue

        if method == "cottas_gzip":
            if not ensure_cottas_available():
                if not allow_index_failure:
                    return False, method_results
                mark_cottas_method_unavailable(method)
                continue
            artifact_name = f"{input_stem}.cottas.gz"
            out_container = f"{target_out_container}/{artifact_name}"
            command = (
                "set -euo pipefail; "
                f"rm -f {shlex.quote(out_container)}; "
                f"gzip -c {shlex.quote(cottas_container)} > {shlex.quote(out_container)}"
            )
            if not run_container_command(method=method, artifact_name=artifact_name, command=command):
                return False, method_results
            if status_indent is not None:
                print(f"{status_indent}- {method}: {artifact_name} {success_symbol()}")
            continue

        if method == "cottas_brotli":
            if not ensure_cottas_available():
                if not allow_index_failure:
                    return False, method_results
                mark_cottas_method_unavailable(method)
                continue
            artifact_name = f"{input_stem}.cottas.br"
            out_container = f"{target_out_container}/{artifact_name}"
            command = (
                "set -euo pipefail; "
                f"rm -f {shlex.quote(out_container)}; "
                f"brotli -q 7 -c {shlex.quote(cottas_container)} > {shlex.quote(out_container)}"
            )
            if not run_container_command(method=method, artifact_name=artifact_name, command=command):
                return False, method_results
            if status_indent is not None:
                print(f"{status_indent}- {method}: {artifact_name} {success_symbol()}")
            continue

    if metrics_dir is not None and run_id is not None and timestamp is not None:
        write_raw_compression_metrics_artifact(
            metrics_dir=metrics_dir,
            run_id=run_id,
            timestamp=timestamp,
            output_name=metrics_output_name,
            rdf_name=rdf_path.name,
            source_rdf_path=rdf_path,
            selected_methods=methods,
            method_results=method_results,
            index_warnings=index_warnings,
            auxiliary_stages=auxiliary_stage_results,
        )

    return True, method_results


def run_containerized_partitioned_representation_methods(
    *,
    source_rdf_path: Path,
    out_dir: Path,
    image_ref: str,
    methods: list[str],
    wrapper_log_path: Path,
    metrics_dir: Path | None = None,
    run_id: str | None = None,
    timestamp: str | None = None,
    output_name: str,
    target_chunk_bytes: int,
    min_chunk_bytes: int,
    max_chunk_bytes: int,
    expected_triples: int | None = None,
    index_warnings: list[dict] | None = None,
):
    """Run partitioned compression in an ephemeral Docker-managed volume.

    The source and final outputs are the primary bind mounts. When interactive
    progress is enabled, a small metrics sidecar is mounted as well. Chunks,
    DuckDB scratch data, HDT/COTTAS merge intermediates, and stage timing files
    stay in a named volume that is removed in ``finally`` on both success and
    failure. A short JSON handoff carries the container-side metrics back to
    the host without exposing the temporary workspace.
    """
    ensure_dir(out_dir)
    if not source_rdf_path.is_file():
        eprint(f"Error: RDF source file not found: {source_rdf_path}. See log: {wrapper_log_path}")
        return False, {}

    safe_output_name = safe_metrics_name(output_name)
    volume_name = (
        f"vcf-rdfizer-{safe_output_name[:40]}-{os.getpid()}-{time.time_ns()}"
    )
    result_path = out_dir / f".{safe_output_name}.partitioned-results.json"
    progress_host_path = (
        progress_event_path(metrics_dir, "partitioned", safe_output_name)
        if progress_events_enabled()
        else None
    )
    progress_container_ref = container_progress_path(progress_host_path, metrics_dir)
    method_results: dict[str, dict] = {}
    volume_created = False

    source_resolved = source_rdf_path.resolve()
    out_resolved = out_dir.resolve()
    try:
        # Mount the source separately, even when it lives below the output
        # directory. This keeps the aggregate read-only inside the runner.
        source_mount = f"{source_resolved.parent}:/data/in:ro"
        source_container = f"/data/in/{source_resolved.name}"

        if run([*docker_cmd_prefix(), "volume", "create", volume_name]) != 0:
            eprint(f"Error: unable to create temporary Docker volume '{volume_name}'. See log: {wrapper_log_path}")
            return False, {}
        volume_created = True

        # A named volume is normally initialized by Docker as root. Make its
        # workspace writable for the mapped host user before running COTTAS.
        init_cmd = [
            *docker_run_base(as_user=False),
            "--mount",
            f"type=volume,source={volume_name},target=/work",
            image_ref,
            "bash",
            "-lc",
            "chmod 1777 /work",
        ]
        if run(init_cmd) != 0:
            eprint(f"Error: unable to initialize temporary Docker volume '{volume_name}'. See log: {wrapper_log_path}")
            return False, {}

        command = [
            *docker_run_base(),
            *docker_hdt_index_env_args(),
            *docker_hdt_merge_env_args(),
            *docker_cottas_merge_env_args(),
            "--mount",
            f"type=volume,source={volume_name},target=/work",
        ]
        if source_mount is not None:
            command.extend(["-v", source_mount])
        command.extend(
            [
                "-v",
                f"{out_resolved}:/data/out",
                *(
                    ["-v", f"{metrics_dir.resolve()}:/data/metrics"]
                    if metrics_dir is not None and progress_container_ref is not None
                    else []
                ),
                image_ref,
                "python3",
                PARTITIONED_COMPRESSION_RUNNER_CONTAINER,
                "--source",
                source_container,
                "--output-dir",
                "/data/out",
                "--output-name",
                output_name,
                "--methods",
                ",".join(methods),
                "--target-chunk-bytes",
                str(target_chunk_bytes),
                "--min-chunk-bytes",
                str(min_chunk_bytes),
                "--max-chunk-bytes",
                str(max_chunk_bytes),
                *(["--expected-triples", str(expected_triples)] if expected_triples is not None else []),
                "--result-path",
                f"/data/out/{result_path.name}",
                *(
                    ["--progress-path", progress_container_ref]
                    if progress_container_ref is not None
                    else []
                ),
            ]
        )
        if index_warnings is not None:
            command.append("--allow-index-failures")
        with ProgressSession(progress_host_path, f"Partitioned compression: {output_name}"):
            run_exit_code = run(command)
        payload = None
        if result_path.is_file():
            try:
                payload = json.loads(result_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                eprint(f"Error: invalid partitioned compression result: {exc}. See log: {wrapper_log_path}")

        if payload is not None:
            for warning in payload.get("index_warnings", []):
                artifact_path = str(warning.get("artifact_path", ""))
                if artifact_path.startswith("/data/out/"):
                    warning["artifact_path"] = str(
                        out_dir / artifact_path.removeprefix("/data/out/")
                    )
                if warning not in (index_warnings or []):
                    if index_warnings is not None:
                        index_warnings.append(warning)
                    eprint(
                        "Warning: "
                        f"{str(warning.get('format', 'representation')).upper()} index generation "
                        f"failed for '{warning.get('artifact_path', output_name)}'; "
                        "continuing with the remaining pipeline. "
                        f"{warning.get('message', '')}"
                    )
            method_results = payload.get("methods", {})
            for method, result in method_results.items():
                artifact_path = out_dir / compression_artifact_name_for_method(
                    Path(f"{output_name}.nt"), method
                )
                result["output_path"] = str(artifact_path)
                result["output_size_bytes"] = int(file_size_bytes(artifact_path) or 0)
                details = result.setdefault("details", {})
                if method == "hdt":
                    index_path = find_hdt_index_sidecar(artifact_path)
                    details["index_path"] = str(index_path) if index_path else ""
                    details["index_size_bytes"] = int(file_size_bytes(index_path) or 0) if index_path else 0
                if "source_paths" in details:
                    details["source_paths"] = [str(source_rdf_path)]
                for chunk in details.get("chunks", []):
                    if isinstance(chunk, dict) and chunk.get("path"):
                        chunk["path"] = Path(str(chunk["path"])).name
                details["workspace"] = "docker-volume"
                details["workspace_cleanup"] = "removed"
            if metrics_dir is not None:
                try:
                    write_partitioned_container_stage_report(
                        metrics_dir=metrics_dir,
                        output_name=output_name,
                        source_rdf_path=source_rdf_path,
                        payload=payload,
                    )
                except OSError as exc:
                    eprint(
                        "Warning: failed to preserve detailed partitioned container metrics: "
                        f"{exc}"
                    )

        missing_methods = set(methods) - set(method_results)
        if missing_methods and payload is not None and int(payload.get("exit_code", 1)) == 0:
            eprint(
                "Error: partitioned compression did not return results for: "
                + ", ".join(sorted(missing_methods))
                + f". See log: {wrapper_log_path}"
            )
            return False, method_results

        if run_exit_code != 0 or payload is None or int(payload.get("exit_code", 1)) != 0:
            error = payload.get("error") if payload else "container did not return a result"
            eprint(f"Error: partitioned compression failed: {error}. See log: {wrapper_log_path}")
            return False, method_results

        if metrics_dir is not None and run_id is not None and timestamp is not None:
            write_raw_compression_metrics_artifact(
                metrics_dir=metrics_dir,
                run_id=run_id,
                timestamp=timestamp,
                output_name=output_name,
                rdf_name="__partitioned_compression__",
                source_rdf_path=out_dir,
                selected_methods=methods,
                method_results=method_results,
                index_warnings=index_warnings,
            )
        return True, method_results
    finally:
        # The result handoff is not a user artifact; remove it before volume
        # cleanup so only final compression outputs and normal metrics remain.
        try:
            result_path.unlink()
        except OSError:
            pass
        if volume_created:
            cleanup_exit_code = run([*docker_cmd_prefix(), "volume", "rm", "-f", volume_name])
            if cleanup_exit_code != 0:
                eprint(
                    f"Warning: failed to remove temporary Docker volume '{volume_name}'. "
                    f"See log: {wrapper_log_path}"
                )


def run_partitioned_representation_methods_for_rdf_files(
    *,
    rdf_paths: list[Path],
    source_rdf_path: Path | None = None,
    out_dir: Path,
    image_ref: str,
    methods: list[str],
    wrapper_log_path: Path,
    metrics_dir: Path | None = None,
    run_id: str | None = None,
    timestamp: str | None = None,
    output_name: str,
    target_chunk_bytes: int,
    min_chunk_bytes: int,
    max_chunk_bytes: int,
    expected_triples: int | None = None,
    index_warnings: list[dict] | None = None,
):
    """Dispatch aggregate RDF to the ephemeral container pipeline.

    Partitioned compression intentionally accepts one logical aggregate. The
    full and compression CLI modes create that aggregate before this call;
    callers using this helper directly must provide exactly one source path.
    This prevents a second host-side implementation from reintroducing large
    intermediate files.
    """
    if source_rdf_path is None:
        if len(rdf_paths) != 1:
            eprint(
                "Error: partitioned compression requires one aggregate RDF source; "
                "combine RDF parts before calling this helper."
            )
            return False, {}
        source_rdf_path = rdf_paths[0]
    return run_containerized_partitioned_representation_methods(
        source_rdf_path=source_rdf_path,
        out_dir=out_dir,
        image_ref=image_ref,
        methods=methods,
        wrapper_log_path=wrapper_log_path,
        metrics_dir=metrics_dir,
        run_id=run_id,
        timestamp=timestamp,
        output_name=output_name,
        target_chunk_bytes=target_chunk_bytes,
        min_chunk_bytes=min_chunk_bytes,
        max_chunk_bytes=max_chunk_bytes,
        expected_triples=expected_triples,
        index_warnings=index_warnings,
    )


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
    sample_workflow: SampleWorkflow,
    rdf_storage_mode: str,
    methods: list[str],
    hdt_strategy: str,
    chunk_target_bytes: int,
    chunk_min_bytes: int,
    chunk_max_bytes: int,
    spark_partitions: int | None,
    keep_tsv: bool,
    keep_rmlstreamer_rdf_output: bool,
    remove_rdf_storage_output: bool,
    run_id: str,
    timestamp: str,
    wrapper_log_path: Path,
    run_tracker: RunTracker | None = None,
):
    """Execute full pipeline: per-input TSV -> RDF -> compression -> metrics."""
    print("Step 3/5: Processing per-input pipeline (TSV -> RDF -> compression)")
    if spark_partitions is not None:
        print(f"  Spark partition hint: {spark_partitions}")
    print(f"  Sample representation: {sample_workflow.representation}")
    intermediate_dir = tsv_dir.parent
    ensure_dir(tsv_dir)
    ensure_dir(out_dir)
    ensure_dir(metrics_dir)

    selected_methods = list(methods)
    use_partitioned_hdt = should_use_partitioned_hdt(
        mode="full",
        methods=selected_methods,
        hdt_strategy=hdt_strategy,
        rdf_storage_mode=rdf_storage_mode,
    )
    partitioned_methods = [
        method for method in selected_methods if method in PARTITIONED_COMPRESSION_METHODS
    ]
    non_partitioned_methods = [
        method for method in selected_methods if method not in PARTITIONED_COMPRESSION_METHODS
    ]
    use_partitioned_compression = (
        use_partitioned_hdt
        or any(method in COTTAS_COMPRESSION_METHODS for method in selected_methods)
    )

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
    index_warnings: list[dict] = []

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
        input_index_warnings: list[dict] = []

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
        expected_tsv_suffixes = ["records.tsv", "header_lines.tsv", "file_metadata.tsv"]
        if sample_workflow.helper_strategy != "none":
            expected_tsv_suffixes.extend(["sample_calls.tsv", "sample_format_values.tsv"])
        for suffix in expected_tsv_suffixes:
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

        tsv_metrics = run_tsv_conversion_with_metrics(
            input_mount_dir=input_mount_dir,
            container_input=container_input,
            tsv_dir=tsv_dir,
            metrics_dir=metrics_dir,
            image_ref=image_ref,
            run_id=run_id,
            timestamp=timestamp,
            prefix=expected_prefix,
        )
        tsv_exit_code = tsv_metrics.get("exit_code")
        if int(0 if tsv_exit_code is None else tsv_exit_code) != 0:
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
        sample_calls_tsv = tsv_dir / f"{prefix}.sample_calls.tsv"
        sample_format_tsv = tsv_dir / f"{prefix}.sample_format_values.tsv"
        try:
            if sample_workflow.helper_strategy == SAMPLE_HELPER_STRATEGY_MATERIALIZED:
                build_sample_support_tsvs(
                    records_tsv=triplet["records"],
                    sample_calls_tsv=sample_calls_tsv,
                    sample_format_tsv=sample_format_tsv,
                )
            elif sample_workflow.helper_strategy == "header-only":
                # RMLStreamer sees valid, empty canonical sources. Their
                # equivalent triples are appended directly after base mapping.
                write_sample_support_headers(sample_calls_tsv, sample_format_tsv)
            elif sample_workflow.helper_strategy != "none":
                raise RuntimeError(
                    f"unknown sample helper strategy: {sample_workflow.helper_strategy}"
                )
        except Exception as exc:
            fail_current(
                "tsv-derivation",
                f"failed generating sample helper TSVs for '{prefix}': {exc}. "
                f"See log: {wrapper_log_path}",
            )
            continue

        triplet["sample_calls"] = sample_calls_tsv
        triplet["sample_format_values"] = sample_format_tsv
        safe_prefix = slugify(prefix)
        generated_rules = generated_rules_dir / f"{safe_prefix}.rules.ttl"
        render_rules_for_triplet(
            rules_path,
            generated_rules,
            triplet["records"].name,
            triplet["headers"].name,
            triplet["metadata"].name,
            triplet["sample_calls"].name,
            triplet["sample_format_values"].name,
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
        progress_host_path = (
            progress_event_path(metrics_dir, "rmlstreamer", safe_prefix)
            if progress_events_enabled()
            else None
        )
        progress_container_ref = container_progress_path(progress_host_path, metrics_dir)

        run_cmd = [
            *docker_run_base(),
            "-v",
            f"{str(input_mount_dir)}:/data/in:ro",
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
            f"RDF_STORAGE_MODE={rdf_storage_mode}",
            "-e",
            f"SPARK_PARTITIONS={spark_partitions or ''}",
            "-e",
            f"RUN_ID={run_id}",
            "-e",
            f"TIMESTAMP={timestamp}",
            "-e",
            f"TSV_EXIT_CODE={int(tsv_metrics.get('exit_code') or 0)}",
            "-e",
            (
                "TSV_WALL_SECONDS="
                + (
                    "null"
                    if tsv_metrics.get("wall_seconds") is None
                    else f"{float(tsv_metrics.get('wall_seconds')):.6f}"
                )
            ),
            "-e",
            (
                "TSV_USER_SECONDS="
                + (
                    "null"
                    if tsv_metrics.get("user_seconds") is None
                    else f"{float(tsv_metrics.get('user_seconds')):.6f}"
                )
            ),
            "-e",
            (
                "TSV_SYS_SECONDS="
                + (
                    "null"
                    if tsv_metrics.get("sys_seconds") is None
                    else f"{float(tsv_metrics.get('sys_seconds')):.6f}"
                )
            ),
            "-e",
            (
                "TSV_MAX_RSS_KB="
                + (
                    "null"
                    if tsv_metrics.get("max_rss_kb") is None
                    else str(int(tsv_metrics.get("max_rss_kb")))
                )
            ),
            "-e",
            f"TSV_OUTPUT_SIZE_BYTES={int(tsv_metrics.get('output_size_bytes') or 0)}",
            "-e",
            (
                "TSV_OUTPUT_PATH="
                + "|".join(
                    [
                        f"/data/tsv/{expected_prefix}.records.tsv",
                        f"/data/tsv/{expected_prefix}.header_lines.tsv",
                        f"/data/tsv/{expected_prefix}.file_metadata.tsv",
                    ]
                )
            ),
            "-e",
            f"IN_VCF={container_input}",
            "-e",
            "LOGDIR=/data/metrics",
        ]
        if progress_container_ref is not None:
            run_cmd.extend(["-e", f"PROGRESS_FILE={progress_container_ref}"])
        run_cmd.extend([image_ref, "bash", "/opt/vcf-rdfizer/run_conversion.sh"])
        with ProgressSession(progress_host_path, f"RMLStreamer: {prefix}"):
            rdf_exit_code = run(run_cmd)
        if rdf_exit_code != 0:
            fail_current(
                "rdf-conversion",
                f"RMLStreamer step failed for '{prefix}'. See log: {wrapper_log_path}",
            )
            continue
        print(f"    * RDF conversion {success_symbol()}")
        if run_tracker is not None:
            run_tracker.mark(f"Input {idx}: RDF conversion completed for {prefix}")

        triples_produced = read_conversion_total_triples(metrics_dir, output_name, run_id)

        if rdf_storage_mode == "space-optimized":
            # The optimized mode leaves one gzip stream assembled from the
            # RMLStreamer parts; chunk planning will decompress it incrementally.
            raw_rdf_files = [out_dir / output_name / f"{output_name}.nt.gz"]
        else:
            # Plain storage yields one merged RDF artifact per sample.
            raw_rdf_files = [out_dir / output_name / f"{output_name}.nt"]

        missing_raw_rdf = [path for path in raw_rdf_files if not path.is_file()]
        if missing_raw_rdf:
            fail_current(
                "rdf-discovery",
                "expected RDF output was not produced: "
                + ", ".join(str(path) for path in missing_raw_rdf)
                + f". See log: {wrapper_log_path}",
            )
            continue

        sample_stats = None
        if sample_workflow.emitter is not None:
            print(
                f"    * Streaming {sample_workflow.representation} multi-sample RDF "
                "(no expanded helper TSVs)"
            )
            try:
                sample_stats = emit_sample_representation(
                    sample_workflow,
                    records_tsv=triplet["records"],
                    header_lines_tsv=triplet["headers"],
                    rdf_path=raw_rdf_files[0],
                )
            except Exception as exc:
                fail_current(
                    f"{sample_workflow.representation}-sample-rdf-streaming",
                    f"failed streaming {sample_workflow.representation} sample RDF "
                    f"for '{prefix}': {exc}. "
                    f"See log: {wrapper_log_path}",
                )
                continue
            if triples_produced is not None:
                triples_produced += int(sample_stats["triples"])
            if sample_workflow.representation == "expanded":
                print(
                    "    * Sample calls streamed: "
                    f"{sample_stats['sample_calls']:,}; FORMAT values: "
                    f"{sample_stats['format_values']:,}"
                )
            else:
                print(
                    "    * Condensed matrices streamed: "
                    f"{sample_stats['matrices']:,}; FORMAT vectors: "
                    f"{sample_stats['format_vectors']:,}; reusable samples: "
                    f"{sample_stats['samples']:,}"
                )

        if triples_produced is None:
            triples_produced = count_triples_in_nt_files(raw_rdf_files)
        if sample_stats is not None and triples_produced is not None:
            update_conversion_metrics_after_sample_stream(
                metrics_dir=metrics_dir,
                output_name=output_name,
                run_id=run_id,
                rdf_path=raw_rdf_files[0],
                total_triples=triples_produced,
                sample_stats=sample_stats,
            )
        if triples_produced is not None:
            saw_triple_counts = True
            total_triples_produced += triples_produced
            print(f"    * Triples produced: {triples_produced:,}")

        if run_tracker is not None:
            for raw_rdf_path in raw_rdf_files:
                run_tracker.track_raw_rdf(raw_rdf_path)

        method_results_by_file: dict[str, dict[str, dict]] = {}
        partitioned_representation_results: dict[str, dict] = {}
        if selected_methods:
            # Ordinary methods process the single aggregate. HDT and COTTAS
            # consume one shared, record-safe chunk stream when partitioning is active.
            per_file_methods = selected_methods
            if use_partitioned_compression:
                per_file_methods = non_partitioned_methods

            for raw_rdf_path in raw_rdf_files:
                if not per_file_methods:
                    method_results_by_file[raw_rdf_path.name] = {}
                    continue
                ok, method_results = run_compression_methods_for_rdf(
                    rdf_path=raw_rdf_path,
                    out_dir=out_dir / output_name,
                    target_out_dir=out_dir / output_name,
                    image_ref=image_ref,
                    methods=per_file_methods,
                    wrapper_log_path=wrapper_log_path,
                    status_indent=None,
                    metrics_dir=metrics_dir,
                    run_id=run_id,
                    timestamp=timestamp,
                    output_name=output_name,
                    expected_triples=triples_produced,
                    index_warnings=input_index_warnings,
                )
                if not ok:
                    fail_current(
                        "compression",
                        f"compression failed for '{raw_rdf_path.name}'. See log: {wrapper_log_path}",
                    )
                    break
                method_results_by_file[raw_rdf_path.name] = method_results

            if not input_failed and use_partitioned_compression and partitioned_methods:
                ok, partitioned_representation_results = run_partitioned_representation_methods_for_rdf_files(
                    rdf_paths=[],
                    source_rdf_path=raw_rdf_files[0],
                    out_dir=out_dir / output_name,
                    image_ref=image_ref,
                    methods=partitioned_methods,
                    wrapper_log_path=wrapper_log_path,
                    metrics_dir=metrics_dir,
                    run_id=run_id,
                    timestamp=timestamp,
                    output_name=output_name,
                    target_chunk_bytes=chunk_target_bytes,
                    min_chunk_bytes=chunk_min_bytes,
                    max_chunk_bytes=chunk_max_bytes,
                    expected_triples=triples_produced,
                    index_warnings=input_index_warnings,
                )
                if not ok:
                    fail_current(
                        "compression",
                        f"partitioned compression failed for '{output_name}'. See log: {wrapper_log_path}",
                    )
        for warning in input_index_warnings:
            warning.setdefault("input_index", idx)
            warning.setdefault("input_vcf", input_vcf)
            warning.setdefault("expected_prefix", expected_prefix)
            if warning not in index_warnings:
                index_warnings.append(warning)
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
            # Full mode always has one aggregate RDF source, so metrics are
            # written once per VCF input regardless of the storage mode.
            aggregated_results = dict(method_results_by_file.get(raw_rdf_files[0].name, {}))
            aggregated_results.update(partitioned_representation_results)
            combined_size_before_cleanup = sum(raw_size_before_cleanup_by_file.values())
            write_compression_metrics_artifacts(
                metrics_dir=metrics_dir,
                run_id=run_id,
                timestamp=timestamp,
                output_name=output_name,
                source_rdf_path=raw_rdf_files[0],
                combined_size_bytes=combined_size_before_cleanup,
                selected_methods=selected_methods,
                method_results=aggregated_results,
                index_warnings=input_index_warnings,
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
                tsv_metrics=tsv_metrics,
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

        rdf_storage_removed = False
        if selected_methods and (
            remove_rdf_storage_output or not keep_rmlstreamer_rdf_output
        ):
            # Cleanup raw RDF only after every selected compression method has
            # completed successfully for that specific RDF artifact.
            cleanup_failed = False
            warning_formats = {
                warning.get("format")
                for warning in input_index_warnings
                if warning.get("format") in {"hdt", "cottas"}
            }
            optional_failed_methods = {
                method
                for method in selected_methods
                if (
                    (method in HDT_COMPRESSION_METHODS and "hdt" in warning_formats)
                    or (method in COTTAS_COMPRESSION_METHODS and "cottas" in warning_formats)
                )
            }
            if use_partitioned_compression:
                missing_or_failed_hdt = []
                for method in partitioned_methods:
                    result = partitioned_representation_results.get(method)
                    if (
                        method not in optional_failed_methods
                        and (result is None or int(result.get("exit_code", 1)) != 0)
                    ):
                        missing_or_failed_hdt.append(method)
                if missing_or_failed_hdt:
                    fail_current(
                        "rdf-cleanup-validation",
                        "refusing to remove raw RDF before the final merged compression pipeline "
                        f"completed successfully for '{output_name}'. Pending/failed: "
                        f"{', '.join(missing_or_failed_hdt)}. See log: {wrapper_log_path}",
                    )
                    cleanup_failed = True
            for raw_rdf_path in raw_rdf_files:
                if cleanup_failed:
                    break
                method_results = method_results_by_file.get(raw_rdf_path.name, {})
                methods_to_validate = selected_methods
                if use_partitioned_compression:
                    methods_to_validate = non_partitioned_methods
                missing_or_failed = []
                for method in methods_to_validate:
                    result = method_results.get(method)
                    if (
                        method not in optional_failed_methods
                        and (result is None or int(result.get("exit_code", 1)) != 0)
                    ):
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

                if optional_failed_methods:
                    eprint(
                        f"Warning: retaining raw RDF '{raw_rdf_path.name}' because "
                        "one or more COTTAS/HDT index-dependent outputs were unavailable."
                    )
                    break

                # In space-optimized mode the aggregate `.nt.gz` is itself
                # the gzip artifact. Do not remove it when gzip was selected.
                if (
                    rdf_storage_mode == "space-optimized"
                    and method_results.get("gzip", {}).get("output_path") == str(raw_rdf_path)
                ):
                    continue
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
            rdf_storage_removed = not any(path.exists() for path in raw_rdf_files)

        if raw_rdf_files:
            output_root = out_dir / output_name
            raw_total_size = sum(raw_size_before_cleanup_by_file.values())

            if keep_rmlstreamer_rdf_output:
                raw_note = "retained via --keep-rmlstreamer-rdf-output"
            elif rdf_storage_removed and remove_rdf_storage_output:
                raw_note = "removed via --remove-rdf-storage-output"
            elif rdf_storage_removed and selected_methods:
                raw_note = "removed after successful compression"
            elif selected_methods and rdf_storage_mode == "space-optimized" and "gzip" in selected_methods:
                raw_note = "retained because it is also the selected gzip artifact"
            elif selected_methods:
                raw_note = "retained"
            else:
                raw_note = "kept (compression methods set to none)"

            first_path = raw_rdf_files[0]
            print(f"    * Output directory: {output_root}")
            print(f"      - RDF aggregate: {first_path.name}")
            raw_text = format_bytes(raw_total_size)
            print(
                f"      - {rdf_label_for_path(first_path)}: {raw_text} "
                f"({raw_note})"
            )

            if selected_methods:
                for method in selected_methods:
                    if use_partitioned_compression and method in PARTITIONED_COMPRESSION_METHODS:
                        result = partitioned_representation_results.get(method)
                        label = compression_method_label_for_path(first_path, method)
                        if not result or int(result.get("exit_code", 1)) != 0:
                            print(f"      - {label}: not generated")
                        else:
                            print(
                                f"      - {label}: {format_bytes(int(result.get('output_size_bytes') or 0))} "
                                f"({result.get('output_path', '')})"
                            )
                        continue
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
                        print(f"      - {label}: {format_bytes(method_total)}")
            else:
                print("      - Compression: none selected")
                print(f"      - Final RDF size (no compression): {format_bytes(raw_total_size)}")
        else:
            for raw_rdf_path in raw_rdf_files:
                hdt_path = (out_dir / output_name) / f"{raw_rdf_path.stem}.hdt"
                rdf_size = file_size_bytes(raw_rdf_path)
                nt_note = None
                method_results = method_results_by_file.get(raw_rdf_path.name, {})
                if raw_rdf_path.exists() and keep_rmlstreamer_rdf_output:
                    nt_note = "retained via --keep-rmlstreamer-rdf-output"
                elif raw_rdf_path.exists() and selected_methods and rdf_storage_mode == "space-optimized" and "gzip" in selected_methods:
                    nt_note = "retained because it is also the selected gzip artifact"
                elif not raw_rdf_path.exists() and remove_rdf_storage_output:
                    nt_note = "removed via --remove-rdf-storage-output"
                elif not raw_rdf_path.exists() and selected_methods:
                    nt_note = "removed after successful compression"
                elif not raw_rdf_path.exists() and not selected_methods:
                    nt_note = "kept (compression methods set to none)"
                else:
                    nt_note = "retained"
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
            for tsv_path in (
                triplet["records"],
                triplet["headers"],
                triplet["metadata"],
                triplet.get("sample_calls"),
                triplet.get("sample_format_values"),
            ):
                if tsv_path is None:
                    continue
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

    index_warning_report = None
    if index_warnings:
        index_warning_report = write_index_warnings_report(
            metrics_dir=metrics_dir,
            run_id=run_id,
            warnings=index_warnings,
        )
        eprint(
            f"Index generation warnings were recorded for {len(index_warnings)} item(s): "
            f"{index_warning_report}"
        )
        print(f"Index warnings: {index_warning_report}")

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

    if index_warning_report is not None:
        print("Conversion process finished with index warnings.")
    else:
        print("Conversion process finished.")
    if run_tracker is not None:
        run_tracker.mark(
            "Full pipeline finished successfully"
            + (f" with index warnings; report: {index_warning_report}" if index_warning_report else "")
        )
    return 0


def run_compress_mode(
    *,
    rdf_path: Path,
    out_dir: Path,
    metrics_dir: Path,
    run_id: str,
    timestamp: str,
    image_ref: str,
    methods: list[str],
    hdt_strategy: str,
    chunk_target_bytes: int,
    chunk_min_bytes: int,
    chunk_max_bytes: int,
    wrapper_log_path: Path,
):
    """Execute compression-only mode for a designated RDF file."""
    print("Step 3/3: Compressing RDF input")
    if not methods:
        print("No compression methods selected (`none`). Nothing to do.")
        return 0

    if rdf_path.name.endswith(".gz") and compression_uses_hdt(methods) and hdt_strategy == "single":
        eprint(
            "Error: --hdt-strategy single cannot read a gzip aggregate without materializing "
            "an uncompressed RDF file; use --hdt-strategy partitioned."
        )
        return 2

    if any(method in HDT_COMPRESSION_METHODS for method in methods):
        file_size = file_size_bytes(rdf_path) or 0
        if file_size > 5 * 1024 * 1024 * 1024:
            eprint(
                "Warning: selected HDT compression for an .nt file larger than 5 GB. "
                "This may fail due to memory limits depending on environment."
            )

    ensure_dir(out_dir)
    input_stem = rdf_output_basename(rdf_path)
    use_partitioned_compression = compression_uses_partitioning(methods) and (
        any(method in COTTAS_COMPRESSION_METHODS for method in methods)
        or should_use_partitioned_hdt(
            mode="compress",
            methods=methods,
            hdt_strategy=hdt_strategy,
        )
    )
    if use_partitioned_compression:
        raw_methods = [method for method in methods if method not in PARTITIONED_COMPRESSION_METHODS]
        partitioned_methods = [method for method in methods if method in PARTITIONED_COMPRESSION_METHODS]
        method_results = {}
        if raw_methods:
            ok, raw_method_results = run_compression_methods_for_rdf(
                rdf_path=rdf_path,
                out_dir=out_dir,
                image_ref=image_ref,
                methods=raw_methods,
                wrapper_log_path=wrapper_log_path,
                status_indent="  ",
                metrics_dir=metrics_dir,
                run_id=run_id,
                timestamp=timestamp,
                output_name=input_stem,
            )
            if not ok:
                return 1
            method_results.update(raw_method_results)
        ok, partitioned_results = run_partitioned_representation_methods_for_rdf_files(
            rdf_paths=[],
            source_rdf_path=rdf_path,
            out_dir=out_dir / input_stem,
            image_ref=image_ref,
            methods=partitioned_methods,
            wrapper_log_path=wrapper_log_path,
            metrics_dir=metrics_dir,
            run_id=run_id,
            timestamp=timestamp,
            output_name=input_stem,
            target_chunk_bytes=chunk_target_bytes,
            min_chunk_bytes=chunk_min_bytes,
            max_chunk_bytes=chunk_max_bytes,
        )
        if not ok:
            return 1
        method_results.update(partitioned_results)
    else:
        ok, method_results = run_compression_methods_for_rdf(
            rdf_path=rdf_path,
            out_dir=out_dir,
            image_ref=image_ref,
            methods=methods,
            wrapper_log_path=wrapper_log_path,
            status_indent="  ",
            metrics_dir=metrics_dir,
            run_id=run_id,
            timestamp=timestamp,
            output_name=input_stem,
        )
        if not ok:
            return 1

    target_out_dir = out_dir / input_stem
    source_size_bytes = int(file_size_bytes(rdf_path) or 0)
    try:
        write_compression_metrics_artifacts(
            metrics_dir=metrics_dir,
            run_id=run_id,
            timestamp=timestamp,
            output_name=input_stem,
            source_rdf_path=rdf_path,
            combined_size_bytes=source_size_bytes,
            selected_methods=methods,
            method_results=method_results,
        )
        update_metrics_csv_with_compression(
            metrics_csv=metrics_dir / "metrics.csv",
            run_id=run_id,
            timestamp=timestamp,
            output_name=input_stem,
            output_dir=target_out_dir,
            combined_size_bytes=source_size_bytes,
            selected_methods=methods,
            method_results=method_results,
        )
    except OSError as exc:
        eprint(f"Error: unable to write compression metrics: {exc}")
        return 1
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
    if (
        path.name.endswith(".cottas")
        or path.name.endswith(".cottas.gz")
        or path.name.endswith(".cottas.br")
    ):
        return "cottas"
    if path.name.endswith(".nt.gz") or path.suffix == ".gz":
        return "gzip"
    if path.name.endswith(".nt.br") or path.suffix == ".br":
        return "brotli"
    if path.suffix == ".hdt":
        return "hdt"
    raise ValueError(
        "Compressed input must end with .nt.gz, .nt.br, .hdt, .cottas, .cottas.gz, or .cottas.br"
    )


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
    if fmt == "cottas":
        for suffix in (".cottas.gz", ".cottas.br", ".cottas"):
            if path.name.endswith(suffix):
                return f"{path.name[: -len(suffix)]}.nt"
    return f"{path.stem}.nt"


def run_index_mode(
    *,
    index_path: Path,
    index_format: str,
    metrics_dir: Path,
    image_ref: str,
    wrapper_log_path: Path,
    run_id: str | None = None,
    timestamp: str | None = None,
):
    """Generate or regenerate the query index for one existing artifact.

    HDT writes its index as a versioned sibling sidecar. COTTAS stores its
    query index in the Parquet artifact itself, so the Docker-side adapter
    rewrites that file atomically with the requested index order.
    """
    format_label = index_format.upper()
    print(f"Step 3/3: Regenerating {format_label} index")
    ensure_dir(metrics_dir)

    existing_index_path = (
        find_hdt_index_sidecar(index_path) if index_format == "hdt" else None
    )
    mount_name = "hdt" if index_format == "hdt" else "cottas"
    source_container = f"/data/{mount_name}/{index_path.name}"
    if index_format == "hdt":
        command = (
            "set -euo pipefail; "
            f"{shlex.quote(HDT_INDEX_HELPER_CONTAINER)} {shlex.quote(source_container)}"
        )
    else:
        command = (
            "set -euo pipefail; "
            'PYTHON_BIN="${COTTAS_PYTHON_BIN:-$(command -v python3 || true)}"; '
            'if [[ -z "$PYTHON_BIN" || ! -x "$PYTHON_BIN" ]]; then '
            'echo "Missing pycottas Python executable in container" >&2; exit 127; fi; '
            f'"$PYTHON_BIN" {shlex.quote(COTTAS_TOOL_CONTAINER)} reindex '
            f"{shlex.quote(source_container)} spo"
        )
    safe_input = safe_metrics_name(index_path.name)
    timing_host = metrics_dir / "timings" / "index" / f"{safe_input}.txt"
    timing_host.parent.mkdir(parents=True, exist_ok=True)
    timing_container = f"/data/metrics/timings/index/{safe_input}.txt"
    timed_command = (
        "set -euo pipefail; "
        f"rm -f {shlex.quote(timing_container)}; "
        'if [[ -x /usr/bin/time ]] && /usr/bin/time --version >/dev/null 2>&1; then '
        f"/usr/bin/time -v -o {shlex.quote(timing_container)} -- bash -lc {shlex.quote(command)}; "
        "else "
        f"{{ time -p bash -lc {shlex.quote(command)}; }} > {shlex.quote(timing_container)} 2>&1; "
        "fi"
    )
    input_size_bytes = int(file_size_bytes(index_path) or 0)
    cmd = [
        *docker_run_base(),
        *docker_hdt_index_env_args(),
        *docker_cottas_merge_env_args(),
        "-v",
        f"{str(index_path.parent)}:/data/{mount_name}",
        "-v",
        f"{str(metrics_dir.resolve())}:/data/metrics",
        image_ref,
        "bash",
        "-lc",
        timed_command,
    ]

    started = time.perf_counter()
    exit_code = run(cmd)
    elapsed = time.perf_counter() - started
    timing = parse_time_log_metrics(timing_host)
    index_path_after = (
        find_hdt_index_sidecar(index_path)
        if index_format == "hdt"
        else (index_path if file_size_bytes(index_path) else None)
    )
    index_ready = index_path_after is not None
    final_code = int(exit_code) if int(exit_code) != 0 else (0 if index_ready else 1)
    index_was_present = existing_index_path is not None or index_format == "cottas"
    payload = {
        "run_id": run_id,
        "timestamp": timestamp,
        "index_format": index_format,
        "input_path": str(index_path),
        "input_size_bytes": input_size_bytes,
        "index_path": str(index_path_after) if index_path_after else "",
        "index_location": "sidecar" if index_format == "hdt" else "embedded",
        "exit_code": final_code,
        "wall_seconds": elapsed,
        "timing": {
            "wall_seconds": elapsed,
            "user_seconds": timing.get("user_seconds"),
            "sys_seconds": timing.get("sys_seconds"),
            "max_rss_kb": timing.get("max_rss_kb"),
        },
        "index_status": (
            "regenerated" if index_was_present else "generated"
        ) if index_ready else "failed",
        "index_size_bytes": file_size_bytes(index_path_after) if index_path_after else 0,
    }
    if index_format == "hdt":
        # Preserve the field used by the original HDT-only metrics payload.
        payload["hdt_path"] = str(index_path)
    else:
        payload["cottas_path"] = str(index_path)
    metrics_path = metrics_dir / "stages" / "index" / f"{index_format}-{safe_input}.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    if final_code != 0:
        eprint(f"Error: {format_label} index regeneration failed. See log: {wrapper_log_path}")
        return 1

    print(f"{format_label} index ready: {index_path_after}")
    print(f"Index metrics: {metrics_path}")
    return 0


def run_hdt_index_mode(
    *,
    hdt_path: Path,
    metrics_dir: Path,
    image_ref: str,
    wrapper_log_path: Path,
):
    """Backward-compatible wrapper for the HDT-only index helper."""
    return run_index_mode(
        index_path=hdt_path,
        index_format="hdt",
        metrics_dir=metrics_dir,
        image_ref=image_ref,
        wrapper_log_path=wrapper_log_path,
    )


def run_decompress_mode(
    *,
    compressed_path: Path,
    decompressed_out: Path,
    metrics_dir: Path,
    run_id: str,
    timestamp: str,
    image_ref: str,
    wrapper_log_path: Path,
):
    """Execute decompression-only mode (.gz/.br/.hdt/.cottas -> RDF)."""
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
    elif fmt == "hdt":
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
    else:
        # COTTAS is seekable, so packaged `.cottas.gz`/`.cottas.br` inputs are
        # unwrapped inside the container before pycottas reads them. The
        # temporary representation never appears on the host filesystem.
        cottas_input = source_container
        cleanup = ""
        if compressed_path.name.endswith(".cottas.gz"):
            cottas_input = "/work/vcf-rdfizer-cottas-input"
            cleanup = (
                f"rm -f {shlex.quote(cottas_input)}; "
                f"trap 'rm -f {shlex.quote(cottas_input)}' EXIT; "
                f"gzip -dc {shlex.quote(source_container)} > {shlex.quote(cottas_input)}; "
            )
        elif compressed_path.name.endswith(".cottas.br"):
            cottas_input = "/work/vcf-rdfizer-cottas-input"
            cleanup = (
                f"rm -f {shlex.quote(cottas_input)}; "
                f"trap 'rm -f {shlex.quote(cottas_input)}' EXIT; "
                f"brotli -d -c {shlex.quote(source_container)} > {shlex.quote(cottas_input)}; "
            )
        command = (
            "set -euo pipefail; "
            f"{cleanup}"
            'COTTAS_PYTHON_BIN="${COTTAS_PYTHON_BIN:-$(command -v python3 || true)}"; '
            'if [[ -z "$COTTAS_PYTHON_BIN" ]]; then '
            'echo "Missing pycottas Python executable in container" >&2; exit 127; '
            "fi; "
            f'"$COTTAS_PYTHON_BIN" /opt/vcf-rdfizer/cottas_tool.py decompress '
            f"{shlex.quote(cottas_input)} {shlex.quote(output_container)}"
        )

    safe_input = safe_metrics_name(compressed_path.name)
    timing_host = metrics_dir / "timings" / "decompression" / f"{safe_input}.txt"
    timing_host.parent.mkdir(parents=True, exist_ok=True)
    timing_container = f"/data/metrics/timings/decompression/{safe_input}.txt"
    timed_command = (
        "set -euo pipefail; "
        f"rm -f {shlex.quote(timing_container)}; "
        'if [[ -x /usr/bin/time ]] && /usr/bin/time --version >/dev/null 2>&1; then '
        f"/usr/bin/time -v -o {shlex.quote(timing_container)} -- bash -lc {shlex.quote(command)}; "
        "else "
        f"{{ time -p bash -lc {shlex.quote(command)}; }} > {shlex.quote(timing_container)} 2>&1; "
        "fi"
    )
    input_size_bytes = int(file_size_bytes(compressed_path) or 0)
    cmd = [
        *docker_run_base(),
        "-v",
        f"{str(compressed_path.parent)}:/data/in:ro",
        "-v",
        f"{str(decompressed_out.parent)}:/data/out",
        "-v",
        f"{str(metrics_dir.resolve())}:/data/metrics",
        image_ref,
        "bash",
        "-lc",
        timed_command,
    ]
    started = time.perf_counter()
    exit_code = run(cmd)
    elapsed = time.perf_counter() - started
    timing = parse_time_log_metrics(timing_host)
    stage_payload = {
        "run_id": run_id,
        "timestamp": timestamp,
        "format": fmt,
        "input_path": str(compressed_path),
        "input_size_bytes": input_size_bytes,
        "output_path": str(decompressed_out),
        "output_size_bytes": int(file_size_bytes(decompressed_out) or 0),
        # Decompression is deliberately a single pass. Counting N-Triples
        # here would reread a cohort-scale output solely for observability.
        "output_triples": None,
        "exit_code": int(exit_code),
        "timing": {
            "wall_seconds": elapsed,
            "user_seconds": timing.get("user_seconds"),
            "sys_seconds": timing.get("sys_seconds"),
            "max_rss_kb": timing.get("max_rss_kb"),
        },
    }
    report_path = metrics_dir / "stages" / "decompression" / f"{safe_input}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(stage_payload, indent=2) + "\n", encoding="utf-8")
    if exit_code != 0:
        eprint(f"Error: decompression failed. See log: {wrapper_log_path}")
        return 1

    print(f"Done. Decompressed file: {decompressed_out}")
    print(f"Decompression metrics: {report_path}")
    return 0


def run_validation_mode(
    *,
    vcf_path: Path,
    rdf_gzip_path: Path,
    representation: str,
    validation_id: str,
    results_dir: Path,
    metrics_dir: Path,
    run_id: str,
    timestamp: str,
    image_ref: str,
    filter_oracle: str,
    wrapper_log_path: Path,
):
    """Run VCF/RDF semantic queries with ephemeral in-container RDF expansion.

    The only mounted RDF source is the input ``.nt.gz`` file. The validation
    runner inflates it under the container's ``/work`` temporary filesystem and
    removes that source before the container exits; no raw N-Triples are
    materialized in the user-selected output directory.
    """
    # An empty directory may be left behind if Docker itself fails before the
    # runner starts. Reuse only that empty shell; never overwrite reports.
    results_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        *docker_run_base(),
        "--init",
        "-v",
        f"{str(vcf_path.parent)}:/data/vcf:ro",
        "-v",
        f"{str(rdf_gzip_path.parent)}:/data/rdf:ro",
        "-v",
        f"{str(results_dir)}:/data/validation",
        image_ref,
        "/opt/pycottas-venv/bin/python",
        "/opt/vcf-rdfizer/validation/validation_runner.py",
        "--vcf",
        f"/data/vcf/{vcf_path.name}",
        "--rdf-gz",
        f"/data/rdf/{rdf_gzip_path.name}",
        "--representation",
        representation,
        "--results-dir",
        "/data/validation",
        "--dataset-id",
        validation_id,
        "--filter-oracle",
        filter_oracle,
        "--scratch-dir",
        "/work",
    ]
    started = time.perf_counter()
    exit_code = run(cmd)
    elapsed = time.perf_counter() - started
    summary_path = results_dir / "summary.json"
    summary = None
    if summary_path.is_file():
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            summary = None
    report_path = metrics_dir / "stages" / "validation" / f"{safe_metrics_name(validation_id)}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "timestamp": timestamp,
                "vcf_path": str(vcf_path),
                "rdf_gzip_path": str(rdf_gzip_path),
                "representation": representation,
                "results_dir": str(results_dir),
                "input_rdf_size_bytes": int(file_size_bytes(rdf_gzip_path) or 0),
                "exit_code": int(exit_code),
                "status": summary.get("status") if isinstance(summary, dict) else None,
                "timing": {"wall_seconds": elapsed},
                "temporary_rdf": {
                    "decompressed_inside_container": True,
                    "persisted_on_host": False,
                    "cleanup_confirmed_by_runner": (
                        summary.get("temporaryRdf", {}).get("cleanupConfirmed")
                        if isinstance(summary, dict)
                        else False
                    ),
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    if exit_code != 0:
        eprint(f"Error: validation failed. See results: {results_dir}")
        eprint(f"See log for details: {wrapper_log_path}")
        return 1
    print(f"Validation results: {results_dir}")
    print(f"Validation metrics: {report_path}")
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
            "  Full pipeline (plain aggregate):\n"
            "    vcf_rdfizer.py -m full -i ./vcf_files --rdf-storage-mode plain "
            "--representations hdt -o ./results\n"
            "  Full pipeline (space-optimized aggregate):\n"
            "    vcf_rdfizer.py -m full -i ./vcf_files --rdf-storage-mode space-optimized "
            "--representations hdt,cottas --rdf-compression none -o ./results\n"
            "  Condensed multi-sample representation:\n"
            "    vcf_rdfizer.py -m full -i ./cohort.vcf.gz --sample-representation condensed "
            "--rdf-storage-mode space-optimized --representations hdt -o ./results\n"
            "  Space-optimized full pipeline with shared HDT/COTTAS chunks:\n"
            "    vcf_rdfizer.py -m full -i ./vcf_files --rdf-storage-mode space-optimized "
            "--representations hdt,cottas --rdf-compression none "
            "--chunk-target-bytes 536870912 -o ./results\n"
            "  Ultra-small full pipeline (remove aggregate RDF after compression):\n"
            "    vcf_rdfizer.py -m full -i ./vcf_files --rdf-storage-mode space-optimized "
            "--representations hdt --rdf-compression none --hdt-strategy partitioned "
            "--remove-rdf-storage-output -o ./results\n"
            "  Queryable HDT plus gzip-packaged HDT:\n"
            "    vcf_rdfizer.py -m full -i ./vcf_files --rdf-storage-mode space-optimized "
            "--rdf-compression none --representations hdt --artifact-compression gzip -o ./results\n"
            "  TSV-only benchmark:\n"
            "    vcf_rdfizer.py -m tsv -i ./vcf_files -o ./results\n"
            "  Compression-only:\n"
            "    vcf_rdfizer.py -m compress --rdf ./results/out/sample/sample.nt "
            "--rdf-compression gzip --representations hdt --artifact-compression gzip -o ./results\n"
            "  Decompression-only:\n"
            "    vcf_rdfizer.py -m decompress -C ./results/out/sample/sample.nt.gz -o ./results\n"
            "  Semantic VCF/RDF validation (expanded graph):\n"
            "    vcf_rdfizer.py -m validation -i ./sample.vcf.gz --rdf ./results/sample/sample.nt.gz "
            "--sample-representation expanded -o ./validation-results\n"
            "  Semantic VCF/RDF validation (condensed graph):\n"
            "    vcf_rdfizer.py -m validation -i ./cohort.vcf.gz --rdf ./results/cohort/cohort.nt.gz "
            "--sample-representation condensed -o ./validation-results\n"
            "  Generate or regenerate an index for an existing HDT:\n"
            "    vcf_rdfizer.py -m index -H ./results/sample/sample.hdt -o ./results\n"
            "  Generate or regenerate an index for an existing COTTAS file:\n"
            "    vcf_rdfizer.py -m index --cottas ./results/sample/sample.cottas -o ./results\n"
        ),
    )
    parser.add_argument(
        "-m",
        "--mode",
        choices=["full", "compress", "decompress", "tsv", "index", "validation"],
        default="full",
        help="Run mode: full pipeline, TSV benchmark, compression, decompression, validation, or index-only regeneration",
    )
    parser.add_argument(
        "-i",
        "--input",
        default=None,
        help="VCF file or directory (required for --mode full/tsv; file required for --mode validation)",
    )
    parser.add_argument(
        "--rdf",
        default=None,
        help="Input RDF file (.nt or .nt.gz) for --mode compress; .nt.gz required for --mode validation",
    )
    parser.add_argument(
        "-C",
        "--compressed-input",
        default=None,
        help="Compressed RDF input (.nt.gz/.nt.br/.hdt/.cottas[.gz|.br]) for --mode decompress",
    )
    parser.add_argument(
        "-H",
        "--hdt",
        default=None,
        help="Existing .hdt file for --mode index; creates or regenerates its versioned .hdt.index.* sidecar",
    )
    parser.add_argument(
        "--cottas",
        default=None,
        help="Existing .cottas file for --mode index; rebuilds its embedded query index in place",
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
        "--sample-representation",
        choices=sorted(SAMPLE_REPRESENTATION_CHOICES),
        default="expanded",
        help=(
            "Genotype representation for full/validation mode: expanded emits one SampleCall and "
            "FORMAT value resource per sample; condensed emits a shared SampleSet and "
            "one sample-ordered value vector per FORMAT key (default: expanded)"
        ),
    )
    parser.add_argument(
        "--rdf-storage-mode",
        choices=sorted(RDF_STORAGE_MODES),
        default=None,
        help=(
            "Full-mode aggregate storage: plain keeps one .nt; space-optimized builds one .nt.gz "
            "from RMLStreamer parts before record-safe HDT/COTTAS chunking"
        ),
    )
    parser.add_argument(
        "-P",
        "--spark-partitions",
        default=None,
        help=(
            "Optional full-mode Spark partition hint (positive integer). "
            "Sets spark.default.parallelism and spark.sql.shuffle.partitions "
            "inside RMLStreamer."
        ),
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
        "--rdf-compression",
        default=DEFAULT_RDF_COMPRESSION,
        help=(
            "Final raw RDF codecs (comma-separated): gzip,brotli, or none "
            f"(default: {DEFAULT_RDF_COMPRESSION})"
        ),
    )
    parser.add_argument(
        "--representations",
        default=DEFAULT_REPRESENTATIONS,
        help=(
            "Queryable RDF representations (comma-separated): hdt,cottas, or none "
            f"(default: {DEFAULT_REPRESENTATIONS})"
        ),
    )
    parser.add_argument(
        "--artifact-compression",
        default=DEFAULT_ARTIFACT_COMPRESSION,
        help=(
            "Packaging codecs applied to each selected representation (comma-separated): "
            "gzip,brotli, or none "
            f"(default: {DEFAULT_ARTIFACT_COMPRESSION})"
        ),
    )
    # Keep the old flat selector only for existing automation. It is hidden
    # from help and documentation; new callers should use the three explicit
    # compression-plan options above.
    parser.add_argument(
        "-c",
        "--compression",
        dest="legacy_compression",
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--hdt-strategy",
        choices=sorted(HDT_STRATEGY_CHOICES),
        default=DEFAULT_HDT_STRATEGY,
        help=(
            "HDT generation strategy: auto uses partitioned HDT+hdtc merge for full-mode aggregate storage, "
            "single uses one rdf2hdt run, partitioned forces chunked HDT generation"
        ),
    )
    parser.add_argument(
        "--chunk-target-bytes",
        default=str(DEFAULT_CHUNK_TARGET_BYTES),
        help="Target uncompressed RDF bytes per HDT/COTTAS chunk (default: 536870912)",
    )
    parser.add_argument(
        "--chunk-min-bytes",
        default=str(DEFAULT_CHUNK_MIN_BYTES),
        help="Minimum uncompressed RDF bytes before flushing a chunk group (default: 134217728)",
    )
    parser.add_argument(
        "--chunk-max-bytes",
        default=str(DEFAULT_CHUNK_MAX_BYTES),
        help="Maximum uncompressed RDF bytes allowed in one chunk (default: 1073741824)",
    )
    parser.add_argument("-k", "--keep-tsv", action="store_true", help="Keep TSV intermediates")
    parser.add_argument(
        "-e",
        "--estimate-size",
        action="store_true",
        help="Print a rough storage estimate before running conversion",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable terminal compression/conversion progress updates",
    )
    parser.add_argument(
        "--validation-id",
        default=None,
        help="Validation result identifier (default: source VCF basename)",
    )
    parser.add_argument(
        "--filter-oracle",
        choices=("auto", "bcftools", "cyvcf2"),
        default="auto",
        help="FILTER oracle for validation mode (default: auto)",
    )
    rdf_output_group = parser.add_mutually_exclusive_group()
    rdf_output_group.add_argument(
        "-R",
        "--keep-rmlstreamer-rdf-output",
        dest="keep_rmlstreamer_rdf_output",
        action="store_true",
        help="Keep the aggregate RDF output produced by RMLStreamer in full mode",
    )
    rdf_output_group.add_argument(
        "--remove-rdf-storage-output",
        action="store_true",
        help="Explicitly remove the aggregate .nt/.nt.gz output after successful compression",
    )
    args = parser.parse_args()

    global _PROGRESS_ALLOWED
    _PROGRESS_ALLOWED = not args.no_progress

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
    metrics_dir = None
    mode = args.mode
    spark_partitions = None
    chunk_target_bytes = DEFAULT_CHUNK_TARGET_BYTES
    chunk_min_bytes = DEFAULT_CHUNK_MIN_BYTES
    chunk_max_bytes = DEFAULT_CHUNK_MAX_BYTES

    step1_label = "Step 1/5" if mode == "full" else "Step 1/3"

    try:
        chunk_target_bytes = parse_positive_int(
            args.chunk_target_bytes, name="--chunk-target-bytes"
        )
        chunk_min_bytes = parse_positive_int(
            args.chunk_min_bytes, name="--chunk-min-bytes"
        )
        chunk_max_bytes = parse_positive_int(
            args.chunk_max_bytes, name="--chunk-max-bytes"
        )
        if chunk_min_bytes > chunk_target_bytes:
            raise ValueError("--chunk-min-bytes must be <= --chunk-target-bytes")
        if chunk_target_bytes > chunk_max_bytes:
            raise ValueError("--chunk-target-bytes must be <= --chunk-max-bytes")

        # Mode-specific argument validation and canonical path resolution.
        if mode == "full":
            if args.input is None:
                raise ValueError("--input is required in --mode full")
            if args.rdf_storage_mode is None:
                raise ValueError("--rdf-storage-mode is required in --mode full")
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
            sample_workflow = resolve_sample_workflow(
                args.sample_representation,
                rules_path,
            )
            validate_mode_dirs([out_root, out_dir, tsv_dir, metrics_root])
            if args.legacy_compression is not None:
                if (
                    args.rdf_compression != DEFAULT_RDF_COMPRESSION
                    or args.representations != DEFAULT_REPRESENTATIONS
                    or args.artifact_compression != DEFAULT_ARTIFACT_COMPRESSION
                ):
                    raise ValueError(
                        "--compression cannot be combined with --rdf-compression, "
                        "--representations, or --artifact-compression."
                    )
                full_methods = parse_compression_methods(args.legacy_compression)
            else:
                full_methods = build_compression_methods(
                    rdf_compression=args.rdf_compression,
                    representations=args.representations,
                    artifact_compression=args.artifact_compression,
                )
            if (
                args.rdf_storage_mode == "space-optimized"
                and args.hdt_strategy == "single"
                and compression_uses_hdt(full_methods)
            ):
                raise ValueError(
                    "--hdt-strategy single cannot be used with space-optimized HDT input; "
                    "use --hdt-strategy partitioned"
                )
            if args.spark_partitions is not None:
                spark_partitions = parse_positive_int(
                    args.spark_partitions, name="--spark-partitions"
                )
            full_uses_partitioning = compression_uses_partitioning(full_methods) and (
                any(method in COTTAS_COMPRESSION_METHODS for method in full_methods)
                or should_use_partitioned_hdt(
                    mode="full",
                    methods=full_methods,
                    hdt_strategy=args.hdt_strategy,
                    rdf_storage_mode=args.rdf_storage_mode,
                )
            )
            output_plans = {}
            for index, prefix in enumerate(expected_prefixes, start=1):
                output_name = slugify(prefix) or slugify(args.out_name)
                rdf_name = f"{output_name}.nt"
                if args.rdf_storage_mode == "space-optimized":
                    rdf_name += ".gz"
                output_plans[f"input {index} ({prefix})"] = planned_output_paths(
                    out_dir=out_dir,
                    output_name=output_name,
                    rdf_name=rdf_name,
                    methods=full_methods,
                    partitioned=full_uses_partitioning,
                )
            validate_no_output_collisions(output_plans)
        elif mode == "tsv":
            if args.spark_partitions is not None:
                raise ValueError("--spark-partitions is only valid in --mode full")
            if args.input is None:
                raise ValueError("--input is required in --mode tsv")
            input_path = Path(args.input).expanduser().resolve()
            (
                input_mount_dir,
                container_inputs,
                _input_metrics_target,
                expected_prefixes,
            ) = resolve_input_snapshot(input_path)
            validate_mode_dirs([out_root, out_dir, tsv_dir, metrics_root])
        elif mode == "validation":
            if args.spark_partitions is not None:
                raise ValueError("--spark-partitions is only valid in --mode full")
            if args.input is None:
                raise ValueError("--input is required in --mode validation")
            validation_vcf_path = Path(args.input).expanduser().resolve()
            if not validation_vcf_path.is_file() or not is_vcf_file(validation_vcf_path):
                raise ValueError("Validation input must be an existing .vcf or .vcf.gz file")
            if not args.rdf:
                raise ValueError("--rdf is required in --mode validation")
            validation_rdf_gzip_path = Path(args.rdf).expanduser().resolve()
            if not validation_rdf_gzip_path.is_file() or not validation_rdf_gzip_path.name.endswith(".nt.gz"):
                raise ValueError("Validation RDF input must be an existing .nt.gz file")
            validation_id = args.validation_id or vcf_output_prefix(validation_vcf_path)
            if not re.fullmatch(r"[A-Za-z0-9._-]+", validation_id):
                raise ValueError("--validation-id may contain only letters, digits, dot, underscore, and hyphen")
            validation_results_dir = out_dir / "validation" / validation_id
            if validation_results_dir.exists() and (
                not validation_results_dir.is_dir() or any(validation_results_dir.iterdir())
            ):
                raise ValueError(
                    f"Refusing to overwrite existing validation results: {validation_results_dir}. "
                    "Choose --validation-id or --out with a new destination."
                )
            validate_mode_dirs([out_root, out_dir, metrics_root])
        elif mode == "compress":
            if args.spark_partitions is not None:
                raise ValueError("--spark-partitions is only valid in --mode full")
            if not args.rdf:
                raise ValueError("--rdf is required in --mode compress")
            rdf_path = Path(args.rdf).expanduser().resolve()
            if not rdf_path.exists() or not rdf_path.is_file():
                raise ValueError(f"RDF input file not found: {rdf_path}")
            if rdf_path.suffix != ".nt" and not rdf_path.name.endswith(".nt.gz"):
                raise ValueError("Compression input must be a .nt or .nt.gz file")
            if args.legacy_compression is not None:
                if (
                    args.rdf_compression != DEFAULT_RDF_COMPRESSION
                    or args.representations != DEFAULT_REPRESENTATIONS
                    or args.artifact_compression != DEFAULT_ARTIFACT_COMPRESSION
                ):
                    raise ValueError(
                        "--compression cannot be combined with --rdf-compression, "
                        "--representations, or --artifact-compression."
                    )
                methods = parse_compression_methods(args.legacy_compression)
            else:
                methods = build_compression_methods(
                    rdf_compression=args.rdf_compression,
                    representations=args.representations,
                    artifact_compression=args.artifact_compression,
                )
            if (
                rdf_path.name.endswith(".gz")
                and args.hdt_strategy == "single"
                and compression_uses_hdt(methods)
            ):
                raise ValueError(
                    "--hdt-strategy single cannot be used with a .nt.gz HDT input; "
                    "use --hdt-strategy partitioned"
                )
            validate_mode_dirs([out_root, out_dir, metrics_root])
            compression_uses_partitioning_for_input = compression_uses_partitioning(methods) and (
                any(method in COTTAS_COMPRESSION_METHODS for method in methods)
                or should_use_partitioned_hdt(
                    mode="compress",
                    methods=methods,
                    hdt_strategy=args.hdt_strategy,
                )
            )
            output_name = rdf_output_basename(rdf_path)
            validate_no_output_collisions(
                {
                    f"RDF input {rdf_path.name}": planned_output_paths(
                        out_dir=out_dir,
                        output_name=output_name,
                        rdf_name=None,
                        methods=methods,
                        partitioned=compression_uses_partitioning_for_input,
                    )
                }
            )
        elif mode == "index":
            if args.spark_partitions is not None:
                raise ValueError("--spark-partitions is only valid in --mode full")
            if bool(args.hdt) == bool(args.cottas):
                raise ValueError(
                    "provide exactly one of --hdt or --cottas in --mode index"
                )
            if args.hdt:
                index_format = "hdt"
                index_path = Path(args.hdt).expanduser().resolve()
                if index_path.suffix != ".hdt":
                    raise ValueError("HDT index input must end with .hdt")
            else:
                index_format = "cottas"
                index_path = Path(args.cottas).expanduser().resolve()
                if index_path.suffix != ".cottas":
                    raise ValueError("COTTAS index input must end with .cottas")
            if not index_path.exists() or not index_path.is_file():
                raise ValueError(
                    f"{index_format.upper()} input file not found: {index_path}"
                )
            validate_mode_dirs([out_root, out_dir, metrics_root])
        else:
            if args.spark_partitions is not None:
                raise ValueError("--spark-partitions is only valid in --mode full")
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
            if decompressed_out.exists():
                raise ValueError(
                    f"Refusing to overwrite existing output file: {decompressed_out}. "
                    "Choose a different --decompress-out path and try again."
                )
            if decompressed_out.parent.exists() and not decompressed_out.parent.is_dir():
                raise ValueError(
                    f"decompression output parent is not a directory: {decompressed_out.parent}"
                )
    except ValueError as exc:
        eprint(f"Error: {exc}")
        return 2

    if mode in {"full", "tsv"}:
        metrics_source_paths = []
        for container_input in container_inputs:
            try:
                relative_input = Path(container_input).relative_to("/data/in")
            except ValueError:
                relative_input = Path(container_input).name
            metrics_source_paths.append((input_mount_dir / relative_input).resolve())
        metrics_source_root = input_path
    elif mode == "compress":
        metrics_source_paths = [rdf_path]
        metrics_source_root = rdf_path
    elif mode == "validation":
        metrics_source_paths = [validation_vcf_path, validation_rdf_gzip_path]
        metrics_source_root = validation_vcf_path
    elif mode == "index":
        metrics_source_paths = [index_path]
        metrics_source_root = index_path
    else:
        metrics_source_paths = [compressed_path]
        metrics_source_root = compressed_path

    source_label = metrics_run_label(metrics_source_paths, metrics_source_root)
    metrics_dir = metrics_run_directory(metrics_root, source_label, run_id)
    manifest_options = {
        "requested_image": args.image,
        "requested_image_version": args.image_version,
        "sample_representation": args.sample_representation if mode == "full" else None,
        "rdf_storage_mode": args.rdf_storage_mode if mode == "full" else None,
        "compression_methods": (
            full_methods if mode == "full" else methods if mode == "compress" else []
        ),
        "hdt_strategy": args.hdt_strategy if mode in {"full", "compress"} else None,
        "chunk_target_bytes": chunk_target_bytes if mode in {"full", "compress"} else None,
        "chunk_min_bytes": chunk_min_bytes if mode in {"full", "compress"} else None,
        "chunk_max_bytes": chunk_max_bytes if mode in {"full", "compress"} else None,
        "spark_partitions": spark_partitions if mode == "full" else None,
        "index_format": index_format if mode == "index" else None,
        "decompression_format": fmt if mode == "decompress" else None,
        "validation_representation": args.sample_representation if mode == "validation" else None,
        "validation_rdf_gzip": str(validation_rdf_gzip_path) if mode == "validation" else None,
    }
    try:
        manifest_path = write_run_manifest(
            metrics_dir=metrics_dir,
            run_id=run_id,
            timestamp=timestamp,
            mode=mode,
            source_label=source_label,
            source_paths=metrics_source_paths,
            out_root=out_root,
            options=manifest_options,
        )
    except OSError as exc:
        eprint(f"Error: unable to create metrics directory '{metrics_dir}': {exc}")
        return 1

    print(f"{step1_label}: Validating inputs {success_symbol()}")
    print(f"  Metrics: {metrics_dir}")

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

    wrapper_log_path = metrics_dir / "logs" / "wrapper.log"
    progress_log_path = metrics_dir / "logs" / "progress.log"
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
            update_run_manifest(
                metrics_dir,
                resolved_image=image_ref,
                image_version_requested=version_requested,
            )
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
        elif mode == "tsv":
            tsv_write_target = tsv_dir if tsv_dir.exists() else tsv_dir.parent
            metrics_write_target = metrics_dir if metrics_dir.exists() else metrics_dir.parent
            writable_targets = [
                (tsv_write_target, True),
                (metrics_write_target, True),
                (metrics_dir / "tsv_metrics.csv", False),
            ]
        elif mode == "compress":
            out_write_target = out_dir if out_dir.exists() else out_dir.parent
            metrics_write_target = metrics_dir if metrics_dir.exists() else metrics_dir.parent
            writable_targets = [
                (out_write_target, True),
                (metrics_write_target, True),
            ]
        elif mode == "validation":
            validation_write_target = (
                validation_results_dir.parent
                if validation_results_dir.parent.exists()
                else validation_results_dir.parent.parent
            )
            metrics_write_target = metrics_dir if metrics_dir.exists() else metrics_dir.parent
            writable_targets = [
                (validation_write_target, True),
                (metrics_write_target, True),
            ]
        elif mode == "index":
            metrics_write_target = metrics_dir if metrics_dir.exists() else metrics_dir.parent
            writable_targets = [
                (index_path.parent, True),
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
                sample_workflow=sample_workflow,
                rdf_storage_mode=args.rdf_storage_mode,
                methods=full_methods,
                hdt_strategy=args.hdt_strategy,
                chunk_target_bytes=chunk_target_bytes,
                chunk_min_bytes=chunk_min_bytes,
                chunk_max_bytes=chunk_max_bytes,
                spark_partitions=spark_partitions,
                keep_tsv=args.keep_tsv,
                keep_rmlstreamer_rdf_output=args.keep_rmlstreamer_rdf_output,
                remove_rdf_storage_output=args.remove_rdf_storage_output,
                run_id=run_id,
                timestamp=timestamp,
                wrapper_log_path=wrapper_log_path,
                run_tracker=run_tracker,
            )
        if mode == "tsv":
            # TSV-only benchmark mode.
            return run_tsv_mode(
                input_mount_dir=input_mount_dir,
                container_inputs=container_inputs,
                expected_prefixes=expected_prefixes,
                tsv_dir=tsv_dir,
                metrics_dir=metrics_dir,
                image_ref=image_ref,
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
                metrics_dir=metrics_dir,
                run_id=run_id,
                timestamp=timestamp,
                image_ref=image_ref,
                methods=methods,
                hdt_strategy=args.hdt_strategy,
                chunk_target_bytes=chunk_target_bytes,
                chunk_min_bytes=chunk_min_bytes,
                chunk_max_bytes=chunk_max_bytes,
                wrapper_log_path=wrapper_log_path,
            )
        if mode == "validation":
            return run_validation_mode(
                vcf_path=validation_vcf_path,
                rdf_gzip_path=validation_rdf_gzip_path,
                representation=args.sample_representation,
                validation_id=validation_id,
                results_dir=validation_results_dir,
                metrics_dir=metrics_dir,
                run_id=run_id,
                timestamp=timestamp,
                image_ref=image_ref,
                filter_oracle=args.filter_oracle,
                wrapper_log_path=wrapper_log_path,
            )
        if mode == "index":
            return run_index_mode(
                index_path=index_path,
                index_format=index_format,
                metrics_dir=metrics_dir,
                image_ref=image_ref,
                wrapper_log_path=wrapper_log_path,
                run_id=run_id,
                timestamp=timestamp,
            )
        # Decompression-only mode.
        return run_decompress_mode(
            compressed_path=compressed_path,
            decompressed_out=decompressed_out,
            metrics_dir=metrics_dir,
            run_id=run_id,
            timestamp=timestamp,
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
                keep_rmlstreamer_rdf_output=args.keep_rmlstreamer_rdf_output,
                wrapper_log_path=wrapper_log_path,
            )
            eprint(
                "Interrupt cleanup summary: "
                "removed="
                f"{removed}, failed={failed}, "
                "keep_rmlstreamer_rdf_output="
                f"{str(args.keep_rmlstreamer_rdf_output).lower()}"
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
        try:
            summary_path = write_run_summary(
                metrics_dir=metrics_dir,
                run_id=run_id,
                timestamp=timestamp,
                mode=mode,
                exit_code=result_code,
                elapsed_seconds=elapsed_seconds,
                total_triples=total_triples,
            )
            print(f"Metrics summary: {summary_path}")
        except OSError as exc:
            eprint(f"Warning: failed to write metrics summary: {exc}")

    return result_code


if __name__ == "__main__":
    raise SystemExit(main())
