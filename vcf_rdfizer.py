#!/usr/bin/env python3

import argparse
import csv
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


RMLSTREAMER_JAR_CONTAINER = "/opt/rmlstreamer/RMLStreamer-v2.5.0-standalone.jar"
_COMMAND_LOGGER = None

COMPRESSED_VCF_EXPANSION_FACTOR = 5.0
TSV_OVERHEAD_FACTOR = 1.10
RDF_EXPANSION_LOW_FACTOR = 4.0
RDF_EXPANSION_HIGH_FACTOR = 12.0
METRICS_HEADER = [
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
    "combined_nq_size_bytes",
    "gzip_size_bytes",
    "brotli_size_bytes",
    "hdt_size_bytes",
    "exit_code_gzip",
    "exit_code_brotli",
    "exit_code_hdt",
    "wall_seconds_gzip",
    "user_seconds_gzip",
    "sys_seconds_gzip",
    "max_rss_kb_gzip",
    "wall_seconds_brotli",
    "user_seconds_brotli",
    "sys_seconds_brotli",
    "max_rss_kb_brotli",
    "wall_seconds_hdt",
    "user_seconds_hdt",
    "sys_seconds_hdt",
    "max_rss_kb_hdt",
    "compression_methods",
]


class CommandLogger:
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
    print(*args, file=sys.stderr)


def run(cmd, cwd=None, env=None):
    if _COMMAND_LOGGER is not None:
        return _COMMAND_LOGGER.run(cmd, cwd=cwd, env=env)
    return subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True).returncode


def docker_run_base():
    base = ["sudo", "docker", "run", "--rm"]
    as_user = os.environ.get("VCF_RDFIZER_DOCKER_AS_USER", "1").strip().lower()
    if as_user in {"0", "false", "no"}:
        return base
    getuid = getattr(os, "getuid", None)
    getgid = getattr(os, "getgid", None)
    if callable(getuid) and callable(getgid):
        base.extend(["--user", f"{getuid()}:{getgid()}"])
    return base


def check_docker():
    if shutil.which("docker") is None:
        eprint("Error: Docker is not installed or not on PATH.")
        return False
    code = run(["sudo", "docker", "version"])
    if code != 0:
        eprint("Error: Docker is not available. Is the daemon running?")
        return False
    return True


def is_vcf_file(path: Path) -> bool:
    name = path.name
    return name.endswith(".vcf") or name.endswith(".vcf.gz")


def list_vcfs_in_dir(path: Path):
    files = []
    for item in sorted(path.iterdir()):
        if item.is_file() and is_vcf_file(item):
            files.append(item)
    return files


def resolve_input(input_path: Path):
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
    name = path.name
    if name.endswith(".vcf.gz"):
        return name[: -len(".vcf.gz")]
    if name.endswith(".vcf"):
        return name[: -len(".vcf")]
    return path.stem


def unique_in_order(items):
    seen = set()
    ordered = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def resolve_input_snapshot(input_path: Path):
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


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def format_bytes(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{num_bytes} B"


def file_size_bytes(path: Path):
    if not path.exists() or not path.is_file():
        return None
    return path.stat().st_size


def print_nt_hdt_summary(
    *,
    output_root: Path,
    nt_path: Path,
    hdt_path: Path,
    indent: str = "",
    nt_note: str | None = None,
    nt_size_override: int | None = None,
):
    print(f"{indent}* Output directory: {output_root}")
    nt_size = nt_size_override if nt_size_override is not None else file_size_bytes(nt_path)
    hdt_size = file_size_bytes(hdt_path)

    if nt_size is None:
        nt_text = f"not found at {nt_path}"
    else:
        nt_text = f"{format_bytes(nt_size)} ({nt_path})"
    if nt_note:
        nt_text = f"{nt_text} ({nt_note})"
    print(f"{indent}  - N-Triples (.nt): {nt_text}")

    if hdt_size is None:
        print(f"{indent}  - HDT (.hdt): not generated at {hdt_path}")
    else:
        print(f"{indent}  - HDT (.hdt): {format_bytes(hdt_size)} ({hdt_path})")


def remove_file_with_docker_fallback(
    *,
    path: Path,
    mount_root: Path,
    mount_point: str,
    image_ref: str,
    wrapper_log_path: Path,
) -> bool:
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


def existing_parent(path: Path) -> Path:
    cur = path
    while not cur.exists():
        if cur.parent == cur:
            break
        cur = cur.parent
    return cur


def collect_input_vcfs(input_path: Path):
    if input_path.is_file():
        return [input_path]
    if input_path.is_dir():
        return list_vcfs_in_dir(input_path)
    return []


def estimate_pipeline_sizes(vcf_files, out_dir: Path):
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


def slugify(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "vcf"


def discover_tsv_triplets(tsv_dir: Path):
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
    text = template_rules.read_text()
    text = text.replace('/data/tsv/records.tsv', f'/data/tsv/{records_name}')
    text = text.replace('/data/tsv/header_lines.tsv', f'/data/tsv/{headers_name}')
    text = text.replace('/data/tsv/file_metadata.tsv', f'/data/tsv/{metadata_name}')
    output_rules.write_text(text)


def docker_image_exists(image: str) -> bool:
    return run(["sudo", "docker", "image", "inspect", image]) == 0


def docker_build_image(image: str, repo_root: Path):
    return run(["sudo", "docker", "build", "-t", image, "."], cwd=str(repo_root))


def docker_pull_image(image: str):
    return run(["sudo", "docker", "pull", image])


def resolve_image_ref(image: str, image_version: str | None):
    if ":" in image:
        if image_version is not None:
            raise ValueError("Do not include a tag in --image when using --image-version.")
        return image, False
    if image_version is None:
        return f"{image}:latest", False
    return f"{image}:{image_version}", True


def parse_compression_methods(raw: str):
    value = (raw or "").strip()
    if value == "" or value == "none":
        return []

    methods = []
    for token in value.split(","):
        method = token.strip()
        if not method:
            continue
        if method not in {"gzip", "brotli", "hdt"}:
            raise ValueError(
                f"Unsupported compression method '{method}'. Use gzip,brotli,hdt, or none."
            )
        if method not in methods:
            methods.append(method)
    return methods


def safe_metrics_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")
    return safe or "rdf"


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
    metrics_csv.parent.mkdir(parents=True, exist_ok=True)
    if not metrics_csv.exists():
        with metrics_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=METRICS_HEADER)
            writer.writeheader()

    with metrics_csv.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or METRICS_HEADER
        rows = list(reader)

    if fieldnames != METRICS_HEADER:
        backup = metrics_csv.with_name(f"{metrics_csv.name}.bak-{run_id}")
        shutil.copyfile(metrics_csv, backup)
        fieldnames = METRICS_HEADER
        rows = []

    row = None
    for existing in rows:
        if existing.get("run_id") == run_id and existing.get("output_name") == output_name:
            row = existing
            break

    if row is None:
        row = {name: "" for name in METRICS_HEADER}
        row["run_id"] = run_id
        row["timestamp"] = timestamp
        row["output_name"] = output_name
        row["output_dir"] = str(output_dir)
        rows.append(row)

    row["combined_nq_size_bytes"] = str(int(combined_size_bytes))
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
    }
    row.update(defaults)

    for method, result in method_results.items():
        size_key = f"{method}_size_bytes"
        exit_key = f"exit_code_{method}"
        wall_key = f"wall_seconds_{method}"
        row[size_key] = str(int(result.get("output_size_bytes") or 0))
        row[exit_key] = str(int(result.get("exit_code") or 0))
        wall_val = result.get("wall_seconds")
        row[wall_key] = "null" if wall_val is None else f"{float(wall_val):.6f}"

    with metrics_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=METRICS_HEADER)
        writer.writeheader()
        writer.writerows(rows)


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
    metrics_dir.mkdir(parents=True, exist_ok=True)
    safe_name = safe_metrics_name(output_name)

    for method, result in method_results.items():
        time_log = metrics_dir / f"compression-time-{method}-{safe_name}-{run_id}.txt"
        lines = [
            f"method={method}",
            f"exit_code={result.get('exit_code', 1)}",
            f"wall_seconds={result.get('wall_seconds', 'null')}",
            f"output_path={result.get('output_path', '')}",
            f"output_size_bytes={result.get('output_size_bytes', 0)}",
        ]
        time_log.write_text("\n".join(lines) + "\n", encoding="utf-8")

    gzip_result = method_results.get("gzip", {})
    brotli_result = method_results.get("brotli", {})
    hdt_result = method_results.get("hdt", {})

    payload = {
        "run_id": run_id,
        "timestamp": timestamp,
        "output_dir": str(source_rdf_path.parent),
        "output_name": output_name,
        "compression_methods": ",".join(selected_methods) if selected_methods else "none",
        "combined_nq_path": str(source_rdf_path),
        "combined_nq_size_bytes": int(combined_size_bytes),
        "gzip": {
            "output_gz_path": gzip_result.get("output_path", ""),
            "output_gz_size_bytes": int(gzip_result.get("output_size_bytes") or 0),
            "exit_code": int(gzip_result.get("exit_code") or 0),
            "timing": {
                "wall_seconds": gzip_result.get("wall_seconds"),
                "user_seconds": None,
                "sys_seconds": None,
                "max_rss_kb": None,
            },
        },
        "brotli": {
            "output_brotli_path": brotli_result.get("output_path", ""),
            "output_brotli_size_bytes": int(brotli_result.get("output_size_bytes") or 0),
            "exit_code": int(brotli_result.get("exit_code") or 0),
            "timing": {
                "wall_seconds": brotli_result.get("wall_seconds"),
                "user_seconds": None,
                "sys_seconds": None,
                "max_rss_kb": None,
            },
        },
        "hdt_conversion": {
            "output_hdt_path": hdt_result.get("output_path", ""),
            "output_hdt_size_bytes": int(hdt_result.get("output_size_bytes") or 0),
            "exit_code": int(hdt_result.get("exit_code") or 0),
            "timing": {
                "wall_seconds": hdt_result.get("wall_seconds"),
                "user_seconds": None,
                "sys_seconds": None,
                "max_rss_kb": None,
            },
        },
    }

    metrics_json = metrics_dir / f"compression-metrics-{safe_name}-{run_id}.json"
    metrics_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def validate_mode_dirs(paths):
    for p in paths:
        if p.exists() and not p.is_dir():
            raise ValueError(f"expected a directory path but found a file: {p}")


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
    if build:
        print(f"{step_label}: Ensuring Docker image is available")
        print("  - Building Docker image")
        if docker_build_image(image_ref, repo_root) != 0:
            eprint(f"Error: docker build failed. See log: {wrapper_log_path}")
            return 1
        print(f"{step_label}: Ensuring Docker image is available ✅")
        return 0

    if docker_image_exists(image_ref):
        print(f"{step_label}: Ensuring Docker image is available ✅")
        return 0

    if version_requested:
        print(f"{step_label}: Ensuring Docker image is available")
        print(f"  - Pulling image: {image_ref}")
        if docker_pull_image(image_ref) != 0:
            eprint(f"Error: image version '{image_ref}' not found. See log: {wrapper_log_path}")
            return 2
        print(f"{step_label}: Ensuring Docker image is available ✅")
        return 0

    if no_build:
        eprint(f"Error: image '{image_ref}' not found and --no-build set.")
        return 2

    print(f"{step_label}: Ensuring Docker image is available")
    print("  - Image missing locally, building")
    if docker_build_image(image_ref, repo_root) != 0:
        eprint(f"Error: docker build failed. See log: {wrapper_log_path}")
        return 1
    print(f"{step_label}: Ensuring Docker image is available ✅")
    return 0


def run_compression_methods_for_rdf(
    *,
    rdf_path: Path,
    out_dir: Path,
    image_ref: str,
    methods: list[str],
    wrapper_log_path: Path,
    status_indent: str | None,
):
    in_dir = rdf_path.parent
    input_container = f"/data/in/{rdf_path.name}"
    input_stem = rdf_path.stem
    input_ext = rdf_path.suffix.lstrip(".") or "nt"
    target_out_dir = out_dir / input_stem
    ensure_dir(target_out_dir)
    target_out_container = f"/data/out/{input_stem}"

    method_results: dict[str, dict] = {}

    for method in methods:
        if method == "gzip":
            output_name = f"{input_stem}.{input_ext}.gz"
            out_container = f"{target_out_container}/{output_name}"
            command = f"gzip -c {shlex.quote(input_container)} > {shlex.quote(out_container)}"
        elif method == "brotli":
            output_name = f"{input_stem}.{input_ext}.br"
            out_container = f"{target_out_container}/{output_name}"
            command = f"brotli -q 7 -c {shlex.quote(input_container)} > {shlex.quote(out_container)}"
        else:
            output_name = f"{input_stem}.hdt"
            out_container = f"{target_out_container}/{output_name}"
            command = (
                "set -euo pipefail; "
                "HDT_BIN=/opt/hdt-java/hdt-java-cli/bin/rdf2hdt.sh; "
                "HDT_PROJECT_DIR=/opt/hdt-java/hdt-java-cli; "
                'if [[ ! -x "$HDT_BIN" ]]; then echo "Missing rdf2hdt.sh at $HDT_BIN" >&2; exit 127; fi; '
                'if ! command -v java >/dev/null 2>&1; then echo "Java runtime not found on PATH" >&2; exit 127; fi; '
                'if [[ -f "$HDT_PROJECT_DIR/pom.xml" ]]; then cd "$HDT_PROJECT_DIR"; fi; '
                'bash "$HDT_BIN" '
                f"{shlex.quote(input_container)} {shlex.quote(out_container)}"
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
            command,
        ]
        started = time.perf_counter()
        exit_code = run(cmd)
        elapsed = time.perf_counter() - started
        output_path = target_out_dir / output_name
        method_results[method] = {
            "exit_code": exit_code,
            "wall_seconds": elapsed,
            "output_path": str(output_path),
            "output_size_bytes": int(file_size_bytes(output_path) or 0),
        }
        if exit_code != 0:
            eprint(f"Error: {method} compression failed. See log: {wrapper_log_path}")
            return False, method_results
        if status_indent is not None:
            print(f"{status_indent}- {method}: {output_name} ✅")

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
    compression: str,
    keep_tsv: bool,
    keep_rdf: bool,
    run_id: str,
    timestamp: str,
    wrapper_log_path: Path,
):
    print("Step 3/5: Processing per-input pipeline (TSV -> RDF -> compression)")
    tsv_existed = tsv_dir.exists()
    ensure_dir(tsv_dir)
    ensure_dir(out_dir)
    ensure_dir(metrics_dir)

    selected_methods = parse_compression_methods(compression)

    generated_rules_dir = metrics_dir / "_generated_rules"
    if generated_rules_dir.exists():
        shutil.rmtree(generated_rules_dir, ignore_errors=True)
    ensure_dir(generated_rules_dir)

    total_inputs = len(container_inputs)
    for idx, (container_input, expected_prefix) in enumerate(
        zip(container_inputs, expected_prefixes),
        start=1,
    ):
        print(f"  - Input {idx}/{total_inputs}: {Path(container_input).name}")

        tsv_cmd = [
            *docker_run_base(),
            "-v",
            f"{str(input_mount_dir)}:/data/in:ro",
            "-v",
            f"{str(tsv_dir)}:/data/tsv",
            image_ref,
            "/opt/vcf-rdfizer/vcf_as_tsv.sh",
            container_input,
            "/data/tsv",
        ]
        if run(tsv_cmd) != 0:
            eprint(f"Error: TSV conversion failed. See log: {wrapper_log_path}")
            return 1
        print("    * TSV conversion ✅")

        try:
            tsv_triplets = discover_tsv_triplets(tsv_dir)
        except ValueError as exc:
            eprint(f"Error: {exc}")
            eprint(f"See log for details: {wrapper_log_path}")
            return 1

        triplets_by_prefix = {triplet["prefix"]: triplet for triplet in tsv_triplets}
        if expected_prefix not in triplets_by_prefix:
            eprint(
                f"Error: TSV conversion did not produce the expected triplet for '{expected_prefix}'."
            )
            eprint(f"See log for details: {wrapper_log_path}")
            return 1

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
            f"RUN_ID={run_id}",
            "-e",
            f"TIMESTAMP={timestamp}",
            "-e",
            f"IN_VCF={input_metrics_target}",
            "-e",
            "LOGDIR=/data/metrics",
            image_ref,
            "/opt/vcf-rdfizer/run_conversion.sh",
        ]
        if run(run_cmd) != 0:
            eprint(f"Error: RMLStreamer step failed for '{prefix}'. See log: {wrapper_log_path}")
            return 1
        print("    * RDF conversion ✅")

        nt_path = out_dir / output_name / f"{output_name}.nt"
        nq_path = out_dir / output_name / f"{output_name}.nq"

        if nt_path.exists():
            source_rdf_path = nt_path
        elif nq_path.exists():
            source_rdf_path = nq_path
        else:
            # Keep full mode behavior aligned with compression mode: try expected .nt path.
            source_rdf_path = nt_path

        method_results: dict[str, dict] = {}
        if selected_methods:
            ok, method_results = run_compression_methods_for_rdf(
                rdf_path=source_rdf_path,
                out_dir=out_dir,
                image_ref=image_ref,
                methods=selected_methods,
                wrapper_log_path=wrapper_log_path,
                status_indent=None,
            )
            if not ok:
                return 1
        print("    * Compression ✅")

        hdt_path = out_dir / output_name / f"{output_name}.hdt"
        nt_size_before_cleanup = file_size_bytes(nt_path)
        nq_size_before_cleanup = file_size_bytes(nq_path)
        source_size_before_cleanup = int(file_size_bytes(source_rdf_path) or 0)

        try:
            write_compression_metrics_artifacts(
                metrics_dir=metrics_dir,
                run_id=run_id,
                timestamp=timestamp,
                output_name=output_name,
                source_rdf_path=source_rdf_path,
                combined_size_bytes=source_size_before_cleanup,
                selected_methods=selected_methods,
                method_results=method_results,
            )
            update_metrics_csv_with_compression(
                metrics_csv=metrics_dir / "metrics.csv",
                run_id=run_id,
                timestamp=timestamp,
                output_name=output_name,
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

        nt_note = None
        if not keep_rdf and selected_methods:
            removed_any = False
            for raw_rdf_path in (nt_path, nq_path):
                if raw_rdf_path.exists():
                    if not remove_file_with_docker_fallback(
                        path=raw_rdf_path,
                        mount_root=out_dir,
                        mount_point="/data/out",
                        image_ref=image_ref,
                        wrapper_log_path=wrapper_log_path,
                    ):
                        return 1
                    removed_any = True
            if removed_any:
                nt_note = "removed, set --keep-rdf to retain"
            else:
                nt_note = "cleanup skipped"
        elif not keep_rdf and not selected_methods:
            nt_note = "kept (compression methods set to none)"
        elif keep_rdf:
            nt_note = "retained via --keep-rdf"

        summary_nt_path = nt_path if nt_size_before_cleanup is not None else nq_path
        summary_nt_size = (
            nt_size_before_cleanup
            if nt_size_before_cleanup is not None
            else nq_size_before_cleanup
        )
        print_nt_hdt_summary(
            output_root=out_dir,
            nt_path=summary_nt_path,
            hdt_path=hdt_path,
            indent="    ",
            nt_note=nt_note,
            nt_size_override=summary_nt_size,
        )

        if not keep_tsv:
            for tsv_path in (triplet["records"], triplet["headers"], triplet["metadata"]):
                if tsv_path.exists():
                    if not remove_file_with_docker_fallback(
                        path=tsv_path,
                        mount_root=tsv_dir,
                        mount_point="/data/tsv",
                        image_ref=image_ref,
                        wrapper_log_path=wrapper_log_path,
                    ):
                        return 1

    if not keep_tsv:
        if not tsv_existed:
            shutil.rmtree(tsv_dir, ignore_errors=True)
        else:
            print("Note: TSV directory existed; skipping cleanup.")

    print("Conversion process finished.")
    return 0


def run_compress_mode(
    *,
    nq_path: Path,
    out_dir: Path,
    image_ref: str,
    methods: list[str],
    wrapper_log_path: Path,
):
    print("Step 3/3: Compressing RDF input")
    if not methods:
        print("No compression methods selected (`none`). Nothing to do.")
        return 0

    ensure_dir(out_dir)
    ok, _method_results = run_compression_methods_for_rdf(
        rdf_path=nq_path,
        out_dir=out_dir,
        image_ref=image_ref,
        methods=methods,
        wrapper_log_path=wrapper_log_path,
        status_indent="  ",
    )
    if not ok:
        return 1

    input_stem = nq_path.stem
    target_out_dir = out_dir / input_stem
    nt_path = nq_path if nq_path.suffix == ".nt" else nq_path.with_suffix(".nt")
    hdt_path = target_out_dir / f"{input_stem}.hdt"
    print_nt_hdt_summary(output_root=target_out_dir, nt_path=nt_path, hdt_path=hdt_path, indent="  ")
    print("Conversion process finished.")
    return 0


def detect_compressed_format(path: Path):
    if path.name.endswith(".nq.gz") or path.name.endswith(".nt.gz") or path.suffix == ".gz":
        return "gzip"
    if path.name.endswith(".nq.br") or path.name.endswith(".nt.br") or path.suffix == ".br":
        return "brotli"
    if path.suffix == ".hdt":
        return "hdt"
    raise ValueError("Compressed input must end with .gz, .br, or .hdt")


def default_decompressed_name(path: Path, fmt: str):
    if fmt == "gzip":
        if path.name.endswith(".nq.gz"):
            return path.name[: -len(".gz")]
        if path.name.endswith(".nt.gz"):
            return path.name[: -len(".gz")]
        return f"{path.stem}.nt"
    if fmt == "brotli":
        if path.name.endswith(".nq.br"):
            return path.name[: -len(".br")]
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
    print("Step 3/3: Decompressing RDF input")
    fmt = detect_compressed_format(compressed_path)
    ensure_dir(decompressed_out.parent)

    source_container = f"/data/in/{compressed_path.name}"
    output_container = f"/data/out/{decompressed_out.name}"

    if fmt == "gzip":
        command = f"gzip -dc {shlex.quote(source_container)} > {shlex.quote(output_container)}"
    elif fmt == "brotli":
        command = f"brotli -d -c {shlex.quote(source_container)} > {shlex.quote(output_container)}"
    else:
        command = (
            "set -euo pipefail; "
            "HDT2RDF_BIN=/opt/hdt-java/hdt-java-cli/bin/hdt2rdf.sh; "
            "HDT_PROJECT_DIR=/opt/hdt-java/hdt-java-cli; "
            'if [[ ! -x "$HDT2RDF_BIN" ]]; then echo "Missing hdt2rdf.sh at $HDT2RDF_BIN" >&2; exit 127; fi; '
            'if ! command -v java >/dev/null 2>&1; then echo "Java runtime not found on PATH" >&2; exit 127; fi; '
            'if [[ -f "$HDT_PROJECT_DIR/pom.xml" ]]; then cd "$HDT_PROJECT_DIR"; fi; '
            'bash "$HDT2RDF_BIN" '
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
    parser = argparse.ArgumentParser(
        description="VCF-RDFizer Docker wrapper",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  Full pipeline:\n"
            "    vcf_rdfizer.py -m full -i ./vcf_files -r ./rules/default_rules.ttl\n"
            "  Compression-only:\n"
            "    vcf_rdfizer.py -m compress -q ./out/sample/sample.nt -c gzip,brotli\n"
            "  Decompression-only:\n"
            "    vcf_rdfizer.py -m decompress -C ./out/gzip/sample.nt.gz\n"
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
        "--nq",
        "--nt",
        "--rdf",
        dest="nq",
        default=None,
        help="Input RDF file (.nt or .nq) for --mode compress",
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
    parser.add_argument("-o", "--out", default="./out", help="RDF output directory")
    parser.add_argument("-t", "--tsv", default="./tsv", help="TSV output directory")
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
    parser.add_argument("-M", "--metrics", default="./run_metrics", help="Metrics output directory")
    parser.add_argument(
        "-c",
        "--compression",
        default="gzip,brotli,hdt",
        help="Compression methods (gzip,brotli,hdt,none)",
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
        action="store_true",
        help="Keep merged N-Triples outputs after compression in full mode",
    )
    args = parser.parse_args()

    if args.build and args.no_build:
        eprint("Error: --build and --no-build are mutually exclusive.")
        return 2

    repo_root = Path(__file__).resolve().parent
    out_dir = Path(args.out).expanduser().resolve()
    tsv_dir = Path(args.tsv).expanduser().resolve()
    metrics_dir = Path(args.metrics).expanduser().resolve()
    mode = args.mode

    step1_label = "Step 1/5" if mode == "full" else "Step 1/3"

    try:
        if mode == "full":
            if args.input is None:
                raise ValueError("--input is required in --mode full")
            input_path = Path(args.input).expanduser().resolve()
            (
                input_mount_dir,
                container_inputs,
                input_metrics_target,
                expected_prefixes,
            ) = resolve_input_snapshot(input_path)
            if args.rules is None:
                rules_path = (repo_root / "rules" / "default_rules.ttl").resolve()
            else:
                rules_path = Path(args.rules).expanduser().resolve()
            if not rules_path.exists() or not rules_path.is_file():
                raise ValueError(f"rules file not found: {rules_path}")
            validate_mode_dirs([out_dir, tsv_dir, metrics_dir])
            parse_compression_methods(args.compression)
        elif mode == "compress":
            if not args.nq:
                raise ValueError("--nq is required in --mode compress")
            nq_path = Path(args.nq).expanduser().resolve()
            if not nq_path.exists() or not nq_path.is_file():
                raise ValueError(f"RDF input file not found: {nq_path}")
            if nq_path.suffix not in {".nq", ".nt"}:
                raise ValueError("Compression input must be a .nq or .nt file")
            methods = parse_compression_methods(args.compression)
            validate_mode_dirs([out_dir, metrics_dir])
        else:
            if not args.compressed_input:
                raise ValueError("--compressed-input is required in --mode decompress")
            compressed_path = Path(args.compressed_input).expanduser().resolve()
            if not compressed_path.exists() or not compressed_path.is_file():
                raise ValueError(f"Compressed input file not found: {compressed_path}")
            fmt = detect_compressed_format(compressed_path)
            validate_mode_dirs([out_dir, metrics_dir])
            if args.decompress_out is None:
                default_name = default_decompressed_name(compressed_path, fmt)
                decompressed_out = out_dir / Path(default_name).stem / default_name
            else:
                decompressed_out = Path(args.decompress_out).expanduser().resolve()
            if decompressed_out.exists() and decompressed_out.is_dir():
                raise ValueError(f"decompression output path is a directory: {decompressed_out}")
            if decompressed_out.parent.exists() and not decompressed_out.parent.is_dir():
                raise ValueError(
                    f"decompression output parent is not a directory: {decompressed_out.parent}"
                )
    except ValueError as exc:
        eprint(f"Error: {exc}")
        return 2

    print(f"{step1_label}: Validating inputs ✅")

    if mode == "full" and args.estimate_size:
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

    run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    wrapper_log_path = metrics_dir / ".wrapper_logs" / f"wrapper-{run_id}.log"
    global _COMMAND_LOGGER
    _COMMAND_LOGGER = CommandLogger(wrapper_log_path)
    print(f"  Detailed logs: {wrapper_log_path}")

    try:
        if not check_docker():
            eprint(f"See log for details: {wrapper_log_path}")
            return 2

        try:
            image_ref, version_requested = resolve_image_ref(args.image, args.image_version)
        except ValueError as exc:
            eprint(f"Error: {exc}")
            return 2

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
            return image_code

        if mode == "full":
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
                compression=args.compression,
                keep_tsv=args.keep_tsv,
                keep_rdf=args.keep_rdf,
                run_id=run_id,
                timestamp=timestamp,
                wrapper_log_path=wrapper_log_path,
            )
        if mode == "compress":
            return run_compress_mode(
                nq_path=nq_path,
                out_dir=out_dir,
                image_ref=image_ref,
                methods=methods,
                wrapper_log_path=wrapper_log_path,
            )
        return run_decompress_mode(
            compressed_path=compressed_path,
            decompressed_out=decompressed_out,
            image_ref=image_ref,
            wrapper_log_path=wrapper_log_path,
        )
    finally:
        if _COMMAND_LOGGER is not None:
            _COMMAND_LOGGER.close()
            _COMMAND_LOGGER = None


if __name__ == "__main__":
    raise SystemExit(main())
