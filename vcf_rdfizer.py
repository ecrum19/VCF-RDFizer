#!/usr/bin/env python3

import argparse
import re
import shlex
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path


RMLSTREAMER_JAR_CONTAINER = "/opt/rmlstreamer/RMLStreamer-v2.5.0-standalone.jar"
_COMMAND_LOGGER = None

COMPRESSED_VCF_EXPANSION_FACTOR = 5.0
TSV_OVERHEAD_FACTOR = 1.10
RDF_EXPANSION_LOW_FACTOR = 4.0
RDF_EXPANSION_HIGH_FACTOR = 12.0


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

    # Backward-compatible fallback for legacy aggregate TSV naming.
    # legacy_records = tsv_dir / "records.tsv"
    # legacy_headers = tsv_dir / "header_lines.tsv"
    # legacy_metadata = tsv_dir / "file_metadata.tsv"
    # if legacy_records.exists() and legacy_headers.exists() and legacy_metadata.exists():
    #     return [
    #         {
    #             "prefix": "records",
    #             "records": legacy_records,
    #             "headers": legacy_headers,
    #             "metadata": legacy_metadata,
    #         }
    #     ]

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
        return f"{image}:1.0.0", False
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
    print(f"{step_label}: Ensuring Docker image is available")
    if build:
        print("  - Building Docker image")
        if docker_build_image(image_ref, repo_root) != 0:
            eprint(f"Error: docker build failed. See log: {wrapper_log_path}")
            return 1
        return 0

    if docker_image_exists(image_ref):
        return 0

    if version_requested:
        print(f"  - Pulling image: {image_ref}")
        if docker_pull_image(image_ref) != 0:
            eprint(f"Error: image version '{image_ref}' not found. See log: {wrapper_log_path}")
            return 2
        return 0

    if no_build:
        eprint(f"Error: image '{image_ref}' not found and --no-build set.")
        return 2

    print("  - Image missing locally, building")
    if docker_build_image(image_ref, repo_root) != 0:
        eprint(f"Error: docker build failed. See log: {wrapper_log_path}")
        return 1
    return 0


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
    run_id: str,
    timestamp: str,
    wrapper_log_path: Path,
):
    print("Step 3/5: Converting VCF to TSV")
    tsv_existed = tsv_dir.exists()
    ensure_dir(tsv_dir)
    total_inputs = len(container_inputs)
    for idx, container_input in enumerate(container_inputs, start=1):
        print(f"  - TSV conversion {idx}/{total_inputs}: {Path(container_input).name}")
        tsv_cmd = [
            "sudo",
            "docker",
            "run",
            "--rm",
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

    print("Step 4/5: Running Conversion with RMLStreamer")
    ensure_dir(out_dir)
    ensure_dir(metrics_dir)

    try:
        tsv_triplets = discover_tsv_triplets(tsv_dir)
    except ValueError as exc:
        eprint(f"Error: {exc}")
        eprint(f"See log for details: {wrapper_log_path}")
        return 1

    expected_order = unique_in_order(expected_prefixes)
    triplets_by_prefix = {triplet["prefix"]: triplet for triplet in tsv_triplets}
    missing_prefixes = [prefix for prefix in expected_order if prefix not in triplets_by_prefix]
    if missing_prefixes:
        eprint(
            "Error: TSV conversion did not produce expected triplets for: "
            + ", ".join(missing_prefixes)
        )
        eprint(f"See log for details: {wrapper_log_path}")
        return 1

    ignored_prefixes = sorted(set(triplets_by_prefix.keys()) - set(expected_order))
    if ignored_prefixes:
        print("  - Ignoring unrelated TSV triplets in output directory")

    tsv_triplets = [triplets_by_prefix[prefix] for prefix in expected_order]

    generated_rules_dir = metrics_dir / "_generated_rules"
    if generated_rules_dir.exists():
        shutil.rmtree(generated_rules_dir, ignore_errors=True)
    ensure_dir(generated_rules_dir)

    conversion_output_names = []
    for triplet in tsv_triplets:
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
        conversion_output_names.append(output_name)
        container_generated_rules = f"/data/rules/{generated_rules.name}"

        print(f"  - Converting '{prefix}' into RDF")
        run_cmd = [
            "sudo",
            "docker",
            "run",
            "--rm",
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

    print("Step 5/5: Compressing outputs")
    compression_out_name = conversion_output_names[0] if len(conversion_output_names) == 1 else ""
    compression_cmd = [
        "sudo",
        "docker",
        "run",
        "--rm",
        "-v",
        f"{str(out_dir)}:/data/out",
        "-v",
        f"{str(metrics_dir)}:/data/metrics",
        "-e",
        "OUT_ROOT_DIR=/data/out",
        "-e",
        f"OUT_NAME={compression_out_name}",
        "-e",
        "LOGDIR=/data/metrics",
        "-e",
        f"RUN_ID={run_id}",
        "-e",
        f"TIMESTAMP={timestamp}",
        image_ref,
        "/opt/vcf-rdfizer/compression.sh",
        "-m",
        compression,
    ]
    if run(compression_cmd) != 0:
        eprint(f"Error: compression step failed. See log: {wrapper_log_path}")
        return 1

    if not keep_tsv:
        if not tsv_existed:
            shutil.rmtree(tsv_dir, ignore_errors=True)
        else:
            print("Note: TSV directory existed; skipping cleanup.")

    print("Done. See output and metrics directories for results.")
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
    in_dir = nq_path.parent
    input_container = f"/data/in/{nq_path.name}"
    input_stem = nq_path.stem
    input_ext = nq_path.suffix.lstrip(".") or "nt"

    for method in methods:
        method_dir = out_dir / method
        ensure_dir(method_dir)

        if method == "gzip":
            output_name = f"{input_stem}.{input_ext}.gz"
            out_container = f"/data/out/{method}/{output_name}"
            command = f"gzip -c {shlex.quote(input_container)} > {shlex.quote(out_container)}"
        elif method == "brotli":
            output_name = f"{input_stem}.{input_ext}.br"
            out_container = f"/data/out/{method}/{output_name}"
            command = f"brotli -q 7 -c {shlex.quote(input_container)} > {shlex.quote(out_container)}"
        else:
            output_name = f"{input_stem}.hdt"
            out_container = f"/data/out/{method}/{output_name}"
            command = (
                "set -euo pipefail; "
                "HDT_BIN=/opt/hdt-java/hdt-java-cli/bin/rdf2hdt.sh; "
                'if [[ ! -x "$HDT_BIN" ]]; then echo "Missing rdf2hdt.sh at $HDT_BIN" >&2; exit 127; fi; '
                'if ! command -v java >/dev/null 2>&1; then echo "Java runtime not found on PATH" >&2; exit 127; fi; '
                'bash "$HDT_BIN" '
                f"{shlex.quote(input_container)} {shlex.quote(out_container)}"
            )

        print(f"  - {method}: {output_name}")
        cmd = [
            "sudo",
            "docker",
            "run",
            "--rm",
            "-v",
            f"{str(in_dir)}:/data/in:ro",
            "-v",
            f"{str(out_dir)}:/data/out",
            image_ref,
            "bash",
            "-lc",
            command,
        ]
        if run(cmd) != 0:
            eprint(f"Error: {method} compression failed. See log: {wrapper_log_path}")
            return 1

    print("Done. Compressed outputs written to mode subdirectories in --out.")
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
            'if [[ ! -x "$HDT2RDF_BIN" ]]; then echo "Missing hdt2rdf.sh at $HDT2RDF_BIN" >&2; exit 127; fi; '
            'if ! command -v java >/dev/null 2>&1; then echo "Java runtime not found on PATH" >&2; exit 127; fi; '
            'bash "$HDT2RDF_BIN" '
            f"{shlex.quote(source_container)} {shlex.quote(output_container)}"
        )

    cmd = [
        "sudo",
        "docker",
        "run",
        "--rm",
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
        help="Image tag/version to use (e.g. 1.2.3). Defaults to 1.0.0 if omitted and --image has no tag.",
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
        help="Compression methods for compression.sh (gzip,brotli,hdt,none)",
    )
    parser.add_argument("-k", "--keep-tsv", action="store_true", help="Keep TSV intermediates")
    parser.add_argument(
        "-e",
        "--estimate-size",
        action="store_true",
        help="Print a rough storage estimate before running conversion",
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

    if mode == "full":
        print("Step 1/5: Validating inputs")
    else:
        print("Step 1/3: Validating inputs")

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
                decompressed_out = out_dir / "decompressed" / default_decompressed_name(compressed_path, fmt)
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

    if mode == "full" and args.estimate_size:
        vcf_files = collect_input_vcfs(input_path)
        estimate = estimate_pipeline_sizes(vcf_files, out_dir)
        print("Preflight size estimate (rough):")
        print(f"  - Input VCF total: {format_bytes(estimate['input_bytes'])}")
        print(f"  - Estimated TSV intermediates: {format_bytes(estimate['tsv_bytes'])}")
        print(
            "  - Estimated RDF N-Triples output: "
            f"{format_bytes(estimate['rdf_low_bytes'])} to {format_bytes(estimate['rdf_high_bytes'])}"
        )
        print(
            f"  - Free disk at {estimate['disk_anchor']}: {format_bytes(estimate['free_disk_bytes'])}"
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
    print(f"Detailed logs: {wrapper_log_path}")

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
