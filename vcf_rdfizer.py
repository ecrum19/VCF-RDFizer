#!/usr/bin/env python3

import argparse
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path


RMLSTREAMER_JAR_CONTAINER = "/opt/rmlstreamer/RMLStreamer-v2.5.0-standalone.jar"


def eprint(*args):
    print(*args, file=sys.stderr)


def run(cmd, cwd=None, env=None):
    return subprocess.run(cmd, cwd=cwd, env=env).returncode


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


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


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


def main():
    parser = argparse.ArgumentParser(description="VCF-RDFizer Docker wrapper")
    parser.add_argument("--input", required=True, help="VCF file or directory")
    parser.add_argument(
        "--rules",
        default=None,
        help="RML mapping rules .ttl (default: <repo>/rules/default_rules.ttl)",
    )
    parser.add_argument("--out", default="./out", help="RDF output directory")
    parser.add_argument("--tsv", default="./tsv", help="TSV output directory")
    parser.add_argument(
        "--image",
        default="ecrum19/vcf-rdfizer",
        help="Docker image repo (no tag) or full image reference",
    )
    parser.add_argument(
        "--image-version",
        default=None,
        help="Image tag/version to use (e.g. 1.2.3). Defaults to latest if omitted.",
    )
    parser.add_argument("--build", action="store_true", help="Force docker build")
    parser.add_argument("--no-build", action="store_true", help="Fail if image missing")
    parser.add_argument(
        "--out-name",
        default="rdf",
        help="Fallback output directory/file basename when a TSV basename cannot be inferred",
    )
    parser.add_argument("--metrics", default="./run_metrics", help="Metrics output directory")
    parser.add_argument(
        "--compression",
        default="gzip,brotli,hdt",
        help="Compression methods for compression.sh (gzip,brotli,hdt,none)",
    )
    parser.add_argument("--keep-tsv", action="store_true", help="Keep TSV intermediates")
    args = parser.parse_args()

    if args.build and args.no_build:
        eprint("Error: --build and --no-build are mutually exclusive.")
        return 2

    repo_root = Path(__file__).resolve().parent
    input_path = Path(args.input).expanduser().resolve()
    if args.rules is None:
        rules_path = (repo_root / "rules" / "default_rules.ttl").resolve()
    else:
        rules_path = Path(args.rules).expanduser().resolve()
    out_dir = Path(args.out).expanduser().resolve()
    tsv_dir = Path(args.tsv).expanduser().resolve()
    metrics_dir = Path(args.metrics).expanduser().resolve()

    print("Step 1/5: Validating inputs")
    try:
        input_dir, container_input = resolve_input(input_path)
    except ValueError as exc:
        eprint(f"Error: {exc}")
        return 2

    if not rules_path.exists() or not rules_path.is_file():
        eprint(f"Error: rules file not found: {rules_path}")
        return 2

    for p in [out_dir, tsv_dir, metrics_dir]:
        if p.exists() and not p.is_dir():
            eprint(f"Error: expected a directory path but found a file: {p}")
            return 2

    run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    if not check_docker():
        return 2

    try:
        image_ref, version_requested = resolve_image_ref(args.image, args.image_version)
    except ValueError as exc:
        eprint(f"Error: {exc}")
        return 2

    print("Step 2/5: Ensuring Docker image is available")
    if args.build:
        if docker_build_image(image_ref, repo_root) != 0:
            eprint("Error: docker build failed.")
            return 1
    else:
        if not docker_image_exists(image_ref):
            if version_requested:
                print(f"Image {image_ref} not found locally. Attempting to pull...")
                if docker_pull_image(image_ref) != 0:
                    eprint(f"Error: image version '{image_ref}' not found.")
                    return 2
            else:
                if args.no_build:
                    eprint(f"Error: image '{image_ref}' not found and --no-build set.")
                    return 2
                if docker_build_image(image_ref, repo_root) != 0:
                    eprint("Error: docker build failed.")
                    return 1

    print("Step 3/5: Converting VCF to TSV")
    tsv_existed = tsv_dir.exists()
    ensure_dir(tsv_dir)
    tsv_cmd = [
        "sudo",
        "docker",
        "run",
        "--rm",
        "-v",
        f"{str(input_dir)}:/data/in:ro",
        "-v",
        f"{str(tsv_dir)}:/data/tsv",
        image_ref,
        "/opt/vcf-rdfizer/vcf_as_tsv.sh",
        container_input,
        "/data/tsv",
    ]
    if run(tsv_cmd) != 0:
        eprint("Error: TSV conversion failed.")
        return 1

    print("Step 4/5: Running Conversion with RMLStreamer")
    ensure_dir(out_dir)
    ensure_dir(metrics_dir)

    try:
        tsv_triplets = discover_tsv_triplets(tsv_dir)
    except ValueError as exc:
        eprint(f"Error: {exc}")
        return 1

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

        output_name = safe_prefix or slugify(args.out_name)
        conversion_output_names.append(output_name)
        container_generated_rules = f"/data/rules/{generated_rules.name}"

        print(f"  - Converting '{prefix}' -> out/{output_name}")
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
            f"IN_VCF={container_input}",
            "-e",
            "LOGDIR=/data/metrics",
            image_ref,
            "/opt/vcf-rdfizer/run_conversion.sh",
        ]
        if run(run_cmd) != 0:
            eprint(f"Error: RMLStreamer step failed for '{prefix}'.")
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
        args.compression,
    ]
    if run(compression_cmd) != 0:
        eprint("Error: compression step failed.")
        return 1

    if not args.keep_tsv:
        if not tsv_existed:
            shutil.rmtree(tsv_dir, ignore_errors=True)
        else:
            print("Note: TSV directory existed; skipping cleanup.")

    print("Done. See output and metrics directories for results and statistics about the conversion process.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
