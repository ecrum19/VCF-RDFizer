#!/usr/bin/env python3

import argparse
import shutil
import subprocess
import sys
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
    code = run(["docker", "version"])
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


def docker_image_exists(image: str) -> bool:
    return run(["docker", "image", "inspect", image]) == 0


def docker_build_image(image: str, repo_root: Path):
    return run(["docker", "build", "-t", image, "."], cwd=str(repo_root))


def main():
    parser = argparse.ArgumentParser(description="VCF-RDFizer Docker wrapper")
    parser.add_argument("--input", required=True, help="VCF file or directory")
    parser.add_argument("--rules", required=True, help="RML mapping rules .ttl")
    parser.add_argument("--out", default="./out", help="RDF output directory")
    parser.add_argument("--tsv", default="./tsv", help="TSV output directory")
    parser.add_argument("--image", default="vcf-rdfizer:latest", help="Docker image tag")
    parser.add_argument("--build", action="store_true", help="Force docker build")
    parser.add_argument("--no-build", action="store_true", help="Fail if image missing")
    parser.add_argument("--out-name", default="rdf", help="Output name for run_conversion.sh")
    parser.add_argument("--metrics", default="./run_metrics", help="Metrics output directory")
    parser.add_argument("--keep-tsv", action="store_true", help="Keep TSV intermediates")
    args = parser.parse_args()

    if args.build and args.no_build:
        eprint("Error: --build and --no-build are mutually exclusive.")
        return 2

    repo_root = Path(__file__).resolve().parent
    input_path = Path(args.input).expanduser().resolve()
    rules_path = Path(args.rules).expanduser().resolve()
    out_dir = Path(args.out).expanduser().resolve()
    tsv_dir = Path(args.tsv).expanduser().resolve()
    metrics_dir = Path(args.metrics).expanduser().resolve()

    print("Step 1/4: Validating inputs")
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

    rules_dir = rules_path.parent
    rules_name = rules_path.name
    container_rules = f"/data/rules/{rules_name}"

    if not check_docker():
        return 2

    print("Step 2/4: Ensuring Docker image is available")
    if args.build:
        if docker_build_image(args.image, repo_root) != 0:
            eprint("Error: docker build failed.")
            return 1
    else:
        if not docker_image_exists(args.image):
            if args.no_build:
                eprint(f"Error: image '{args.image}' not found and --no-build set.")
                return 2
            if docker_build_image(args.image, repo_root) != 0:
                eprint("Error: docker build failed.")
                return 1

    print("Step 3/4: Converting VCF to TSV")
    tsv_existed = tsv_dir.exists()
    ensure_dir(tsv_dir)
    tsv_cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{str(input_dir)}:/data/in:ro",
        "-v",
        f"{str(tsv_dir)}:/data/tsv",
        args.image,
        "/opt/vcf-rdfizer/vcf_as_tsv.sh",
        container_input,
        "/data/tsv",
    ]
    if run(tsv_cmd) != 0:
        eprint("Error: TSV conversion failed.")
        return 1

    print("Step 4/4: Running RMLStreamer")
    ensure_dir(out_dir)
    ensure_dir(metrics_dir)
    run_cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{str(rules_dir)}:/data/rules:ro",
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
        f"IN={container_rules}",
        "-e",
        "OUT_DIR=/data/out",
        "-e",
        f"OUT_NAME={args.out_name}",
        "-e",
        f"IN_VCF={container_input}",
        "-e",
        "LOGDIR=/data/metrics",
        args.image,
        "/opt/vcf-rdfizer/run_conversion.sh",
    ]
    if run(run_cmd) != 0:
        eprint("Error: RMLStreamer step failed.")
        return 1

    if not args.keep_tsv:
        if not tsv_existed:
            shutil.rmtree(tsv_dir, ignore_errors=True)
        else:
            print("Note: TSV directory existed; skipping cleanup.")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
