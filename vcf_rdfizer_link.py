#!/usr/bin/env python3
"""Inspect, scaffold, preview and run data-linking plug-ins without Docker."""

import argparse
from dataclasses import asdict, replace
from datetime import datetime
import json
from pathlib import Path
import shutil
import sys
import time

from vcf_rdfizer_linking.inputs import read_rdf, read_vcf
from vcf_rdfizer_linking.manifest import discover, load_manifest, select
from vcf_rdfizer_linking.reference import acquire_reference, check_assembly, IntervalIndex
from vcf_rdfizer_linking.runner import DEFAULT_CACHE, LinkRunError, run_linkers, run_stage


def add_link_arguments(parser):
    parser.add_argument("--link", help="Comma-separated linker IDs (full/link modes)")
    parser.add_argument("--linker-path", action="append", default=[], help="Additional linker search directory (repeatable)")
    parser.add_argument("--links-cache", default=str(DEFAULT_CACHE), help="Reference and HTTP response cache directory")
    parser.add_argument("--offline", action="store_true", help="Disable linker network access; local references and cached responses only")
    parser.add_argument("--links-cache-only", action="store_true", help="Alias for --offline for linking")
    parser.add_argument("--assembly", help="Input assembly when reference metadata is absent/unrecognized; cannot override a mismatch")
    parser.add_argument("--links-contact-email", help="Contact address sent in live linker User-Agent headers")


def selected_linkers(args):
    import re
    manifests = select(args.link or "", args.linker_path)
    if args.links_contact_email:
        if not re.fullmatch(r"[^\s<>@]+@[^\s<>@]+\.[^\s<>@]+", args.links_contact_email):
            raise ValueError("--links-contact-email must be an email address")
        manifests = [replace(m, contact_email=args.links_contact_email) if m.tier == 3 else m for m in manifests]
    return manifests


def run_options(args):
    return {"cache_dir": Path(args.links_cache).expanduser(),
            "offline": args.offline or args.links_cache_only, "assembly": args.assembly}


def run_posthoc(args):
    """Host-only --mode link, with the standard run_metrics tree."""
    from vcf_rdfizer import (RunTracker, metrics_run_directory, metrics_run_label,
                             write_run_manifest, write_run_summary, rdf_output_basename)
    try:
        if not args.rdf or not args.link:
            raise ValueError("--mode link requires --rdf and --link")
        rdf = Path(args.rdf).expanduser().resolve()
        if not rdf.is_file() or not rdf.name.endswith((".nt", ".nt.gz")):
            raise ValueError("--rdf must be an existing .nt or .nt.gz file")
        manifests = selected_linkers(args)
        root = Path(args.out).expanduser().resolve()
        output = root / (rdf_output_basename(rdf) + ".links.nt")
        if output.exists():
            raise ValueError(f"Refusing to overwrite existing linkset: {output}")
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S%f")
    timestamp = datetime.now().isoformat(timespec="seconds")
    metrics = metrics_run_directory(root / "run_metrics", metrics_run_label([rdf], rdf), run_id)
    started, tracker, report, code = time.monotonic(), None, None, 1
    try:
        write_run_manifest(metrics_dir=metrics, run_id=run_id, timestamp=timestamp,
                           mode="link", source_label=rdf.name, source_paths=[rdf], out_root=root,
                           options={"link": [m.id for m in manifests], **{k: str(v) if isinstance(v, Path) else v for k, v in run_options(args).items()}})
        tracker = RunTracker(metrics / "logs" / "progress.log")
        report = run_stage(rdf, manifests, output, metrics_dir=metrics, tracker=tracker, **run_options(args))
        print(f"Wrote {output} ({report['link_triples']} links); metrics: {metrics}")
        code = 0
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
    except KeyboardInterrupt:
        print("Linking interrupted; no partial side-graph published", file=sys.stderr)
        code = 130
    finally:
        if tracker:
            tracker.close()
            write_run_summary(metrics_dir=metrics, run_id=run_id, timestamp=timestamp, mode="link",
                              exit_code=code, elapsed_seconds=time.monotonic() - started,
                              total_triples=report["triples"] if report else None)
    return code


def build_parser():
    parser = argparse.ArgumentParser(prog="vcf-rdfizer-link", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list", help="List installed IDs, versions, tiers, references and licensing")
    listing.add_argument("--linker-path", action="append", default=[])
    listing.add_argument("--json", action="store_true")
    commands.add_parser("keys", help="Describe the supported join-key contract")
    init = commands.add_parser("init", help="Copy an annotated example into a new linker directory")
    init.add_argument("-o", "--output", required=True)
    init.add_argument("--example", default="rsid-dbsnp", choices=["rsid-dbsnp", "gene-demo", "rsid-ensembl"])
    check = commands.add_parser("check", help="Parse a manifest and verify its reference (no resolver execution)")
    check.add_argument("directory")
    check.add_argument("--json", action="store_true")
    add_link_arguments(check)
    dry = commands.add_parser("dry-run", help="Preview first N VCF records; no network or persistent writes")
    dry.add_argument("directory")
    dry.add_argument("-i", "--input", required=True)
    dry.add_argument("--limit", type=int, default=100)
    add_link_arguments(dry)
    run = commands.add_parser("run", help="Write a side-graph from VCF or an existing RDF aggregate")
    inputs = run.add_mutually_exclusive_group(required=True)
    inputs.add_argument("-i", "--input", help="VCF to link using the default subject templates")
    inputs.add_argument("--rdf", help="Existing .nt/.nt.gz; subjects are read from the graph")
    run.add_argument("-o", "--output", required=True, help="New .links.nt output file")
    add_link_arguments(run)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        if args.command == "keys":
            print("TokenJoin: field ID (splitOn ';') or INFO/<key> (splitOn ','); accept uses full regex matches; {TOKEN} is percent-encoded.")
            print("IntervalJoin: CHROM exact match; [POS, POS + len(REF) - 1], 1-based closed; explicit DNA alleles only. GFF3 attribute -> {ID}.")
            print("Subjects: vcfl:VariantCall or vcfl:VCFRecord. AlleleJoin is not implemented.")
        elif args.command == "list":
            manifests = discover(args.linker_path)
            if args.json:
                print(json.dumps([asdict(m) for m in manifests.values()], default=str, indent=2))
            else:
                for m in manifests.values():
                    print(f"{m.id} {m.version} — tier {m.tier}: {m.title}")
                    print(f"  License: {m.license}; terms: {m.terms_of_use or 'unspecified'}")
                    if m.reference:
                        print(f"  Reference: {m.reference.url}; {m.reference.assembly}; sha256:{m.reference.sha256}")
        elif args.command == "init":
            output = Path(args.output).expanduser()
            if output.exists():
                raise ValueError(f"Refusing to overwrite {output}")
            source = discover()[args.example].directory
            shutil.copytree(source, output, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
            print(f"Wrote {output}. Change its id before adding it to a search path, then run check.")
        elif args.command == "check":
            m = load_manifest(Path(args.directory))
            if m.reference:
                if args.assembly:
                    check_assembly(args.assembly, m.reference.assembly)
                path = acquire_reference(m.reference, Path(args.links_cache), offline=args.offline or args.links_cache_only)
                IntervalIndex(path, m.reference)
            print(json.dumps({"ok": True, "id": m.id, "tier": m.tier, "assembly": m.reference.assembly if m.reference else None,
                              "note": "Input assembly is checked at run time; resolver code was not executed"}, indent=2))
        elif args.command == "dry-run":
            if args.limit < 1:
                raise ValueError("--limit must be positive")
            report = run_linkers(read_vcf(Path(args.input).expanduser(), args.limit), [load_manifest(Path(args.directory))], None, dry_run=True, **run_options(args))
            print(json.dumps(report, indent=2))
        else:
            manifests = selected_linkers(args)
            output = Path(args.output).expanduser()
            if not output.name.endswith(".nt"):
                raise ValueError("Link output must end in .nt")
            if output.with_suffix(".json").exists():
                raise ValueError(f"Refusing to overwrite existing report: {output.with_suffix('.json')}")
            records = read_rdf(Path(args.rdf).expanduser()) if args.rdf else read_vcf(Path(args.input).expanduser())
            report = run_linkers(records, manifests, output, **run_options(args))
            from vcf_rdfizer_linking.session import atomic_bytes
            atomic_bytes(output.with_suffix(".json"), (json.dumps(report, indent=2) + "\n").encode())
            print(f"Wrote {output} ({report['link_triples']} links)")
        return 0
    except (ValueError, OSError) as exc:
        if getattr(args, "json", False):
            print(json.dumps({"ok": False, "errors": [str(exc)]}))
        else:
            print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
