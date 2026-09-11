"""One linking implementation for previews, full conversion, and existing RDF."""

from dataclasses import asdict
from datetime import datetime, timezone
import importlib.util
import hashlib
from itertools import islice
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import time


from . import Link, LinkKey, LinkerContext
from .inputs import Source
from .manifest import VCFL, absolute_iri, template_object
from .reference import IntervalIndex, acquire_reference, check_assembly
from .session import CachedSession, NetworkPolicy, atomic_bytes

# Re-exported from .reference, which has no rdflib dependency, so the CLI can
# offer it as an argument default without importing this module.
from .reference import DEFAULT_CACHE  # noqa: F401
PROV_TIME = "http://www.w3.org/ns/prov#generatedAtTime"


class LinkRunError(ValueError):
    def __init__(self, message, report):
        super().__init__(message)
        self.report = report


def keys_for(record, manifest):
    if manifest.strategy == "token":
        value = record.id
        if manifest.field.startswith("INFO/"):
            wanted = manifest.field.split("/", 1)[1]
            values = [part.split("=", 1)[1] for part in record.info.split(";")
                      if "=" in part and part.split("=", 1)[0] == wanted]
            if len(values) > 1:
                raise ValueError(f"Duplicate INFO key {wanted} on {record.record}")
            value = values[0] if values else "."
        return [LinkKey(token=token) for token in dict.fromkeys(value.split(manifest.split_on))
                if token and token != "." and re.fullmatch(manifest.accept, token)]
    # POS and GFF3 intervals are both 1-based closed. Explicit REF spans only;
    # END, symbolic alleles and breakends require a different coordinate policy.
    if not re.fullmatch(r"[ACGTNacgtn]+", record.ref) or any(
            not re.fullmatch(r"[ACGTNacgtn]+|\.", alt) for alt in record.alt.split(",")):
        return []
    try:
        start = int(record.pos)
    except ValueError as exc:
        raise ValueError(f"Invalid POS on {record.record}: {record.pos!r}") from exc
    if start < 1 or record.chrom in {"", "."}:
        raise ValueError(f"Invalid interval key on {record.record}")
    return [LinkKey(chrom=record.chrom, start=start, end=start + len(record.ref) - 1)]


def load_resolver(manifest):
    path = manifest.directory / "resolver.py"
    spec = importlib.util.spec_from_file_location(f"vcfr_linker_{manifest.id.replace('-', '_')}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not callable(getattr(module, "resolve", None)):
        raise ValueError(f"{path} must define resolve(batch, ctx)")
    return module.resolve


def triple(subject, predicate, obj):
    return f"<{absolute_iri(subject)}> <{absolute_iri(str(predicate))}> {obj} .\n"


def literal(value):
    from vcf_rdfizer import _ntriples_literal
    return _ntriples_literal(str(value))


def run_linkers(records, manifests, output: Path | None, *, cache_dir=DEFAULT_CACHE,
                offline=False, assembly=None, dry_run=False, policy=None, session_factory=CachedSession,
                progress=None):
    """Deduplicate all keys on disk before any resolution; publish only on success.

    Dry-run writes no persistent artifacts, imports no resolver code, and never
    fetches a reference. It resolves local tiers and reports Tier 3 batch plans.
    """
    # Deferred so the module imports without rdflib: the CLI loads it while
    # building its argument parser, long before any linking runs.
    from rdflib import Literal, RDF, URIRef, XSD
    started = time.monotonic()
    report = {"status": "running", "records": 0, "triples": 0, "appended_bytes": 0,
              "output": str(output) if output else None, "dry_run": dry_run, "linkers": [], "preview": []}
    if not manifests:
        raise ValueError("Select at least one linker")
    if not dry_run and output is None:
        raise ValueError("An output path is required")
    if output and not output.name.endswith(".nt"):
        raise ValueError("Link output must end in .nt")
    if output and output.exists():
        raise ValueError(f"Refusing to overwrite existing linkset: {output}")
    policy = policy or NetworkPolicy(manifests)
    cache_dir = Path(cache_dir).expanduser()
    sources, stats_by_id = {}, {}
    for manifest in manifests:
        stats = {"id": manifest.id, "version": manifest.version, "tier": manifest.tier,
                 "manifest_sha256": hashlib.sha256((manifest.directory / "linker.ttl").read_bytes()).hexdigest(),
                 "reference_digest": manifest.reference.sha256 if manifest.reference else None,
                 "assembly": manifest.reference.assembly if manifest.reference else None,
                 "unique_keys": 0, "skipped_records": 0, "links": 0, "requests": 0,
                 "cache_hits": 0, "bytes_transferred": 0, "final_service_status": None,
                 "status": "pending", "wall_seconds": 0}
        if manifest.tier == 3:
            stats["resolver_sha256"] = hashlib.sha256((manifest.directory / "resolver.py").read_bytes()).hexdigest()
        stats_by_id[manifest.id] = stats
        report["linkers"].append(stats)
    current = None
    try:
        with tempfile.TemporaryDirectory(prefix="vcfr-link-keys-") as work:
            db = sqlite3.connect(str(Path(work) / "joins.sqlite"))
            try:
                db.execute("CREATE TABLE keys (linker TEXT, key TEXT, subject TEXT, source TEXT, PRIMARY KEY(linker,key,subject,source))")
                db.execute("CREATE TABLE links (linker TEXT, source TEXT, subject TEXT, predicate TEXT, object TEXT, PRIMARY KEY(linker,source,subject,predicate,object))")
                # Finish key extraction and assembly checks for every linker first.
                for record in records:
                    absolute_iri(record.source)
                    if record.source not in sources:
                        for manifest in manifests:
                            if manifest.reference:
                                check_assembly(record.reference, manifest.reference.assembly, assembly)
                        sources[record.source] = record.reference
                    elif sources[record.source] != record.reference:
                        raise ValueError(f"Conflicting reference metadata for {record.source}")
                    if isinstance(record, Source):
                        continue
                    report["records"] += 1
                    if progress and report["records"] % 10_000 == 0:
                        progress(f"Indexed {report['records']} records for linking")
                    for manifest in manifests:
                        keys = keys_for(record, manifest)
                        if not keys:
                            stats_by_id[manifest.id]["skipped_records"] += 1
                        subject = record.call if manifest.subject == str(VCFL.VariantCall) else record.record
                        absolute_iri(subject)
                        db.executemany("INSERT OR IGNORE INTO keys VALUES (?,?,?,?)", [
                            (manifest.id, json.dumps(asdict(key), sort_keys=True), subject, record.source) for key in keys])
                db.commit()
                for manifest in manifests:
                    current = stats_by_id[manifest.id]
                    linker_start = time.monotonic()
                    current["status"] = "running"
                    current["unique_keys"] = db.execute("SELECT COUNT(DISTINCT key) FROM keys WHERE linker=?", (manifest.id,)).fetchone()[0]
                    if progress:
                        progress(f"Linking {manifest.id}: {current['unique_keys']} unique keys")
                    reference_index, session, resolver = None, None, None
                    try:
                        if manifest.tier == 2:
                            path = acquire_reference(manifest.reference, cache_dir, offline=offline, dry_run=dry_run, stats=current)
                            reference_index = IntervalIndex(path, manifest.reference)
                        if manifest.tier == 3:
                            current["batches"] = (current["unique_keys"] + manifest.batch_size - 1) // manifest.batch_size
                            current["max_requests_per_run"] = manifest.max_requests
                            if dry_run:
                                current["status"] = "planned"
                                continue
                            session = session_factory(manifest, cache_dir, policy, offline=offline)
                            resolver = load_resolver(manifest)
                        rows = db.execute("SELECT DISTINCT key FROM keys WHERE linker=? ORDER BY key", (manifest.id,))
                        while True:
                            raw_keys = [r[0] for r in islice(rows, manifest.batch_size)]
                            if not raw_keys:
                                break
                            batch = [LinkKey(**json.loads(key)) for key in raw_keys]
                            encoded = dict(zip(batch, raw_keys))
                            if manifest.tier == 1:
                                links = (Link(key, template_object(manifest, key.token)) for key in batch)
                            elif manifest.tier == 2:
                                links = (Link(key, template_object(manifest, identifier)) for key in batch for identifier in reference_index.overlaps(key))
                            else:
                                links = resolver(tuple(batch), LinkerContext(session))
                            for link in links:
                                if not isinstance(link, Link) or link.key not in encoded:
                                    raise ValueError(f"{manifest.id}: resolver returned a link for an undispatched key")
                                obj = absolute_iri(link.object)
                                db.execute("INSERT OR IGNORE INTO links SELECT linker,source,subject,?,? FROM keys WHERE linker=? AND key=?",
                                           (manifest.predicate, obj, manifest.id, encoded[link.key]))
                        current["links"] = db.execute("SELECT COUNT(*) FROM links WHERE linker=?", (manifest.id,)).fetchone()[0]
                        current["status"] = "success"
                    except BaseException:
                        current["status"] = "failed"
                        raise
                    finally:
                        if session:
                            current.update(session.stats)
                        current["wall_seconds"] = round(time.monotonic() - linker_start, 6)
                db.commit()
                report["preview"] = [triple(s, p, f"<{o}>").rstrip() for s, p, o in db.execute("SELECT DISTINCT subject,predicate,object FROM links ORDER BY subject,predicate,object LIMIT 20")]
                report["link_triples"] = db.execute("SELECT COUNT(*) FROM (SELECT DISTINCT subject,predicate,object FROM links)").fetchone()[0]
                if not dry_run:
                    output.parent.mkdir(parents=True, exist_ok=True)
                    with tempfile.NamedTemporaryFile(dir=output.parent, suffix=".nt", delete=False) as handle:
                        temporary = Path(handle.name)
                    try:
                        from vcf_rdfizer import _append_rdf_atomically

                        def produce(emit):
                            for s, p, o in db.execute("SELECT DISTINCT subject,predicate,object FROM links ORDER BY subject,predicate,object"):
                                emit(triple(s, p, f"<{o}>"))
                            for source in sorted(sources):
                                for manifest in manifests:
                                    node = source + "#linkset/" + manifest.id
                                    count = db.execute("SELECT COUNT(*) FROM links WHERE linker=? AND source=?", (manifest.id, source)).fetchone()[0]
                                    emit(triple(node, RDF.type, URIRef(VCFL.Linkset).n3()))
                                    emit(triple(node, VCFL.producedBy, URIRef(f"https://w3id.org/vcf-rdfizer/linker/{manifest.id}/{manifest.version}").n3()))
                                    emit(triple(node, VCFL.linkCount, Literal(count, datatype=XSD.integer).n3()))
                                    emit(triple(node, VCFL.source, URIRef(source).n3()))
                                    emit(triple(node, VCFL.manifestDigest, literal("sha256:" + stats_by_id[manifest.id]["manifest_sha256"])))
                                    if manifest.tier == 3:
                                        emit(triple(node, VCFL.resolverDigest, literal("sha256:" + stats_by_id[manifest.id]["resolver_sha256"])))
                                        for digest in sorted({r["sha256"] for r in stats_by_id[manifest.id].get("responses", [])}):
                                            emit(triple(node, VCFL.responseDigest, literal("sha256:" + digest)))
                                    emit(triple(node, PROV_TIME, Literal(datetime.now(timezone.utc).isoformat(), datatype=XSD.dateTime).n3()))
                                    if manifest.reference:
                                        emit(triple(node, VCFL.referenceDigest, literal("sha256:" + manifest.reference.sha256)))
                                        emit(triple(node, VCFL.assembly, literal(manifest.reference.assembly)))
                        _append_rdf_atomically(temporary, report, produce)
                        # Hard-link publication is atomic and fails if another run
                        # claimed this output after preflight; never overwrite it.
                        os.link(temporary, output)
                    finally:
                        temporary.unlink(missing_ok=True)
            finally:
                db.close()
        report["status"] = "success"
        return report
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = str(exc)
        if current and current["status"] == "running":
            current["status"] = "failed"
        raise LinkRunError(str(exc), report) from exc
    finally:
        report["wall_seconds"] = round(time.monotonic() - started, 6)


def run_stage(rdf_path, manifests, output, *, metrics_dir, tracker=None, **options):
    """Record success and failure using the wrapper's existing run manifest."""
    from vcf_rdfizer import (update_run_manifest, ProgressSession,
                             progress_event_path, progress_events_enabled)
    from .inputs import read_rdf

    stage = metrics_dir / "stages" / "linking" / (output.stem + ".json")
    event_path = progress_event_path(metrics_dir, "linking", output.stem) if progress_events_enabled() else None
    try:
        with ProgressSession(event_path, f"Linking: {output.stem}") as session:
            def progress(message):
                if tracker:
                    tracker.mark(message)
                if event_path:
                    with event_path.open("a", encoding="utf-8") as handle:
                        handle.write(json.dumps({"stage": "linking", "phase": "working", "detail": message}) + "\n")
                    session.poll_events()
            report = run_linkers(read_rdf(rdf_path), manifests, output, progress=progress, **options)
    except LinkRunError as exc:
        report = exc.report
        raise
    finally:
        if "report" in locals():
            atomic_bytes(stage, (json.dumps(report, indent=2) + "\n").encode())
            # Preserve other inputs' linking reports in a directory conversion.
            reports = {p.stem: json.loads(p.read_text()) for p in stage.parent.glob("*.json")}
            update_run_manifest(metrics_dir, linking=reports)
            if tracker:
                tracker.mark(f"Linking {report['status']}: {output.name}")
    return report
