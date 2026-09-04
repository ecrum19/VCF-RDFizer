"""One declarative fixture, three derived artifacts.

The mutation harness needs a VCF, the RDF graph that VCF should convert to, and
the parser summary the validator's oracle should compute from it. Deriving all
three from a single specification below is what keeps them honest: a fixture
change cannot make the graph and the oracle disagree by accident.

``test_validation_container_unit.py`` closes the loop in CI by running the real
``parse_vcf`` over :func:`write_vcf` output and asserting it equals
:func:`parser_summary`, so the hand-derived oracle is pinned to the real one.

The records are chosen to exercise every branch the validation queries have:
a transition SNV, a transversion SNV, a deletion shape, a multi-allelic site,
a no-ALT site, PASS and non-PASS FILTER values, a missing genotype, a
non-diploid genotype, and a non-GT FORMAT field that nothing currently checks.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

VCFR = "https://w3id.org/vcf-rdfizer/vocab#"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"
XSD_POSITIVE_INTEGER = "http://www.w3.org/2001/XMLSchema#positiveInteger"

def _runner():
    """Load the shipped validation runner (it lives outside the package path)."""
    import importlib.util

    path = Path(__file__).resolve().parents[1] / "src" / "validation" / "validation_runner.py"
    spec = importlib.util.spec_from_file_location("validation_runner_fixture", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


SOURCE_FILE = "fixture.vcf"
SAMPLES = ("HG001", "HG002")
FILE_FORMAT = "VCFv4.2"
FILE_DATE = "20260101"
SOURCE_SOFTWARE = "vcf-rdfizer-fixture"
REFERENCE_GENOME = "GRCh38"

#: ``##`` meta-information lines, in file order. The ``#CHROM`` line is not one
#: of these; ``header_count`` below counts only the ``##`` lines.
HEADER_LINES: tuple[tuple[str, str], ...] = (
    ("fileformat", FILE_FORMAT),
    ("fileDate", FILE_DATE),
    ("source", SOURCE_SOFTWARE),
    ("reference", REFERENCE_GENOME),
    ("FILTER", '<ID=q10,Description="Quality below 10">'),
    ("INFO", '<ID=AC,Number=A,Type=Integer,Description="Allele count">'),
    ("INFO", '<ID=DB,Number=0,Type=Flag,Description="dbSNP membership">'),
    ("FORMAT", '<ID=GT,Number=1,Type=String,Description="Genotype">'),
    ("FORMAT", '<ID=DP,Number=1,Type=Integer,Description="Read depth">'),
    # Exercise the structured header paths: a contig with every optional
    # attribute, one with none, and a symbolic ALT declaration.
    ("contig", "<ID=20,length=64444167,md5=b0a5f3e1,assembly=GRCh38>"),
    ("contig", "<ID=21>"),
    ("ALT", '<ID=DEL,Description="Deletion relative to the reference">'),
    # An unrecognized key: it must keep only the base HeaderLine type.
    ("phasing", "partial"),
)


@dataclass(frozen=True)
class FixtureRecord:
    """One VCF data line and everything derived from it."""

    row_id: str
    chrom: str
    pos: int
    record_id: str
    ref: str
    alt: str
    qual: str
    filter_value: str
    info: str
    format_keys: tuple[str, ...]
    #: One ``:``-joined payload per sample, aligned to ``format_keys``.
    sample_payloads: tuple[str, ...] = field(default_factory=tuple)


RECORDS: tuple[FixtureRecord, ...] = (
    # Transition SNV, PASS, both samples called.
    FixtureRecord("1", "20", 100, "rs100", "A", "G", "50", "PASS", "AC=1;DB",
                  ("GT", "DP"), ("0|1:30", "0/0:28")),
    # Transversion SNV in the same 1 Mb window as record 1: a POS swap between
    # these two is invisible to every aggregate query.
    FixtureRecord("2", "20", 300, ".", "A", "C", "99", "PASS", "AC=2",
                  ("GT", "DP"), ("1/1:41", "0/1:35")),
    # Deletion shape in a different window, failing FILTER, missing genotype.
    FixtureRecord("3", "20", 2000200, "rs200", "AAT", "A", "12.5", "q10", "AC=1",
                  ("GT", "DP"), ("0/1:15", "./.:0")),
    # Multi-allelic: excluded from q06, classified MULTIALLELIC by q02.
    FixtureRecord("4", "21", 500, ".", "C", "T,G", ".", "PASS", "AC=1,1",
                  ("GT", "DP"), ("1/2:22", "0/1:19")),
    # No ALT, haploid genotype: exercises NO_ALT and HAPLOID_* branches.
    FixtureRecord("5", "21", 900, ".", "G", ".", "7", "q10", "DB",
                  ("GT", "DP"), ("0:11", "1:9")),
    # A second transition SNV sharing record 1's contig and 1 Mb window. A
    # REF/ALT permutation between records 1 and 6 leaves every aggregate
    # identical, which is the blind spot the record digest is meant to close.
    FixtureRecord("6", "20", 800, ".", "C", "T", "60", "PASS", "AC=1",
                  ("GT", "DP"), ("0/1:33", "0/1:31")),
)

TRANSITIONS = {("A", "G"), ("G", "A"), ("C", "T"), ("T", "C")}


# ---------------------------------------------------------------------------
# VCF
# ---------------------------------------------------------------------------
def vcf_text() -> str:
    lines = [f"##{key}={value}" for key, value in HEADER_LINES]
    lines.append(
        "#" + "\t".join(
            ["CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO", "FORMAT", *SAMPLES]
        )
    )
    for record in RECORDS:
        lines.append("\t".join([
            record.chrom, str(record.pos), record.record_id, record.ref, record.alt,
            record.qual, record.filter_value, record.info,
            ":".join(record.format_keys), *record.sample_payloads,
        ]))
    return "\n".join(lines) + "\n"


def write_vcf(path: Path) -> Path:
    path.write_text(vcf_text(), encoding="utf-8")
    return path


def records_tsv_text() -> str:
    """The `vcf_as_tsv.sh` output for this fixture, used by the RDF emitters."""
    header = ["SOURCE_FILE", "ROW_ID", "CHROM", "POS", "ID", "REF", "ALT", "QUAL",
              "FILTER", "INFO", "FORMAT", " ".join(SAMPLES)]
    rows = ["\t".join(header)]
    for record in RECORDS:
        rows.append("\t".join([
            SOURCE_FILE, record.row_id, record.chrom, str(record.pos), record.record_id,
            record.ref, record.alt, record.qual, record.filter_value, record.info,
            ":".join(record.format_keys), " ".join(record.sample_payloads),
        ]))
    return "\n".join(rows) + "\n"


def header_lines_tsv_text() -> str:
    rows = ["\t".join(["SOURCE_FILE", "HEADER_INDEX", "HEADER_KEY", "HEADER_VALUE", "RAW_LINE"])]
    for index, (key, value) in enumerate(HEADER_LINES, start=1):
        rows.append("\t".join([SOURCE_FILE, str(index), key, value, f"{key}={value}"]))
    return "\n".join(rows) + "\n"


# ---------------------------------------------------------------------------
# RDF graph
# ---------------------------------------------------------------------------
def _literal(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    if value == ".":
        return f'"{escaped}"^^<{VCFR}Null>'
    return f'"{escaped}"'


def _info_entries(record: "FixtureRecord") -> list[tuple[str, str | None]]:
    """Split an INFO column into (key, value) pairs; a bare key is a Flag."""
    entries: list[tuple[str, str | None]] = []
    if record.info in ("", "."):
        return entries
    for item in record.info.split(";"):
        if "=" in item:
            key, value = item.split("=", 1)
            entries.append((key, value))
        else:
            entries.append((item, None))
    return entries


def base_triples(*, include_qual: bool = True, include_info: bool = True) -> list[str]:
    """The triples RMLStreamer produces from `default_rules.ttl`.

    ``include_qual`` is on because the shipped mapping now emits ``vcfr:qual``;
    turning it off models the pre-fix mapping, which is how the harness shows
    that dropping QUAL is now detected. ``include_info`` models the structured
    INFO representation.
    """
    file_uri = f"file://{SOURCE_FILE}"
    header_uri = f"{file_uri}#header"
    out = [
        f"<{file_uri}> <{RDF_TYPE}> <{VCFR}VCFFile> .",
        f"<{file_uri}> <{VCFR}fileFormat> {_literal(FILE_FORMAT)} .",
        f"<{file_uri}> <{VCFR}sourceSoftware> {_literal(SOURCE_SOFTWARE)} .",
        f"<{file_uri}> <{VCFR}referenceGenome> {_literal(REFERENCE_GENOME)} .",

        f"<{file_uri}> <{VCFR}hasHeader> <{header_uri}> .",
        f"<{header_uri}> <{RDF_TYPE}> <{VCFR}VCFHeader> .",
    ]
    for index, (key, value) in enumerate(HEADER_LINES, start=1):
        line_uri = f"{file_uri}#header/line/{index}"
        out += [
            f"<{header_uri}> <{VCFR}hasHeaderLine> <{line_uri}> .",
            f"<{line_uri}> <{RDF_TYPE}> <{VCFR}HeaderLine> .",
            f"<{line_uri}> <{VCFR}headerKey> {_literal(key)} .",
            f"<{line_uri}> <{VCFR}headerValue> {_literal(value)} .",
        ]
    for record in RECORDS:
        record_uri = f"{file_uri}#record/{record.row_id}"
        call_uri = f"{file_uri}#call/{record.row_id}"
        out += [
            f"<{file_uri}> <{VCFR}hasRecord> <{record_uri}> .",
            f"<{record_uri}> <{RDF_TYPE}> <{VCFR}VCFRecord> .",
            f"<{record_uri}> <{VCFR}chrom> {_literal(record.chrom)} .",
            f'<{record_uri}> <{VCFR}pos> "{record.pos}"^^<{XSD_INTEGER}> .',
            f"<{record_uri}> <{VCFR}recordId> {_literal(record.record_id)} .",
            f"<{record_uri}> <{VCFR}ref> {_literal(record.ref)} .",
            f"<{record_uri}> <{VCFR}alt> {_literal(record.alt)} .",
            f"<{record_uri}> <{VCFR}hasCall> <{call_uri}> .",
            f"<{call_uri}> <{RDF_TYPE}> <{VCFR}VariantCall> .",
            f"<{call_uri}> <{VCFR}filter> {_literal(record.filter_value)} .",
            f"<{call_uri}> <{VCFR}infoRaw> {_literal(record.info)} .",
            f"<{call_uri}> <{VCFR}formatRaw> {_literal(':'.join(record.format_keys))} .",
        ]
    return out


def build_graph(
    representation: str = "expanded", *, include_qual: bool = True,
    include_info: bool = True, include_headers: bool = True,
) -> str:
    """Return the N-Triples graph for this fixture, sample triples included.

    The sample triples come from the project's own emitters rather than being
    written out here, so the fixture tracks the real implementation.
    """
    import tempfile

    import vcf_rdfizer

    if representation not in {"expanded", "condensed"}:
        raise ValueError(f"unknown representation: {representation}")

    with tempfile.TemporaryDirectory() as td:
        tmp_path = Path(td)
        records_tsv = tmp_path / f"{SOURCE_FILE}.records.tsv"
        records_tsv.write_text(records_tsv_text(), encoding="utf-8")
        headers_tsv = tmp_path / f"{SOURCE_FILE}.header_lines.tsv"
        headers_tsv.write_text(header_lines_tsv_text(), encoding="utf-8")

        graph = tmp_path / "graph.nt"
        graph.write_text(
            "\n".join(base_triples(include_qual=include_qual, include_info=include_info)) + "\n",
            encoding="utf-8",
        )
        if include_qual or include_info:
            vcf_rdfizer.append_record_detail_rdf(
                records_tsv, headers_tsv, graph,
                emit_qual=include_qual, emit_info=include_info,
                progress_interval_records=0,
            )
        if representation == "expanded":
            vcf_rdfizer.append_expanded_sample_rdf(records_tsv, graph, progress_interval_records=0)
        else:
            vcf_rdfizer.append_condensed_sample_rdf(
                records_tsv, headers_tsv, graph, progress_interval_records=0
            )
        if include_headers:
            vcf_rdfizer.append_header_representation_rdf(headers_tsv, graph)
        return graph.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Parser oracle
# ---------------------------------------------------------------------------
def _classify_shape(ref: str, alt: str) -> str:
    import re

    ref, alt = ref.upper(), alt.upper()
    if alt == ".":
        return "NO_ALT"
    if "," in alt:
        return "MULTIALLELIC"
    if alt == "*" or "[" in alt or "]" in alt or (alt.startswith("<") and alt.endswith(">")):
        return "SYMBOLIC_OR_BREAKEND"
    if not re.fullmatch(r"[ACGTN]+", ref) or not re.fullmatch(r"[ACGTN]+", alt):
        return "OTHER"
    if len(ref) == len(alt) == 1:
        return "SNV"
    if len(ref) == len(alt):
        return "MNV_OR_EQUAL_LENGTH_SUBSTITUTION"
    return "INSERTION_SHAPE" if len(ref) < len(alt) else "DELETION_SHAPE"


def _classify_genotype(raw: str) -> str:
    normalized = raw.replace("|", "/")
    if "." in normalized:
        return "MISSING"
    alleles = normalized.split("/")
    if len(alleles) == 1:
        return "HAPLOID_REF" if alleles[0] == "0" else "HAPLOID_ALT"
    if len(alleles) == 2:
        if alleles[0] == alleles[1]:
            return "HOM_REF" if alleles[0] == "0" else "HOM_ALT"
        return "HET"
    return "OTHER_PLOIDY"


def _genotype_of(record: FixtureRecord, sample_index: int) -> str:
    payload = record.sample_payloads[sample_index].split(":")
    return payload[record.format_keys.index("GT")]


def _format_shape() -> dict:
    """FORMAT counters the census needs, mirroring SampleRecordStream widening."""
    records_with_column = records_with_keys = 0
    occurrences = slots = non_empty = 0
    distinct: set[str] = set()
    for record in RECORDS:
        if record.format_keys:
            records_with_column += 1
        payload_fields = [p.split(":") if p else [] for p in record.sample_payloads]
        width = max([len(record.format_keys), *(len(f) for f in payload_fields)], default=0)
        keys = [
            record.format_keys[i] if i < len(record.format_keys) and record.format_keys[i]
            else f"FIELD_{i + 1}"
            for i in range(width)
        ]
        if SAMPLES and keys:
            records_with_keys += 1
            distinct.update(keys)
            occurrences += width
            slots += width * len(SAMPLES)
            non_empty += sum(
                1 for f in payload_fields for i in range(width)
                if i < len(f) and f[i] != ""
            )
    return {
        "recordsWithFormatColumn": records_with_column,
        "recordsWithFormatKeys": records_with_keys,
        "formatKeyOccurrences": occurrences,
        "formatValueSlots": slots,
        "nonEmptyFormatValues": non_empty,
        "distinctFormatKeyCount": len(distinct),
    }


def _header_shape() -> dict:
    """Header counters the census needs, derived with the shipped parser."""
    runner = _runner()
    classes: Counter[str] = Counter()
    filters = alts = contigs = described = 0
    attributes: Counter[str] = Counter()
    for key, value in HEADER_LINES:
        class_name = runner.HEADER_LINE_CLASSES.get(key.lower())
        if class_name is None:
            continue
        classes[class_name] += 1
        if key.lower() not in {"filter", "alt", "contig"}:
            continue
        fields = runner.parse_structured_header_fields(value)
        if not (fields.get("ID") or "").strip():
            continue
        if key.lower() == "contig":
            contigs += 1
            for attribute in ("length", "md5", "assembly"):
                if fields.get(attribute):
                    attributes[attribute] += 1
            continue
        if key.lower() == "filter":
            filters += 1
        else:
            alts += 1
        if fields.get("Description"):
            described += 1
    return {
        "headerLineClassCounts": dict(classes),
        "filterDefinitionCount": filters,
        "altDefinitionCount": alts,
        "contigCount": contigs,
        "contigAttributeCounts": dict(attributes),
        "describedDefinitionCount": described,
    }


def _info_shape() -> dict:
    """INFO counters the census needs, mirroring the emitter's typing rules."""
    declared = {}
    for key, value in HEADER_LINES:
        if key != "INFO":
            continue
        import re as _re

        fields = dict(_re.findall(r'(\w+)=("[^"]*"|[^,>]*)', value))
        ident = fields.get("ID", "").strip()
        if ident:
            declared[ident] = fields.get("Type", "String").strip('"')
    values = flags = typed_int = typed_dec = 0
    keys: set[str] = set()
    for record in RECORDS:
        for key, value in _info_entries(record):
            values += 1
            keys.add(key)
            if value is None:
                flags += 1
                continue
            if "," in value or value == ".":
                continue
            kind = declared.get(key, "String")
            try:
                if kind == "Integer":
                    int(value); typed_int += 1
                elif kind == "Float":
                    float(value); typed_dec += 1
            except ValueError:
                pass
    return {
        "infoValueCount": values,
        "infoDefinitionCount": len(keys),
        "infoFlagCount": flags,
        "infoTypedIntegerCount": typed_int,
        "infoTypedDecimalCount": typed_dec,
    }


def parser_summary(
    representation: str = "expanded", *, include_qual: bool = True,
    include_info: bool = True, include_headers: bool = True,
) -> dict:
    """The summary ``parse_vcf`` must produce for this fixture.

    ``representation`` selects the census expectation, which depends on which
    genotype emitter ran. ``include_qual`` mirrors ``build_graph`` so a graph
    built without QUAL is compared against an expectation that also omits it.
    """
    import re

    density: Counter[tuple[str, int]] = Counter()
    shapes: Counter[str] = Counter()
    filters: Counter[tuple[str, str]] = Counter()
    genotypes: Counter[tuple[str, str]] = Counter()
    ac_an: Counter[tuple[int, int]] = Counter()
    transitions = transversions = biallelic_snvs = single_alt = q06_eligible = 0

    for record in RECORDS:
        density[(record.chrom, (record.pos - 1) // 1_000_000)] += 1
        shapes[_classify_shape(record.ref, record.alt)] += 1
        ref_upper, alt_upper = record.ref.upper(), record.alt.upper()
        if (re.fullmatch(r"[ACGT]", ref_upper) and re.fullmatch(r"[ACGT]", alt_upper)
                and ref_upper != alt_upper):
            biallelic_snvs += 1
            if (ref_upper, alt_upper) in TRANSITIONS:
                transitions += 1
            else:
                transversions += 1
        status = ("PASS" if record.filter_value == "PASS"
                  else "NOT_APPLIED" if record.filter_value == "." else "FAILED")
        filters[(status, record.filter_value)] += 1

        for index, sample in enumerate(SAMPLES):
            genotypes[(sample, _classify_genotype(_genotype_of(record, index)))] += 1

        if record.alt != "." and "," not in record.alt:
            single_alt += 1
            an = ac = 0
            for index in range(len(SAMPLES)):
                alleles = _genotype_of(record, index).replace("|", "/").split("/")
                if any(a == "." for a in alleles):
                    continue
                if len(alleles) not in (1, 2) or any(a not in ("0", "1") for a in alleles):
                    continue
                an += len(alleles)
                ac += sum(int(a) for a in alleles)
            if an:
                ac_an[(an, ac)] += 1
                q06_eligible += 1

    summary = {
        "sampleCount": len(SAMPLES),
        "samples": list(SAMPLES),
        "totalRecords": len(RECORDS),
        "gtRecordCount": sum(1 for record in RECORDS if "GT" in record.format_keys),
        "singleAltRecordCount": single_alt,
        "q06EligibleSiteCount": q06_eligible,
        "headerLineCount": len(HEADER_LINES),
        "headerValueCount": sum(1 for _key, value in HEADER_LINES if value != ""),
        **_format_shape(),
        **_info_shape(),
        "fileFormat": FILE_FORMAT,
        "referenceGenome": REFERENCE_GENOME,
        "sourceSoftware": SOURCE_SOFTWARE,
        "fileDate": FILE_DATE,
        **_header_shape(),
        "q07_file_metadata": {
            "fileFormat": FILE_FORMAT,
            "referenceGenome": REFERENCE_GENOME,
            "sourceSoftware": SOURCE_SOFTWARE,
        },
        "q08_header_line_census": [
            {"headerKey": key, "lineCount": count}
            for key, count in sorted(Counter(key for key, _ in HEADER_LINES).items())
        ],
        "q01_record_density_1mb": [
            {"chrom": chrom, "windowIndex": window, "recordCount": count}
            for (chrom, window), count in sorted(density.items())
        ],
        "q02_variant_shape_counts": [
            {"variantClass": name, "recordCount": count} for name, count in sorted(shapes.items())
        ],
        "q03_titv": {
            "biallelicSnvCount": biallelic_snvs,
            "transitionCount": transitions,
            "transversionCount": transversions,
            "tiTvRatio": transitions / transversions if transversions else None,
        },
        "q04_filter_distribution": [
            {"filterStatus": status, "filterLexical": lexical, "recordCount": count}
            for (status, lexical), count in sorted(filters.items())
        ],
        "q05_sample_genotype_counts": [
            {"sampleId": sample, "genotypeClass": genotype_class, "callCount": count}
            for (sample, genotype_class), count in sorted(genotypes.items())
        ],
        "q06_ac_an_distribution": [
            {"an": an, "ac": ac, "siteCount": count, "af": ac / an}
            for (an, ac), count in sorted(ac_an.items())
        ],
    }
    # The census and digest expectations come from the shipped derivations
    # rather than hand-written copies, so the fixture cannot drift from them.
    runner = _runner()
    summary.update(runner.expected_census(
        summary, representation,
        info_representation="structured" if include_info else "raw",
        header_representation="structured" if include_headers else "basic",
    ))
    source_component = runner.rml_uri_component(SOURCE_FILE)
    digest: Counter[str] = Counter()
    for record in RECORDS:
        record_iri = (
            f"file://{source_component}#record/"
            f"{runner.rml_uri_component(record.row_id)}"
        )
        digest[runner.record_digest_bucket([
            record_iri, record.chrom, str(record.pos), record.record_id,
            record.ref, record.alt,
            record.qual if include_qual else "",
            record.filter_value, record.info,
        ])] += 1
    summary["q11_record_digest"] = [
        {"bucket": bucket, "recordCount": count} for bucket, count in sorted(digest.items())
    ]

    # Value-level digests, derived the same way the runner derives them.
    sample_components = [
        runner.rml_uri_component(uri)
        for uri in runner.sample_uri_ids(list(SAMPLES))
    ]
    info_digest: Counter[str] = Counter()
    format_digest: Counter[str] = Counter()
    for record in RECORDS:
        row_component = runner.rml_uri_component(record.row_id)
        call_iri = f"file://{source_component}#call/{row_component}"
        if include_info:
            for key, value in _info_entries(record):
                if value is None:
                    continue
                info_iri = f"{call_iri}/info/{runner.rml_uri_component(key)}"
                info_digest[runner.record_digest_bucket([info_iri, value])] += 1
        payload_fields = [p.split(":") if p else [] for p in record.sample_payloads]
        width = max([len(record.format_keys), *(len(f) for f in payload_fields)], default=0)
        keys = [
            record.format_keys[i] if i < len(record.format_keys) and record.format_keys[i]
            else f"FIELD_{i + 1}"
            for i in range(width)
        ]
        for key_index, key in enumerate(keys):
            key_component = runner.rml_uri_component(key)
            if representation == "expanded":
                for sample_index, fields in enumerate(payload_fields):
                    cell = fields[key_index] if key_index < len(fields) else ""
                    if not cell:
                        continue
                    value_iri = (
                        f"file://{source_component}#sample/{row_component}"
                        f"/{sample_components[sample_index]}/fmt/{key_component}"
                    )
                    format_digest[runner.record_digest_bucket([value_iri, cell])] += 1
            else:
                encoded = "\t".join(
                    (fields[key_index] if key_index < len(fields) and fields[key_index]
                     else ".")
                    for fields in payload_fields
                )
                vector_iri = f"{call_iri}/matrix/fmt/{key_component}"
                format_digest[runner.record_digest_bucket([vector_iri, encoded])] += 1
    summary["q12_info_value_digest"] = [
        {"bucket": b, "valueCount": c} for b, c in sorted(info_digest.items())
    ]
    summary["q13_format_value_digest"] = [
        {"bucket": b, "valueCount": c} for b, c in sorted(format_digest.items())
    ]
    if not include_qual:
        summary["q09_predicate_census"] = [
            row for row in summary["q09_predicate_census"]
            if not row["predicate"].endswith("#qual")
        ]
    return summary
