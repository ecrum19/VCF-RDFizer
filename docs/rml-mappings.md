# Custom RML mappings

`--rules` accepts any RML mapping, so you can change what RDF the pipeline
produces without touching the wrapper. This document is the full contract, the
tooling that checks it, and an honest account of what a custom mapping costs you.

---

## 1. The tooling first

`vcf-rdfizer-rules` is installed alongside `vcf-rdfizer` and exists so that the
contract is discoverable and checkable, rather than something you find out three
hours into a run.

```bash
vcf-rdfizer-rules columns          # what a mapping can reference
vcf-rdfizer-rules init -o my.ttl   # annotated copy of the shipped default
vcf-rdfizer-rules check my.ttl     # validate before spending a run on it
```

`check` reports:

- logical-source paths the wrapper cannot rewrite per input;
- referenced columns no generated TSV provides (typos such as `CHROMOSOME`);
- which `--sample-representation` values remain usable;
- whether the mapping forces the large sample helper tables to be materialized.

Exit code is `0` when the mapping is usable and `1` when it is not; `--json`
gives the same report machine-readably.

The checks are deliberately **lexical**, not a full Turtle parse. The tool stays
dependency-free, and the mistakes worth catching before a long run are wrong
paths and misspelled column names — not subtle RML semantics. A mapping that
passes `check` can still be wrong; it just will not be wrong in the two ways
that waste the most time.

## 2. The contract

### Rule 1 — keep the five `csvw:url` values verbatim

Full mode processes one VCF at a time and rewrites those exact literal strings
to the per-input file names. Any other path is left untouched and will not
resolve.

| Logical source | Contents |
| --- | --- |
| `/data/tsv/records.tsv` | One row per VCF data line |
| `/data/tsv/header_lines.tsv` | One row per `##` header line |
| `/data/tsv/file_metadata.tsv` | One row summarising the source VCF |
| `/data/tsv/sample_calls.tsv` | Helper: one row per variant × sample |
| `/data/tsv/sample_format_values.tsv` | Helper: one row per variant × sample × FORMAT key |

### Rule 2 — only reference columns the pipeline writes

`vcf-rdfizer-rules columns` is authoritative. A unit test runs
[`src/vcf_as_tsv.sh`](../src/vcf_as_tsv.sh) and asserts those lists still match
what it emits, so the documentation cannot drift from the implementation.

The last column of `records.tsv` is the whitespace-joined sample IDs from the
`#CHROM` line (or `SAMPLES` when the VCF declares none). Its *name* varies per
input, so it **cannot be referenced by a fixed name** — which is one of the
reasons genotype RDF is emitted by the wrapper instead.

### Rule 3 — think before consuming the helper tables

The four built-in sample maps are recognised by the wrapper, which then keeps
`sample_calls.tsv` and `sample_format_values.tsv` header-only and streams
genotype RDF itself. A mapping that consumes them in any other way forces both
to be materialized in full — one row per variant × sample, and one per
variant × sample × FORMAT key. On a cohort VCF that is the largest intermediate
the pipeline can produce.

Such a mapping is **rejected** in `--sample-representation condensed`, which
would otherwise emit the expanded and condensed genotype graphs simultaneously
and restore exactly the inflation condensed mode exists to avoid. Remove the
helper-table consumers, or use expanded mode. A custom mapping with no
helper-table consumers works in both modes.

## 3. What a custom mapping does *not* control

This is the part most easily missed. Three families of triple are emitted by the
wrapper, not by RML, and they appear in your graph regardless of your mapping:

| Triples | Turned off with |
| --- | --- |
| Genotypes (`SampleCall`/`FormatFieldValue`, or the condensed equivalents) | nothing — one emitter always runs; choose which with `--sample-representation` |
| `QUAL`, and structured `InfoFieldValue` nodes | `--info-representation raw` still emits QUAL |
| Header-line subclasses and lifted FILTER/ALT/contig/INFO/FORMAT attributes | `--header-representation basic` |

The reasons are in [`architecture.md`](architecture.md#4-where-the-split-leaks-and-why):
RML cannot choose a datatype or a class per row, and the alternatives would
require materializing helper tables. It is a real constraint on the extension
point, not an oversight, but a mapping author who assumes `--rules` is the sole
source of truth for the graph will be surprised.

## 4. What a custom mapping costs in validation

The semantic validation suite has three layers. A custom mapping affects one of
them.

Queries `q09`–`q13` — the predicate census, the class census, and the three
identity digests — assume the shipped mapping's predicate inventory and IRI
templates. A custom mapping changes both **by design**.

`validation_runner.py` has a `--mapping-policy report-only` setting for exactly
this case: it records those five queries without failing on them, leaving the
run to rest on the aggregate comparisons (`q01`–`q08`).

> **Known gap.** The `vcf-rdfizer` wrapper does **not** currently forward
> `--mapping-policy`, so the runner always executes with the default `strict`.
> A custom mapping validated through `vcf-rdfizer --validate` or
> `--mode validation` will therefore report `MISMATCH` on `q09`–`q13` even when
> the conversion is correct. Until the flag is exposed, either read past those
> five rows in `comparison.json`, or invoke the runner directly inside the
> container with `--mapping-policy report-only`. Tracked in
> [`roadmap.md`](roadmap.md).

Once that gap is closed, the trade-off is the intended one: with a custom
mapping you keep distributional checking and lose the completeness and
permutation checks. That is correct behaviour — the alternative is a false
failure on every custom run — but it means a custom mapping is validated less
thoroughly than the default one, and you should know that before treating a
`PASS` as equivalent.

Graph-integrity preflights (blank nodes, empty terms, duplicate statements) and
SHACL are mapping-independent and still apply in full.

## 5. Worked start

```bash
vcf-rdfizer-rules init -o my_rules.ttl
$EDITOR my_rules.ttl
vcf-rdfizer-rules check my_rules.ttl

vcf-rdfizer --mode full \
  -i ./cohort.vcf.gz \
  --rules my_rules.ttl \
  --rdf-storage-mode plain \
  -o ./results
```

Recommendations that come from experience rather than from the checker:

1. **Preserve the stable subject templates** (`file://{SOURCE_FILE}`,
   `file://{SOURCE_FILE}#record/{ROW_ID}`, `file://{SOURCE_FILE}#call/{ROW_ID}`)
   if you want joins with the wrapper-emitted triples to keep working. The
   genotype, QUAL, INFO and header emitters all mint IRIs against those
   templates and will not follow a mapping that changes them.
2. **Add, don't replace.** A mapping that extends the default with additional
   `rr:TriplesMap` blocks keeps the full validation suite meaningful for the
   parts it did not change; one that rebuilds the graph from scratch does not.
3. **Test on a small VCF first.** `test/test_vcf_files/test-100.vcf` converts in
   seconds and exercises the same code path as a cohort file.

## 6. SHACL

The SHACL constraints are maintained in the vocabulary repository, not here:
[`vcf-rdfizer-vocabulary.shacl.ttl`](https://github.com/ecrum19/VCF-RDFizer-vocabulary/blob/main/shacl/vcf-rdfizer-vocabulary.shacl.ttl).
Point `--shacl-shapes` at them to check a graph structurally, independently of
the VCF. Note the known contradiction inside the published shapes, recorded in
[`vcf-coverage.md`](vcf-coverage.md#an-open-conflict-inside-the-vocabulary).

---

## See also

- [Conversion](conversion.md) — what the default mapping produces
- [Architecture](architecture.md) — why some triples bypass RML
- [Validation](validation.md) — what changes under a custom mapping
- [`rules/README.md`](../rules/README.md) — the mapping directory itself
