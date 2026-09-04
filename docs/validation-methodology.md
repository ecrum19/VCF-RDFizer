# How validation coverage is measured

*Part of the [VCF-RDFizer documentation](README.md). How to run the validator:
[`validation.md`](validation.md). Current results:
[`vcf-coverage.md`](vcf-coverage.md).*

The semantic validation suite compares a converted RDF graph against summaries
computed independently from the source VCF. That answers "do these agree?" but
not "what would we have missed?" — and a validator nobody has tried to fool is
an assumption, not evidence.

This document describes the mutation-testing method used to answer the second
question, and how to reproduce the number.

## The problem with self-reported coverage

Before this harness existed, the suite's coverage was described in prose. That
description was wrong in a way nobody had noticed: `QUAL` is extracted from
every VCF into `records.tsv` and then never mapped into RDF, and no validation
check could have detected it, because none of them look at QUAL. A test suite
that passes on a graph missing an entire VCF column is not measuring what its
documentation claims.

## Method

Mutation testing treats the validator as the system under test. A *correct*
graph is deliberately corrupted in a specific, named way, and the validator is
asked for its verdict. If the verdict stops being `PASS`, the mutation is
**detected**. The proportion detected is the **mutation score**.

Three properties make the result trustworthy:

**The fixture cannot lie to itself.** The VCF, the RDF graph, and the parser
oracle are all derived from one declarative specification in
[`test/validation_fixtures.py`](../test/validation_fixtures.py). The graph's
genotype triples come from the project's own emitters rather than being written
by hand, so the fixture tracks the real implementation. A container test closes
the loop by running the real `parse_vcf` over the fixture VCF and asserting it
equals the derived oracle.

**The harness measures shipped code.** Mutations are evaluated through
`evaluate_validation()` in `validation_runner.py` — the single function that
also decides real runs. There is no second implementation of the decision logic
for the tests to agree with.

**Known gaps are assertions, not comments.** A mutation the suite cannot catch
carries a `known_undetected` reason, and the harness asserts it is *still* not
detected. Closing a gap therefore **fails a test**, forcing both the catalogue
and [`vcf-coverage.md`](vcf-coverage.md) to be updated. Coverage cannot silently
drift in either direction.

## The catalogue

[`test/validation_mutations.py`](../test/validation_mutations.py) holds one
entry per named corruption: an id, the VCF element it targets, the graph edit,
the check expected to catch it, and — where applicable — why it is not caught.

Mutations cover dropped and duplicated records, corrupted CHROM/POS/ALT values,
retyped literals, permuted values between records, dropped and corrupted FILTER
strings, flipped genotypes, dropped sample calls, non-GT FORMAT values, QUAL,
INFO, header lines, file metadata, missing-token policy, representation
profiles, and spurious predicates.

A mutation may target a triple the shipped mapping does not emit yet: the
fixture opts that shape in, so the harness answers "if we emitted this, would
we notice it breaking?" ahead of the fix. That is how the QUAL gap was measured
before QUAL was mapped.

## Three independent layers

The suite is not one check but three, deliberately independent so that a defect
has to evade all of them:

1. **Aggregate comparison** — six deterministic VCF summaries recomputed from
   the graph and compared exactly. Catches distributional error.
2. **Census and digest** — the graph's predicate and class inventory compared
   against what the VCF implies (`q09`, `q10`), and per-record and per-value
   identity digests (`q11`–`q13`). Catches missing, extraneous, permuted and
   altered data that leaves distributions intact.
3. **Graph integrity** — blank nodes, empty or whitespace-only terms, and
   duplicated statements. Catches malformed emission with no reference to the
   VCF's contents.
4. **SHACL** — the shapes the vocabulary publishes, checked with `pyshacl` via
   `--shacl-shapes`. Catches structural and datatype error, also independent of
   the VCF.

Duplicate detection is the one check that cannot be a query. A SPARQL store
holds a set, so a repeated line is collapsed on load and is invisible to every
other check here. It is found by comparing the statements the parser read
against the distinct triples the store holds; the difference is the number of
redundant statements. When either number is unavailable the check reports
`NOT_EVALUATED`, never `PASS` - a check that could not run must not look like a
clean result.

The SHACL layer earned its place immediately: it found three conformance
violations the other two could not see, two of which were fixed (QUAL typed as
a plain literal instead of `xsd:decimal`/`vcfr:Null`; `##fileDate` untyped
instead of `xsd:date`) and one of which is a contradiction inside the
vocabulary itself, recorded in [`vcf-coverage.md`](vcf-coverage.md).

SHACL is opt-in because `pyshacl` loads the graph into memory and does not
scale to a cohort-sized aggregate.

### Identity digests, and why they are histograms

`q11`–`q13` hash each record, INFO value and FORMAT value **together with its
own IRI**, then bucket on the first byte of the hash. Two properties matter:

- **Binding identity into the hash is the whole point.** Digesting values alone
  would give a permutation the same multiset and change nothing.
- **A histogram needs no ordering guarantee.** `GROUP_CONCAT` would have been
  the obvious alternative and is not portable, because SPARQL does not define
  its order. Bucketing is order-independent by construction, and keeps the
  result at most 256 rows for a graph of any size. A mismatch is localized by
  re-querying only the differing buckets.

Fields are separated by U+001F, which cannot occur in a VCF field, so no shift
of a field boundary can forge a match.

## Four engines, two layers

The host layer runs queries under **rdflib**, in process, needing no Docker, so
the whole mutation catalogue runs in the normal test loop. rdflib is also an
independent SPARQL implementation, which incidentally guards against queries
that only work on one engine.

The container layer is the authority:
[`test/cross_engine_agreement.py`](../test/cross_engine_agreement.py) runs every
validation query under **Comunica, QLever, native HDT and native COTTAS** inside
the image, across both representations, and asserts they return identical
values. It then runs the shipped validation decision under each engine
separately, because engines agreeing with each other while all being wrong is a
real failure mode that only the Python oracle rules out.

That second layer is not ceremony. QLever canonicalises numeric literals at
index time, reporting `"100"^^xsd:integer` as `xsd:int`. The POS datatype
preflight originally required exactly `xsd:integer`, so it flagged every record
under QLever while passing under Comunica - every QLever run would have ended
`BLOCKED_BY_PREFLIGHT`. Only cross-engine execution surfaces that class of bug.

It has since caught two more. Comunica's HDT engine treats a bare filesystem
path as a link to dereference, so the source must be addressed as
`hdt@<path>`; and cyvcf2's `raw_header` is htslib's *normalised* header, which
injects `##FILTER=<ID=PASS,Description="All filters passed">` into files that
never declared it - making the oracle expect a `FilterDefinition` the graph
could not contain. The oracle now reads the header block from the file itself,
which is the same text the conversion reads.

**When adding a query**, prefer datatype-*family* checks and lexical
comparisons over anything that assumes a store's internal representation, and
run the agreement script before trusting it.

### Measuring, not just comparing

Because every engine answers the same query set against the same graph in the
same container, a multi-engine run is also a controlled benchmark, and the
suite records it: per-query wall time for each engine, setup time (a QLever
index build, or an HDT/COTTAS artifact build) kept separate from query time,
and the **oracle** - what it costs to compute the same answers directly from
the VCF with cyvcf2.

That last figure makes the comparison meaningful rather than merely internal.
The suite computes every expected value twice, once by parsing and once by
querying, so the two costs are for identical work on identical input. The
report is written as `benchmark.json` plus a long-format `benchmark.csv`, one
row per engine and query. See [`validation.md`](validation.md#timings-and-comparing-sparql-against-the-parser).

## Reproducing the score

```bash
pip install rdflib

VCF_RDFIZER_MUTATION_REPORT=mutation-score.json \
  python -m unittest test.test_validation_mutation_unit -v
```

`mutation-score.json` records the total, the detected count, the score, and a
per-mutation row with its status and any recorded gap reason.

Cross-engine agreement, inside the image:

```bash
docker build -t vcf-rdfizer:local .
docker run --rm -v "$PWD:/repo:ro" vcf-rdfizer:local \
  /opt/pycottas-venv/bin/python /repo/test/cross_engine_agreement.py
```

All four engines are compared by default; pass a comma-separated subset as the
first argument to narrow it.

Both run in CI via
[`.github/workflows/validation-mutation.yml`](../.github/workflows/validation-mutation.yml).

## Interpreting the number

The mutation score is relative to the catalogue, not to the space of all
possible conversion bugs. A high score means "almost every corruption we
thought to write down is caught" — it is a lower bound on blindness, not a
proof of correctness. Growing the catalogue is as valuable as raising the
score, and a score that rises without the catalogue growing means nothing. Its value is that it is falsifiable, reproducible, and
moves in a direction you can point at.

Current score and the full gap list: [`vcf-coverage.md`](vcf-coverage.md).

## What a validation PASS means

A `PASS` now means more than it did: with the census and the identity digests,
the graph must contain exactly the predicates and classes the VCF implies, and
every record, INFO value and FORMAT value must hash to the same bucket as its
counterpart in the source. It reliably catches dropped
records, misclassified variants, flipped genotypes, corrupted FILTER strings,
allele-count errors, missing header lines, and altered file metadata.

It is **not** proof of a faithful record-by-record round-trip. Treat it as a
regression gate. The precise limits are enumerated in
[`vcf-coverage.md`](vcf-coverage.md) and in the "What is *not* tested" section
of [`validation.md`](validation.md).


---

## See also

- [Validation](validation.md) — running the suite, and what it does and does not test
- [VCF coverage matrix](vcf-coverage.md) — the current score and the full gap list
- [Data linking design](datalinking-design.md) — how third-party linksets would plug into this harness
- [Roadmap](roadmap.md) — the coverage gaps with named fixes
