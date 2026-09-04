# VCF-RDFizer documentation

VCF-RDFizer converts VCF files into RDF, optionally into compressed and
queryable representations (HDT, COTTAS), and can prove that the result still
reproduces the source VCF's semantics.

The [top-level README](../README.md) is the task-oriented quick start: install,
flags, worked commands. **These documents are the explanation** — how each part
works, why it was built that way, and where it stops working.

Every document here states its own limits. If you only read one page before
deciding whether the tool fits your problem, read
[Limitations](limitations.md).

---

## Start here

| If you want to… | Read |
| --- | --- |
| Understand how the tool is put together | [Architecture](architecture.md) |
| Know exactly what happens to a VCF | [Conversion](conversion.md) |
| Decide what to convert *into* | [Representations](representations.md) |
| Trust the output | [Validation](validation.md) |
| Know what the tool cannot do | [Limitations](limitations.md) |

## The whole set

### How it works

- **[Architecture](architecture.md)** — the host/container split, what runs
  where, the three places the split deliberately leaks, the failure policy, and
  the pinned toolchain.
- **[Conversion](conversion.md)** — VCF → TSV → RDF stage by stage: the `awk`
  parser and its blind spots, the RML mapping, the wrapper's own emitters, the
  IRI templates, datatype and missing-value decisions.
- **[Representations](representations.md)** — the compression plan's three
  independent decisions, record-safe chunking, HDT via native `hdtc`, the
  bounded COTTAS merge, round-trip verification, and index maintenance.
- **[Output and metrics](output-and-metrics.md)** — the output layout, the
  `run_metrics/` tree, input size accounting, progress, interrupts, exit codes.

### How to extend it

- **[Custom RML mappings](rml-mappings.md)** — the `--rules` contract, the
  `vcf-rdfizer-rules` CLI, and an honest account of what a custom mapping does
  *not* control and what it costs in validation.
- **[Data linking design](datalinking-design.md)** — *proposal, not
  implemented.* A plug-in architecture for connecting the graph to external
  resources (rsIDs, genes, clinical assertions) with declarative linkers,
  reference bundles, live-API safeguards, and provenance.
- **[Privacy policy design](privacy-policy-design.md)** — *proposal, not
  implemented.* Granular, machine-readable disclosure control over parts of the
  graph: an ODRL profile with graph selectors, three enforcement tiers, and
  verification — plus a candid account of why access control is not
  anonymization when the genotypes are themselves identifiers.

### What the graph looks like

- **[Sample representations](sample-representation-guide.md)** — expanded versus
  condensed genotype shapes, worked examples, scaling arithmetic, the trade-off,
  and an assessment of query-time decoding options.
- **[VCF coverage matrix](vcf-coverage.md)** — element by element: is it
  represented, and would a corruption of it be detected. Includes the current
  mutation score and the vocabulary alignment gaps.

### Whether to trust it

- **[Validation](validation.md)** — how to run the semantic suite, which
  artifact and engine to choose, what the thirteen queries and the preflights
  check, and a candid "what is *not* tested".
- **[Validation methodology](validation-methodology.md)** — how coverage is
  *measured* rather than asserted: the mutation-testing harness, why known gaps
  are assertions rather than comments, and how to reproduce the score.

### Where it is going

- **[Limitations](limitations.md)** — everything the tool cannot do, does badly,
  or does surprisingly, in one place.
- **[Roadmap](roadmap.md)** — what is planned, what is known-broken, and what
  has been assessed and deliberately rejected.

---

## Reading paths

**"I have a VCF and I want RDF."**
[README](../README.md) → [Conversion](conversion.md) →
[Sample representations](sample-representation-guide.md) →
[Representations](representations.md)

**"I need to defend this output in a paper."**
[Validation](validation.md) → [Validation methodology](validation-methodology.md)
→ [VCF coverage matrix](vcf-coverage.md) → [Limitations](limitations.md)

**"I want to change what RDF comes out."**
[Architecture](architecture.md) → [Conversion](conversion.md) →
[Custom RML mappings](rml-mappings.md)

**"I want to add my own domain's links."**
[Data linking design](datalinking-design.md) →
[Custom RML mappings](rml-mappings.md) →
[Validation methodology](validation-methodology.md)

**"I need to release only part of this cohort."**
[Privacy policy design](privacy-policy-design.md) →
[Sample representations](sample-representation-guide.md) →
[Conversion §6](conversion.md#6-iri-templates) →
[Validation methodology](validation-methodology.md)

**"A cohort-scale run just failed."**
[Representations §10](representations.md#10-limitations) →
[Output and metrics §2](output-and-metrics.md#2-the-run-metrics-directory) →
[Limitations §1](limitations.md#1-operational)

---

## A note on how these documents are written

Three conventions, kept deliberately:

1. **Claims are measured, not asserted.** Where a document says a defect would
   be caught, there is a named mutation in
   [`test/validation_mutations.py`](../test/validation_mutations.py) that proves
   it — and where one would *not* be caught, that gap is also an assertion, so
   closing it fails a test rather than passing silently.
2. **Limitations sit next to capabilities**, not in a footnote. A section that
   describes what something does also describes where it stops.
3. **Design rationale is recorded**, including for options that were assessed
   and rejected, so the same ground is not re-covered later.

Related files outside `docs/`: [`rules/README.md`](../rules/README.md) (the
mapping directory), [`test/README.md`](../test/README.md) (the test suite),
[`changelog.md`](../changelog.md), [`scripts/RELEASING.md`](../scripts/RELEASING.md),
and [`ACKNOWLEDGEMENTS.md`](../ACKNOWLEDGEMENTS.md).
