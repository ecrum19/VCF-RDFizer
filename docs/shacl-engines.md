# SHACL engines: what was measured, and porting to Rudof

The shape layer uses pyshacl for every node-local shape, validated a batch of
records at a time, and QLever for the `full` profile's `sh:sparql` constraints,
each run once over the whole graph ([validation.md](validation.md#sparql-constraints-run-once-over-the-whole-graph)).
pyshacl stays because it is the engine every published result was produced
with. This page records the evaluation behind that split, and what replacing
pyshacl with a native engine would take. That replacement is future work
([roadmap item 8](roadmap.md#8-a-native-shacl-engine-rudof)).

## Why the full profile did not scale

The full profile was gated at 10M triples because pyshacl holds the graph in
memory. Measuring it showed a second, larger problem, independent of the
engine: four of its 40 SPARQL constraints grow faster than the graph.

| Constraint | As published | Cost |
| --- | --- | --- |
| Records of one CHROM are contiguous | compares every triple of records | O(n³) in records |
| POS non-decreasing within a CHROM | compares every pair of records | O(n²) |
| Non-missing record IDs are unique | pairs records on the ID before dropping `.`, which most IDs are | O(n²) |
| Field cardinality (INFO and FORMAT) | sample OPTIONALs outside the FORMAT branch, so an INFO value matches every sample | values × samples |

On 100 and 300 HG005 records, Rudof took 5.1 s and 122 s for the full profile,
close to the cube of the record count. On 3,000 records (0.52M triples) Rudof
did not finish it in four hours, and pyshacl had not after 2.5 hours. The validator now replaces the four
with linear equivalents, which agree with the published queries on every
fault-injection graph and on 300 random record arrangements
(`test/test_shacl_sparql_unit.py`).

## Rudof

[Rudof](https://github.com/rudof-project/rudof) is a Rust SHACL and ShEx engine.
Version 0.3.39 was evaluated in November 2026 on one benchmark host (8-core Xeon
Silver 4309Y, 31 GB RAM), inside the `vcf-rdfizer:3.3.2` image because its
binary needs glibc 2.39.

### Parity with pyshacl

On the 115 fault-injection graphs (113 mutations and two controls), with the
core and full profiles, Rudof gives pyshacl's verdict once three things are
supplied:

1. **RDFS typing.** Rudof does no inference. pyshacl validates with the
   vocabulary and RDFS inference, so without it Rudof misses every `sh:class`
   that relies on a superclass. The typing the validator already writes for
   QLever (`shacl_sparql.write_rdfs_typing`) plus the vocabulary's triples is
   enough. A full RDFS closure is not: it over-generates, with 358 results across the graphs.
2. **Declared prefixes.** The published SPARQL constraints carry no
   `sh:prefixes`. pyshacl supplies the shapes file's prefixes, and Rudof stops
   with "Prefix not found". `shacl_sparql.with_prefixes` does the same job.
3. **A workaround for its arithmetic** (below).

With those, the core profile agrees on every graph. The full profile gives a
different verdict on two graphs, the two blank-node mutations (issue 2 below).
Rudof ran all 690 of its runs in about 2 minutes, against about 25 minutes for
pyshacl's 230.

### Scale

| HG005 slice | Triples | Profile | Rudof (in memory) | pyshacl |
| --- | ---: | --- | ---: | ---: |
| 3,000 records | 0.52M | core | 6 s, 1.12 GB | 144 s, 1.27 GB |
| 3,000 records | 0.52M | full, published | over 4 h | over 2.5 h |
| 3,000 records | 0.52M | full, linear equivalents | 611 s, 1.42 GB | |

Rudof is about 24 times faster than pyshacl on the core profile, with the same
memory per triple, so it would still need the record batches. On the full
profile it evaluates each SPARQL constraint once per focus node, like pyshacl,
and three field-value constraints each took over 120 s on 3,000 records. Its
endpoint mode does the same over HTTP: 1,671 queries for the core profile on
300 records. Whole-graph queries on QLever ran all 40 in 4 s on the 0.52M-triple slice.

### Issues found in Rudof 0.3.39

To report upstream:

1. **`a - b + c` is parsed as `a - (b + c)`.** SPARQL arithmetic is
   left-associative. 17 item-count expressions in the full profile have this
   form (`STRLEN(x) - STRLEN(REPLACE(x, ",", "")) + 1`), so every one returns
   the wrong count. Rewriting each as `a - (b - 1)` works around it.
2. **A blank-node `$this` is pre-bound with `VALUES`.** `VALUES` cannot hold a
   blank node, so the query fails to parse ("expected UNDEF") and Rudof exits
   1. VCF-RDFizer never writes blank nodes, but two mutations do.
3. **`xsd:int` and `xsd:integer` are not compared.** `sh:minInclusive` and
   `sh:maxInclusive` treat them as incomparable, though both are numeric.
   QLever returns integers as `xsd:int`, so against a QLever endpoint every
   range check fails: 4,578 false violations on 300 records.

Two other differences are not Rudof bugs. On the `retype_pos_as_string`
mutation, Rudof follows SPARQL and treats a string compared with a number as an
error, where pyshacl's rdflib compares them leniently. On `corrupt_qual`, Rudof
reports extra datatype and pattern results on the literal `"0"`, probably through
the `sh:or` of `NullLiteralShape`. The cause is not confirmed.

### What porting would take

1. Run Rudof on each record batch in place of pyshacl, with the typing and the
   vocabulary's triples added to the batch.
2. Keep the SPARQL constraints on QLever. Rudof's per-focus-node evaluation
   gains nothing there.
3. Read Rudof's Turtle report into the same result list, and check it against
   pyshacl on every fault-injection graph, as the QLever layer is checked.
4. Ship a Rudof binary in the image, built for its glibc.
5. Report the three issues above, and drop the arithmetic workaround once
   fixed.

## Issues found in the shapes

For the vocabulary ([vcf-rdfizer-vocabulary](https://github.com/ecrum19/vcf-rdfizer-vocabulary)):

1. **No `sh:prefixes`.** SHACL-SPARQL requires a constraint to declare its
   prefixes. The published ones rely on the engine filling them in.
2. **The four constraints above.** Publishing the linear equivalents would let
   the validator drop its pinned replacements (`SCALABLE_EQUIVALENTS` in
   `src/validation/shacl_sparql.py`).
3. **The validation fixture fails the full profile.** Its records go chr20,
   chr21, chr20, so both ordering constraints fire on the clean controls, and
   the expanded control also gets a `/` phase indicator on a haploid genotype.
   A mutation that only adds a result already present in the control is not a
   detection, so the full profile's detection count must be measured against
   the controls. Either the fixture or the shapes need to change.
