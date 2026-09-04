# Compression and queryable representations

What VCF-RDFizer can produce from an RDF aggregate, how each artifact is built,
what is verified about it, and where each one runs out of road.

---

## 1. Three independent decisions

Compression is deliberately not one option. It is three:

| Decision | Flag | Values | Default |
| --- | --- | --- | --- |
| How the aggregate is staged | `--rdf-storage-mode` | `plain`, `space-optimized` | *(required in full mode)* |
| Which raw RDF artifacts to keep | `--rdf-compression` | `gzip`, `brotli`, `none` | `gzip,brotli` |
| Which queryable representations to build | `--representations` | `hdt`, `cottas`, `none` | `hdt` |
| How to package those representations | `--artifact-compression` | `gzip`, `brotli`, `none` | `none` |

Each selector takes a comma-separated list. `none` must appear alone.
`--artifact-compression` requires at least one selected representation.

The gzip used by `--rdf-storage-mode space-optimized` is **staging**, not
automatically a final artifact. It is retained as the gzip artifact only when
`gzip` is also selected in `--rdf-compression`.

For the smallest output: `--rdf-compression none`, one representation, and
`--remove-rdf-storage-output`.

## 2. The artifact matrix

| Artifact | Queryable | Built by | Verified by |
| --- | --- | --- | --- |
| `.nt` | with any RDF store | the merge step | Raptor syntax check during validation |
| `.nt.gz`, `.nt.br` | after decompression | `gzip` / `brotli` | — |
| `.hdt` + `.hdt.index.v1-1` | yes, directly | `hdtc create` (+ `hdtc index`) | streaming decode + triple-count equality |
| `.cottas` | yes, directly | `pycottas` + PyArrow merge | streaming decode + triple-count equality |
| `.hdt.gz`, `.hdt.br`, `.cottas.gz`, `.cottas.br` | **no** | packaging | — |

The packaged forms are archives. Keep the unwrapped `.hdt` / `.cottas` if
queries must run without a decompression step.

## 3. Record-safe chunking

When HDT or COTTAS is selected, the aggregate is read sequentially and split
into chunks on **complete N-Triples line boundaries**. Only one uncompressed
chunk exists at a time: it is consumed by both converters and removed before the
next is read. That property is what makes a `space-optimized` `.nt.gz` aggregate
usable without ever expanding a second full raw copy.

| Flag | Meaning | Default |
| --- | --- | --- |
| `--chunk-target-bytes` | target uncompressed bytes per chunk | 512 MiB |
| `--chunk-min-bytes` | minimum before a chunk group is flushed | 128 MiB |
| `--chunk-max-bytes` | hard ceiling; boundaries stay on complete lines | 1 GiB |

`--hdt-strategy` chooses the policy: `auto` (build chunks and merge with native
`hdtc`), `partitioned` (always chunk), `single` (one `rdf2hdt` run). `single`
cannot consume a gzip stream without expanding it, so it is incompatible with
`space-optimized`.

The whole partitioned stage runs in an **ephemeral Docker-managed volume**.
Chunks, scratch, and merge files never reach the output directory, and the
volume is removed on success and on failure alike.

## 4. HDT

HDT generation, merging, and indexing use the pinned Rust **`hdtc` 1.1.0**, not
`hdtCat`, `hdtSearch.sh`, or any Java HDT process. This is not a stylistic
preference: the JVM path fails with `java.lang.OutOfMemoryError` on
cohort-scale graphs, while `hdtc create` merges chunk HDTs through disk-backed
external sorts and `hdtc index` streams BitmapTriples to build the
object/predicate orderings. The sidecar it produces is the canonical HDT v1-1
file, `<file>.hdt.index.v1-1`, readable by hdt-java and hdt-cpp.

Memory is bounded by two environment variables, both defaulting to `512M` in
the image and forwarded by the wrapper when set on the host:

```bash
HDT_MERGE_MEMORY_LIMIT=2G HDT_INDEX_MEMORY_LIMIT=2G vcf-rdfizer ...
```

Lower values reduce in-memory sort buffers and increase temporary I/O. Both
stages need scratch space in the container's `/work`, which lives on the
**Docker data volume** — free space in the output filesystem does not help.

An HDT whose data is readable but whose sidecar could not be built is a
*degraded success* in full mode: it is validated with the index check skipped,
marked `index_status: "failed"`, reported in `reports/index_warnings.json`, and
the raw RDF is retained so it can be repaired later with `--mode index`.

## 5. COTTAS

COTTAS is a Parquet-based representation with its index **inside** the artifact;
there is no sidecar. Chunk conversion uses
`pycottas.rdf2cottas(..., disk=True)` with a fresh container-local DuckDB
workspace per operation.

The final merge deliberately does **not** call `pycottas.cat`. In version 1.1.0
that runs a global `DISTINCT` plus `ORDER BY` through an unbounded in-memory
DuckDB connection, which gets OOM-killed on large condensed graphs. VCF-RDFizer
instead performs a **k-way PyArrow merge** of the already `spo`-sorted Parquet
chunks: it holds at most `COTTAS_MERGE_BATCH_ROWS` (default 2048) rows per
input, drops adjacent duplicate triples, and writes the result incrementally.
There is no graph-wide hash table and no external-sort spill directory; memory
is a function of the batch size and the chunk count, not of the graph size.

A COTTAS failure degrades differently from HDT: because the `.cottas` file
itself is unusable, dependent COTTAS artifacts are marked *not generated* rather
than published with a warning.

## 6. Round-trip verification

Every HDT and COTTAS base artifact is verified **before** packaging and before
raw RDF cleanup: [`src/validate_compression.py`](../src/validate_compression.py)
reads the source triple count, streams the artifact back through its native
decoder, and requires equality. Compression **fails closed** if the artifact
cannot be decoded or the counts differ. Results and the
`source_triples`/`decoded_triples` pair land in the per-run compression JSON and
in the HDT/COTTAS columns of `metrics.csv`.

Be clear about what this proves and what it does not. A matching triple count
proves the artifact decodes and contains the right *number* of statements. It
does not prove the statements are the right ones. The stronger claim comes from
running the semantic suite against the decoded artifact — `--validate-artifacts
hdt,cottas` — which re-derives every VCF summary from it. See
[`validation.md`](validation.md#which-artifact-is-validated).

## 7. Index maintenance

`--mode index` regenerates an existing artifact's index in place. It is the one
deliberate exception to "never overwrite a planned artifact".

| Input | Behaviour |
| --- | --- |
| `--hdt file.hdt` | Existing versioned sidecars are moved aside, regenerated, and restored if indexing fails; incomplete replacements are removed first |
| `--cottas file.cottas` | The artifact is rewritten atomically through the same bounded streaming Parquet rewrite with the default `spo` index; the original stays in place if it fails |

No conversion, packaging, or decompression output is produced. Standalone index
mode is **strict** — unlike the in-run degradation above, a failure is a failure.
The same operation runs automatically after each partitioned HDT merge.

## 8. Decompression

`--mode decompress` decodes `.nt.gz`, `.nt.br`, `.hdt`, `.cottas`, `.cottas.gz`
and `.cottas.br` back to N-Triples. A packaged COTTAS is unwrapped **inside the
container** before `pycottas` writes the decoded output, so the intermediate
unwrapped file never appears on the host.

## 9. Choosing

| Situation | Selection |
| --- | --- |
| Load into an existing triple store | `--representations none --rdf-compression gzip` |
| Queryable, single artifact, smallest footprint | `--representations hdt --rdf-compression none --remove-rdf-storage-output` |
| Comparing HDT against COTTAS | `--representations hdt,cottas` |
| Archival transfer | `--artifact-compression brotli` on top of the chosen representation |
| Memory-constrained host | `space-optimized` + `partitioned` + lower `--chunk-*-bytes` |

## 10. Limitations

- **Docker volume space, not output space, is the binding constraint** for
  partitioned merges and HDT indexing. This is the single most common cause of
  a failed large run, and the error surfaces as an OOM/SIGKILL rather than as a
  disk message.
- **`exit_code=-9` (or `137`) means the kernel or Docker OOM-killer intervened**,
  not that the RDF is invalid. Check `stderr_tail`, `max_rss_kb`, and the
  workspace free-space samples in `stages/partitioned/<sample>.json` before
  suspecting the data.
- **COTTAS is the more fragile path.** If it cannot fit the available memory
  even at a reduced `COTTAS_MERGE_BATCH_ROWS`, `--representations hdt` is
  independent and remains queryable.
- **Packaged representations are not queryable**, which is easy to forget when
  `--artifact-compression` is set and `--remove-rdf-storage-output` has removed
  the alternative.
- **Neither HDT nor COTTAS carries named graphs**, matching the conversion's
  triples-only output.
- **No incremental update.** Adding variants means reconverting and rebuilding
  the representation from scratch.

---

## See also

- [Architecture](architecture.md) — where these stages run
- [Output and metrics](output-and-metrics.md) — what each stage records
- [Validation](validation.md) — proving a representation still means what the VCF meant
- [CLI reference](cli-reference.md) — every flag, with constraints
