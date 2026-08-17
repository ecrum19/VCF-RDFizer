# Partitioned HDT Construction and HDTCat Merging in VCF-RDFizer

## Abstract

VCF-RDFizer supports a partitioned construction strategy for large HDT
outputs. Instead of converting one very large RDF N-Triples file in a
single `rdf2hdt` invocation, the RDF output is divided into bounded chunks.
Each chunk is converted independently to HDT, the intermediate HDT files are
merged with HDTCat, and a query index is generated for the final HDT. This
reduces the working-set size of individual conversion processes while
preserving a single HDT representation of the complete RDF dataset.

## Method

### RDF chunk planning

The partitioned strategy operates on line-oriented N-Triples files produced by
the batch RDF layout. Existing RDF part files are used as the first partition
boundaries. Files larger than the configured maximum are re-split at complete
line boundaries, preserving triple integrity. Smaller files are coalesced into
chunk inputs until the target size is approached.

Three parameters control this process:

| Parameter | Default | Purpose |
| --- | ---: | --- |
| `--hdt-target-chunk-bytes` | 512 MiB | Target uncompressed RDF size of a chunk |
| `--hdt-min-chunk-bytes` | 128 MiB | Minimum accumulated size before a group is flushed |
| `--hdt-max-chunk-bytes` | 1 GiB | Maximum size of an individual chunk before line-preserving splitting |

The planner is implemented by
[`plan_partitioned_hdt_chunks`](./vcf_rdfizer.py#L665), with oversized-file
splitting handled by [`split_nt_file_for_hdt`](./vcf_rdfizer.py#L619). The
resulting files are temporary inputs named `chunk-XXXXX.nt`.

### Independent HDT conversion

Each planned RDF chunk is converted independently:

```text
rdf2hdt chunk-00000.nt chunk-00000.hdt
rdf2hdt chunk-00001.nt chunk-00001.hdt
...
```

The conversion work is performed inside the VCF-RDFizer Docker image. The
image provides `rdf2hdt` from HDT-C++ and resolves the executable through the
`RDF2HDT_BIN` environment variable or known installation paths. Independent
conversion limits the memory and processing cost of each `rdf2hdt` process.

### Balanced HDTCat merge

The intermediate HDT files are merged pairwise in a balanced binary tree. For
four chunks, the merge sequence is equivalent to:

```text
hdtCat chunk-00000.hdt chunk-00001.hdt merge-r01-00000.hdt
hdtCat chunk-00002.hdt chunk-00003.hdt merge-r01-00001.hdt
hdtCat merge-r01-00000.hdt merge-r01-00001.hdt merge-r02-00000.hdt
```

For `n` chunk files, the procedure performs `n - 1` merge operations over at
most `ceil(log2(n))` rounds. If a round contains an odd number of files, the
unpaired file is carried forward unchanged. This merge strategy avoids
constructing the final HDT through one large conversion operation while
retaining one HDT artifact for the complete input dataset.

The conversion and merge orchestration is implemented by
[`run_partitioned_hdt_methods_for_rdf_files`](./vcf_rdfizer.py#L2556). The
merge tree is currently executed sequentially; the balanced structure controls
intermediate merge sizes and depth but does not yet provide parallel execution.

### Final index generation

Intermediate HDT indexes are not combined. After the final HDT has been
created, VCF-RDFizer runs `hdtGenerateIndex` once on that final file:

```text
hdtGenerateIndex sample.hdt
```

The resulting artifacts are:

```text
sample.hdt
sample.hdt.index
```

Generating the index from the final HDT ensures that the index corresponds to
the final HDT dictionary and layout. The index-generation step is implemented
at [the final-index stage](./vcf_rdfizer.py#L2767).

If `hdt_gzip` or `hdt_brotli` is selected, the final HDT is compressed only
after merging and indexing:

```text
gzip -c sample.hdt > sample.hdt.gz
brotli -q 7 -c sample.hdt > sample.hdt.br
```

The uncompressed `.hdt` and `.hdt.index` remain the query-oriented artifacts.

## Pipeline integration

The strategy is selected with `--hdt-strategy`:

| Strategy | Behavior |
| --- | --- |
| `auto` | Uses partitioned HDT for full-mode batch RDF output; preserves the single-file path otherwise |
| `partitioned` | Explicitly enables chunked HDT generation where the partitioned pipeline is dispatched |
| `single` | Converts each RDF input with one `rdf2hdt` invocation |

In full batch mode, non-HDT methods such as gzip and Brotli remain associated
with their individual RDF part files. HDT-based methods are removed from that
per-file loop and are instead applied once to all RDF parts belonging to the
sample. This produces one final sample-level HDT artifact. The dispatch is
implemented in [full-mode compression orchestration](./vcf_rdfizer.py#L3166).

In compression-only mode, the same partitioned helper can process one `.nt`
input by splitting it into chunks before conversion. In full aggregate mode,
the current dispatch remains single-file because the partitioned full-mode
path is gated on `--rdf-layout batch`.

## Metrics and failure handling

Every chunk conversion, HDTCat merge, index-generation operation, and optional
post-merge compression is executed as a separate timed Docker stage. The
wrapper records wall time using its outer process timer and obtains CPU time
and maximum resident memory from `/usr/bin/time -v`. For the aggregated HDT
metric:

- wall times are summed across all sequential HDT build, merge, and index stages;
- user and system CPU times are summed across those stages; and
- maximum resident memory is the maximum observed across stages.

The raw metrics include a `__partitioned_hdt__` artifact containing the source
file count, prepared input count, chunk count, total chunk bytes, merge-round
count, and final index size. The implementation writes this artifact at
[`write_raw_compression_metrics_artifact`](./vcf_rdfizer.py#L2817).

Raw RDF files are removed only after all selected compression methods, including
the final partitioned HDT pipeline, have completed successfully. If a chunk
conversion, merge, or index-generation stage fails, the run reports the
failure and the raw RDF cleanup is refused.

## Reproducibility

The following command enables the strategy explicitly for full batch RDF
processing:

```bash
vcf-rdfizer \
  --mode full \
  --input ./vcf_files \
  --rdf-layout batch \
  --compression hdt \
  --hdt-strategy partitioned \
  --hdt-target-chunk-bytes 536870912 \
  --hdt-min-chunk-bytes 134217728 \
  --hdt-max-chunk-bytes 1073741824 \
  --out ./results
```

The required HDTCat and index-generation executables are resolved from the
HDT Java package installed in the Docker image, or from the corresponding
environment variables and known executable paths. The candidate resolution is
defined in [`vcf_rdfizer.py`](./vcf_rdfizer.py#L137), and the Docker image
installs the HDT Java distribution in
[`Dockerfile`](./Dockerfile#L63).

## Limitations

The current implementation has four relevant limitations. It assumes
line-oriented N-Triples input for safe splitting; chunk conversion and HDTCat
merging are sequential; intermediate HDTs and temporary RDF chunks require
additional disk space; and the final query index is regenerated rather than
incrementally merged. These constraints make the approach robust and simple
to reproduce, while leaving parallel stage execution and more advanced
storage management as future optimizations.

## Software references

- [HDT Java and HDTCat](https://github.com/rdfhdt/hdt-java/tree/master/hdt-java-cli)
- [HDT-C++](https://github.com/rdfhdt/hdt-cpp)
