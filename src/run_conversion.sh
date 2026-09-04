#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# TSV -> RDF conversion runner (RMLStreamer)
# ------------------------------------------------------------------------------
# Responsibilities:
# 1) run RMLStreamer with stable output naming
# 2) normalize Spark part outputs to .nt
# 3) aggregate part files into a single <sample>.nt or <sample>.nt.gz
# 4) collect conversion timing + output metrics
# 5) upsert conversion row in run_metrics/metrics.csv
# ------------------------------------------------------------------------------

# ---------- Config ----------
JAR=${JAR:-RMLStreamer-v2.5.0-standalone.jar}
IN=${IN:-rules/default_rules.ttl}
IN_VCF=${IN_VCF:-input.vcf}
OUT_NAME=${OUT_NAME:-rdf}
OUT_DIR=${OUT_DIR:-run_output}
OUT="$OUT_DIR/$OUT_NAME"
# Full-mode storage policy. `plain` keeps a normal aggregate N-Triples file;
# `space-optimized` appends gzip members and removes each source part after it
# has been compressed. Plain storage is the default for direct script use.
RDF_STORAGE_MODE=${RDF_STORAGE_MODE:-plain}
if [[ "$RDF_STORAGE_MODE" != "plain" && "$RDF_STORAGE_MODE" != "space-optimized" ]]; then
  echo "ERROR: invalid RDF_STORAGE_MODE='$RDF_STORAGE_MODE' (expected plain or space-optimized)." >&2
  exit 2
fi
LOGDIR=${LOGDIR:-run_metrics}
mkdir -p "$LOGDIR" "$OUT_DIR" "$OUT"

RUN_ID=${RUN_ID:-$(date +%Y%m%dT%H%M%S)}
TIMESTAMP=${TIMESTAMP:-$(date +"%Y-%m-%dT%H:%M:%S")}
SAFE_OUT_NAME=$(printf "%s" "$OUT_NAME" | tr -cs 'A-Za-z0-9._-' '_')
if [[ -z "$SAFE_OUT_NAME" ]]; then
  SAFE_OUT_NAME="rdf"
fi
MERGED_NT="$OUT/$OUT_NAME.nt"
if [[ "$RDF_STORAGE_MODE" == "space-optimized" ]]; then
  FINAL_RDF="${MERGED_NT}.gz"
else
  FINAL_RDF="$MERGED_NT"
fi
if [[ -e "$FINAL_RDF" ]]; then
  echo "ERROR: refusing to overwrite existing RDF output '$FINAL_RDF'." >&2
  exit 2
fi
# Keep RMLStreamer parts isolated from final artifacts. This lets the wrapper
# preserve unrelated files in an existing sample directory.
PARTS_DIR="$OUT/.rmlstreamer-parts-${SAFE_OUT_NAME}-$$"
if [[ -e "$PARTS_DIR" ]]; then
  echo "ERROR: temporary RMLStreamer directory already exists: '$PARTS_DIR'." >&2
  exit 2
fi
cleanup_parts_dir() {
  rm -rf "$PARTS_DIR"
}
trap cleanup_parts_dir EXIT
PROGRESS_FILE=${PROGRESS_FILE:-}
PROGRESS_READY=0
# One invocation owns its metrics directory, so output names—not timestamps—
# make the stage files both stable and immediately discoverable.
TIME_LOG_DIR="$LOGDIR/timings/conversion"
METRICS_JSON_DIR="$LOGDIR/stages/conversion"
mkdir -p "$TIME_LOG_DIR" "$METRICS_JSON_DIR"
TIME_LOG="$TIME_LOG_DIR/${SAFE_OUT_NAME}.txt"
METRICS_JSON="$METRICS_JSON_DIR/${SAFE_OUT_NAME}.json"
METRICS_CSV="$LOGDIR/metrics.csv"
TSV_EXIT_CODE=${TSV_EXIT_CODE:-0}
TSV_WALL_SECONDS=${TSV_WALL_SECONDS:-null}
TSV_USER_SECONDS=${TSV_USER_SECONDS:-null}
TSV_SYS_SECONDS=${TSV_SYS_SECONDS:-null}
TSV_MAX_RSS_KB=${TSV_MAX_RSS_KB:-null}
TSV_OUTPUT_SIZE_BYTES=${TSV_OUTPUT_SIZE_BYTES:-0}
TSV_OUTPUT_PATH=${TSV_OUTPUT_PATH:-}
METRICS_HEADER="run_id,timestamp,output_name,output_dir,exit_code_java,wall_seconds_java,user_seconds_java,sys_seconds_java,max_rss_kb_java,input_mapping_size_bytes,input_vcf_size_bytes,output_dir_size_bytes,output_triples,jar,mapping_file,output_path,exit_code_tsv,wall_seconds_tsv,user_seconds_tsv,sys_seconds_tsv,max_rss_kb_tsv,tsv_output_size_bytes,tsv_output_path"


# Detect the local `stat` dialect once. `stat_size` is called for every
# RMLStreamer part on every progress tick, so probing the dialect per call
# doubles the number of processes this script spawns for no benefit.
STAT_FILE_FLAVOR=""
detect_stat_file_flavor() {
  if [[ -n "$STAT_FILE_FLAVOR" ]]; then
    return
  fi
  if stat -c%s . >/dev/null 2>&1; then
    STAT_FILE_FLAVOR="gnu"
  elif stat -f%z . >/dev/null 2>&1; then
    STAT_FILE_FLAVOR="bsd"
  else
    STAT_FILE_FLAVOR="wc"
  fi
}

# Return byte size for file or directory (GNU + BSD compatible).
stat_size() {
  local path="$1"

  # --- CASE 1: Regular file ---
  if [[ -f "$path" ]]; then
    detect_stat_file_flavor
    case "$STAT_FILE_FLAVOR" in
      gnu) stat -c%s "$path" ;;
      bsd) stat -f%z "$path" ;;
      *) wc -c < "$path" | tr -d ' ' ;;
    esac
    return
  fi

    # --- CASE 2: Directory ---
  if [[ -d "$path" ]]; then
    # Linux (GNU du)
    if du -sb "$path" >/dev/null 2>&1; then
      du -sb "$path" | awk '{print $1}'
    # macOS/BSD (no -b)
    elif du -sk "$path" >/dev/null 2>&1; then
      # du -sk gives KB → multiply by 1024
      local kb
      kb=$(du -sk "$path" | awk '{print $1}')
      echo $((kb * 1024))
    else
      echo 0
    fi
    return
  fi

  echo 0
}

# Write sparse machine-readable progress events for the host-side Rich display.
# Progress is deliberately best-effort: a UI sidecar must never make a data
# conversion fail.
progress_emit() {
  local stage="$1"
  local phase="$2"
  local completed="${3:-null}"
  local total="${4:-null}"
  local unit="${5:-}"
  local parts="${6:-null}"
  if [[ -z "$PROGRESS_FILE" ]]; then
    return 0
  fi
  if (( PROGRESS_READY == 0 )); then
    mkdir -p "$(dirname "$PROGRESS_FILE")" >/dev/null 2>&1 || return 0
    PROGRESS_READY=1
  fi
  printf '{"stage":"%s","phase":"%s","completed":%s,"total":%s,"unit":"%s","parts":%s}\n' \
    "$stage" "$phase" "$completed" "$total" "$unit" "$parts" \
    >> "$PROGRESS_FILE" 2>/dev/null || true
}

# RMLStreamer writes part files while it runs. Summing their metadata once per
# second gives useful throughput feedback without reading or counting RDF.
# One directory walk fills both the byte total and the part count, and the
# leading-dot test uses parameter expansion rather than a `basename` process.
PART_BYTES=0
PART_COUNT=0
scan_parts() {
  local file name size
  PART_BYTES=0
  PART_COUNT=0
  shopt -s nullglob
  for file in "$PARTS_DIR"/*; do
    name="${file##*/}"
    if [[ ! -f "$file" || "$name" == .* ]]; then
      continue
    fi
    size=$(stat_size "$file")
    PART_BYTES=$((PART_BYTES + size))
    PART_COUNT=$((PART_COUNT + 1))
  done
  shopt -u nullglob
}

# Report comparable input VCF bytes.
# - .vcf    -> on-disk bytes
# - .vcf.gz -> decompressed bytes
# - dir     -> sum of normalized sizes for contained .vcf/.vcf.gz files
#
# `vcf_rdfizer_gzip.py` answers this from the file's own structure for BGZF
# (what bgzip/bcftools/tabix emit) and for ordinary single-member gzip, so the
# common case costs milliseconds instead of a full decompression pass. It falls
# back to inflating internally when the structure cannot settle the answer, and
# the shell falls back to `gzip -dc` if the helper is unavailable. The method
# used is recorded alongside the size so the metric stays auditable.
VCF_SIZE_HELPER=${VCF_SIZE_HELPER:-/opt/vcf-rdfizer/vcf_rdfizer_gzip.py}

# Prints "<bytes> <method>". Callers run this in a command substitution, so the
# method has to travel out with the value rather than through a global.
vcf_size_with_method() {
  local path="$1"
  local total=0
  local method="" entry entry_size entry_method measured

  if [[ -f "$path" ]]; then
    if [[ "$path" == *.vcf.gz ]]; then
      if [[ -f "$VCF_SIZE_HELPER" ]] && measured=$(python3 "$VCF_SIZE_HELPER" --print-method "$path" 2>/dev/null); then
        printf '%s\n' "$measured"
        return
      fi
      printf '%s inflate-shell\n' "$(gzip -dc "$path" | wc -c | tr -d ' ')"
      return
    fi
    printf '%s stat\n' "$(stat_size "$path")"
    return
  fi

  if [[ -d "$path" ]]; then
    shopt -s nullglob
    for file in "$path"/*.vcf "$path"/*.vcf.gz; do
      if [[ ! -f "$file" ]]; then
        continue
      fi
      entry=$(vcf_size_with_method "$file")
      entry_size="${entry%% *}"
      entry_method="${entry#* }"
      total=$((total + entry_size))
      if [[ -z "$method" ]]; then
        method="$entry_method"
      elif [[ "$method" != "$entry_method" ]]; then
        method="mixed"
      fi
    done
    shopt -u nullglob
    printf '%s %s\n' "$total" "${method:-none}"
    return
  fi

  printf '0 none\n'
}

# Backwards-compatible size-only accessor for direct callers of this script.
normalized_vcf_size() {
  local result
  result=$(vcf_size_with_method "$1")
  printf '%s\n' "${result%% *}"
}

have_gnu_time() { [[ -x /usr/bin/time ]] && /usr/bin/time --version >/dev/null 2>&1; }

# Return stable content hash for duplicate part detection.
hash_file_sha256() {
  local path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
    return
  fi
  # Last-resort fallback when SHA utilities are unavailable.
  cksum "$path" | awk '{print $1":"$2}'
}

# Count triples via non-comment RDF lines ending in '.'.
#
# `grep -c` counts in one process and, in the C locale, works byte-wise instead
# of validating UTF-8 for every line. On a cohort-scale aggregate this is
# several times faster than piping through `awk` or `wc -l`, and it is the last
# unavoidable full pass over the RDF output. `grep` exits 1 when nothing
# matched, so the `|| true` sits inside the pipeline's last stage to keep a
# genuine `gzip` failure visible under `set -o pipefail`.
TRIPLE_LINE_REGEX='^[[:space:]]*[^#].*\.[[:space:]]*$'

count_triple_lines() {
  local path="$1"
  if [[ "$path" == *.gz ]]; then
    gzip -dc "$path" | { LC_ALL=C grep -cE "$TRIPLE_LINE_REGEX" || true; }
    return
  fi
  { LC_ALL=C grep -cE "$TRIPLE_LINE_REGEX" "$path" || true; }
}

count_triples_json() {
  local path="$1"
  local total=0

  if [[ -f "$path" ]]; then
    local count
    count=$(count_triple_lines "$path")
    echo "{"
    printf "  \"%s\": %s,\n" "$path" "$count"
    printf "  \"TOTAL\": %s\n" "$count"
    echo "}"
    return
  fi

  echo "{"
  shopt -s nullglob

  for f in "$path"/*; do
    if [[ -f "$f" ]]; then
      local count
      count=$(count_triple_lines "$f")
      total=$((total + count))
      printf "  \"%s\": %s,\n" "$f" "$count"
    fi
  done

  shopt -u nullglob
  printf "  \"TOTAL\": %s\n" "$total"
  echo "}"
}

# Replace plain VCF null marker literals (`"."`) with typed null literals.
# Ontology alignment:
#   "."  ->  "."^^vcfr:Null
#
# The rewrite is per-line and has no cross-line context, so it is applied to
# each RMLStreamer part as it is streamed into the aggregate. Running it as a
# separate pass over the finished aggregate instead would read and rewrite the
# complete RDF output a second time.
NULL_LITERAL_SED='s/"\."([[:space:]]+\.)/"."^^<https:\/\/w3id.org\/vcf-rdfizer\/vocab#Null>\1/g'
annotate_null_literals_stream() {
  sed -E "$NULL_LITERAL_SED" "$1"
}


# Convert elapsed clock text from `time` output to numeric seconds.
elapsed_to_seconds() {
  awk -F':' '{
    if (NF==3) { h=$1+0; m=$2+0; s=$3+0; printf("%.3f", h*3600 + m*60 + s) }
    else if (NF==2) { m=$1+0; s=$2+0; printf("%.3f", m*60 + s) }
    else { s=$1+0; printf("%.3f", s) }
  }'
}

JAVA_VERSION=$(java -version 2>&1 | head -n1 | sed 's/"/\\"/g')

# Minimal GC logging off by default to keep things simple; uncomment if you want it.
# GC_OPTS="-Xlog:gc*:file=$LOGDIR/gc-$RUN_ID.log:time,uptime,level,tags" # Java 9+
# or for Java 8: GC_OPTS="-Xloggc:$LOGDIR/gc-$RUN_ID.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
GC_OPTS=${GC_OPTS:-}

# Optional low-cost Spark partition hint for RMLStreamer execution.
# When set, this caps Spark default parallelism and shuffle partitions to
# reduce tiny output-part overproduction without introducing expensive
# repartition/shuffle stages in the pipeline.
SPARK_PARTITIONS=${SPARK_PARTITIONS:-}
JAVA_SPARK_OPTS=()
if [[ -n "$SPARK_PARTITIONS" ]]; then
  if [[ "$SPARK_PARTITIONS" =~ ^[1-9][0-9]*$ ]]; then
    JAVA_SPARK_OPTS+=("-Dspark.default.parallelism=$SPARK_PARTITIONS")
    JAVA_SPARK_OPTS+=("-Dspark.sql.shuffle.partitions=$SPARK_PARTITIONS")
  else
    echo "WARNING: ignoring invalid SPARK_PARTITIONS='$SPARK_PARTITIONS' (expected positive integer)." >&2
  fi
fi

# Build Java command in a macOS Bash-3-compatible way:
# with `set -u`, expanding an empty array (`"${arr[@]}"`) raises "unbound variable".
JAVA_CMD=(java -jar "$JAR" toFile -m "$IN" -o "$PARTS_DIR")
if (( ${#JAVA_SPARK_OPTS[@]} > 0 )); then
  JAVA_CMD=(java "${JAVA_SPARK_OPTS[@]}" -jar "$JAR" toFile -m "$IN" -o "$PARTS_DIR")
fi

# ---------- Pre-run ----------
IN_SIZE=$(stat_size "$IN")
VCF_SIZE_INFO=$(vcf_size_with_method "$IN_VCF")
VCF_SIZE="${VCF_SIZE_INFO%% *}"
VCF_SIZE_METHOD="${VCF_SIZE_INFO#* }"

# ---------- Run RMLStreamer with timing ----------
EXIT_CODE=0
run_rmlstreamer() {
  if have_gnu_time; then
    /usr/bin/time -v -o "$TIME_LOG" -- "${JAVA_CMD[@]}"
  else
    { time -p "${JAVA_CMD[@]}"; } >"$TIME_LOG" 2>&1
  fi
}

if [[ -n "$PROGRESS_FILE" ]]; then
  progress_emit "rmlstreamer" "started" 0 null bytes 0
  run_rmlstreamer &
  RMLSTREAMER_PID=$!
  while kill -0 "$RMLSTREAMER_PID" >/dev/null 2>&1; do
    scan_parts
    progress_emit "rmlstreamer" "heartbeat" "$PART_BYTES" null bytes "$PART_COUNT"
    sleep 1
  done
  wait "$RMLSTREAMER_PID" || EXIT_CODE=$?
  scan_parts
  if (( EXIT_CODE == 0 )); then
    progress_emit "rmlstreamer" "complete" "$PART_BYTES" null bytes "$PART_COUNT"
  else
    progress_emit "rmlstreamer" "failed" "$PART_BYTES" null bytes "$PART_COUNT"
  fi
else
  run_rmlstreamer || EXIT_CODE=$?
fi

# Normalize output files to .nt for downstream line-oriented processing. The
# wrapper does not infer or rewrite RDF syntax beyond preserving the part as a
# line-oriented file; callers must validate whether the stream is NT or NQ.
for RDF_FILE in "$PARTS_DIR"/*; do
  if [[ ! -f "$RDF_FILE" ]]; then
    continue
  fi
  if [[ "$RDF_FILE" == *.nt ]]; then
    continue
  fi
  mv "$RDF_FILE" "${RDF_FILE}.nt"
done

# Merge all RMLStreamer output parts into one aggregate named after the output
# basename. Stream merge + delete each part immediately to avoid temporary 2x
# disk spikes.
shopt -s nullglob
PART_FILES=("$PARTS_DIR"/*.nt)
PART_TOTAL=${#PART_FILES[@]}
PART_INDEX=0
if (( PART_TOTAL > 0 )); then
  progress_emit "rdf-aggregate" "started" 0 "$PART_TOTAL" parts 0
fi
if [[ "$RDF_STORAGE_MODE" == "space-optimized" ]]; then
    MERGED_RDF="${MERGED_NT}.gz"
    MERGED_TMP="${MERGED_RDF}.partial.$$"
    # Keep duplicate-part protection in the compressed path as well. Hashing
    # each part is linear in the bytes already being read and avoids retaining
    # an uncompressed duplicate just to compare it later.
    SEEN_HASH_FILE="$PARTS_DIR/.seen_part_hashes"
    SEEN_MAP_FILE="$PARTS_DIR/.seen_part_hash_map"
    : > "$SEEN_HASH_FILE"
    : > "$SEEN_MAP_FILE"
    # Feed an empty stream on stdin for GNU and BSD gzip compatibility.
    gzip -c < /dev/null > "$MERGED_TMP"
    for PART_NT in "${PART_FILES[@]}"; do
      PART_HASH=$(hash_file_sha256 "$PART_NT")
      if grep -Fqx "$PART_HASH" "$SEEN_HASH_FILE"; then
        FIRST_SEEN=$(awk -F'\t' -v hash="$PART_HASH" '$1 == hash { print $2; exit }' "$SEEN_MAP_FILE")
        echo "WARNING: skipping duplicate RDF part '$PART_NT' (same content as '$FIRST_SEEN')." >&2
        rm -f "$PART_NT"
        PART_INDEX=$((PART_INDEX + 1))
        progress_emit "rdf-aggregate" "part" "$PART_INDEX" "$PART_TOTAL" parts "$PART_INDEX"
        continue
      fi
      printf "%s\n" "$PART_HASH" >> "$SEEN_HASH_FILE"
      printf "%s\t%s\n" "$PART_HASH" "$PART_NT" >> "$SEEN_MAP_FILE"
      # Concatenated gzip members form one valid sequential gzip stream while
      # allowing each completed RMLStreamer part to be deleted immediately.
      annotate_null_literals_stream "$PART_NT" | gzip -c >> "$MERGED_TMP"
      rm -f "$PART_NT"
      PART_INDEX=$((PART_INDEX + 1))
      progress_emit "rdf-aggregate" "part" "$PART_INDEX" "$PART_TOTAL" parts "$PART_INDEX"
    done
    rm -f "$SEEN_HASH_FILE" "$SEEN_MAP_FILE"
    mv "$MERGED_TMP" "$MERGED_RDF"
    OUTPUT_PATH="$MERGED_RDF"
  elif (( ${#PART_FILES[@]} > 0 )); then
    : > "$MERGED_NT"
    # Defensive dedupe: some Spark/RMLStreamer runs can emit identical part
    # files for the same dataset. Skip exact duplicate part payloads to avoid
    # doubling every triple in the merged output.
    SEEN_HASH_FILE="$PARTS_DIR/.seen_part_hashes"
    SEEN_MAP_FILE="$PARTS_DIR/.seen_part_hash_map"
    : > "$SEEN_HASH_FILE"
    : > "$SEEN_MAP_FILE"
    for PART_NT in "${PART_FILES[@]}"; do
      if [[ "$PART_NT" == "$MERGED_NT" ]]; then
        continue
      fi
      PART_HASH=$(hash_file_sha256 "$PART_NT")
      if grep -Fqx "$PART_HASH" "$SEEN_HASH_FILE"; then
        FIRST_SEEN=$(awk -F'\t' -v hash="$PART_HASH" '$1 == hash { print $2; exit }' "$SEEN_MAP_FILE")
        echo "WARNING: skipping duplicate RDF part '$PART_NT' (same content as '$FIRST_SEEN')." >&2
        rm -f "$PART_NT"
        PART_INDEX=$((PART_INDEX + 1))
        progress_emit "rdf-aggregate" "part" "$PART_INDEX" "$PART_TOTAL" parts "$PART_INDEX"
        continue
      fi
      printf "%s\n" "$PART_HASH" >> "$SEEN_HASH_FILE"
      printf "%s\t%s\n" "$PART_HASH" "$PART_NT" >> "$SEEN_MAP_FILE"
      annotate_null_literals_stream "$PART_NT" >> "$MERGED_NT"
      rm -f "$PART_NT"
      PART_INDEX=$((PART_INDEX + 1))
      progress_emit "rdf-aggregate" "part" "$PART_INDEX" "$PART_TOTAL" parts "$PART_INDEX"
    done
    rm -f "$SEEN_HASH_FILE" "$SEEN_MAP_FILE"
  else
    : > "$MERGED_NT"
    OUTPUT_PATH="$MERGED_NT"
  fi
shopt -u nullglob
if [[ "$RDF_STORAGE_MODE" != "space-optimized" ]]; then
  OUTPUT_PATH="$MERGED_NT"
fi
if (( PART_TOTAL > 0 )); then
  progress_emit "rdf-aggregate" "complete" "$PART_INDEX" "$PART_TOTAL" parts "$PART_INDEX"
fi

rm -rf "$PARTS_DIR"
trap - EXIT

OUT_SIZE=$(stat_size "$OUTPUT_PATH")
TRIPLES_JSON=$(count_triples_json "$OUTPUT_PATH")

# Parse timing
WALL_SEC=""
USER_SEC=""
SYS_SEC=""
MAX_RSS_KB=""

if have_gnu_time; then
  ELAPSED=$(awk -F': ' '/Elapsed \(wall clock\) time/ {print $2}' "$TIME_LOG")
  WALL_SEC=$(printf "%s" "$ELAPSED" | elapsed_to_seconds)

  USER_SEC=$(awk -F': ' '/User time \(seconds\)/ {print $2}' "$TIME_LOG")
  SYS_SEC=$(awk -F': '  '/System time \(seconds\)/ {print $2}' "$TIME_LOG")
  MAX_RSS_KB=$(awk -F': ' '/Maximum resident set size/ {print $2}' "$TIME_LOG")
else
  WALL_SEC=$(awk '/^real/ {print $2}' "$TIME_LOG")   # already a float
  USER_SEC=$(awk '/^user/ {print $2}' "$TIME_LOG")
  SYS_SEC=$(awk  '/^sys/  {print $2}' "$TIME_LOG")
fi

# ---------- Persist conversion metrics JSON ----------
cat > "$METRICS_JSON" <<EOF
{
  "run_id": "$RUN_ID",
  "timestamp": "$TIMESTAMP",
  "command": "$(printf '%q ' "${JAVA_CMD[@]}")",
  "exit_code": $EXIT_CODE,
  "timing": {
    "wall_seconds": ${WALL_SEC:-null},
    "user_seconds": ${USER_SEC:-null},
    "sys_seconds": ${SYS_SEC:-null},
    "max_rss_kb": ${MAX_RSS_KB:-null}
  },
  "artifacts": {
    "jar": "$JAR",
    "input_path": "$IN",
    "input_size_bytes": $IN_SIZE,
    "input_vcf_size_bytes": $VCF_SIZE,
    "input_vcf_size_method": "$VCF_SIZE_METHOD",
    "output_path": "$OUTPUT_PATH",
    "output_size_bytes": $OUT_SIZE,
    "output_triples": $TRIPLES_JSON
  },
  "rdf_storage": {
    "mode": "$RDF_STORAGE_MODE",
    "compressed": $( [[ "$OUTPUT_PATH" == *.gz ]] && echo true || echo false ),
    "serialization": "ntriples"
  },
  "tsv_generation": {
    "exit_code": ${TSV_EXIT_CODE},
    "timing": {
      "wall_seconds": ${TSV_WALL_SECONDS},
      "user_seconds": ${TSV_USER_SECONDS},
      "sys_seconds": ${TSV_SYS_SECONDS},
      "max_rss_kb": ${TSV_MAX_RSS_KB}
    },
    "output_size_bytes": ${TSV_OUTPUT_SIZE_BYTES},
    "output_path": "$TSV_OUTPUT_PATH"
  },
  "java": {
    "version_header": "$JAVA_VERSION"
  }
}
EOF

# ---------- Save/append CSV ----------
# Header if file doesn't exist (or mismatch)
TOTAL_TRIPLES=$(echo "$TRIPLES_JSON" | grep '"TOTAL"' | awk -F': ' '{print $2}' | tr -d '", ')
if [[ ! -f "$METRICS_CSV" ]]; then
  echo "$METRICS_HEADER" > "$METRICS_CSV"
else
  EXISTING_HEADER=$(head -n 1 "$METRICS_CSV")
  if [[ "$EXISTING_HEADER" != "$METRICS_HEADER" ]]; then
    BACKUP="$LOGDIR/metrics_csv_bak_${RUN_ID}.csv"
    cp "$METRICS_CSV" "$BACKUP"
    echo "WARNING: metrics header mismatch; backed up to $BACKUP and creating new metrics file." >&2
    echo "$METRICS_HEADER" > "$METRICS_CSV"
  fi
fi

csv_fields=(
  "$RUN_ID"
  "$TIMESTAMP"
  "$OUT_NAME"
  "$OUT"
  "$EXIT_CODE"
  "${WALL_SEC:-}"
  "${USER_SEC:-}"
  "${SYS_SEC:-}"
  "${MAX_RSS_KB:-}"
  "$IN_SIZE"
  "$VCF_SIZE"
  "$OUT_SIZE"
  "$TOTAL_TRIPLES"
  "$JAR"
  "$IN"
  "$OUTPUT_PATH"
  "$TSV_EXIT_CODE"
  "$TSV_WALL_SECONDS"
  "$TSV_USER_SECONDS"
  "$TSV_SYS_SECONDS"
  "$TSV_MAX_RSS_KB"
  "$TSV_OUTPUT_SIZE_BYTES"
  "$TSV_OUTPUT_PATH"
)
( IFS=,; echo "${csv_fields[*]}" ) >> "$METRICS_CSV"

echo "Done."
echo "JSON: $METRICS_JSON"
echo "CSV:  $METRICS_CSV"

if [[ "$EXIT_CODE" -ne 0 ]]; then
  echo "Error: conversion command failed with exit code $EXIT_CODE." >&2
  exit "$EXIT_CODE"
fi
