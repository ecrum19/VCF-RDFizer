#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# TSV -> RDF conversion runner (RMLStreamer)
# ------------------------------------------------------------------------------
# Responsibilities:
# 1) run RMLStreamer with stable output naming
# 2) normalize Spark part outputs to .nt
# 3) optionally aggregate part files into a single <sample>.nt
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
AGGREGATE_RDF=${AGGREGATE_RDF:-1}
LOGDIR=${LOGDIR:-run_metrics}
mkdir -p "$LOGDIR" "$OUT_DIR"

RUN_ID=${RUN_ID:-$(date +%Y%m%dT%H%M%S)}
TIMESTAMP=${TIMESTAMP:-$(date +"%Y-%m-%dT%H:%M:%S")}
SAFE_OUT_NAME=$(printf "%s" "$OUT_NAME" | tr -cs 'A-Za-z0-9._-' '_')
if [[ -z "$SAFE_OUT_NAME" ]]; then
  SAFE_OUT_NAME="rdf"
fi
TIME_LOG_DIR="$LOGDIR/conversion_time/${SAFE_OUT_NAME}"
METRICS_JSON_DIR="$LOGDIR/conversion_metrics/${SAFE_OUT_NAME}"
mkdir -p "$TIME_LOG_DIR" "$METRICS_JSON_DIR"
TIME_LOG="$TIME_LOG_DIR/${RUN_ID}.txt"
METRICS_JSON="$METRICS_JSON_DIR/${RUN_ID}.json"
METRICS_CSV="$LOGDIR/metrics.csv"
METRICS_HEADER="run_id,timestamp,output_name,output_dir,exit_code_java,wall_seconds_java,user_seconds_java,sys_seconds_java,max_rss_kb_java,input_mapping_size_bytes,input_vcf_size_bytes,output_dir_size_bytes,output_triples,jar,mapping_file,output_path"


# Return byte size for file or directory (GNU + BSD compatible).
stat_size() {
  local path="$1"

  # --- CASE 1: Regular file ---
  if [[ -f "$path" ]]; then
    # Linux (GNU coreutils)
    if stat -c%s "$path" >/dev/null 2>&1; then
      stat -c%s "$path"
    # macOS/BSD
    elif stat -f%z "$path" >/dev/null 2>&1; then
      stat -f%z "$path"
    else
      wc -c < "$path" | tr -d ' '
    fi
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

# Report comparable input VCF bytes.
# - .vcf    -> on-disk bytes
# - .vcf.gz -> decompressed bytes
# - dir     -> sum of normalized sizes for contained .vcf/.vcf.gz files
normalized_vcf_size() {
  local path="$1"
  local total=0

  if [[ -f "$path" ]]; then
    if [[ "$path" == *.vcf.gz ]]; then
      gzip -dc "$path" | wc -c | tr -d ' '
      return
    fi
    stat_size "$path"
    return
  fi

  if [[ -d "$path" ]]; then
    shopt -s nullglob
    for file in "$path"/*.vcf "$path"/*.vcf.gz; do
      if [[ ! -f "$file" ]]; then
        continue
      fi
      size=$(normalized_vcf_size "$file")
      total=$((total + size))
    done
    shopt -u nullglob
    echo "$total"
    return
  fi

  echo 0
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
count_triples_json() {
  local path="$1"
  local total=0

  if [[ -f "$path" ]]; then
    local count
    count=$( (grep -E '^[[:space:]]*[^#].*\.[[:space:]]*$' "$path" || true) | wc -l | tr -d ' ' )
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
      count=$( (grep -E '^[[:space:]]*[^#].*\.[[:space:]]*$' "$f" || true) | wc -l | tr -d ' ' )
      total=$((total + count))
      printf "  \"%s\": %s,\n" "$f" "$count"
    fi
  done

  shopt -u nullglob
  printf "  \"TOTAL\": %s\n" "$total"
  echo "}"
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

JAVA_CMD=(java "${JAVA_SPARK_OPTS[@]}" -jar "$JAR" toFile -m "$IN" -o "$OUT_DIR/$OUT_NAME")

# Ensure repeated runs with the same OUT_NAME do not accumulate old artifacts.
if [[ -d "$OUT_DIR/$OUT_NAME" ]]; then
  rm -rf "$OUT_DIR/$OUT_NAME"
fi

# ---------- Pre-run ----------
IN_SIZE=$(stat_size "$IN")
VCF_SIZE=$(normalized_vcf_size "$IN_VCF")

# ---------- Run RMLStreamer with timing ----------
EXIT_CODE=0
if have_gnu_time; then
  /usr/bin/time -v -o "$TIME_LOG" -- "${JAVA_CMD[@]}" || EXIT_CODE=$?
else
  { time -p "${JAVA_CMD[@]}"; } >"$TIME_LOG" 2>&1 || EXIT_CODE=$?
fi

# ---------- Post-run normalization ----------
mkdir -p "$OUT_DIR/$OUT_NAME"

# Normalize output files to .nt for downstream compression/HDT conversion.
for RDF_FILE in "$OUT_DIR/$OUT_NAME"/*; do
  if [[ ! -f "$RDF_FILE" ]]; then
    continue
  fi
  if [[ "$RDF_FILE" == *.nt ]]; then
    continue
  fi
  mv "$RDF_FILE" "${RDF_FILE}.nt"
done

# Merge all RMLStreamer output parts into one N-Triples file named after output
# basename when AGGREGATE_RDF=1. Stream merge + delete each part immediately to
# avoid temporary 2x disk spikes.
if [[ "$AGGREGATE_RDF" == "1" ]]; then
  MERGED_NT="$OUT_DIR/$OUT_NAME/$OUT_NAME.nt"
  shopt -s nullglob
  PART_FILES=("$OUT_DIR/$OUT_NAME"/*.nt)
  if (( ${#PART_FILES[@]} > 0 )); then
    : > "$MERGED_NT"
    # Defensive dedupe: some Spark/RMLStreamer runs can emit identical part
    # files for the same dataset. Skip exact duplicate part payloads to avoid
    # doubling every triple in the merged output.
    SEEN_HASH_FILE="$OUT_DIR/$OUT_NAME/.seen_part_hashes.$$"
    SEEN_MAP_FILE="$OUT_DIR/$OUT_NAME/.seen_part_hash_map.$$"
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
        continue
      fi
      printf "%s\n" "$PART_HASH" >> "$SEEN_HASH_FILE"
      printf "%s\t%s\n" "$PART_HASH" "$PART_NT" >> "$SEEN_MAP_FILE"
      cat "$PART_NT" >> "$MERGED_NT"
      rm -f "$PART_NT"
    done
    rm -f "$SEEN_HASH_FILE" "$SEEN_MAP_FILE"
  else
    : > "$MERGED_NT"
  fi
  shopt -u nullglob
  OUTPUT_PATH="$MERGED_NT"
else
  OUTPUT_PATH="$OUT_DIR/$OUT_NAME"
fi

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
    "output_path": "$OUTPUT_PATH",
    "output_size_bytes": $OUT_SIZE,
    "output_triples": $TRIPLES_JSON
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
)
( IFS=,; echo "${csv_fields[*]}" ) >> "$METRICS_CSV"

echo "Done."
echo "JSON: $METRICS_JSON"
echo "CSV:  $METRICS_CSV"

if [[ "$EXIT_CODE" -ne 0 ]]; then
  echo "Error: conversion command failed with exit code $EXIT_CODE." >&2
  exit "$EXIT_CODE"
fi
