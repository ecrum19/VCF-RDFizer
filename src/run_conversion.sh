#!/bin/bash

#!/usr/bin/env bash
set -euo pipefail

# ---------- Config ----------
JAR=${JAR:-RMLStreamer-v2.5.0-standalone.jar}
IN=${IN:-rules/default_rules.ttl}
IN_VCF=${IN_VCF:-input.vcf}
OUT_NAME=${OUT_NAME:-rdf}
OUT_DIR=${OUT_DIR:-run_output}
OUT="$OUT_DIR/$OUT_NAME"
LOGDIR=${LOGDIR:-run_metrics}
mkdir -p "$LOGDIR" "$OUT_DIR"

RUN_ID=${RUN_ID:-$(date +%Y%m%dT%H%M%S)}
TIMESTAMP=${TIMESTAMP:-$(date +"%Y-%m-%dT%H:%M:%S")}
TIME_LOG="$LOGDIR/time-$RUN_ID.txt"
METRICS_JSON="$LOGDIR/metrics-$RUN_ID.json"
METRICS_CSV="$LOGDIR/metrics.csv"
METRICS_HEADER="run_id,timestamp,output_name,output_dir,exit_code_java,wall_seconds_java,user_seconds_java,sys_seconds_java,max_rss_kb_java,input_mapping_size_bytes,input_vcf_size_bytes,output_dir_size_bytes,output_triples,jar,mapping_file,output_path,combined_nq_size_bytes,gzip_size_bytes,brotli_size_bytes,hdt_size_bytes,exit_code_gzip,exit_code_brotli,exit_code_hdt,wall_seconds_gzip,user_seconds_gzip,sys_seconds_gzip,max_rss_kb_gzip,wall_seconds_brotli,user_seconds_brotli,sys_seconds_brotli,max_rss_kb_brotli,wall_seconds_hdt,user_seconds_hdt,sys_seconds_hdt,max_rss_kb_hdt,compression_methods"


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
}

have_gnu_time() { [[ -x /usr/bin/time ]] && /usr/bin/time --version >/dev/null 2>&1; }

# Count triples via the number of lines in produced output dir:
count_triples_json() {
  local path="$1"
  local total=0

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

JAVA_CMD=(java -jar "$JAR" toFile -m "$IN" -o "$OUT_DIR/$OUT_NAME")

# Ensure repeated runs with the same OUT_NAME do not accumulate old artifacts.
if [[ -d "$OUT_DIR/$OUT_NAME" ]]; then
  rm -rf "$OUT_DIR/$OUT_NAME"
fi

# ---------- Pre-run ----------
IN_SIZE=$(stat_size "$IN")
VCF_SIZE=$(stat_size "$IN_VCF")

# ---------- Run with timing ----------
EXIT_CODE=0
if have_gnu_time; then
  /usr/bin/time -v -o "$TIME_LOG" -- "${JAVA_CMD[@]}" || EXIT_CODE=$?
else
  { time -p "${JAVA_CMD[@]}"; } >"$TIME_LOG" 2>&1 || EXIT_CODE=$?
fi

# ---------- Post-run ----------
mkdir -p "$OUT_DIR/$OUT_NAME"

# Normalize output files to .nq for downstream compression
for NO_EXT_FILE in "$OUT_DIR/$OUT_NAME"/*; do
  if [[ -f "$NO_EXT_FILE" && "$NO_EXT_FILE" != *.nq ]]; then
    mv "$NO_EXT_FILE" "${NO_EXT_FILE}.nq"
  fi
done

# Merge all RMLStreamer output parts into one N-Quads file named after the TSV basename/output name.
# Stream merge + delete each part immediately to avoid temporary 2x disk spikes.
MERGED_NQ="$OUT_DIR/$OUT_NAME/$OUT_NAME.nq"

shopt -s nullglob
PART_FILES=("$OUT_DIR/$OUT_NAME"/*.nq)
if (( ${#PART_FILES[@]} > 0 )); then
  : > "$MERGED_NQ"
  for PART_NQ in "${PART_FILES[@]}"; do
    if [[ "$PART_NQ" == "$MERGED_NQ" ]]; then
      continue
    fi
    cat "$PART_NQ" >> "$MERGED_NQ"
    rm -f "$PART_NQ"
  done
else
  : > "$MERGED_NQ"
fi
shopt -u nullglob

OUT_SIZE=$(stat_size "$OUT_DIR/$OUT_NAME")
TRIPLES_JSON=$(count_triples_json "$OUT_DIR/$OUT_NAME")

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

# ---------- Save JSON ----------
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
    "output_path": "$OUT_DIR/$OUT_NAME",
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
    BACKUP="$METRICS_CSV.bak-$RUN_ID"
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
  "$OUT"
  "" "" "" "" "" "" "" "" "" "" "" "" "" "" "" "" "" "" "" ""
)
( IFS=,; echo "${csv_fields[*]}" ) >> "$METRICS_CSV"

echo "Done."
echo "JSON: $METRICS_JSON"
echo "CSV:  $METRICS_CSV"

if [[ "$EXIT_CODE" -ne 0 ]]; then
  echo "Error: conversion command failed with exit code $EXIT_CODE." >&2
  exit "$EXIT_CODE"
fi
