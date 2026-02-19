#!/usr/bin/env bash
set -euo pipefail

# ---------- Config ----------
# Root output directory; contains one or more output subdirs
OUT_ROOT_DIR=${OUT_ROOT_DIR:-run_output}
# Optional single output name (compress only this subdir)
OUT_NAME=${OUT_NAME:-}

# Metrics directory
LOGDIR=${LOGDIR:-run_metrics}

# rdf2hdt binary
HDT=${RDF2HDT:-/opt/hdt-java/hdt-java-cli/bin/rdf2hdt.sh}

# Base URI for rdf2hdt (reserved for future use)
BASE_URI=${BASE_URI:-http://example.org/base}

RUN_ID=${RUN_ID:-$(date +%Y%m%dT%H%M%S)}
TIMESTAMP=${TIMESTAMP:-$(date +"%Y-%m-%dT%H:%M:%S")}

mkdir -p "$LOGDIR" "$OUT_ROOT_DIR"

METRICS_CSV="$LOGDIR/metrics.csv"
METRICS_HEADER="run_id,timestamp,output_name,output_dir,exit_code_java,wall_seconds_java,user_seconds_java,sys_seconds_java,max_rss_kb_java,input_mapping_size_bytes,input_vcf_size_bytes,output_dir_size_bytes,output_triples,jar,mapping_file,output_path,combined_nq_size_bytes,gzip_size_bytes,brotli_size_bytes,hdt_size_bytes,exit_code_gzip,exit_code_brotli,exit_code_hdt,wall_seconds_gzip,user_seconds_gzip,sys_seconds_gzip,max_rss_kb_gzip,wall_seconds_brotli,user_seconds_brotli,sys_seconds_brotli,max_rss_kb_brotli,wall_seconds_hdt,user_seconds_hdt,sys_seconds_hdt,max_rss_kb_hdt,compression_methods"

# ---------- Compression selection ----------
# Usage: compression.sh [-m gzip,brotli,hdt|none]
# Or set COMPRESSION_METHODS env var (default: gzip,brotli,hdt)
COMPRESSION_METHODS=${COMPRESSION_METHODS:-gzip,brotli,hdt}

while getopts ":m:h" opt; do
  case "$opt" in
    m) COMPRESSION_METHODS="$OPTARG" ;;
    h)
      echo "Usage: $0 [-m gzip,brotli,hdt|none]"
      exit 0
      ;;
    \?)
      echo "Error: invalid option -$OPTARG" >&2
      exit 2
      ;;
    :)
      echo "Error: option -$OPTARG requires an argument" >&2
      exit 2
      ;;
  esac
done

COMPRESSION_METHODS_CSV=${COMPRESSION_METHODS//,/|}

DO_GZIP=0
DO_BROTLI=0
DO_HDT=0

if [[ -n "${COMPRESSION_METHODS// }" && "$COMPRESSION_METHODS" != "none" ]]; then
  IFS=',' read -r -a METHODS_ARR <<< "$COMPRESSION_METHODS"
  for method in "${METHODS_ARR[@]}"; do
    m="${method// /}"
    case "$m" in
      gzip) DO_GZIP=1 ;;
      brotli) DO_BROTLI=1 ;;
      hdt) DO_HDT=1 ;;
      "" ) ;;
      *)
        echo "Error: unsupported compression method '$m'. Use gzip,brotli,hdt, or none." >&2
        exit 2
        ;;
    esac
  done
fi

ANY_COMPRESS=$((DO_GZIP + DO_BROTLI + DO_HDT))

if (( DO_HDT == 1 )); then
  if [[ ! -x "$HDT" ]]; then
    echo "Error: rdf2hdt script not found or not executable at '$HDT'." >&2
    exit 2
  fi
  if ! command -v java >/dev/null 2>&1; then
    echo "Error: Java runtime is required for HDT conversion but was not found on PATH." >&2
    exit 2
  fi
fi

GZIP_ROOT="$OUT_ROOT_DIR/gzip"
BROTLI_ROOT="$OUT_ROOT_DIR/brotli"
HDT_ROOT="$OUT_ROOT_DIR/hdt"

if (( DO_GZIP == 1 )); then
  mkdir -p "$GZIP_ROOT"
fi
if (( DO_BROTLI == 1 )); then
  mkdir -p "$BROTLI_ROOT"
fi
if (( DO_HDT == 1 )); then
  mkdir -p "$HDT_ROOT"
fi

# Resolve output directories to compress
OUTPUT_DIRS=()
if [[ -n "$OUT_NAME" ]]; then
  OUTPUT_DIRS=("$OUT_ROOT_DIR/$OUT_NAME")
else
  mapfile -t OUTPUT_DIRS < <(find "$OUT_ROOT_DIR" -maxdepth 1 -type d \
    ! -path "$OUT_ROOT_DIR" \
    ! -path "$GZIP_ROOT" \
    ! -path "$BROTLI_ROOT" \
    ! -path "$HDT_ROOT" | sort)
  if (( ${#OUTPUT_DIRS[@]} == 0 )); then
    OUTPUT_DIRS=("$OUT_ROOT_DIR")
  fi
fi

if (( ${#OUTPUT_DIRS[@]} == 0 )); then
  echo "Error: no output directories found in '$OUT_ROOT_DIR'." >&2
  exit 2
fi

# ---------- Helper functions ----------
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

have_gnu_time() { [[ -x /usr/bin/time ]] && /usr/bin/time --version >/dev/null 2>&1; }

# Count triples via number of non-comment lines ending in "."
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

# CSV header
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

# ---------- Main loop over output dirs ----------
OVERALL_EXIT=0

for OUT in "${OUTPUT_DIRS[@]}"; do
  if [[ ! -d "$OUT" ]]; then
    echo "WARNING: output directory '$OUT' not found, skipping." >&2
    OVERALL_EXIT=1
    continue
  fi

  BASENAME=$(basename "$OUT")
  SAFE_BASENAME=$(printf "%s" "$BASENAME" | tr -cs 'A-Za-z0-9._-' '_')
  if [[ -z "$SAFE_BASENAME" ]]; then
    SAFE_BASENAME="rdf"
  fi

  TIME_LOG_GZIP="$LOGDIR/compression-time-gzip-${SAFE_BASENAME}-${RUN_ID}.txt"
  TIME_LOG_BROTLI="$LOGDIR/compression-time-brotli-${SAFE_BASENAME}-${RUN_ID}.txt"
  TIME_LOG_HDT="$LOGDIR/compression-time-hdt-${SAFE_BASENAME}-${RUN_ID}.txt"
  METRICS_JSON="$LOGDIR/compression-metrics-${SAFE_BASENAME}-${RUN_ID}.json"

  OUT_SIZE=$(stat_size "$OUT")
  TRIPLES_JSON=$(count_triples_json "$OUT")
  TOTAL_TRIPLES=$(echo "$TRIPLES_JSON" | grep '"TOTAL"' | awk -F': ' '{print $2}' | tr -d '", ')

  shopt -s nullglob
  NT_FILES=("$OUT"/*.nt)
  NQ_FILES=("$OUT"/*.nq)
  shopt -u nullglob
  PRIMARY_NT="$OUT/${BASENAME}.nt"
  PRIMARY_NQ="$OUT/${BASENAME}.nq"

  if (( ${#NT_FILES[@]} == 0 && ${#NQ_FILES[@]} == 0 )); then
    echo "WARNING: no .nt or .nq files found in '$OUT'; skipping compression for this output." >&2
    SOURCE_RDF=""
    SOURCE_EXT=""
    NQ_SIZE=0
    GZ_PATH=""
    GZ_SIZE=0
    BROTLI_PATH=""
    BROTLI_SIZE=0
    HDT_PATH=""
    HDT_SIZE=0
    EXIT_CODE_GZIP=$(( DO_GZIP == 1 ? 1 : 0 ))
    EXIT_CODE_BROTLI=$(( DO_BROTLI == 1 ? 1 : 0 ))
    EXIT_CODE_HDT=$(( DO_HDT == 1 ? 1 : 0 ))
    WALL_SEC_GZIP="null"
    USER_SEC_GZIP="null"
    SYS_SEC_GZIP="null"
    MAX_RSS_KB_GZIP="null"
    WALL_SEC_BROTLI="null"
    USER_SEC_BROTLI="null"
    SYS_SEC_BROTLI="null"
    MAX_RSS_KB_BROTLI="null"
    WALL_SEC_HDT="null"
    USER_SEC_HDT="null"
    SYS_SEC_HDT="null"
    MAX_RSS_KB_HDT="null"
    if (( ANY_COMPRESS > 0 )); then
      OVERALL_EXIT=1
    fi
  else
    if (( ANY_COMPRESS > 0 )); then
      if [[ -f "$PRIMARY_NT" ]]; then
        SOURCE_RDF="$PRIMARY_NT"
        SOURCE_EXT="nt"
      elif (( ${#NT_FILES[@]} == 1 )); then
        SOURCE_RDF="${NT_FILES[0]}"
        SOURCE_EXT="nt"
      elif [[ -f "$PRIMARY_NQ" ]]; then
        SOURCE_RDF="$PRIMARY_NQ"
        SOURCE_EXT="nq"
      elif (( ${#NQ_FILES[@]} == 1 )); then
        SOURCE_RDF="${NQ_FILES[0]}"
        SOURCE_EXT="nq"
      else
        echo "WARNING: unable to determine a unique primary RDF file in '$OUT'." >&2
        SOURCE_RDF=""
        SOURCE_EXT=""
      fi

      if [[ -z "$SOURCE_RDF" ]]; then
        NQ_SIZE=0
        EXIT_CODE_GZIP=$(( DO_GZIP == 1 ? 1 : 0 ))
        EXIT_CODE_BROTLI=$(( DO_BROTLI == 1 ? 1 : 0 ))
        EXIT_CODE_HDT=$(( DO_HDT == 1 ? 1 : 0 ))
        WALL_SEC_GZIP="null"
        USER_SEC_GZIP="null"
        SYS_SEC_GZIP="null"
        MAX_RSS_KB_GZIP="null"
        WALL_SEC_BROTLI="null"
        USER_SEC_BROTLI="null"
        SYS_SEC_BROTLI="null"
        MAX_RSS_KB_BROTLI="null"
        WALL_SEC_HDT="null"
        USER_SEC_HDT="null"
        SYS_SEC_HDT="null"
        MAX_RSS_KB_HDT="null"
        OVERALL_EXIT=1
      else
        NQ_SIZE=$(stat_size "$SOURCE_RDF")
      fi
    else
      SOURCE_RDF=""
      SOURCE_EXT=""
      NQ_SIZE=0
    fi

    # ----- gzip combined RDF with timing -----
    if (( DO_GZIP == 1 )) && [[ -n "${SOURCE_RDF:-}" ]]; then
      GZ_PATH="$GZIP_ROOT/${BASENAME}.${SOURCE_EXT}.gz"
      EXIT_CODE_GZIP=0

      if have_gnu_time; then
        /usr/bin/time -v -o "$TIME_LOG_GZIP" -- gzip -c "$SOURCE_RDF" > "$GZ_PATH" || EXIT_CODE_GZIP=$?
      else
        { time -p gzip -c "$SOURCE_RDF" > "$GZ_PATH"; } >"$TIME_LOG_GZIP" 2>&1 || EXIT_CODE_GZIP=$?
      fi

      GZ_SIZE=$(stat_size "$GZ_PATH")

      WALL_SEC_GZIP=""
      USER_SEC_GZIP=""
      SYS_SEC_GZIP=""
      MAX_RSS_KB_GZIP=""

      if have_gnu_time; then
        ELAPSED=$(awk -F': ' '/Elapsed \(wall clock\) time/ {print $2}' "$TIME_LOG_GZIP")
        WALL_SEC_GZIP=$(printf "%s" "$ELAPSED" | elapsed_to_seconds)

        USER_SEC_GZIP=$(awk -F': ' '/User time \(seconds\)/ {print $2}' "$TIME_LOG_GZIP")
        SYS_SEC_GZIP=$(awk -F': '  '/System time \(seconds\)/ {print $2}' "$TIME_LOG_GZIP")
        MAX_RSS_KB_GZIP=$(awk -F': ' '/Maximum resident set size/ {print $2}' "$TIME_LOG_GZIP")
      else
        WALL_SEC_GZIP=$(awk '/^real/ {print $2}' "$TIME_LOG_GZIP")
        USER_SEC_GZIP=$(awk '/^user/ {print $2}' "$TIME_LOG_GZIP")
        SYS_SEC_GZIP=$(awk  '/^sys/  {print $2}' "$TIME_LOG_GZIP")
        MAX_RSS_KB_GZIP=""
      fi

      [[ -z "$MAX_RSS_KB_GZIP" ]] && MAX_RSS_KB_GZIP="null"
      if [[ "$EXIT_CODE_GZIP" -ne 0 ]]; then
        OVERALL_EXIT=1
      fi
    else
      GZ_PATH=""
      GZ_SIZE=0
      EXIT_CODE_GZIP=0
      WALL_SEC_GZIP="null"
      USER_SEC_GZIP="null"
      SYS_SEC_GZIP="null"
      MAX_RSS_KB_GZIP="null"
    fi

    # ----- brotli combined RDF with timing -----
    if (( DO_BROTLI == 1 )) && [[ -n "${SOURCE_RDF:-}" ]]; then
      BROTLI_PATH="$BROTLI_ROOT/${BASENAME}.${SOURCE_EXT}.br"
      EXIT_CODE_BROTLI=0

      if have_gnu_time; then
        /usr/bin/time -v -o "$TIME_LOG_BROTLI" -- brotli -q 7 -c "$SOURCE_RDF" > "$BROTLI_PATH" || EXIT_CODE_BROTLI=$?
      else
        { time -p brotli -q 7 -c "$SOURCE_RDF" > "$BROTLI_PATH"; } >"$TIME_LOG_BROTLI" 2>&1 || EXIT_CODE_BROTLI=$?
      fi

      BROTLI_SIZE=$(stat_size "$BROTLI_PATH")

      WALL_SEC_BROTLI=""
      USER_SEC_BROTLI=""
      SYS_SEC_BROTLI=""
      MAX_RSS_KB_BROTLI=""

      if have_gnu_time; then
        ELAPSED=$(awk -F': ' '/Elapsed \(wall clock\) time/ {print $2}' "$TIME_LOG_BROTLI")
        WALL_SEC_BROTLI=$(printf "%s" "$ELAPSED" | elapsed_to_seconds)
        USER_SEC_BROTLI=$(awk -F': ' '/User time \(seconds\)/ {print $2}' "$TIME_LOG_BROTLI")
        SYS_SEC_BROTLI=$(awk -F': '  '/System time \(seconds\)/ {print $2}' "$TIME_LOG_BROTLI")
        MAX_RSS_KB_BROTLI=$(awk -F': ' '/Maximum resident set size/ {print $2}' "$TIME_LOG_BROTLI")
      else
        WALL_SEC_BROTLI=$(awk '/^real/ {print $2}' "$TIME_LOG_BROTLI")
        USER_SEC_BROTLI=$(awk '/^user/ {print $2}' "$TIME_LOG_BROTLI")
        SYS_SEC_BROTLI=$(awk  '/^sys/  {print $2}' "$TIME_LOG_BROTLI")
        MAX_RSS_KB_BROTLI=""
      fi

      [[ -z "$MAX_RSS_KB_BROTLI" ]] && MAX_RSS_KB_BROTLI="null"
      if [[ "$EXIT_CODE_BROTLI" -ne 0 ]]; then
        OVERALL_EXIT=1
      fi
    else
      BROTLI_PATH=""
      BROTLI_SIZE=0
      EXIT_CODE_BROTLI=0
      WALL_SEC_BROTLI="null"
      USER_SEC_BROTLI="null"
      SYS_SEC_BROTLI="null"
      MAX_RSS_KB_BROTLI="null"
    fi

    # ----- Convert combined RDF to HDT with timing -----
    if (( DO_HDT == 1 )) && [[ -n "${SOURCE_RDF:-}" ]]; then
      HDT_PATH="$HDT_ROOT/$BASENAME.hdt"
      EXIT_CODE_HDT=0

      if have_gnu_time; then
        /usr/bin/time -v -o "$TIME_LOG_HDT" -- bash "$HDT" "$SOURCE_RDF" "$HDT_PATH" || EXIT_CODE_HDT=$?
      else
        { time -p bash "$HDT" "$SOURCE_RDF" "$HDT_PATH"; } >"$TIME_LOG_HDT" 2>&1 || EXIT_CODE_HDT=$?
      fi

      HDT_SIZE=$(stat_size "$HDT_PATH")

      WALL_SEC_HDT=""
      USER_SEC_HDT=""
      SYS_SEC_HDT=""
      MAX_RSS_KB_HDT=""

      if have_gnu_time; then
        ELAPSED=$(awk -F': ' '/Elapsed \(wall clock\) time/ {print $2}' "$TIME_LOG_HDT")
        WALL_SEC_HDT=$(printf "%s" "$ELAPSED" | elapsed_to_seconds)

        USER_SEC_HDT=$(awk -F': ' '/User time \(seconds\)/ {print $2}' "$TIME_LOG_HDT")
        SYS_SEC_HDT=$(awk -F': '  '/System time \(seconds\)/ {print $2}' "$TIME_LOG_HDT")
        MAX_RSS_KB_HDT=$(awk -F': ' '/Maximum resident set size/ {print $2}' "$TIME_LOG_HDT")
      else
        WALL_SEC_HDT=$(awk '/^real/ {print $2}' "$TIME_LOG_HDT")
        USER_SEC_HDT=$(awk '/^user/ {print $2}' "$TIME_LOG_HDT")
        SYS_SEC_HDT=$(awk  '/^sys/  {print $2}' "$TIME_LOG_HDT")
        MAX_RSS_KB_HDT=""
      fi

      [[ -z "$MAX_RSS_KB_HDT" ]] && MAX_RSS_KB_HDT="null"
      if [[ "$EXIT_CODE_HDT" -ne 0 ]]; then
        if [[ "$EXIT_CODE_HDT" -eq 127 ]]; then
          echo "ERROR: HDT conversion failed with exit code 127 (command not found)." >&2
          echo "ERROR: Check dependencies used by '$HDT' and the timing log '$TIME_LOG_HDT'." >&2
          if [[ -f "$TIME_LOG_HDT" ]]; then
            tail -n 20 "$TIME_LOG_HDT" >&2 || true
          fi
        fi
        OVERALL_EXIT=1
      fi
    else
      HDT_PATH=""
      HDT_SIZE=0
      EXIT_CODE_HDT=0
      WALL_SEC_HDT="null"
      USER_SEC_HDT="null"
      SYS_SEC_HDT="null"
      MAX_RSS_KB_HDT="null"
    fi
  fi

  cat > "$METRICS_JSON" <<EOF
{
  "run_id": "$RUN_ID",
  "timestamp": "$TIMESTAMP",
  "output_dir": "$OUT",
  "output_name": "$BASENAME",
  "compression_methods": "$COMPRESSION_METHODS",
  "output_triples": $TRIPLES_JSON,
  "combined_nq_path": "${SOURCE_RDF:-}",
  "combined_nq_size_bytes": ${NQ_SIZE:-0},
  "gzip": {
    "output_gz_path": "${GZ_PATH:-}",
    "output_gz_size_bytes": ${GZ_SIZE:-0},
    "exit_code": ${EXIT_CODE_GZIP:-0},
    "timing": {
      "wall_seconds": ${WALL_SEC_GZIP:-null},
      "user_seconds": ${USER_SEC_GZIP:-null},
      "sys_seconds": ${SYS_SEC_GZIP:-null},
      "max_rss_kb": ${MAX_RSS_KB_GZIP:-null}
    }
  },
  "brotli": {
    "output_brotli_path": "${BROTLI_PATH:-}",
    "output_brotli_size_bytes": ${BROTLI_SIZE:-0},
    "exit_code": ${EXIT_CODE_BROTLI:-0},
    "timing": {
      "wall_seconds": ${WALL_SEC_BROTLI:-null},
      "user_seconds": ${USER_SEC_BROTLI:-null},
      "sys_seconds": ${SYS_SEC_BROTLI:-null},
      "max_rss_kb": ${MAX_RSS_KB_BROTLI:-null}
    }
  },
  "hdt_conversion": {
    "output_hdt_path": "${HDT_PATH:-}",
    "output_hdt_size_bytes": ${HDT_SIZE:-0},
    "exit_code": ${EXIT_CODE_HDT:-0},
    "timing": {
      "wall_seconds": ${WALL_SEC_HDT:-null},
      "user_seconds": ${USER_SEC_HDT:-null},
      "sys_seconds": ${SYS_SEC_HDT:-null},
      "max_rss_kb": ${MAX_RSS_KB_HDT:-null}
    }
  }
}
EOF

  tmp_csv=$(mktemp)
  awk -F',' -v OFS=',' \
    -v run_id="$RUN_ID" \
    -v timestamp="$TIMESTAMP" \
    -v output_name="$BASENAME" \
    -v output_dir="$OUT" \
    -v combined_nq_size_bytes="${NQ_SIZE:-0}" \
    -v gzip_size_bytes="${GZ_SIZE:-0}" \
    -v brotli_size_bytes="${BROTLI_SIZE:-0}" \
    -v hdt_size_bytes="${HDT_SIZE:-0}" \
    -v exit_code_gzip="${EXIT_CODE_GZIP:-0}" \
    -v exit_code_brotli="${EXIT_CODE_BROTLI:-0}" \
    -v exit_code_hdt="${EXIT_CODE_HDT:-0}" \
    -v wall_seconds_gzip="$WALL_SEC_GZIP" \
    -v user_seconds_gzip="$USER_SEC_GZIP" \
    -v sys_seconds_gzip="$SYS_SEC_GZIP" \
    -v max_rss_kb_gzip="$MAX_RSS_KB_GZIP" \
    -v wall_seconds_brotli="$WALL_SEC_BROTLI" \
    -v user_seconds_brotli="$USER_SEC_BROTLI" \
    -v sys_seconds_brotli="$SYS_SEC_BROTLI" \
    -v max_rss_kb_brotli="$MAX_RSS_KB_BROTLI" \
    -v wall_seconds_hdt="$WALL_SEC_HDT" \
    -v user_seconds_hdt="$USER_SEC_HDT" \
    -v sys_seconds_hdt="$SYS_SEC_HDT" \
    -v max_rss_kb_hdt="$MAX_RSS_KB_HDT" \
    -v compression_methods="$COMPRESSION_METHODS_CSV" \
    -v conv_exit_code="" \
    -v conv_wall="" \
    -v conv_user="" \
    -v conv_sys="" \
    -v conv_rss="" \
    -v conv_in_size="" \
    -v conv_vcf_size="" \
    -v conv_out_size="" \
    -v conv_triples="" \
    -v conv_jar="" \
    -v conv_mapping="" \
    -v conv_output="" \
    'BEGIN { updated=0 }
     NR==1 { print; next }
     $1==run_id && $3==output_name {
       $17=combined_nq_size_bytes
       $18=gzip_size_bytes
       $19=brotli_size_bytes
       $20=hdt_size_bytes
       $21=exit_code_gzip
       $22=exit_code_brotli
       $23=exit_code_hdt
       $24=wall_seconds_gzip
       $25=user_seconds_gzip
       $26=sys_seconds_gzip
       $27=max_rss_kb_gzip
       $28=wall_seconds_brotli
       $29=user_seconds_brotli
       $30=sys_seconds_brotli
       $31=max_rss_kb_brotli
       $32=wall_seconds_hdt
       $33=user_seconds_hdt
       $34=sys_seconds_hdt
       $35=max_rss_kb_hdt
       $36=compression_methods
       updated=1
     }
     { print }
     END {
       if (updated==0) {
         print run_id,timestamp,output_name,output_dir,conv_exit_code,conv_wall,conv_user,conv_sys,conv_rss,conv_in_size,conv_vcf_size,conv_out_size,conv_triples,conv_jar,conv_mapping,conv_output,combined_nq_size_bytes,gzip_size_bytes,brotli_size_bytes,hdt_size_bytes,exit_code_gzip,exit_code_brotli,exit_code_hdt,wall_seconds_gzip,user_seconds_gzip,sys_seconds_gzip,max_rss_kb_gzip,wall_seconds_brotli,user_seconds_brotli,sys_seconds_brotli,max_rss_kb_brotli,wall_seconds_hdt,user_seconds_hdt,sys_seconds_hdt,max_rss_kb_hdt,compression_methods
       }
     }' "$METRICS_CSV" > "$tmp_csv"
  mv "$tmp_csv" "$METRICS_CSV"

  echo "Done for $OUT."
  echo "  JSON metrics: $METRICS_JSON"
  echo
done

echo "Compression finished."
echo "CSV summary: $METRICS_CSV"

if [[ "$OVERALL_EXIT" -ne 0 ]]; then
  echo "Compression completed with one or more errors." >&2
fi
exit "$OVERALL_EXIT"
