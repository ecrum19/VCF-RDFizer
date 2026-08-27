#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: ensure_hdt_index.sh <file.hdt>" >&2
  exit 2
fi

HDT_PATH=$1
if [[ ! -f "$HDT_PATH" ]]; then
  echo "HDT file not found: $HDT_PATH" >&2
  exit 2
fi

HDTC_BIN=${HDTC_BIN:-/usr/local/bin/hdtc}
if [[ ! -x "$HDTC_BIN" ]]; then
  echo "Missing Java-free HDT indexer: $HDTC_BIN" >&2
  exit 127
fi

# hdtc implements the canonical HDT v1-1 index format without a JVM. It streams
# BitmapTriples through disk-backed external sorters, so the memory setting is
# a soft buffer budget rather than a Java heap requirement. Keep sort runs in a
# disposable directory; callers may point the work root at a larger/faster disk.
HDT_INDEX_WORK_ROOT=${HDT_INDEX_WORK_ROOT:-/work}
HDT_INDEX_MEMORY_LIMIT=${HDT_INDEX_MEMORY_LIMIT:-512M}
if [[ ! -d "$HDT_INDEX_WORK_ROOT" || ! -w "$HDT_INDEX_WORK_ROOT" ]]; then
  echo "HDT index work directory is not writable: $HDT_INDEX_WORK_ROOT" >&2
  exit 2
fi

INDEX_WORK_DIR=$(mktemp -d "$HDT_INDEX_WORK_ROOT/vcf-rdfizer-hdt-index.XXXXXXXX")
INDEX_BACKUP_DIR="$INDEX_WORK_DIR/existing-indexes"
mkdir -p "$INDEX_BACKUP_DIR"
INDEX_REGEN_COMPLETE=0
cleanup() {
  cleanup_status=$?
  set +e
  if [[ "$INDEX_REGEN_COMPLETE" -eq 0 ]]; then
    shopt -s nullglob
    # Remove every incomplete replacement before restoring the known-good
    # sidecars. hdtc writes the final sibling directly, so interruption can
    # otherwise leave a partial file behind.
    for generated in "${HDT_PATH}".index.*; do
      rm -f -- "$generated"
    done
    for backup in "$INDEX_BACKUP_DIR"/*; do
      mv -- "$backup" "$(dirname "$HDT_PATH")/$(basename "$backup")"
    done
  fi
  rm -rf -- "$INDEX_WORK_DIR"
  trap - EXIT
  exit "$cleanup_status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# Move all versioned sidecars out of the way so this command really regenerates
# the index. If indexing fails, the EXIT trap restores the previous sidecars.
shopt -s nullglob
for existing_index in "${HDT_PATH}".index.*; do
  if [[ -e "$existing_index" ]]; then
    mv -- "$existing_index" "$INDEX_BACKUP_DIR/$(basename "$existing_index")"
  fi
done

# This is a dedicated index command: it does not query, rewrite, or decode the
# source HDT. The output is the hdt-java/hdt-cpp-compatible sibling sidecar.
"$HDTC_BIN" --quiet index "$HDT_PATH" \
  --memory-limit "$HDT_INDEX_MEMORY_LIMIT" \
  --temp-dir "$INDEX_WORK_DIR"

shopt -s nullglob
# hdtc writes the canonical v1-1 index as ``.hdt.index.v1-1``.
INDEX_CANDIDATES=("${HDT_PATH}".index.*)
INDEX_PATH=""
for candidate in "${INDEX_CANDIDATES[@]}"; do
  if [[ -f "$candidate" && -s "$candidate" ]]; then
    INDEX_PATH=$candidate
    break
  fi
done

if [[ -z "$INDEX_PATH" ]]; then
  echo "hdtc did not create a non-empty index beside: $HDT_PATH" >&2
  exit 1
fi

INDEX_REGEN_COMPLETE=1
echo "HDT index ready: $INDEX_PATH"
