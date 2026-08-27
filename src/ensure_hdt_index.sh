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

HDT_JAVA_HOME=${HDT_JAVA_HOME:-/opt/hdt-java}
HDT_SEARCH_BIN=${HDT_SEARCH_BIN:-$HDT_JAVA_HOME/bin/hdtSearch.sh}
if [[ ! -x "$HDT_SEARCH_BIN" ]]; then
  echo "Missing HDT Java search launcher: $HDT_SEARCH_BIN" >&2
  exit 127
fi

# The HDT Java launcher defaults to a 1 GiB heap. Its default "recommended"
# indexer still builds and sorts object lists in that heap, so a large but
# otherwise valid HDT can fail here after the merge has already succeeded.
# HDT Java 3.0.10 provides an external-sort indexer for this case. Keep its
# sequences, bitmap sub-indexes, and sort runs in a disposable directory so
# peak heap usage is bounded by the indexer's chunk budget.
HDT_INDEX_WORK_ROOT=${HDT_INDEX_WORK_ROOT:-/work}
if [[ ! -d "$HDT_INDEX_WORK_ROOT" || ! -w "$HDT_INDEX_WORK_ROOT" ]]; then
  echo "HDT index work directory is not writable: $HDT_INDEX_WORK_ROOT" >&2
  exit 2
fi

INDEX_WORK_DIR=$(mktemp -d "$HDT_INDEX_WORK_ROOT/vcf-rdfizer-hdt-index.XXXXXXXX")
INDEX_BACKUP_DIR="$INDEX_WORK_DIR/existing-indexes"
mkdir -p "$INDEX_BACKUP_DIR"
INDEX_REGEN_COMPLETE=0
cleanup() {
  if [[ "$INDEX_REGEN_COMPLETE" -eq 0 ]]; then
    shopt -s nullglob
    for backup in "$INDEX_BACKUP_DIR"/*; do
      mv -- "$backup" "$(dirname "$HDT_PATH")/$(basename "$backup")"
    done
  fi
  rm -rf -- "$INDEX_WORK_DIR"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# `mapIndexedHDT()` reuses an existing sidecar when one is present. Move all
# versioned sidecars out of the way so this command really regenerates the
# index. If indexing fails, the EXIT trap restores the previous sidecars.
shopt -s nullglob
for existing_index in "${HDT_PATH}".index.*; do
  if [[ -e "$existing_index" ]]; then
    mv -- "$existing_index" "$INDEX_BACKUP_DIR/$(basename "$existing_index")"
  fi
done

HDT_INDEX_OPTIONS="bitmaptriples.indexmethod=disk;bitmaptriples.sequence.disk=true;bitmaptriples.sequence.disk.subindex=true;bitmaptriples.sequence.disk.location=$INDEX_WORK_DIR"

# hdtSearch uses HDT Java's mapIndexedHDT(), which creates the sibling index
# lazily. Sending only `exit` initializes the index without running a query or
# materializing an unbounded result set.
printf 'exit\n' | "$HDT_SEARCH_BIN" -quiet -options "$HDT_INDEX_OPTIONS" "$HDT_PATH" >/dev/null

shopt -s nullglob
# HDT Java 3.0.10 writes the v1-1 index as ``.hdt.index.v1-1``.
INDEX_CANDIDATES=("${HDT_PATH}".index.*)
INDEX_PATH=""
for candidate in "${INDEX_CANDIDATES[@]}"; do
  if [[ -f "$candidate" && -s "$candidate" ]]; then
    INDEX_PATH=$candidate
    break
  fi
done

if [[ -z "$INDEX_PATH" ]]; then
  echo "HDT Java did not create a non-empty index beside: $HDT_PATH" >&2
  exit 1
fi

INDEX_REGEN_COMPLETE=1
echo "HDT index ready: $INDEX_PATH"
