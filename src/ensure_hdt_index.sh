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

# hdtSearch uses HDT Java's mapIndexedHDT(), which creates the sibling index
# lazily. Sending only `exit` initializes the index without running a query or
# materializing an unbounded result set.
printf 'exit\n' | "$HDT_SEARCH_BIN" "$HDT_PATH" >/dev/null

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

echo "HDT index ready: $INDEX_PATH"
