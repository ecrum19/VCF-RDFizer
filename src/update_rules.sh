#!/usr/bin/env bash
set -euo pipefail

# Script: update_rules.sh
# Usage: ./update_rules.sh RECORDS_TSV [HEADERS_TSV] [METADATA_TSV] [RULES_FILE]
# Example: ./update_rules.sh /data/tsv/records.tsv /data/tsv/header_lines.tsv /data/tsv/file_metadata.tsv rules/default_rules.ttl

RULES_FILE=${RULES_FILE:-rules/default_rules.ttl}

if [ "$#" -lt 1 ] || [ "$#" -gt 4 ]; then
  echo "Usage: $0 RECORDS_TSV [HEADERS_TSV] [METADATA_TSV] [RULES_FILE]" >&2
  exit 1
fi

RECORDS_TSV="$1"
HEADERS_TSV="${2:-/data/tsv/header_lines.tsv}"
METADATA_TSV="${3:-/data/tsv/file_metadata.tsv}"
if [ "$#" -eq 4 ]; then
  RULES_FILE="$4"
fi

# Make sure rules file exists
if [ ! -f "$RULES_FILE" ]; then
  echo "Error: $RULES_FILE not found in current directory." >&2
  exit 1
fi

escape_replacement() {
  # Escape characters meaningful to sed replacement text.
  printf '%s' "$1" | sed -e 's/[&#]/\\&/g'
}

ESCAPED_RECORDS_TSV=$(escape_replacement "$RECORDS_TSV")
ESCAPED_HEADERS_TSV=$(escape_replacement "$HEADERS_TSV")
ESCAPED_METADATA_TSV=$(escape_replacement "$METADATA_TSV")

# Replace default TSV locations in the mapping.
sed -i.bak -E \
  -e "s#(csvw:url \")/data/tsv/records\\.tsv(\";)#\1${ESCAPED_RECORDS_TSV}\2#g" \
  -e "s#(csvw:url \")/data/tsv/header_lines\\.tsv(\";)#\1${ESCAPED_HEADERS_TSV}\2#g" \
  -e "s#(csvw:url \")/data/tsv/file_metadata\\.tsv(\";)#\1${ESCAPED_METADATA_TSV}\2#g" \
  "$RULES_FILE"

echo "Updated TSV source paths in $RULES_FILE"
echo "  records:  $RECORDS_TSV"
echo "  headers:  $HEADERS_TSV"
echo "  metadata: $METADATA_TSV"
echo "Backup created as ${RULES_FILE}.bak"
