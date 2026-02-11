#!/usr/bin/env bash
set -euo pipefail

# Script: update_rules.sh
# Usage: ./update_rules.sh NEW_FILE_NAME
# Example: ./update_rules.sh my_sample.tsv

RULES_FILE="rules.ttl"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 NEW_FILE_NAME" >&2
  exit 1
fi

NEW_FILE="$1"

# Make sure rules.ttl exists
if [ ! -f "$RULES_FILE" ]; then
  echo "Error: $RULES_FILE not found in current directory." >&2
  exit 1
fi

# Escape '&' in the replacement text for sed
ESCAPED_NEW_FILE=${NEW_FILE//&/\\&}

# Replace the value in the csvw:url line, keep the rest intact
# Changes: csvw:url "something";  ->  csvw:url "NEW_FILE";
sed -i.bak -E "s#(csvw:url \")[^\"]*(\";)#\1${ESCAPED_NEW_FILE}\2#" "$RULES_FILE"

echo "Updated csvw:url in $RULES_FILE to \"${NEW_FILE}\""
echo "Backup created as ${RULES_FILE}.bak"