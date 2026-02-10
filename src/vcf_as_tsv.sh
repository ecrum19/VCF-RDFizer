#!/bin/bash
set -euo pipefail

# Usage check
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <input_dir> <output_dir>"
  exit 1
fi

input_dir="$1"
output_dir="$2"

if [ ! -d "$input_dir" ]; then
  echo "Error: input directory '$input_dir' not found."
  exit 1
fi

mkdir -p "$output_dir"

mapfile -t files < <(find "$input_dir" -maxdepth 1 -type f \( -name '*.vcf' -o -name '*.vcf.gz' \) | sort)

if [ "${#files[@]}" -eq 0 ]; then
  echo "No .vcf or .vcf.gz files found in '$input_dir'."
  exit 0
fi

for infile in "${files[@]}"; do
  base="$(basename "$infile")"
  if [[ "$base" == *.vcf.gz ]]; then
    reader_cmd=(gzip -dc)
    base="${base%.vcf.gz}"
  elif [[ "$base" == *.vcf ]]; then
    reader_cmd=(cat)
    base="${base%.vcf}"
  else
    echo "Skipping unsupported file: $infile"
    continue
  fi

  outfile="${output_dir}/${base}.tsv"

  # Process: skip metadata, find #CHROM line, strip '#', and output variants
  "${reader_cmd[@]}" "$infile" | awk '
    BEGIN { FS = OFS = "\t" }
    /^#CHROM\t/ {
      sub(/^#/, "", $1);  # remove leading # from first field (#CHROM -> CHROM)
      print;
      next
    }
    /^[^#]/ { print }
  ' > "$outfile"

  echo "✅ Wrote: $outfile"
done
