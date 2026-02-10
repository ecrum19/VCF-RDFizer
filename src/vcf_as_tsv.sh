#!/bin/bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <input_path> <output_dir>"
  exit 1
fi

input_path="$1"
output_dir="$2"

mkdir -p "$output_dir"

files=()
if [ -f "$input_path" ]; then
  case "$input_path" in
    *.vcf|*.vcf.gz)
      files+=("$input_path")
      ;;
    *)
      echo "Error: input file must end with .vcf or .vcf.gz"
      exit 1
      ;;
  esac
elif [ -d "$input_path" ]; then
  mapfile -t files < <(find "$input_path" -maxdepth 1 -type f \( -name '*.vcf' -o -name '*.vcf.gz' \) | sort)
else
  echo "Error: input path '$input_path' not found."
  exit 1
fi

if [ "${#files[@]}" -eq 0 ]; then
  echo "No .vcf or .vcf.gz files found in '$input_path'."
  exit 1
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
