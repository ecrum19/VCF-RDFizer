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
  source_file="$base"
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

  records_out="${output_dir}/${base}.records.tsv"
  headers_out="${output_dir}/${base}.header_lines.tsv"
  metadata_out="${output_dir}/${base}.file_metadata.tsv"

  printf "SOURCE_FILE\tROW_ID\tCHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLES\n" > "$records_out"
  printf "SOURCE_FILE\tHEADER_INDEX\tHEADER_KEY\tHEADER_VALUE\tRAW_LINE\n" > "$headers_out"
  printf "SOURCE_FILE\tFILE_FORMAT\tFILE_DATE\tSOURCE_SOFTWARE\tREFERENCE_GENOME\tHEADER_COUNT\tRECORD_COUNT\n" > "$metadata_out"

  # Writes per-VCF records/header/metadata TSVs used by the mapping pipeline.
  "${reader_cmd[@]}" "$infile" | awk '
    function trim_cr(s) {
      sub(/\r$/, "", s)
      return s
    }

    BEGIN { FS = OFS = "\t" }
    /^##/ {
      raw = trim_cr(substr($0, 3))
      key = raw
      value = ""
      eq_pos = index(raw, "=")
      if (eq_pos > 0) {
        key = substr(raw, 1, eq_pos - 1)
        value = substr(raw, eq_pos + 1)
      }
      header_index++
      print source_file, header_index, key, value, raw >> headers_out

      key_lc = tolower(key)
      if (key_lc == "fileformat") {
        file_format = value
      } else if (key_lc == "filedate") {
        file_date = value
      } else if (key_lc == "source") {
        source_software = value
      } else if (key_lc == "reference") {
        reference_genome = value
      }
      next
    }
    /^#CHROM\t/ {
      for (i = 1; i <= NF; i++) {
        $i = trim_cr($i)
      }
      sub(/^#/, "", $1)
      header_index++
      print source_file, header_index, "CHROM_HEADER", $0, $0 >> headers_out
      next
    }
    /^[^#]/ {
      for (i = 1; i <= NF; i++) {
        $i = trim_cr($i)
      }

      row_id++
      chrom = $1
      pos = $2
      rec_id = $3
      ref = $4
      alt = $5
      qual = (NF >= 6 ? $6 : "")
      filter = (NF >= 7 ? $7 : "")
      info = (NF >= 8 ? $8 : "")
      format = (NF >= 9 ? $9 : "")
      samples = ""
      if (NF >= 10) {
        samples = $10
        for (i = 11; i <= NF; i++) {
          samples = samples "|" $i
        }
      }

      print source_file, row_id, chrom, pos, rec_id, ref, alt, qual, filter, info, format, samples >> records_out
      next
    }
    END {
      print source_file, file_format, file_date, source_software, reference_genome, header_index + 0, row_id + 0 >> metadata_out
    }
  ' source_file="$source_file" \
    records_out="$records_out" \
    headers_out="$headers_out" \
    metadata_out="$metadata_out"

  echo "✅ Wrote: $records_out"
  echo "✅ Wrote: $headers_out"
  echo "✅ Wrote: $metadata_out"
done
