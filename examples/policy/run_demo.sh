#!/usr/bin/env bash
# The v0.1.0 policy demonstrator end to end, without Docker:
# attach the policies, evaluate one release view per requester, check each view
# against the source VCFs, and print the decision grid.
#
#   examples/policy/run_demo.sh [OUT_DIR]      # default: ./policy-demo-out (must not exist)
#
# PROFILE=condensed runs it on the condensed graphs instead of the expanded ones.
set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
OUT="${1:-policy-demo-out}"
PROFILE="${PROFILE:-expanded}"
POLICY="$HERE/policy.ttl"
RDF=("$HERE"/converted/"$PROFILE"/P00*.nt.gz)
VCF=("$HERE"/P00*.vcf)
POLICY_CMD=(python3 "$HERE/../../vcf_rdfizer_policy.py")

[[ -e "$OUT" ]] && { echo "error: $OUT exists; choose a new directory" >&2; exit 2; }
mkdir -p "$OUT"

echo "== the policy"
"${POLICY_CMD[@]}" explain --policy "$POLICY"

echo; echo "== attach: policies in the graph"
"${POLICY_CMD[@]}" attach --rdf "${RDF[@]}" --policy "$POLICY" -o "$OUT/cohort-annotated.nt"

# One view per requester in fixture.json: key, assignee IRI, DUO purpose.
while read -r key assignee purpose; do
  echo; echo "== $key ($purpose)"
  "${POLICY_CMD[@]}" evaluate --rdf "${RDF[@]}" --policy "$POLICY" \
      --assignee "$assignee" --purpose "$purpose" -o "$OUT/views/$key"
  "${POLICY_CMD[@]}" check --view "$OUT/views/$key" --policy "$POLICY" --vcf "${VCF[@]}"
done < <(python3 -c 'import json,sys
for k, r in json.load(open(sys.argv[1]))["requesters"].items(): print(k, r["assignee"], r["purpose"])' "$HERE/fixture.json")

echo; echo "== decision grid (records released per file)"
python3 - "$OUT/views" <<'PY'
import json, sys
from pathlib import Path
views = sorted(Path(sys.argv[1]).iterdir())
summaries = {v.name: json.loads((v / "summary.json").read_text()) for v in views}
files = sorted(next(iter(summaries.values()))["files"])
print("requester".ljust(10) + "".join(f.split("//")[1].ljust(10) for f in files) + "triples withheld")
for name, s in summaries.items():
    cells = "".join((str(s["files"][f]["records_released"]) if s["files"][f]["released"] else "withheld").ljust(10)
                    for f in files)
    print(name.ljust(10) + cells + str(s["triples_withheld"]))
PY
