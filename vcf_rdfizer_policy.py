#!/usr/bin/env python3
"""vcf-rdfizer-policy: attach ODRL policies to VCF graphs, evaluate them, and check the result.

    vcf-rdfizer-policy attach   --rdf P*.nt.gz --policy policy.ttl -o annotated.nt
    vcf-rdfizer-policy evaluate --rdf P*.nt.gz --policy policy.ttl --assignee IRI --purpose DUO:0000007 -o views/alz
    vcf-rdfizer-policy check    --view views/alz --policy policy.ttl --vcf P*.vcf
    vcf-rdfizer-policy explain  --policy policy.ttl

v0.1.0 demonstrator: governed release, not anonymization. Runs on the host; no
Docker. Exit codes: 0 success, 1 a check failed, 2 the policy, graph or request
cannot be evaluated. See docs/policy-demonstrator.md.
"""

import argparse
from pathlib import Path
import sys

from vcf_rdfizer_policies import VERSION, PolicyError


def _require_rdflib():
    try:
        import rdflib  # noqa: F401
    except ModuleNotFoundError:
        raise PolicyError("vcf-rdfizer-policy requires 'rdflib': python -m pip install rdflib") from None


def cmd_attach(args):
    from vcf_rdfizer_policies.graphs import load
    from vcf_rdfizer_policies.profile import load_policy
    from vcf_rdfizer_policies.release import attach

    policy_graph, rules = load_policy(args.policy)
    graph = load(args.rdf)
    out = Path(args.out)
    if out.exists():
        raise FileExistsError(f"{out} exists; attach never overwrites")
    counts = attach(graph, policy_graph, rules)
    out.write_text(graph.serialize(format="nt"), encoding="utf-8")
    for asset, n in sorted(counts.items()):
        print(f"{asset}: {n} resource(s) linked")
    print(f"wrote {out}")
    return 0


def cmd_evaluate(args):
    from vcf_rdfizer_policies.decide import Request
    from vcf_rdfizer_policies.graphs import load
    from vcf_rdfizer_policies.profile import load_policy, policy_digest
    from vcf_rdfizer_policies.purposes import purpose_iri
    from vcf_rdfizer_policies.release import evaluate, summary, write_release

    _, rules = load_policy(args.policy)
    request = Request(args.assignee, purpose_iri(args.purpose))
    release = evaluate(load(args.rdf), rules, request)
    write_release(release, args.out, policies={r.policy for r in rules},
                  digest=policy_digest(args.policy))
    counts = summary(release)
    print(f"released {counts['records_released']} record(s), withheld {counts['records_withheld']}; "
          f"{counts['triples_withheld']} triple(s) withheld -> {args.out}")
    return 0


def cmd_check(args):
    from vcf_rdfizer_policies.check import check_view
    from vcf_rdfizer_policies.profile import load_policy

    _, rules = load_policy(args.policy)
    failures = check_view(args.view, args.policy, rules, args.vcf)
    for failure in failures:
        print(f"FAIL {failure}")
    print("PASS" if not failures else f"{len(failures)} failure(s)")
    return 0 if not failures else 1


def cmd_explain(args):
    from vcf_rdfizer_policies.profile import load_policy

    _, rules = load_policy(args.policy)
    for rule in rules:
        who = "anyone" if rule.assignee is None else f"<{rule.assignee}>"
        terms = [f"purpose {c.operator} {', '.join(sorted(p.rsplit('/', 1)[1] for p in c.purposes))}"
                 for c in rule.constraints]
        print(f"{rule.label}: applies to {who}" + (" when " + " and ".join(terms) if terms else "")
              + (f"; duties: {', '.join(d.rsplit('/', 1)[1] for d in rule.duties)}" if rule.duties else ""))
    print("Deny wins; anything no permission covers is withheld.")
    return 0


def build_parser():
    parser = argparse.ArgumentParser(prog="vcf-rdfizer-policy", description=__doc__.split("\n\n")[0])
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    sub = parser.add_subparsers(dest="command", required=True)

    attach = sub.add_parser("attach", help="write the policies into the graph")
    attach.add_argument("--rdf", nargs="+", required=True, help="converted .nt / .nt.gz files")
    attach.add_argument("--policy", required=True, type=Path)
    attach.add_argument("-o", "--out", required=True, help="annotated .nt to create")
    attach.set_defaults(run=cmd_attach)

    evaluate = sub.add_parser("evaluate", help="write one request's release view")
    evaluate.add_argument("--rdf", nargs="+", required=True, help="converted .nt / .nt.gz files")
    evaluate.add_argument("--policy", required=True, type=Path)
    evaluate.add_argument("--assignee", required=True, help="the requesting party's IRI")
    evaluate.add_argument("--purpose", required=True, help="a DUO term, e.g. DUO:0000007")
    evaluate.add_argument("-o", "--out", required=True, type=Path, help="new or empty directory")
    evaluate.set_defaults(run=cmd_evaluate)

    check = sub.add_parser("check", help="verify a view against the source VCFs")
    check.add_argument("--view", required=True, type=Path, help="a directory written by evaluate")
    check.add_argument("--policy", required=True, type=Path)
    check.add_argument("--vcf", nargs="+", required=True, help="the source VCFs")
    check.set_defaults(run=cmd_check)

    explain = sub.add_parser("explain", help="list the policy's rules in plain language")
    explain.add_argument("--policy", required=True, type=Path)
    explain.set_defaults(run=cmd_explain)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        _require_rdflib()
        return args.run(args)
    except (PolicyError, FileExistsError, FileNotFoundError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
