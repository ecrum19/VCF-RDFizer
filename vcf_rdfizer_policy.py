#!/usr/bin/env python3
"""vcf-rdfizer-policy: attach ODRL policies to RDF graphs, evaluate them, and check the result.

    vcf-rdfizer-policy explain  --policy policy.ttl
    vcf-rdfizer-policy attach   --rdf P*.nt.gz --policy policy.ttl -o annotated.nt
    vcf-rdfizer-policy evaluate --rdf P*.nt.gz --policy policy.ttl --assignee IRI --purpose DUO:0000007 -o views/alz
    vcf-rdfizer-policy check    --view views/alz --rdf P*.nt.gz --policy policy.ttl [--vcf P*.vcf]

Selectors and the ownership rule come from a profile (--profile, default the
bundled VCF Core profile; selector types may also be declared in the policy
file), and purposes from a vocabulary (--purposes, default a DUO subset).
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


def _setup(args):
    """(policy graph, profile, vocabulary, rules) from the common arguments."""
    from vcf_rdfizer_policies.policy import load_rules, read_graph
    from vcf_rdfizer_policies.profile import load_profile
    from vcf_rdfizer_policies.vocabulary import Vocabulary

    graph = read_graph(args.policy)
    profile = load_profile(args.profile, extra_graph=graph)
    vocabulary = Vocabulary.load(args.purposes)
    return graph, profile, vocabulary, load_rules(graph, profile, vocabulary)


def cmd_explain(args):
    _, profile, _, rules = _setup(args)
    for rule in rules:
        who = "anyone" if rule.assignee is None else f"<{rule.assignee}>"
        terms = [f"purpose {c.operator} {', '.join(sorted(p.rsplit('/', 1)[1] for p in c.purposes))}"
                 for c in rule.constraints]
        what = f" (a {rule.target.selector.iri.rsplit('#', 1)[-1]})" if hasattr(rule.target, "selector") else ""
        print(f"{rule.label}{what}: applies to {who}" + (" when " + " and ".join(terms) if terms else "")
              + (f"; duties: {', '.join(d.rsplit('/', 1)[1] for d in rule.duties)}" if rule.duties else ""))
    print(f"Deny wins; anything no permission covers is withheld. Profile: <{profile.iri}>.")
    return 0


def cmd_attach(args):
    from vcf_rdfizer_policies.graphs import load
    from vcf_rdfizer_policies.release import attach

    policy_graph, _, _, rules = _setup(args)
    out = Path(args.out)
    if out.exists():
        raise FileExistsError(f"{out} exists; attach never overwrites")
    graph = load(args.rdf)
    for asset, n in sorted(attach(graph, policy_graph, rules).items()):
        print(f"{asset}: {n} resource(s) linked")
    out.write_text(graph.serialize(format="nt"), encoding="utf-8")
    print(f"wrote {out}")
    return 0


def cmd_evaluate(args):
    from vcf_rdfizer_policies.engine import Request
    from vcf_rdfizer_policies.graphs import load
    from vcf_rdfizer_policies.policy import policy_digest
    from vcf_rdfizer_policies.release import evaluate, summary, write_release

    _, profile, vocabulary, rules = _setup(args)
    request = Request(args.assignee, vocabulary.resolve(args.purpose))
    release = evaluate(load(args.rdf), rules, request, profile, vocabulary)
    write_release(release, args.out, policies={r.policy for r in rules}, digest=policy_digest(args.policy))
    counts = summary(release)
    print(f"released {counts['records_released']} record(s), withheld {counts['records_withheld']}; "
          f"{counts['triples_withheld']} triple(s) withheld -> {args.out}")
    return 0


def cmd_check(args):
    from vcf_rdfizer_policies.check import check_view, read_view
    from vcf_rdfizer_policies.graphs import load
    from vcf_rdfizer_policies.vcf_oracle import compare

    _, profile, vocabulary, rules = _setup(args)
    view, manifest, request = read_view(args.view)
    failures = check_view(view, manifest, request, policy_path=args.policy, rules=rules,
                          profile=profile, vocabulary=vocabulary, source=load(args.rdf))
    if args.vcf:
        failures += compare(view, args.vcf, rules=rules, request=request, profile=profile, vocabulary=vocabulary)
    for failure in failures:
        print(f"FAIL {failure}")
    print("PASS" if not failures else f"{len(failures)} failure(s)")
    return 0 if not failures else 1


def build_parser():
    parser = argparse.ArgumentParser(prog="vcf-rdfizer-policy", description=__doc__.split("\n\n")[0])
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--policy", required=True, type=Path, help="ODRL policy file (Turtle)")
    common.add_argument("--profile", action="append", default=[],
                        help="profile file, or 'vcf-core' (default); repeatable")
    common.add_argument("--purposes", type=Path, help="RDFS/SKOS purpose vocabulary (default: a DUO subset)")
    sub = parser.add_subparsers(dest="command", required=True)

    explain = sub.add_parser("explain", parents=[common], help="list the policy's rules in plain language")
    explain.set_defaults(run=cmd_explain)

    attach = sub.add_parser("attach", parents=[common], help="write the policies into the graph")
    attach.add_argument("--rdf", nargs="+", required=True, help=".nt / .nt.gz inputs")
    attach.add_argument("-o", "--out", required=True, help="annotated .nt to create")
    attach.set_defaults(run=cmd_attach)

    evaluate = sub.add_parser("evaluate", parents=[common], help="write one request's release view")
    evaluate.add_argument("--rdf", nargs="+", required=True, help=".nt / .nt.gz inputs")
    evaluate.add_argument("--assignee", required=True, help="the requesting party's IRI")
    evaluate.add_argument("--purpose", required=True, help="a vocabulary term, e.g. DUO:0000007")
    evaluate.add_argument("-o", "--out", required=True, type=Path, help="new or empty directory")
    evaluate.set_defaults(run=cmd_evaluate)

    check = sub.add_parser("check", parents=[common], help="verify a view against its source")
    check.add_argument("--view", required=True, type=Path, help="a directory written by evaluate")
    check.add_argument("--rdf", nargs="+", required=True, help="the source the view was made from")
    check.add_argument("--vcf", nargs="+", help="source VCFs, for the independent record oracle")
    check.set_defaults(run=cmd_check)
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
