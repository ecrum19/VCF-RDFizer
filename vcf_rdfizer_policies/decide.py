"""The v0.1.0 decision rules (docs/policy-demonstrator.md §4) -- the one place they live.

Both `evaluate` and the oracle in `check` call these functions, so the rules
cannot drift apart. The oracle's independence comes from its input (the VCF
text, not the graph), not from a second copy of the rules.
"""

from dataclasses import dataclass

from . import PolicyError
from .profile import FileTarget, RegionTarget, VariantTarget
from .purposes import within


@dataclass(frozen=True)
class Request:
    assignee: str
    purpose: str      # a DUO IRI


@dataclass(frozen=True)
class Decision:
    released: bool
    reason: str


def applies(rule, request) -> bool:
    """Does the rule bind this request: assignee matches, and every constraint holds?"""
    if rule.assignee is not None and rule.assignee != request.assignee:
        return False
    for constraint in rule.constraints:
        inside = any(within(request.purpose, term) for term in constraint.purposes)
        if inside != (constraint.operator == "isAnyOf"):
            return False
    return True


def covers(target, subject) -> bool:
    """Does the target select this subject (a Record, or a file IRI)?"""
    file_iri = subject if isinstance(subject, str) else subject.file
    if isinstance(target, FileTarget):
        return target.iri == file_iri
    if isinstance(subject, str):
        return False      # region and variant targets select records, never whole files
    if isinstance(target, RegionTarget):
        return subject.chrom == target.chrom and target.start <= subject.pos <= target.end
    if isinstance(target, VariantTarget):
        return (subject.chrom, subject.pos, subject.ref) == (target.chrom, target.pos, target.ref) \
            and target.alt in subject.alts
    raise PolicyError(f"unknown target {target!r}")


def check_assemblies(rules, file_assemblies: dict) -> None:
    """Every region or variant rule must name the assembly every file declares."""
    for rule in rules:
        wanted = getattr(rule.target, "assembly", None)
        for file_iri, declared in file_assemblies.items():
            if wanted is not None and declared != wanted:
                raise PolicyError(f"{rule.label} is for {wanted}, but <{file_iri}> declares "
                                  f"{declared or 'no reference genome'}")


def decide(subject, rules, request) -> Decision:
    """Release iff a binding permission covers the file and no binding prohibition covers the subject."""
    binding = [rule for rule in rules if applies(rule, request)]
    for rule in binding:
        if rule.kind == "prohibition" and covers(rule.target, subject):
            return Decision(False, f"withheld: {rule.label}")
    file_iri = subject if isinstance(subject, str) else subject.file
    for rule in binding:
        if rule.kind == "permission" and covers(rule.target, file_iri):
            return Decision(True, f"released: {rule.label}")
    return Decision(False, f"withheld: no permission covers <{file_iri}> for this purpose")
