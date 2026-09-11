#!/usr/bin/env python3
"""VCF Core vocabulary terms and the VCF 4.5 parsers the RDF emitters need.

The conversion splits in two. The RML mapping in ``rules/default_rules.ttl``
carries every field whose RDF datatype is the same for every row of the TSV.
Everything else lives here and in the ``append_*_rdf`` emitters of
``vcf_rdfizer.py``: a value whose datatype depends on the row (QUAL, ALT, ID,
FILTER, INFO), and any value that has to be decomposed before it can be
represented (a structured header line, an ALT list, a GT string, a
comma-separated ``Number=A``/``R``/``G`` payload, a breakend expression).

This module holds only pure functions and constants, so the parsers can be
tested without producing RDF.

Target vocabulary: VCF Core (``https://w3id.org/vcf-core/vocab#``), which
replaces the retired ``https://w3id.org/vcf-rdfizer/vocab#`` namespace. No
version IRI is pinned here: the converter targets the namespace, and the
vocabulary's own release cadence is independent of this converter's.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import quote_plus

#: The VCF Core vocabulary namespace. Formerly
#: ``https://w3id.org/vcf-rdfizer/vocab#``, which is retired and serves a
#: deprecation document rather than redirecting here.
VCFC_NAMESPACE = "https://w3id.org/vcf-core/vocab#"

RDF_TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
FALDO_NAMESPACE = "http://biohackathon.org/resource/faldo#"

XSD_NAMESPACE = "http://www.w3.org/2001/XMLSchema#"
XSD_STRING_URI = f"{XSD_NAMESPACE}string"
XSD_INTEGER_URI = f"{XSD_NAMESPACE}integer"
XSD_DECIMAL_URI = f"{XSD_NAMESPACE}decimal"
XSD_BOOLEAN_URI = f"{XSD_NAMESPACE}boolean"
XSD_DATE_URI = f"{XSD_NAMESPACE}date"
XSD_ANY_URI = f"{XSD_NAMESPACE}anyURI"

#: Every ordinal in the published SHACL profiles is constrained with
#: ``sh:datatype xsd:integer`` and a ``sh:minInclusive`` bound, even where the
#: ontology declares ``rdfs:range xsd:positiveInteger``. A SHACL datatype
#: constraint compares the literal's datatype IRI exactly, so an
#: ``xsd:positiveInteger`` literal fails the shape while an ``xsd:integer``
#: literal satisfies both the shape and the range (the value 1 is in the value
#: space of xsd:positiveInteger however it is written). Ordinals are therefore
#: always serialized as xsd:integer.
XSD_ORDINAL_URI = XSD_INTEGER_URI


def term(local_name: str) -> str:
    """Return the absolute IRI of one VCF Core term."""
    return f"{VCFC_NAMESPACE}{local_name}"


NULL_DATATYPE_URI = term("Null")
VCF_FLOAT_DATATYPE_URI = term("VCFFloat")
GENOTYPE_STRING_DATATYPE_URI = term("GenotypeString")
BREAKEND_STRING_DATATYPE_URI = term("BreakendString")


# ---------------------------------------------------------------------------
# Lexical serialization
# ---------------------------------------------------------------------------

# Characters that ``quote_plus(..., safe="*-._")`` leaves untouched. Sample ids,
# FORMAT keys and numeric row ids are almost always drawn from this set, so the
# fast path below skips percent-encoding entirely for them.
_URI_COMPONENT_PASSTHROUGH_RE = re.compile(r"[A-Za-z0-9*\-._]*\Z")
# Characters that require N-Triples escaping. VCF genotype/FORMAT payloads are
# overwhelmingly free of them, so testing before substituting is worthwhile.
_NTRIPLES_ESCAPE_RE = re.compile(r'[\\"\n\r\t]')
_NTRIPLES_ESCAPES = {
    "\\": "\\\\",
    '"': '\\"',
    "\n": "\\n",
    "\r": "\\r",
    "\t": "\\t",
}


def rml_uri_component(value: str) -> str:
    """Match RMLStreamer's Java URLEncoder-based template substitution.

    The wrapper's directly emitted IRIs have to be byte-identical to the ones
    RMLStreamer mints from the same value, or the two halves of the graph would
    describe different resources.
    """
    if _URI_COMPONENT_PASSTHROUGH_RE.match(value) is not None:
        return value
    encoded = quote_plus(value, safe="*-._", encoding="utf-8", errors="strict")
    # urllib follows current RFC rules and always leaves '~' unescaped, whereas
    # java.net.URLEncoder (used by RMLStreamer 2.5.0) encodes it.
    return encoded.replace("+", "%20").replace("~", "%7E")


def ntriples_string_literal(value: str) -> str:
    """Serialize ``value`` as a plain (xsd:string) N-Triples literal."""
    if _NTRIPLES_ESCAPE_RE.search(value):
        value = _NTRIPLES_ESCAPE_RE.sub(lambda m: _NTRIPLES_ESCAPES[m.group(0)], value)
    return f'"{value}"'


def ntriples_typed_literal(value: str, datatype_uri: str) -> str:
    """Serialize ``value`` as an N-Triples literal with an explicit datatype."""
    return f"{ntriples_string_literal(value)}^^<{datatype_uri}>"


def ntriples_literal(value: str) -> str:
    """Serialize a VCF value, typing the missing token as ``vcfc:Null``.

    The vocabulary's missing-value policy is that the VCF token ``.`` is
    serialized as ``"."^^vcfc:Null`` rather than as a plain string, so that
    missingness stays distinguishable from a literal period.
    """
    if value == ".":
        return f'"."^^<{NULL_DATATYPE_URI}>'
    return ntriples_string_literal(value)


def ordinal_literal(value: int | str) -> str:
    """Serialize a one-based or zero-based ordinal as xsd:integer."""
    return f'"{value}"^^<{XSD_ORDINAL_URI}>'


def is_missing(value: str | None) -> bool:
    """Return True for an absent value or the VCF missing token."""
    return value is None or value == "" or value == "."


# ---------------------------------------------------------------------------
# VCF lexical spaces
# ---------------------------------------------------------------------------

#: VCF 4.5 Float: a decimal/scientific form, or INF/INFINITY/NAN in any case,
#: optionally signed. The non-finite spellings have no XSD numeric datatype, so
#: they are serialized with vcfc:VCFFloat instead of being coerced.
_VCF_FLOAT_FINITE_RE = re.compile(r"^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$")
_VCF_FLOAT_SPECIAL_RE = re.compile(r"^[-+]?(INF|INFINITY|NAN)$", re.IGNORECASE)

#: VCF 4.5 reserves Integer values -2147483648..-2147483641 because BCF cannot
#: encode them; a value in that range is kept lexically rather than typed.
_VCF_INTEGER_RESERVED_LOW = -2147483648
_VCF_INTEGER_RESERVED_HIGH = -2147483641

_GENOTYPE_RE = re.compile(r"^[|/]?[0-9.]+([|/][0-9.]+)*$")


def is_vcf_float(value: str) -> bool:
    """Return True when ``value`` is in the VCF Float lexical space."""
    return bool(_VCF_FLOAT_FINITE_RE.match(value) or _VCF_FLOAT_SPECIAL_RE.match(value))


def is_finite_vcf_float(value: str) -> bool:
    """Return True for a VCF Float that xsd:decimal can carry without loss."""
    return bool(_VCF_FLOAT_FINITE_RE.match(value))


def is_vcf_integer(value: str) -> bool:
    """Return True for a VCF Integer outside the BCF-reserved range."""
    try:
        parsed = int(value)
    except ValueError:
        return False
    return not (_VCF_INTEGER_RESERVED_LOW <= parsed <= _VCF_INTEGER_RESERVED_HIGH)


def is_genotype_string(value: str) -> bool:
    """Return True when ``value`` is in the vcfc:GenotypeString lexical space."""
    return bool(_GENOTYPE_RE.match(value))


# ---------------------------------------------------------------------------
# VCF Number arities
# ---------------------------------------------------------------------------

#: Number token -> the vcfc:VCFNumberArity individual carrying that arityCode.
#: The SPARQL SHACL profile requires fieldArity and fieldNumber to agree, so the
#: two are always derived from the same source token.
ARITY_INDIVIDUALS = {
    "A": "ArityPerAlt",
    "R": "ArityPerAllele",
    "G": "ArityPerGenotype",
    ".": "ArityVariable",
    "LA": "ArityPerLocalAlt",
    "LR": "ArityPerLocalAllele",
    "LG": "ArityPerLocalGenotype",
    "P": "ArityPerGTAllele",
    "M": "ArityPerBaseModification",
}

#: Number tokens whose values are one-per-ALT-allele, so value item i joins ALT
#: allele i+1; and those that are one-per-allele including REF, where item i
#: joins allele i.
_ARITY_PER_ALT = {"A", "LA"}
_ARITY_PER_ALLELE = {"R", "LR"}
_ARITY_PER_GENOTYPE = {"G", "LG"}


def arity_individual(number: str) -> str | None:
    """Return the vcfc arity individual for a Number token, if it is symbolic."""
    return ARITY_INDIVIDUALS.get((number or "").strip())


def number_as_integer(number: str) -> int | None:
    """Return the fixed count declared by a Number token, if it is an integer."""
    token = (number or "").strip()
    if not token.isdigit():
        return None
    return int(token)


#: Keys that flatten fixed-width tuples into one comma list, and the width of
#: each tuple. Recording the width with vcfc:tupleArity lets a consumer regroup
#: them without knowing the key. Which keys are in this set, and whether the
#: list repeats per ALT allele, depends on the VCF version -- see VCFVersion.
TUPLE_ARITIES = {
    "CIPOS": 2,
    "CIEND": 2,
    "CILEN": 2,
    "CICN": 2,
    "CIRUC": 2,
    "CIRB": 2,
    "MEINFO": 4,
    "METRANS": 4,
}


@dataclass(frozen=True)
class ValueItemLink:
    """How one parsed value item joins back to the record's alleles.

    ``allele_index`` is the record-global allele index (0 for REF), which is
    what vcfc:forAllele points at. ``genotype_index`` and ``gt_allele_index``
    carry the Number=G and Number=P orderings, which are ordinals rather than
    allele references. All three being None means the item has a position but no
    resource to join it to, which is correct for a version whose tuple keys
    describe the record rather than one allele.
    """

    allele_index: int | None = None
    genotype_index: int | None = None
    gt_allele_index: int | None = None

    @property
    def is_empty(self) -> bool:
        return (
            self.allele_index is None
            and self.genotype_index is None
            and self.gt_allele_index is None
        )


def value_item_link(number: str, item_index: int, *, tuple_arity: int | None) -> ValueItemLink:
    """Resolve which allele or ordinal one comma-list item belongs to.

    ``item_index`` is zero-based within the comma list. When the field flattens
    fixed-width tuples, the allele stride is the tuple width, so items 0 and 1
    of a per-ALT CIPOS list both belong to ALT allele 1.

    This is the version-neutral form, driven only by the declared Number token.
    Prefer :meth:`VCFVersion.value_item_link`, which also knows which keys are
    positional in a given version despite declaring ``Number=.``.
    """
    token = (number or "").strip()
    stride = tuple_arity or 1
    group = item_index // stride
    if token in _ARITY_PER_ALT:
        return ValueItemLink(allele_index=group + 1)
    if token in _ARITY_PER_ALLELE:
        return ValueItemLink(allele_index=group)
    if token in _ARITY_PER_GENOTYPE:
        return ValueItemLink(genotype_index=group)
    if token == "P":
        return ValueItemLink(gt_allele_index=group)
    return ValueItemLink()


# ---------------------------------------------------------------------------
# VCF versions
# ---------------------------------------------------------------------------
#
# VCF Core ships one SHACL overlay per VCF version, each scoped by the file's
# vcfc:fileFormat, plus an optional vcfc:VCF4xFile subclass that turns the
# version gate on. Three things the converter emits genuinely differ between
# versions, so the emitters take a VCFVersion rather than assuming 4.5:
#
#   * which Number codes exist at all -- R arrived in 4.2, P in 4.4, and
#     LA/LR/LG/M in 4.5;
#   * which INFO keys carry flattened tuples, and whether the tuple repeats per
#     ALT allele. CIPOS is two values for the whole record in 4.1-4.3 and two
#     values per ALT from 4.4; CILEN and CICN only become tuple keys in 4.4;
#   * whether the local-allele and base-modification FORMAT families exist.


@dataclass(frozen=True)
class VCFVersion:
    """One VCF specification version and the conversion behaviour it implies."""

    #: The exact ##fileformat token, e.g. "VCFv4.5".
    token: str
    #: The bare version, e.g. "4.5". This is what --vcf-version accepts.
    short: str
    #: The optional vcfc subclass that activates this version's SHACL gate.
    file_class: str
    #: Number codes this version permits in an INFO / FORMAT declaration.
    info_numbers: frozenset[str]
    format_numbers: frozenset[str]
    #: INFO keys whose value is a flattened tuple, and that tuple's width.
    tuple_keys: dict[str, int]
    #: True when a tuple key's list repeats once per ALT allele (4.4 onward);
    #: False when it carries exactly one tuple for the whole record.
    tuples_per_alt: bool
    #: Whether the LAA/LA/LR/LG local-allele family exists.
    local_alleles: bool
    #: Whether the M/DPM/ADM base-modification families exist.
    base_modifications: bool
    #: Whether GT may carry a leading phase indicator (4.4 onward).
    leading_phase_indicator: bool
    #: Whether EVENT names one event per ALT allele (Number=A, 4.4 onward)
    #: rather than one event for the whole record (Number=1, 4.1-4.3).
    events_per_alt: bool
    #: Whether the EVENTTYPE key exists at all (4.4 onward).
    event_types: bool

    def allows_number(self, number: str, *, is_format: bool) -> bool:
        """Whether a Number token is legal for this version and field kind."""
        token = (number or "").strip()
        if token.isdigit():
            return True
        allowed = self.format_numbers if is_format else self.info_numbers
        return token in allowed

    def tuple_arity(self, field_id: str) -> int | None:
        """The flattened-tuple width of an INFO key in this version, if any."""
        return self.tuple_keys.get(field_id)

    def is_positional(self, field_id: str, number: str) -> bool:
        """Whether a field's comma list has one meaning per position.

        A tuple key is positional even though the specification declares it
        ``Number=.``: the version overlay checks its item count against the ALT
        count, so its items must be materialized. Everything else is positional
        only when its declared Number says so.
        """
        if self.tuple_arity(field_id) is not None:
            return True
        return not value_item_link(number, 0, tuple_arity=None).is_empty

    def value_item_link(self, field_id: str, number: str, item_index: int) -> ValueItemLink:
        """Resolve one comma-list item's allele or ordinal, for this version."""
        arity = self.tuple_arity(field_id)
        if arity is not None:
            if not self.tuples_per_alt:
                # The tuple describes the record, not one allele, so the item
                # has an index but nothing to point vcfc:forAllele at.
                return ValueItemLink()
            return ValueItemLink(allele_index=(item_index // arity) + 1)
        return value_item_link(number, item_index, tuple_arity=None)


def _version(
    short: str,
    *,
    info_numbers: str,
    format_numbers: str,
    tuple_keys: dict[str, int],
    tuples_per_alt: bool,
    local_alleles: bool = False,
    base_modifications: bool = False,
    leading_phase_indicator: bool = False,
    events_per_alt: bool = False,
    event_types: bool = False,
) -> VCFVersion:
    return VCFVersion(
        token=f"VCFv{short}",
        short=short,
        file_class=f"VCF{short.replace('.', '')}File",
        info_numbers=frozenset(info_numbers.split()),
        format_numbers=frozenset(format_numbers.split()),
        tuple_keys=tuple_keys,
        tuples_per_alt=tuples_per_alt,
        local_alleles=local_alleles,
        base_modifications=base_modifications,
        leading_phase_indicator=leading_phase_indicator,
        events_per_alt=events_per_alt,
        event_types=event_types,
    )


#: Tuple keys before 4.4: one (lower, upper) pair per record, and MEINFO /
#: METRANS as one four-value tuple. CILEN and CICN are not yet defined.
_EARLY_TUPLE_KEYS = {"CIPOS": 2, "CIEND": 2, "MEINFO": 4, "METRANS": 4}
#: From 4.4 the confidence-interval family grew and every tuple repeats per ALT.
_LATE_TUPLE_KEYS = {
    "CIPOS": 2,
    "CIEND": 2,
    "CILEN": 2,
    "CICN": 2,
    "MEINFO": 4,
    "METRANS": 4,
}

#: The VCF versions VCF Core supplies a conformance overlay for. VCF 4.0 file
#: labels remain syntactically valid but no 4.0 overlay is claimed, so this
#: converter treats 4.0 as unrecognized: it emits the version-neutral graph and
#: says so, rather than validating a 4.0 file against 4.1 rules.
VCF_VERSIONS: dict[str, VCFVersion] = {
    version.short: version
    for version in (
        _version(
            "4.1",
            info_numbers="A G .",
            format_numbers="A G .",
            tuple_keys=_EARLY_TUPLE_KEYS,
            tuples_per_alt=False,
        ),
        _version(
            "4.2",
            info_numbers="A R G .",
            format_numbers="A R G .",
            tuple_keys=_EARLY_TUPLE_KEYS,
            tuples_per_alt=False,
        ),
        _version(
            "4.3",
            info_numbers="A R G .",
            format_numbers="A R G .",
            tuple_keys=_EARLY_TUPLE_KEYS,
            tuples_per_alt=False,
        ),
        _version(
            "4.4",
            info_numbers="A R G .",
            format_numbers="A R G . P",
            tuple_keys=_LATE_TUPLE_KEYS,
            tuples_per_alt=True,
            leading_phase_indicator=True,
            events_per_alt=True,
            event_types=True,
        ),
        _version(
            "4.5",
            info_numbers="A R G .",
            format_numbers="A R G . P LA LR LG M",
            tuple_keys=_LATE_TUPLE_KEYS,
            tuples_per_alt=True,
            local_alleles=True,
            base_modifications=True,
            leading_phase_indicator=True,
            events_per_alt=True,
            event_types=True,
        ),
    )
}

#: Used when the ##fileformat line is absent, malformed, or names a version with
#: no conformance overlay. It behaves as the newest supported version so no
#: representable content is silently dropped, but no vcfc:VCF4xFile class is
#: emitted, so no version gate is claimed for the graph.
FALLBACK_VERSION = VCF_VERSIONS["4.5"]

_FILEFORMAT_RE = re.compile(r"^\s*VCFv(?P<short>[0-9]+\.[0-9]+)\s*$", re.IGNORECASE)


def parse_vcf_version(file_format: str | None) -> VCFVersion | None:
    """Resolve a ##fileformat value to a supported version, or None.

    Returns None for an absent or malformed value, and for a syntactically valid
    version that has no conformance overlay (notably VCFv4.0). The caller
    decides what to do about it; nothing here guesses.
    """
    if not file_format:
        return None
    match = _FILEFORMAT_RE.match(file_format)
    if not match:
        return None
    return VCF_VERSIONS.get(match.group("short"))


def resolve_vcf_version(file_format: str | None) -> tuple[VCFVersion, bool]:
    """Return the version to convert with, and whether it was recognized."""
    detected = parse_vcf_version(file_format)
    if detected is not None:
        return detected, True
    return FALLBACK_VERSION, False


# ---------------------------------------------------------------------------
# Alleles
# ---------------------------------------------------------------------------

#: The reserved symbolic ALT identifiers VCF 4.5 defines, mapped to the
#: vcfc:SymbolicAlleleType individual for each. Subtypes are matched before
#: their first-level type so ``<DUP:TANDEM>`` does not resolve to ``DUP``.
SYMBOLIC_ALLELE_TYPES = {
    "CNV:TR": "SymbolicTandemRepeat",
    "DUP:TANDEM": "SymbolicTandemDuplication",
    "DEL:ME": "SymbolicMobileElementDeletion",
    "INS:ME": "SymbolicMobileElementInsertion",
    "DEL": "SymbolicDeletion",
    "INS": "SymbolicInsertion",
    "DUP": "SymbolicDuplication",
    "INV": "SymbolicInversion",
    "CNV": "SymbolicCopyNumberVariation",
}

#: The reserved EVENTTYPE codes, mapped to their vcfc:EventType individual.
EVENT_TYPES = {
    "DEL": "EventDeletion",
    "DEL:ME": "EventMobileElementDeletion",
    "INS": "EventInsertion",
    "INS:ME": "EventMobileElementInsertion",
    "DUP": "EventDuplication",
    "DUP:TANDEM": "EventTandemDuplication",
    "DUP:DISPERSED": "EventDispersedDuplication",
    "INV": "EventInversion",
    "TRA": "EventTranslocation",
    "TRA:BALANCED": "EventBalancedTranslocation",
    "TRA:UNBALANCED": "EventUnbalancedTranslocation",
    "CHROMOTHRIPSIS": "EventChromothripsis",
    "CHROMOPLEXY": "EventChromoplexy",
    "BFB": "EventBreakageFusionBridge",
    "DOUBLEMINUTE": "EventDoubleMinute",
}

#: The per-ALT SVCLAIM codes.
SV_CLAIMS = {
    "D": "AbundanceClaim",
    "J": "AdjacencyClaim",
    "DJ": "AbundanceAndAdjacencyClaim",
    "JD": "AbundanceAndAdjacencyClaim",
}

#: ALT forms that assert an unspecified alternate allele rather than a symbolic
#: variant type. gVCF uses <*>; GATK's older spelling is <NON_REF>.
_UNSPECIFIED_ALT_IDS = {"*", "NON_REF"}

_BREAKEND_RE = re.compile(
    r"^(?:"
    r"(?P<before>[ACGTNacgtn]*)(?P<open>[\[\]])(?P<mate>[^\[\]]+)(?P=open)"
    r"|"
    r"(?P<open2>[\[\]])(?P<mate2>[^\[\]]+)(?P=open2)(?P<after>[ACGTNacgtn]*)"
    r")$"
)
_SINGLE_BREAKEND_RE = re.compile(r"^(?:\.[ACGTNacgtn]+|[ACGTNacgtn]+\.)$")


@dataclass(frozen=True)
class ParsedAllele:
    """One allele of a record, classified by its VCF lexical form."""

    index: int
    value: str
    #: The vcfc:AlleleKind individual local name.
    kind: str
    #: The vcfc:SymbolicAlleleType individual local name, for a symbolic ALT
    #: whose identifier is one of the reserved codes.
    symbolic_type: str | None = None
    #: The bare symbolic identifier without angle brackets, used to join an ALT
    #: allele to the ##ALT declaration that defines it.
    symbolic_id: str | None = None
    #: Set for a bracketed or single breakend expression.
    breakend: "ParsedBreakend | None" = None


@dataclass(frozen=True)
class ParsedBreakend:
    """The queryable components of a VCF breakend ALT expression."""

    #: The vcfc:BreakendOrientation individual local name, or None for a single
    #: breakend, which has no bracket and therefore no orientation.
    orientation: str | None
    #: The replacement sequence on the record's own side of the adjacency.
    replacement: str
    #: The mate position, as written: ``chrom:pos``.
    mate: str | None
    is_single: bool = False


_ORIENTATIONS = {
    ("before", "["): "SequenceBeforeLeftBracket",
    ("before", "]"): "SequenceBeforeRightBracket",
    ("after", "["): "SequenceAfterLeftBracket",
    ("after", "]"): "SequenceAfterRightBracket",
}


def parse_breakend(value: str) -> ParsedBreakend | None:
    """Parse a VCF breakend ALT expression into its structural components.

    VCF 4.5 writes an adjacency in one of four bracketed forms -- ``t[p[``,
    ``t]p]``, ``[p[t`` and ``]p]t`` -- where ``t`` is the replacement sequence
    and ``p`` the mate position. A single breakend, whose partner is unknown,
    is written ``.t`` or ``t.`` and has no bracket.
    """
    match = _BREAKEND_RE.match(value)
    if match:
        if match.group("open"):
            bracket = match.group("open")
            return ParsedBreakend(
                orientation=_ORIENTATIONS[("before", bracket)],
                replacement=match.group("before"),
                mate=match.group("mate"),
            )
        bracket = match.group("open2")
        return ParsedBreakend(
            orientation=_ORIENTATIONS[("after", bracket)],
            replacement=match.group("after"),
            mate=match.group("mate2"),
        )
    if _SINGLE_BREAKEND_RE.match(value):
        return ParsedBreakend(
            orientation=None,
            replacement=value.replace(".", ""),
            mate=None,
            is_single=True,
        )
    return None


def classify_allele(value: str, index: int) -> ParsedAllele:
    """Classify one REF or ALT lexical form into a vcfc:AlleleKind.

    ``index`` is the record-global allele index: 0 for REF, then 1..n in source
    ALT order, which is the join key for every Number=A and Number=R value.
    """
    if value == ".":
        return ParsedAllele(index, value, "MissingAllele")
    if value == "*":
        return ParsedAllele(index, value, "OverlappingDeletionAllele")
    if value.startswith("<") and value.endswith(">"):
        identifier = value[1:-1]
        if identifier in _UNSPECIFIED_ALT_IDS:
            return ParsedAllele(
                index, value, "UnspecifiedAllele", symbolic_id=identifier
            )
        return ParsedAllele(
            index,
            value,
            "SymbolicAllele",
            symbolic_type=_symbolic_type_for(identifier),
            symbolic_id=identifier,
        )
    breakend = parse_breakend(value)
    if breakend is not None:
        return ParsedAllele(index, value, "BreakendAllele", breakend=breakend)
    return ParsedAllele(index, value, "BaseSequenceAllele")


def _symbolic_type_for(identifier: str) -> str | None:
    """Resolve a symbolic ALT identifier to its reserved type, subtype first.

    An unreserved identifier such as ``<MY_CALLER_EVENT>`` has no vcfc type;
    it stays a plain SymbolicAllele joined to its ##ALT declaration.
    """
    upper = identifier.upper()
    if upper in SYMBOLIC_ALLELE_TYPES:
        return SYMBOLIC_ALLELE_TYPES[upper]
    # A subtype the vocabulary does not enumerate still resolves to its
    # first-level type, which VCF 4.5 requires to be a reserved code.
    head = upper.split(":", 1)[0]
    return SYMBOLIC_ALLELE_TYPES.get(head)


#: VCF 4.5 allows CHROM to name a breakpoint assembly contig instead of a
#: reference sequence, written in angle brackets. The identifier may not contain
#: brackets, commas or whitespace.
_BRACKETED_CHROM_RE = re.compile(r"^<(?P<identifier>[^<>,\s]+)>$")


def parse_bracketed_chrom(chrom: str) -> str | None:
    """Return the assembly-contig identifier of a bracketed CHROM, else None.

    A bracketed CHROM names a contig from the ##assembly FASTA rather than a
    declared ##contig, so it links to a vcfc:AssemblyContig and must *not* also
    resolve to a contig declaration.
    """
    match = _BRACKETED_CHROM_RE.match(chrom or "")
    return match.group("identifier") if match else None


def parse_alt_alleles(ref: str, alt: str) -> list[ParsedAllele]:
    """Split a record's REF and ALT columns into indexed allele resources.

    Returns REF at index 0 followed by each ALT item in source order. An ALT of
    ``.`` means the record asserts no alternate allele, so only REF is returned.
    """
    alleles = [classify_allele(ref, 0)] if ref else []
    if is_missing(alt):
        return alleles
    for offset, item in enumerate(alt.split(","), start=1):
        alleles.append(classify_allele(item, offset))
    return alleles


# ---------------------------------------------------------------------------
# Genotypes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedGenotypeCall:
    """One allele position within a GT value."""

    call_index: int
    #: The record-global allele index, or None when the position is a no-call.
    allele_index: int | None

    @property
    def is_no_call(self) -> bool:
        return self.allele_index is None


@dataclass(frozen=True)
class ParsedGenotype:
    """A parsed GT value: its calls, ploidy, and phasing."""

    genotype_string: str
    calls: tuple[ParsedGenotypeCall, ...]
    #: True when every separator is ``|``. VCF permits a leading separator for
    #: partial phasing, which is treated as phased for the purposes of the
    #: vcfc:PhasingStatus individual.
    is_phased: bool

    @property
    def ploidy(self) -> int:
        return len(self.calls)


def parse_genotype(value: str) -> ParsedGenotype | None:
    """Parse a GT value into ordered allele calls and a phasing status.

    Returns None when the value is absent or not in the GT lexical space, so a
    malformed field is preserved as a plain FORMAT value rather than producing
    a Genotype resource that would fail vcfc:GenotypeShape.
    """
    if is_missing(value):
        return None
    if not is_genotype_string(value):
        return None
    body = value
    leading_phased = False
    if body[0] in "|/":
        leading_phased = body[0] == "|"
        body = body[1:]
    if not body:
        return None

    tokens = re.split(r"([|/])", body)
    allele_tokens = tokens[0::2]
    separators = tokens[1::2]

    calls = []
    for call_index, token in enumerate(allele_tokens):
        if token == "." or token == "":
            calls.append(ParsedGenotypeCall(call_index, None))
        else:
            try:
                calls.append(ParsedGenotypeCall(call_index, int(token)))
            except ValueError:
                return None

    is_phased = bool(separators) and all(sep == "|" for sep in separators)
    if not separators:
        # A haploid call carries phasing only through the optional leading
        # separator; without one there is nothing to phase against.
        is_phased = leading_phased
    return ParsedGenotype(value, tuple(calls), is_phased)


def genotype_index(allele_indices: list[int]) -> int:
    """VCF Number=G ordering: Index(k1..kP) = sum over m of C(k_m + m - 1, m).

    Used to place a Number=G value item against the genotype it scores.
    """
    from math import comb

    total = 0
    for position, allele in enumerate(sorted(allele_indices), start=1):
        total += comb(allele + position - 1, position)
    return total


# ---------------------------------------------------------------------------
# Structural variation, repeats and base modifications
# ---------------------------------------------------------------------------

#: INFO keys whose value is a per-ALT confidence interval expressed through
#: FALDO rather than a vcfc:ConfidenceInterval, per the SV module's design.
FALDO_INTERVAL_INFO_KEYS = {"CIPOS": "posConfidenceInterval", "CIEND": "endConfidenceInterval"}

#: INFO keys whose value is a per-ALT vcfc:ConfidenceInterval, and the property
#: that attaches it.
CONFIDENCE_INTERVAL_INFO_KEYS = {
    "CILEN": "lenConfidenceInterval",
    "CICN": "copyNumberConfidenceInterval",
}

#: INFO Flag keys that set a boolean directly on the ALT allele.
SV_FLAG_INFO_KEYS = {"IMPRECISE": "isImprecise", "NOVEL": "isNovel"}

#: Per-ALT INFO keys that become a datatype property on the allele.
SV_ALLELE_INFO_KEYS = {"SVLEN": "svLength"}

#: Tandem-repeat INFO keys, in the RN/RUS/RUL/RUC/RB order VCF 4.5 defines, and
#: the vcfc:RepeatSequence property each populates.
REPEAT_SEQUENCE_INFO_KEYS = {
    "RUS": "repeatUnitSequence",
    "RUL": "repeatUnitLength",
    "RUC": "repeatUnitCount",
    "RB": "repeatBases",
}

#: FORMAT keys that carry copy-number information on a sample call.
COPY_NUMBER_FORMAT_KEYS = {
    "CN": "copyNumber",
    "CNQ": "copyNumberQuality",
    "CNL": "copyNumberLikelihood",
    "CNP": "copyNumberPosterior",
}

#: FORMAT keys that carry haplotype identity on a sample call.
HAPLOTYPE_FORMAT_KEYS = {"HAP": "haplotypeId", "AHAP": "ancestralHaplotypeId"}

#: FORMAT keys that carry phase-set information.
PHASE_SET_FORMAT_KEYS = ("PS", "PSL", "PSO", "PSQ")

#: VCF 4.5 defines the base-modification FORMAT families by pattern rather than
#: by a single key: M, DPM and ADM followed by a ChEBI id and the modified base.
BASE_MODIFICATION_KEY_RE = re.compile(r"^(?P<family>M|DPM|ADM)(?P<chebi>[0-9]+)(?P<base>[ACGTUN])$")

#: The ChEBI namespace the SV module's vcfc:modifiedResidue points into.
CHEBI_NAMESPACE = "http://purl.obolibrary.org/obo/CHEBI_"

_BASE_MODIFICATION_PROPERTIES = {
    "M": "modificationFraction",
    "DPM": "modificationDepth",
    "ADM": "modificationAlleleDepth",
}


@dataclass(frozen=True)
class ParsedBaseModification:
    """A FORMAT key from the M/DPM/ADM base-modification families."""

    family: str
    chebi_id: str
    base: str
    #: The vcfc property that carries this family's value.
    value_property: str

    @property
    def residue_uri(self) -> str:
        return f"{CHEBI_NAMESPACE}{self.chebi_id}"

    @property
    def key(self) -> str:
        return f"{self.family}{self.chebi_id}{self.base}"


def parse_base_modification_key(key: str) -> ParsedBaseModification | None:
    """Recognize a base-modification FORMAT key and resolve its ChEBI residue."""
    match = BASE_MODIFICATION_KEY_RE.match(key)
    if not match:
        return None
    family = match.group("family")
    return ParsedBaseModification(
        family=family,
        chebi_id=match.group("chebi"),
        base=match.group("base"),
        value_property=_BASE_MODIFICATION_PROPERTIES[family],
    )


def split_value_items(value: str) -> list[str]:
    """Split a comma-separated VCF value payload into its ordered items."""
    if is_missing(value):
        return []
    return value.split(",")


def parse_local_allele_indices(value: str) -> list[int]:
    """Parse an LAA value into the one-based ALT indices it declares."""
    indices = []
    for item in split_value_items(value):
        if item.isdigit():
            parsed = int(item)
            if parsed >= 1:
                indices.append(parsed)
    return indices


# ---------------------------------------------------------------------------
# Header lines
# ---------------------------------------------------------------------------

#: '##' key (lower-cased) -> the vocabulary subclass for that line. Every entry
#: is a subclass of either vcfc:StructuredHeaderLine or
#: vcfc:UnstructuredHeaderLine, which decides whether the line needs
#: vcfc:HeaderAttribute resources.
HEADER_LINE_CLASSES = {
    "fileformat": "FileFormatHeaderLine",
    "filedate": "FileDateHeaderLine",
    "source": "SourceHeaderLine",
    "reference": "ReferenceHeaderLine",
    "assembly": "AssemblyHeaderLine",
    "pedigreedb": "PedigreeDBHeaderLine",
    "info": "INFOHeaderLine",
    "format": "FORMATHeaderLine",
    "filter": "FILTERHeaderLine",
    "alt": "ALTHeaderLine",
    "contig": "ContigHeaderLine",
    "meta": "MetaHeaderLine",
    "sample": "SampleHeaderLine",
    "pedigree": "PedigreeHeaderLine",
}

#: The subset of HEADER_LINE_CLASSES whose value is an angle-bracketed
#: attribute list. vcfc:StructuredHeaderLineShape requires at least one
#: vcfc:hasAttribute on each of these.
STRUCTURED_HEADER_KEYS = {
    "info",
    "format",
    "filter",
    "alt",
    "contig",
    "meta",
    "sample",
    "pedigree",
}

#: contig attribute -> vocabulary predicate, with the datatype each needs.
CONTIG_ATTRIBUTES = {
    "length": ("contigLength", XSD_INTEGER_URI),
    "md5": ("contigMd5", XSD_STRING_URI),
    "assembly": ("contigAssembly", XSD_STRING_URI),
    "URL": ("contigUrl", XSD_ANY_URI),
}

#: ##INFO attributes beyond ID/Number/Type/Description that the vocabulary
#: represents with a dedicated property.
INFO_EXTRA_ATTRIBUTES = {"Source": "fieldSource", "Version": "fieldVersion"}

#: ##PEDIGREE role attribute -> the vcfc property for that relation. Any other
#: role falls back to vcfc:pedigreeAncestor with a vcfc:ancestorRole literal.
PEDIGREE_ROLE_PROPERTIES = {
    "Mother": "pedigreeMother",
    "Father": "pedigreeFather",
    "Original": "pedigreeOriginal",
}

#: Maps a declared VCF INFO/FORMAT Type to its vocabulary value-type
#: individual. Since vocabulary 3.0.0 these are owl:NamedIndividual members of
#: vcfc:VCFValueType, not classes, so they are only ever used as the object of
#: vcfc:fieldType.
VCF_VALUE_TYPE_INDIVIDUALS = {
    "Integer": "IntegerType",
    "Float": "FloatType",
    "Flag": "FlagType",
    "Character": "CharacterType",
    "String": "StringType",
}


def is_structured_header_value(value: str) -> bool:
    """Return True when a header value is an angle-bracketed attribute list."""
    stripped = (value or "").strip()
    return stripped.startswith("<") and stripped.endswith(">")


def parse_structured_header_fields(value: str) -> dict[str, str]:
    """Parse comma-delimited VCF header attributes while respecting quotes."""
    return dict(parse_structured_header_attributes(value))


def _split_header_attributes(inner: str, *, bracket_aware: bool) -> tuple[list[str], int]:
    """Split a structured header body on its attribute-separating commas.

    A comma separates attributes only outside quotes and, when *bracket_aware*,
    outside a bracketed list. VCF 4.5 writes a ``##META`` allowed-value set as
    ``Values=[a, b, c]``, whose internal commas are part of one attribute;
    splitting on them drops every member after the first, because the fragments
    carry no ``=`` and are discarded as non-attributes.

    Returns the tokens and the final bracket depth, which is non-zero only when
    the input's brackets are unbalanced.
    """
    tokens: list[str] = []
    token: list[str] = []
    in_quotes = False
    escaped = False
    depth = 0
    for character in inner:
        if escaped:
            token.append(character)
            escaped = False
        elif character == "\\" and in_quotes:
            token.append(character)
            escaped = True
        elif character == '"':
            token.append(character)
            in_quotes = not in_quotes
        elif bracket_aware and character == "[" and not in_quotes:
            depth += 1
            token.append(character)
        elif bracket_aware and character == "]" and not in_quotes:
            # Clamped, so a stray ']' cannot make the depth negative and turn a
            # later, genuine '[' into a no-op.
            depth = max(0, depth - 1)
            token.append(character)
        elif character == "," and not in_quotes and depth == 0:
            tokens.append("".join(token))
            token = []
        else:
            token.append(character)
    tokens.append("".join(token))
    return tokens, depth


def parse_structured_header_attributes(value: str) -> list[tuple[str, str]]:
    """Parse a structured header value into ordered (key, value) attributes.

    Order is preserved because vcfc:HeaderAttribute carries a one-based
    vcfc:attributeIndex; VCF 4.5 forbids relying on source attribute order for
    meaning, but the vocabulary keeps it recoverable.
    """
    inner = (value or "").strip()
    if inner.startswith("<") and inner.endswith(">"):
        inner = inner[1:-1]

    tokens, depth = _split_header_attributes(inner, bracket_aware=True)
    if depth:
        # Unbalanced brackets are malformed. Bracket-aware splitting would treat
        # the rest of the line as one token and silently drop every attribute
        # after the stray '[', so fall back to splitting on commas alone.
        tokens, _ = _split_header_attributes(inner, bracket_aware=False)

    attributes: list[tuple[str, str]] = []
    for item in tokens:
        if "=" not in item:
            continue
        key, raw_value = item.split("=", 1)
        parsed_value = raw_value.strip()
        if len(parsed_value) >= 2 and parsed_value[0] == parsed_value[-1] == '"':
            parsed_value = parsed_value[1:-1]
            parsed_value = parsed_value.replace('\\"', '"').replace("\\\\", "\\")
        attributes.append((key.strip(), parsed_value))
    return attributes


def parse_info_entries(info: str) -> list[tuple[str, str | None]]:
    """Split an INFO column into ``(key, value)`` pairs.

    An entry without ``=`` is a Flag: a presence assertion with no value, which
    the vocabulary models as ``vcfc:fieldValueBoolean true``.
    """
    if info in ("", "."):
        return []
    entries: list[tuple[str, str | None]] = []
    for item in info.split(";"):
        if not item:
            continue
        key, separator, value = item.partition("=")
        entries.append((key, value if separator else None))
    return entries


#: ##fileDate has no mandated format. These are the two forms seen in practice
#: that map unambiguously onto xsd:date, which the SHACL shape requires.
FILE_DATE_PATTERNS = (
    (re.compile(r"^(\d{4})(\d{2})(\d{2})$"), "{0}-{1}-{2}"),
    (re.compile(r"^(\d{4})-(\d{2})-(\d{2})$"), "{0}-{1}-{2}"),
)


def file_date_object(value: str) -> str | None:
    """Serialize ##fileDate as xsd:date when its form allows, else lexically.

    Returns None for an absent value so no triple is emitted, matching RML's
    behaviour for an empty reference.
    """
    value = (value or "").strip()
    if not value or value == ".":
        return None
    for pattern, template in FILE_DATE_PATTERNS:
        match = pattern.match(value)
        if match:
            return ntriples_typed_literal(template.format(*match.groups()), XSD_DATE_URI)
    # An unrecognized form is preserved verbatim rather than dropped; the SHACL
    # layer reports it as non-conformant.
    return ntriples_string_literal(value)


def qual_object(value: str) -> str:
    """Serialize QUAL the way vcfc:VariantCallShape accepts it.

    The shape is a disjunction over vcfc:VCFFloat, the XSD numeric datatypes
    and vcfc:Null, which RML cannot satisfy because the branch depends on the
    row. INF/INFINITY/NAN are in the VCF Float lexical space but in no XSD
    numeric one, so they take vcfc:VCFFloat; a finite value takes xsd:decimal
    in its source lexical form, so no precision is gained or lost.
    """
    if is_missing(value):
        return ntriples_typed_literal(".", NULL_DATATYPE_URI)
    if is_finite_vcf_float(value):
        return ntriples_typed_literal(value, XSD_DECIMAL_URI)
    if is_vcf_float(value):
        return ntriples_typed_literal(value, VCF_FLOAT_DATATYPE_URI)
    # A value outside the VCF Float lexical space is kept as a plain literal
    # rather than dropped: a non-conformant graph is more useful than a lossy
    # one, and the SHACL layer reports it.
    return ntriples_string_literal(value)


def numeric_literal(value: str) -> str:
    """Serialize a VCF numeric value with a datatype a numeric shape accepts.

    vcfc:NumericLiteralShape requires an integer- or decimal-derived datatype,
    so a confidence-interval bound cannot be a plain string. A value outside
    both lexical spaces is kept verbatim rather than dropped, and the SHACL
    layer reports it.
    """
    if is_missing(value):
        return ntriples_typed_literal(".", NULL_DATATYPE_URI)
    stripped = value.strip()
    if is_vcf_integer(stripped) and stripped.lstrip("+-").isdigit():
        return ntriples_typed_literal(stripped, XSD_INTEGER_URI)
    if is_finite_vcf_float(stripped):
        return ntriples_typed_literal(stripped, XSD_DECIMAL_URI)
    return ntriples_string_literal(value)


def typed_field_object(value: str, declared_type: str) -> tuple[str, str] | None:
    """Return ``(property_local_name, literal)`` for a typed single value.

    Only a single, non-missing value gets a typed companion property; a comma
    list is represented by vcfc:FieldValueItem resources instead.
    """
    if "," in value or is_missing(value):
        return None
    if declared_type == "Integer":
        if is_vcf_integer(value):
            return "fieldValueInteger", ntriples_typed_literal(value, XSD_INTEGER_URI)
        return None
    if declared_type == "Float":
        # Serialize the source lexical form rather than a reparsed float, so
        # the graph never gains or loses precision relative to the VCF. A
        # non-finite Float has no xsd:decimal form and keeps only fieldValue.
        if is_finite_vcf_float(value):
            return "fieldValueDecimal", ntriples_typed_literal(value, XSD_DECIMAL_URI)
        return None
    return None
