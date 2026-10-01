"""Turtle manifests and deterministic directory/entry-point discovery."""

import importlib.metadata
import math
import os
from dataclasses import dataclass
from pathlib import Path
import re
from string import Formatter
from urllib.parse import quote, urlsplit


class _LazyNamespace:
    """An rdflib Namespace that is not built until a term is asked for.

    rdflib is a required dependency of the data-linking feature, but the CLI
    imports this module while merely building its argument parser -- so a
    checkout without the dependency installed could not print ``--help``, let
    alone run a conversion that does no linking at all. Deferring the import to
    the first term lookup keeps every non-linking code path working, and makes
    the failure land where the feature is actually used.

    Term access behaves exactly as ``rdflib.Namespace``: attribute and item
    lookup both return a ``URIRef``, and ``str()`` gives the namespace IRI.
    """

    __slots__ = ("_iri", "_namespace")

    def __init__(self, iri: str):
        self._iri = iri
        self._namespace = None

    def _resolve(self):
        if self._namespace is None:
            from rdflib import Namespace

            self._namespace = Namespace(self._iri)
        return self._namespace

    def __getattr__(self, name):
        return getattr(self._resolve(), name)

    def __getitem__(self, name):
        return self._resolve()[name]

    def __str__(self):
        return self._iri

    def __repr__(self):
        return f"_LazyNamespace({self._iri!r})"


VCFL = _LazyNamespace("https://w3id.org/vcf-rdfizer/linking#")
# The converter's own linking vocabulary keeps its namespace; only the
# conversion target moved to VCF Core.
VCFC = _LazyNamespace("https://w3id.org/vcf-core/vocab#")
#: Retained alias: the linking stage reads back converted RDF, so it must
#: use whatever namespace the emitters wrote.
VCFR = VCFC
SAFE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")


def absolute_iri(value: str) -> str:
    """Reject terms that cannot safely appear inside N-Triples angle brackets."""
    if (not value or not re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", value)
            or re.search(r'[\x00-\x20<>"{}|^`\\\x7f]', value)):
        raise ValueError(f"Not an absolute, N-Triples-safe IRI: {value!r}")
    parts = urlsplit(value)
    if parts.scheme in {"http", "https"} and not parts.hostname:
        raise ValueError(f"IRI has no host: {value!r}")
    return value


@dataclass(frozen=True)
class Reference:
    url: str
    sha256: str
    assembly: str
    feature_type: str
    id_attribute: str
    format: str = "gff3"          # "gff3" (IntervalJoin) or "sequence-map" (AlleleJoin)


@dataclass(frozen=True)
class Manifest:
    directory: Path
    id: str
    version: str
    title: str
    license: str
    terms_of_use: str
    tier: int
    strategy: str
    field: str
    split_on: str
    accept: str
    subject: str
    predicate: str
    object_template: str
    reference: Reference | None
    endpoint: str
    requests_per_second: float
    max_requests: int
    batch_size: int
    contact_email: str
    contig_aliases: Reference | None = None    # a SequenceMap an IntervalJoin resolves names through
    request_timeout: float = 30.0              # seconds a tier-3 request may take; services differ


def load_manifest(directory: Path) -> Manifest:
    directory = directory.expanduser().resolve()
    if directory.is_file():
        directory = directory.parent
    # rdflib is needed only to read a manifest, which is the point at which the
    # data-linking feature is genuinely in use.
    from rdflib import Graph, Literal, RDF, URIRef  # noqa: F401

    graph = Graph()
    try:
        graph.parse(directory / "linker.ttl", format="turtle")
    except Exception as exc:
        raise ValueError(f"Cannot parse {directory / 'linker.ttl'}: {exc}") from exc
    roots = list(graph.subjects(RDF.type, VCFL.Linker))
    if len(roots) != 1:
        raise ValueError("Manifest must declare exactly one vcfl:Linker")
    root = roots[0]

    allowed = {"id", "version", "title", "license", "termsOfUse", "join", "emit", "reference",
               "field", "splitOn", "accept", "subject", "predicate", "objectTemplate", "format",
               "url", "sha256", "assembly", "featureType", "idAttribute", "endpoint", "contigAliases", "requestTimeout",
               "maxRequestsPerSecond", "maxRequestsPerRun", "batchSize", "contactEmail"}
    for predicate in graph.predicates():
        if str(predicate).startswith(str(VCFL)) and str(predicate)[len(str(VCFL)):] not in allowed:
            raise ValueError(f"Unsupported manifest property: {predicate}")

    def one(node, name, default=None, iri=False, resource=False, url=False):
        values = list(graph.objects(node, VCFL[name]))
        if len(values) > 1 or (not values and default is None):
            raise ValueError(f"Expected one vcfl:{name}")
        if not values:
            return default
        value = values[0]
        if url:
            if not isinstance(value, (URIRef, Literal)):
                raise ValueError(f"vcfl:{name} must be a URL IRI or string")
            return absolute_iri(str(value))
        if resource:
            if isinstance(value, Literal):
                raise ValueError(f"vcfl:{name} must be a resource")
            return value
        if not isinstance(value, URIRef if iri else Literal):
            raise ValueError(f"vcfl:{name} must be {'an IRI' if iri else 'a literal'}")
        return absolute_iri(str(value)) if iri else str(value)

    linker_id = one(root, "id")
    version = one(root, "version")
    if not SAFE_NAME.fullmatch(linker_id) or not SAFE_NAME.fullmatch(version):
        raise ValueError("Linker id/version must be safe names (letters, digits, . _ -)")
    join = one(root, "join", resource=True)
    types = set(graph.objects(join, RDF.type))
    if types == {VCFL.TokenJoin}:
        strategy = "token"
    elif types == {VCFL.IntervalJoin}:
        strategy = "interval"
    elif types == {VCFL.AlleleJoin}:
        strategy = "allele"
    else:
        raise ValueError("Supported joins: TokenJoin, IntervalJoin and AlleleJoin")
    emit = one(root, "emit", resource=True)
    subject = one(emit, "subject", iri=True)
    if subject not in {str(VCFL.VariantCall), str(VCFL.VCFRecord)}:
        raise ValueError("vcfl:subject must be vcfl:VariantCall or vcfl:VCFRecord")
    field = one(join, "field", "ID")
    if field != "ID" and not re.fullmatch(r"INFO/[A-Za-z_][A-Za-z0-9_.]*", field):
        raise ValueError("Token field must be ID or INFO/<key>")
    split_on = one(join, "splitOn", ";" if field == "ID" else ",")
    if not split_on:
        raise ValueError("vcfl:splitOn cannot be empty")
    accept = one(join, "accept", ".+")
    try:
        re.compile(accept)
    except re.error as exc:
        raise ValueError(f"Invalid vcfl:accept regular expression: {exc}") from exc

    reference_node = one(root, "reference", "", resource=True)
    reference = None
    if reference_node:
        formats = {str(VCFL.GFF3): "gff3", str(VCFL.SequenceMap): "sequence-map"}
        reference_format = formats.get(one(reference_node, "format", iri=True))
        if reference_format is None:
            raise ValueError("Reference bundles must be vcfl:GFF3 or vcfl:SequenceMap")
        digest = one(reference_node, "sha256")
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("vcfl:sha256 must contain 64 lowercase hexadecimal digits")
        assembly = one(reference_node, "assembly")
        if not assembly:
            raise ValueError("A reference must declare a nonempty assembly")
        reference = Reference(one(reference_node, "url", url=True), digest, assembly,
                              one(join, "featureType", "gene"), one(join, "idAttribute", "ID"),
                              reference_format)
    aliases_node = one(root, "contigAliases", "", resource=True)
    aliases = None
    if aliases_node:
        if strategy != "interval" or reference is None:
            raise ValueError("vcfl:contigAliases applies to an IntervalJoin with a reference")
        digest = one(aliases_node, "sha256")
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("vcfl:sha256 must contain 64 lowercase hexadecimal digits")
        aliases = Reference(one(aliases_node, "url", url=True), digest, reference.assembly, "", "",
                            "sequence-map")
    resolver = directory / "resolver.py"
    tier = 3 if resolver.is_file() else 2 if reference else 1
    if strategy == "interval" and (reference is None or reference.format != "gff3" or tier == 3):
        raise ValueError("IntervalJoin requires a GFF3 reference and no resolver.py")
    if strategy == "allele" and (reference is None or reference.format != "sequence-map" or tier == 3):
        raise ValueError("AlleleJoin requires a SequenceMap reference and no resolver.py")
    if reference and strategy == "token":
        raise ValueError("A TokenJoin takes no reference bundle")
    template = one(emit, "objectTemplate", "")
    if tier != 3:
        allowed = PLACEHOLDERS[strategy]
        try:
            fields = list(Formatter().parse(template))
            variables = [name for _, name, _, _ in fields if name is not None]
            if variables != [allowed] or any(spec or conv for _, _, spec, conv in fields):
                raise ValueError(f"Template must contain exactly one {{{allowed}}} placeholder")
            absolute_iri(template.format(**{allowed: "example"}))
        except (KeyError, IndexError, ValueError) as exc:
            raise ValueError(f"Invalid vcfl:objectTemplate: {exc}") from exc
    elif template:
        raise ValueError("Tier 3 objects come from the resolver; omit objectTemplate")

    endpoint, rps, max_requests, batch_size, contact, timeout = "", 0.0, 0, 1000, "", 30.0
    if tier == 3:
        endpoint = one(root, "endpoint", iri=True)
        parts = urlsplit(endpoint)
        if parts.scheme != "https" or parts.username or parts.password or parts.fragment:
            raise ValueError("vcfl:endpoint must be an HTTPS URL without credentials or fragment")
        try:
            rps = float(one(root, "maxRequestsPerSecond"))
            max_requests = int(one(root, "maxRequestsPerRun"))
            batch_size = int(one(root, "batchSize"))
        except ValueError as exc:
            raise ValueError("Invalid network budget: expected positive numbers") from exc
        if not math.isfinite(rps) or rps <= 0 or max_requests <= 0 or not 1 <= batch_size <= 1000:
            raise ValueError("Network budgets must be positive; batchSize must be 1..1000")
        contact = one(root, "contactEmail")
        try:
            timeout = float(one(root, "requestTimeout", "30"))
        except ValueError as exc:
            raise ValueError("vcfl:requestTimeout must be a number of seconds") from exc
        if not 1 <= timeout <= 600:
            raise ValueError("vcfl:requestTimeout must be between 1 and 600 seconds")
        if not re.fullmatch(r"[^\s<>@]+@[^\s<>@]+\.[^\s<>@]+", contact):
            raise ValueError("vcfl:contactEmail must be an email address")
    return Manifest(directory, linker_id, version, one(root, "title"),
                    one(root, "license", "unspecified"), one(root, "termsOfUse", "", iri=True),
                    tier, strategy, field, split_on, accept, subject,
                    one(emit, "predicate", iri=True), template, reference, endpoint,
                    rps, max_requests, batch_size, contact, aliases, timeout)


#: The one placeholder each join's objectTemplate must contain.
PLACEHOLDERS = {"token": "TOKEN", "interval": "ID", "allele": "SPDI"}


def template_object(manifest: Manifest, value: str) -> str:
    # An SPDI expression keeps its colons: they are legal in an IRI path, and
    # percent-encoding them would make the identifier unreadable for no gain.
    safe = ":" if manifest.strategy == "allele" else ""
    return absolute_iri(manifest.object_template.format(**{PLACEHOLDERS[manifest.strategy]: quote(value, safe=safe)}))


def discover(search_paths=()) -> dict[str, Manifest]:
    """Reject ambiguous IDs; never silently replace an installed linker."""
    import vcf_rdfizer_data.linkers

    roots = [Path(vcf_rdfizer_data.linkers.__file__).parent]
    roots.extend(Path(p).expanduser() for p in search_paths)
    roots.extend(Path(p).expanduser() for p in os.environ.get("VCF_RDFIZER_LINKER_PATH", "").split(os.pathsep) if p)
    directories = set()
    for root in roots:
        if not root.is_dir():
            raise ValueError(f"Linker search path is not a directory: {root}")
        if (root / "linker.ttl").is_file():
            directories.add(root.resolve())
        else:
            directories.update(p.parent.resolve() for p in root.glob("*/linker.ttl"))
    for entry in importlib.metadata.entry_points(group="vcf_rdfizer.linkers"):
        # An entry point exports a zero-argument function returning a directory.
        directory = Path(entry.load()()).resolve()
        if not (directory / "linker.ttl").is_file():
            raise ValueError(f"Entry point {entry.name} did not return a linker directory")
        directories.add(directory)
    found = {}
    for directory in sorted(directories):
        manifest = load_manifest(directory)
        if manifest.id in found:
            raise ValueError(f"Ambiguous linker id {manifest.id!r}: {found[manifest.id].directory} and {directory}")
        found[manifest.id] = manifest
    return found


def select(raw: str, search_paths=()) -> list[Manifest]:
    found = discover(search_paths)
    names = raw.split(",")
    if not raw or any(not n.strip() for n in names):
        raise ValueError("--link requires comma-separated linker IDs")
    selected = []
    for name in dict.fromkeys(n.strip() for n in names):
        if name not in found:
            raise ValueError(f"Unknown linker {name!r}; run vcf-rdfizer-link list")
        selected.append(found[name])
    return selected
