"""Runner-owned HTTP access: cache, budgets, host pacing, retries and accounting.

Python resolvers are trusted code, not sandboxed code. These guarantees cover
requests made through this session, not arbitrary sockets opened by a plug-in.
"""

import base64
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import importlib.metadata
import json
import json as json_module
import math
from pathlib import Path
import tempfile
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .manifest import Manifest, absolute_iri

MAX_RESPONSE_BYTES = 16 * 1024 * 1024


def atomic_bytes(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(data)
            handle.close()
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Redirects otherwise evade host declarations and request accounting.
        return None


class NetworkPolicy:
    """A capacity-one bucket per host, and one in-flight request per run.

    All selected linkers are registered up front; a host gets the strictest
    declared rate. Retry attempts consume the same per-linker run budget.
    """

    def __init__(self, manifests, *, clock=time.monotonic, sleep=time.sleep):
        self.clock, self.sleep = clock, sleep
        self.lock = threading.Lock()
        self.rates, self.next_request, self.requests = {}, {}, {}
        for manifest in manifests:
            if manifest.tier == 3:
                host = urlsplit(manifest.endpoint).hostname
                self.rates[host] = min(self.rates.get(host, math.inf), manifest.requests_per_second)

    def issue(self, manifest, operation):
        with self.lock:
            count = self.requests.get(manifest.id, 0)
            if count >= manifest.max_requests:
                raise ValueError(f"{manifest.id}: maxRequestsPerRun={manifest.max_requests} exhausted")
            host = urlsplit(manifest.endpoint).hostname
            delay = self.next_request.get(host, 0) - self.clock()
            if delay > 0:
                self.sleep(delay)
            self.next_request[host] = self.clock() + 1 / self.rates[host]
            self.requests[manifest.id] = count + 1
            return operation()

    def defer(self, host, seconds):
        with self.lock:
            self.next_request[host] = max(self.next_request.get(host, 0), self.clock() + seconds)


class Response:
    def __init__(self, status_code, headers, content):
        self.status_code, self.headers, self.content = status_code, headers, content

    def json(self):
        return json.loads(self.content)

    def raise_for_status(self):
        if not 200 <= self.status_code < 300:
            raise ValueError(f"Linker service returned HTTP {self.status_code}")


class CachedSession:
    def __init__(self, manifest: Manifest, cache_dir: Path, policy: NetworkPolicy,
                 *, offline=False, transport=None):
        self.manifest, self.cache_dir, self.policy = manifest, cache_dir, policy
        self.offline = offline
        self.transport = transport or build_opener(NoRedirects()).open
        self.stats = {"requests": 0, "cache_hits": 0, "bytes_transferred": 0,
                      "final_service_status": None, "responses": []}

    def get(self, url, *, params=None):
        if params:
            url += ("&" if "?" in url else "?") + urlencode(sorted(params.items()))
        return self._request("GET", url, None)

    def post(self, url, *, json):
        body = json_module.dumps(json, sort_keys=True, separators=(",", ":")).encode()
        return self._request("POST", url, body)

    def _request(self, method, url, body):
        absolute_iri(url)
        declared, requested = urlsplit(self.manifest.endpoint), urlsplit(url)
        if (requested.scheme, requested.netloc) != (declared.scheme, declared.netloc) or requested.fragment:
            raise ValueError(f"{self.manifest.id}: request outside declared endpoint origin")
        if requested.path != declared.path and not requested.path.startswith(declared.path.rstrip("/") + "/"):
            raise ValueError(f"{self.manifest.id}: request outside declared endpoint path")
        signature = method.encode() + b"\n" + url.encode() + b"\n" + (body or b"")
        request_hash = hashlib.sha256(signature).hexdigest()
        path = self.cache_dir / "responses" / self.manifest.id / self.manifest.version / (request_hash + ".json")
        if path.is_file():
            try:
                cached = json.loads(path.read_text(encoding="utf-8"))
                content = base64.b64decode(cached["body"], validate=True)
                if cached["sha256"] != hashlib.sha256(content).hexdigest() or cached["request_hash"] != request_hash:
                    raise ValueError("digest mismatch")
                response = Response(cached["status"], cached["headers"], content)
                response.raise_for_status()
            except (ValueError, KeyError, TypeError) as exc:
                raise ValueError(f"Corrupt response cache: {path}: {exc}") from exc
            self.stats["cache_hits"] += 1
            self._account_response(request_hash, cached)
            return response
        if self.offline:
            raise ValueError(f"{self.manifest.id}: offline/cache-only response miss: {request_hash}")
        if self.manifest.contact_email == "replace-me@example.org":
            raise ValueError("Set --links-contact-email (or vcfl:contactEmail in a copied manifest) before live requests")
        try:
            version = importlib.metadata.version("vcf-rdfizer")
        except importlib.metadata.PackageNotFoundError:
            version = "development"
        request = Request(url, data=body, method=method, headers={
            "Accept": "application/json", "Content-Type": "application/json",
            "User-Agent": f"VCF-RDFizer/{version} ({self.manifest.contact_email})",
        })
        for attempt in range(4):
            def fetch():
                self.stats["requests"] += 1
                try:
                    raw = self.transport(request, timeout=30)
                except HTTPError as exc:
                    raw = exc
                with raw:
                    content = raw.read(MAX_RESPONSE_BYTES + 1)
                    self.stats["bytes_transferred"] += len(content)
                    self.stats["final_service_status"] = raw.code
                    if len(content) > MAX_RESPONSE_BYTES:
                        raise ValueError("Linker service response exceeds 16 MiB")
                    return Response(raw.code, dict(raw.headers.items()), content)
            try:
                response = self.policy.issue(self.manifest, fetch)
            except (URLError, TimeoutError, OSError) as exc:
                self.stats["final_service_status"] = "network-error"
                raise ValueError(f"{self.manifest.id}: network request failed: {exc}") from exc
            if response.status_code == 429 or 500 <= response.status_code <= 599:
                retry = next((v for k, v in response.headers.items() if k.lower() == "retry-after"), "")
                delay = float(2 ** attempt)
                if retry:
                    try:
                        retry_seconds = float(retry)
                    except ValueError:
                        try:
                            retry_seconds = (parsedate_to_datetime(retry) - datetime.now(timezone.utc)).total_seconds()
                        except (ValueError, TypeError, OverflowError):
                            retry_seconds = 0
                    if math.isfinite(retry_seconds):
                        delay = max(delay, retry_seconds)
                self.policy.defer(requested.hostname, delay)
                if attempt < 3:
                    continue
            response.raise_for_status()
            cached = {"request_hash": request_hash, "status": response.status_code,
                      "headers": response.headers, "body": base64.b64encode(response.content).decode(),
                      "sha256": hashlib.sha256(response.content).hexdigest(),
                      "fetched_at": datetime.now(timezone.utc).isoformat()}
            atomic_bytes(path, (json.dumps(cached, sort_keys=True) + "\n").encode())
            self._account_response(request_hash, cached)
            return response
        raise AssertionError("unreachable")

    def _account_response(self, request_hash, cached):
        self.stats["final_service_status"] = cached["status"]
        self.stats["responses"].append({"request_hash": request_hash,
                                        "sha256": cached["sha256"], "fetched_at": cached["fetched_at"]})
