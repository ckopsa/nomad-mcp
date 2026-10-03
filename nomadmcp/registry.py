"""Whether a pinned image is in its registry.

job_status and cluster_overview ask the registry for the manifest of each
task's image (HEAD /v2/<name>/manifests/<tag>), so a tag that was never
pushed is named before anyone revives the job by hand to find
"Failed to pull ... not found". A registry that asks for a token gets the
anonymous one its challenge offers; this server holds no registry
credential and takes none as an argument, so a private image reads
"unknown".

Each answer is cached per image for five minutes, and each check gives
up after three seconds, so a tool call stays far inside the engine's
30 s.
"""

import http.client
import json
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor


TIMEOUT = 3.0
TTL = 300.0
WORKERS = 8
DOCKER_HUB = "registry-1.docker.io"
ACCEPT = ", ".join((
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
))
CHALLENGE = re.compile(r'(\w+)="([^"]*)"')
# A registry on the loopback is reached directly and over plain HTTP, as
# docker itself does; anything else over HTTPS through the environment's proxy.
_DIRECT = urllib.request.build_opener(urllib.request.ProxyHandler({}))
_PROXIED = urllib.request.build_opener()


def _local(host):
    host = host or ""
    if host.count(":") == 1:
        host = host.rsplit(":", 1)[0]
    return host == "localhost" or host.startswith("127.")


def parse(ref):
    """(host, name, reference) of an image reference, or None when it is not a plain one."""
    if not isinstance(ref, str) or not ref or "${" in ref or any(c.isspace() for c in ref):
        return None
    rest, _, digest = ref.partition("@")
    first, slash, remainder = rest.partition("/")
    if slash and ("." in first or ":" in first or first == "localhost"):
        host, path = first, remainder
    else:
        host, path = DOCKER_HUB, rest
        if "/" not in path:
            path = "library/" + path
    name, colon, tag = path.rpartition(":")
    if not colon or "/" in tag:
        name, tag = path, "latest"
    if not name or not tag:
        return None
    return host, name, digest or tag


class Registry:
    """Asks registries for manifests, and remembers the answers for a while."""

    def __init__(self, timeout=TIMEOUT, ttl=TTL, bases=None):
        self.timeout = timeout
        self.ttl = ttl
        # host -> base URL, for a registry reached at another address (tests).
        self.bases = dict(bases or {})
        self._cache = {}
        self._lock = threading.Lock()

    def check(self, ref):
        """{image_present: True | False | "unknown", registry_status, ...} for one image."""
        with self._lock:
            hit = self._cache.get(ref)
            if hit and time.monotonic() - hit[0] < self.ttl:
                return dict(hit[1])
        answer = self._ask(ref)
        with self._lock:
            self._cache[ref] = (time.monotonic(), answer)
        return dict(answer)

    def check_many(self, refs):
        """{ref: check(ref)} for each distinct ref, asked side by side."""
        refs = list(dict.fromkeys(r for r in refs if r))
        if not refs:
            return {}
        with ThreadPoolExecutor(max_workers=min(WORKERS, len(refs))) as pool:
            return dict(zip(refs, pool.map(self.check, refs)))

    def _base(self, host):
        if host in self.bases:
            return self.bases[host].rstrip("/")
        return ("http://" if _local(host) else "https://") + host

    def _ask(self, ref):
        parts = parse(ref)
        if parts is None:
            return {"image_present": "unknown", "registry_error": "not a plain image reference"}
        host, name, reference = parts
        url = "%s/v2/%s/manifests/%s" % (self._base(host), name,
                                          urllib.parse.quote(reference, safe=":"))
        deadline = time.monotonic() + self.timeout
        try:
            status, headers = self._head(url, None, deadline)
            if status == 401:
                token = self._token(headers.get("WWW-Authenticate"), name, deadline)
                if token:
                    status, headers = self._head(url, token, deadline)
        except (OSError, ValueError, http.client.HTTPException) as err:
            reason = getattr(err, "reason", None) or err
            return {"image_present": "unknown", "registry_error": str(reason)[:200] or type(err).__name__}
        answer = {"registry_status": status}
        if status == 200:
            answer["image_present"] = True
            if headers.get("Docker-Content-Digest"):
                answer["image_digest"] = headers["Docker-Content-Digest"]
        elif status == 404:
            answer["image_present"] = False
        else:
            answer["image_present"] = "unknown"
            if status in (401, 403):
                answer["registry_error"] = "the registry asks for a credential this server does not hold"
        return answer

    def _open(self, request, deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("timed out")
        opener = _DIRECT if _local(request.host) else _PROXIED
        return opener.open(request, timeout=remaining)

    def _head(self, url, token, deadline):
        request = urllib.request.Request(url, method="HEAD", headers={"Accept": ACCEPT})
        if token:
            request.add_header("Authorization", "Bearer " + token)
        try:
            with self._open(request, deadline) as response:
                return response.status, response.headers
        except urllib.error.HTTPError as err:
            return err.code, err.headers

    def _token(self, challenge, name, deadline):
        """The anonymous pull token a Bearer challenge offers, or None."""
        if not challenge or not challenge.lower().startswith("bearer "):
            return None
        fields = dict(CHALLENGE.findall(challenge))
        realm = fields.get("realm")
        if not realm:
            return None
        query = {"scope": fields.get("scope") or "repository:%s:pull" % name}
        if fields.get("service"):
            query["service"] = fields["service"]
        url = realm + ("&" if "?" in realm else "?") + urllib.parse.urlencode(query)
        try:
            with self._open(urllib.request.Request(url), deadline) as response:
                body = json.loads(response.read(65536) or b"{}")
        except urllib.error.HTTPError:
            return None
        return body.get("token") or body.get("access_token")
