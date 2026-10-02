"""The Nomad HTTP API client. It sends GET, and one POST.

WHY GET ONLY: this server is for looking. A change to the cluster goes
through a commit to ckopsa/home-infrastructure, which CI plans on the
pull request and applies on merge. A troubleshooting tool that could also
stop a job or write a variable would be a second, unrecorded path to
production, so there is no such code path here: the one place that
talks to the socket hard-codes the method, and no function takes a
method or a body. The tests hold that line.

THE ONE POST: stop_alloc stops one allocation, so the scheduler places a
fresh one. It is how job_restart gets a merged image running without a
person at a terminal, and it is the only write: its method is the one
literal beside GET, it takes an allocation id and nothing else, and it
sends no body.

The settings come from the environment, the same names the nomad CLI
uses, so the server runs anywhere the CLI does:

  NOMAD_ADDR       http://host:4646, https://host:4646, or
                   unix:///secrets/api.sock (the Task API inside a task)
  NOMAD_TOKEN      sent as X-Nomad-Token; inside a task it is the
                   workload identity (identity { env = true })
  NOMAD_NAMESPACE  the namespace to read; the default is "default"
  NOMAD_MCP_NAMESPACES  the namespaces a tool may name, comma-separated;
                   the first is the default. Unset, NOMAD_NAMESPACE alone
  NOMAD_CACERT     a CA bundle for an https address (optional)

Every failure becomes a NomadError with a short message: a refused
answer to the model, never a stack trace.
"""

import http.client
import json
import os
import socket
import ssl
import threading
import urllib.parse


DEFAULT_ADDR = "http://127.0.0.1:4646"
DEFAULT_TIMEOUT = 10
# A cap on any one answer. The largest honest answer is a busy cluster's
# allocation list; anything past this is a mistake, not data to read.
MAX_RESPONSE = 32 * 1024 * 1024


class NomadError(Exception):
    """A failed read: the status (0 when no answer came) and a short reason."""

    def __init__(self, reason, status=0, path=None):
        Exception.__init__(self, reason)
        self.reason = reason
        self.status = status
        self.path = path


class UnixHTTPConnection(http.client.HTTPConnection):
    """HTTP over a unix socket, for the Task API at /secrets/api.sock.

    The Task API is how a task reaches Nomad without a network address or
    a long-lived token: Nomad mounts the socket into the task's secrets
    directory and accepts the task's workload identity on it.
    """

    def __init__(self, socket_path, timeout=DEFAULT_TIMEOUT):
        # The host name is only for the Host header; the socket is the address.
        http.client.HTTPConnection.__init__(self, "localhost", timeout=timeout)
        self.socket_path = socket_path

    def connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        try:
            sock.connect(self.socket_path)
        except OSError:
            sock.close()
            raise
        self.sock = sock


class Client:
    """Reads the Nomad API with get. stop_alloc is its one write."""

    def __init__(self, addr=None, token=None, namespace=None, timeout=DEFAULT_TIMEOUT,
                 cacert=None, namespaces=None):
        self.addr = (addr or DEFAULT_ADDR).rstrip("/")
        self.token = token or None
        # The namespaces a tool may name; the first is the default. Every
        # request sends self.namespace, and a tool call reads through a copy
        # of the client set to the call's own namespace.
        self.namespaces = ([n.strip() for n in (namespaces or []) if n and n.strip()]
                           or [namespace or "default"])
        self.namespace = self.namespaces[0]
        self.timeout = timeout
        self.cacert = cacert or None
        # One keep-alive connection per thread. The overview makes dozens
        # of reads, and a fresh connection per read pays a name lookup
        # each time; on a LAN whose resolver drops the odd query, that
        # was a 5 second stall every few reads.
        self._local = threading.local()
        parsed = urllib.parse.urlsplit(self.addr)
        self.scheme = parsed.scheme
        if self.scheme == "unix":
            # unix:///secrets/api.sock parses with the path in .path.
            self.socket_path = parsed.path or parsed.netloc
            if not self.socket_path:
                raise ValueError("NOMAD_ADDR unix:// needs a socket path")
        elif self.scheme in ("http", "https"):
            if not parsed.hostname:
                raise ValueError("NOMAD_ADDR needs a host: %s" % self.addr)
            self.host = parsed.hostname
            self.port = parsed.port or (443 if self.scheme == "https" else 4646)
        else:
            raise ValueError("NOMAD_ADDR must be http://, https:// or unix://, not %r" % self.addr)

    @classmethod
    def from_env(cls, environ=None):
        env = os.environ if environ is None else environ
        timeout = env.get("NOMAD_MCP_TIMEOUT")
        return cls(addr=env.get("NOMAD_ADDR") or DEFAULT_ADDR,
                   token=env.get("NOMAD_TOKEN"),
                   namespace=env.get("NOMAD_NAMESPACE"),
                   namespaces=(env.get("NOMAD_MCP_NAMESPACES") or "").split(","),
                   timeout=float(timeout) if timeout else DEFAULT_TIMEOUT,
                   cacert=env.get("NOMAD_CACERT"))

    def describe(self):
        """The address, for a log line. Never the token."""
        return self.addr

    def _connection(self):
        if self.scheme == "unix":
            return UnixHTTPConnection(self.socket_path, timeout=self.timeout)
        if self.scheme == "https":
            context = ssl.create_default_context(cafile=self.cacert)
            return http.client.HTTPSConnection(self.host, self.port, timeout=self.timeout,
                                               context=context)
        return http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)

    def _scrub(self, text):
        text = str(text)
        if self.token:
            text = text.replace(self.token, "***")
        return text

    def get_raw(self, path, params=None):
        """GETs one path. Gives the body as bytes. Raises NomadError."""
        return self._send(path, params, False)

    def stop_alloc(self, alloc_id):
        """Stops one allocation in the client's namespace, so the scheduler
        places a fresh one. The one write. Gives Nomad's answer (its EvalID)."""
        path = "/v1/allocation/%s/stop" % urllib.parse.quote(str(alloc_id), safe="")
        body = self._send(path, None, True)
        try:
            return json.loads(body.decode("utf-8") or "null")
        except ValueError:
            raise NomadError("the answer to POST %s is not JSON" % path, path=path)

    def _send(self, path, params, write):
        verb = "POST" if write else "GET"
        query = {"namespace": self.namespace}
        for key, value in (params or {}).items():
            if value is None:
                continue
            if isinstance(value, bool):
                value = "true" if value else "false"
            query[key] = value
        target = path + "?" + urllib.parse.urlencode(query)
        headers = {"Accept": "application/json", "User-Agent": "nomad-mcp"}
        if self.token:
            headers["X-Nomad-Token"] = self.token
        try:
            status, body = self._exchange(target, headers, write)
        except socket.timeout:
            self._drop()
            raise NomadError("Nomad did not answer %s %s within %ss" % (verb, path, self.timeout),
                             path=path)
        except (OSError, http.client.HTTPException) as exc:
            self._drop()
            raise NomadError(self._scrub("cannot reach Nomad at %s: %s: %s"
                                         % (self.addr, type(exc).__name__, exc)), path=path)
        if len(body) > MAX_RESPONSE:
            raise NomadError("the answer to %s %s is larger than %s bytes" % (verb, path, MAX_RESPONSE),
                             status=status, path=path)
        if status >= 400:
            detail = self._scrub(body[:300].decode("utf-8", "replace").strip())
            if status == 403:
                reason = ("forbidden: the token may not %s %s (%s). The ACL policy needs "
                          "the capability for it." % (verb, path, detail or "Permission denied"))
            elif status == 404:
                reason = "not found: %s %s (%s)" % (verb, path, detail or "404")
            else:
                reason = "Nomad answered %s to %s %s: %s" % (status, verb, path, detail)
            raise NomadError(reason, status=status, path=path)
        return body

    def _drop(self):
        connection = getattr(self._local, "connection", None)
        self._local.connection = None
        if connection is not None:
            connection.close()

    def _exchange(self, target, headers, write=False):
        """One request on this thread's connection. This is the one function
        that writes to the network, and the two methods are written here and
        nowhere else. A kept-alive connection the agent has closed fails on
        first use; that one failure is retried on a fresh connection, which
        is safe because a GET changes nothing. A POST never rides a kept
        connection, so it is never retried and never sent twice."""
        if write:
            self._drop()
        for attempt in (0, 1):
            connection = getattr(self._local, "connection", None)
            reused = connection is not None
            if connection is None:
                connection = self._local.connection = self._connection()
            try:
                if write:
                    connection.request("POST", target, headers=headers)
                else:
                    connection.request("GET", target, headers=headers)
                response = connection.getresponse()
                body = response.read(MAX_RESPONSE + 1)
                if response.will_close or len(body) > MAX_RESPONSE:
                    self._drop()
                return response.status, body
            except socket.timeout:
                raise  # A slow agent stays slow; waiting twice helps nobody.
            except (OSError, http.client.HTTPException):
                self._drop()
                if not reused or attempt:
                    raise
        raise NomadError("unreachable")  # The loop always returns or raises.

    def get(self, path, params=None):
        """GETs one path and parses the JSON. Raises NomadError."""
        body = self.get_raw(path, params)
        try:
            return json.loads(body.decode("utf-8") or "null")
        except ValueError:
            raise NomadError("the answer to GET %s is not JSON" % path, path=path)
