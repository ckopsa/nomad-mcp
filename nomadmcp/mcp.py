"""The MCP surface: JSON-RPC 2.0 over HTTP and over stdio.

The server answers initialize, notifications/initialized, tools/list and
tools/call. Each tools/call answer carries the same JSON two times: as one
text part, and as structuredContent.result. A refusal is a normal answer
with isError true, and its JSON has the field refused.

This is the waymark-bench transport, unchanged in shape, so one engine
drives both rigs the same way.
"""

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import __version__, tools


PROTOCOL_VERSION = "2025-06-18"
SERVER_NAME = "nomad-mcp"
MAX_BODY = 1024 * 1024
INSTRUCTIONS = (
    "This server reads a HashiCorp Nomad cluster and changes nothing: every tool is "
    "read-only. Start with cluster_overview, then follow what it names: job_status and "
    "job_versions for a job, alloc_status and alloc_logs for an allocation, evaluation for "
    "a placement that failed, node_status for a node. Ids may be given as prefixes. "
    "To change the cluster, edit the ckopsa/home-infrastructure repository (job specs are "
    "terraform/nomad-jobs/<job>.hcl, variables and wiring in terraform/*.tf) and open a pull "
    "request: CI plans it, and applies it on merge."
)


def tool_list():
    """Gives the tools with their input schemas."""
    return [
        {
            "name": spec["name"],
            "description": spec["description"],
            "inputSchema": spec["schema"],
            "annotations": {"readOnlyHint": True, "openWorldHint": False},
        }
        for spec in tools.TOOL_SPECS
    ]


class Server:
    """The JSON-RPC dispatch over one Nomad client."""

    def __init__(self, client):
        self.client = client

    def handle(self, message):
        """Answers one JSON-RPC message. Gives None for a notification."""
        if not isinstance(message, dict):
            return error(None, -32600, "the message must be an object")
        method = message.get("method")
        ident = message.get("id")
        params = message.get("params") or {}
        if method is None:
            return error(ident, -32600, "the message needs a method")
        try:
            if method == "initialize":
                result = {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": SERVER_NAME, "version": __version__},
                    "instructions": INSTRUCTIONS,
                }
            elif method in ("notifications/initialized", "notifications/cancelled", "initialized"):
                return None
            elif method == "ping":
                result = {}
            elif method == "tools/list":
                result = {"tools": tool_list()}
            elif method == "tools/call":
                result = self.call_tool(params)
            else:
                return error(ident, -32601, "unknown method: %s" % method)
        except Exception as exc:  # The transport never gives a stack trace.
            return error(ident, -32603, "%s: %s" % (type(exc).__name__, exc))
        if ident is None:
            return None
        return {"jsonrpc": "2.0", "id": ident, "result": result}

    def call_tool(self, params):
        name = params.get("name")
        answer, refused = tools.call(self.client, name, params.get("arguments"))
        text = json.dumps(answer, sort_keys=True)
        return {
            "content": [{"type": "text", "text": text}],
            "structuredContent": {"result": answer},
            "isError": bool(refused),
        }


def error(ident, code, message):
    return {"jsonrpc": "2.0", "id": ident, "error": {"code": code, "message": message}}


# -------------------------------------------------------------- transports


def make_handler(server):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "nomad-mcp/" + __version__

        def log_message(self, fmt, *args):  # The tool calls log their own line.
            pass

        def _send(self, code, payload=None):
            body = b"" if payload is None else json.dumps(payload).encode("utf-8")
            self.send_response(code)
            if body:
                self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

        def do_GET(self):
            # Health says the process answers; it does not ask Nomad, so a
            # Nomad outage never gets this server restarted in a loop.
            if self.path.rstrip("/") in ("/health", "/healthz"):
                self._send(200, {"ok": True, "tools": len(tools.TOOL_SPECS)})
            else:
                self._send(405, {"error": "use POST on /mcp/"})

        def do_DELETE(self):
            self._send(200, {"ok": True})

        def do_POST(self):
            if self.path.rstrip("/") not in ("/mcp", ""):
                self._send(404, {"error": "use /mcp/"})
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                length = 0
            if length > MAX_BODY:
                self._send(413, {"error": "the body is too large"})
                return
            raw = self.rfile.read(length) if length else b""
            try:
                message = json.loads(raw.decode("utf-8") or "null")
            except ValueError:
                self._send(400, error(None, -32700, "the body is not JSON"))
                return
            if isinstance(message, list):
                answers = [a for a in (server.handle(item) for item in message) if a is not None]
                if not answers:
                    self._send(202)
                else:
                    self._send(200, answers)
                return
            answer = server.handle(message)
            if answer is None:
                self._send(202)
            else:
                self._send(200, answer)

    return Handler


def serve_http(client, port, host="127.0.0.1"):
    """Starts the HTTP transport. Gives the HTTPServer."""
    server = Server(client)
    httpd = ThreadingHTTPServer((host, port), make_handler(server))
    httpd.daemon_threads = True
    return httpd


def serve_stdio(client, stdin=None, stdout=None):
    """Runs the stdio transport: one JSON message for each line."""
    server = Server(client)
    stdin = stdin or sys.stdin
    stdout = stdout or sys.stdout
    lock = threading.Lock()
    for line in stdin:
        line = line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except ValueError:
            answer = error(None, -32700, "the line is not JSON")
        else:
            if isinstance(message, list):
                answers = [a for a in (server.handle(item) for item in message) if a is not None]
                answer = answers or None
            else:
                answer = server.handle(message)
        if answer is None:
            continue
        with lock:
            stdout.write(json.dumps(answer) + "\n")
            stdout.flush()
