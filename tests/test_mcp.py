"""The tests of the two transports: HTTP and stdio."""

import io
import json
import threading
import unittest
import urllib.error
import urllib.request

from nomadmcp import mcp, tools
from nomadmcp.nomad import Client

from . import fake


class TransportCase(unittest.TestCase):
    """One MCP server on a free port, over the fake Nomad."""

    def setUp(self):
        self.nomad = fake.FakeNomad()
        self.addCleanup(self.nomad.close)
        self.client = Client(addr=self.nomad.addr, token=fake.TOKEN, timeout=5)
        self.httpd = mcp.serve_http(self.client, 0)
        self.addCleanup(self.httpd.server_close)
        self.addCleanup(self.httpd.shutdown)
        threading.Thread(target=self.httpd.serve_forever, args=(0.05,), daemon=True).start()
        self.base = "http://127.0.0.1:%s" % self.httpd.server_address[1]
        self.url = self.base + "/mcp/"

    def post(self, message):
        """Sends one JSON-RPC message. Gives (status, answer)."""
        request = urllib.request.Request(
            self.url, data=json.dumps(message).encode("utf-8"),
            headers={"Content-Type": "application/json",
                     "Accept": "application/json, text/event-stream"},
            method="POST")
        with urllib.request.urlopen(request, timeout=30) as answer:
            body = answer.read()
            return answer.status, (json.loads(body) if body else None)

    def call(self, name, arguments=None):
        status, answer = self.post({"jsonrpc": "2.0", "id": 9, "method": "tools/call",
                                    "params": {"name": name, "arguments": arguments or {}}})
        self.assertEqual(status, 200)
        return answer["result"]


class TestHttp(TransportCase):

    def test_initialize_and_the_notification(self):
        status, answer = self.post({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                                    "params": {"protocolVersion": mcp.PROTOCOL_VERSION,
                                               "capabilities": {},
                                               "clientInfo": {"name": "test", "version": "1"}}})
        self.assertEqual(status, 200)
        result = answer["result"]
        self.assertEqual(result["protocolVersion"], "2025-06-18")
        self.assertEqual(result["serverInfo"]["name"], "nomad-mcp")
        self.assertIn("tools", result["capabilities"])
        self.assertIn("read-only", result["instructions"])
        self.assertIn("cluster_overview", result["instructions"])
        self.assertIn("ckopsa/home-infrastructure", result["instructions"])
        status, body = self.post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        self.assertEqual(status, 202)
        self.assertIsNone(body)

    def test_tools_list_gives_every_tool_with_a_schema(self):
        status, answer = self.post({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        self.assertEqual(status, 200)
        listed = answer["result"]["tools"]
        self.assertEqual(sorted(t["name"] for t in listed), sorted([
            "cluster_overview", "list_jobs", "job_status", "job_versions", "alloc_status",
            "alloc_logs", "list_nodes", "node_status", "node_host", "list_services", "service",
            "list_variables", "list_deployments", "evaluation", "alloc_exec", "job_restart",
            "restart_allowlist", "alloc_stop", "job_stop"]))
        for tool in listed:
            self.assertTrue(tool["description"])
            self.assertEqual(tool["inputSchema"]["type"], "object")
            self.assertEqual(tool["annotations"]["readOnlyHint"],
                             tool["name"] not in ("alloc_exec", "job_restart", "alloc_stop", "job_stop"))
        variables = next(t for t in listed if t["name"] == "list_variables")
        self.assertIn("never reads", variables["description"])

    def test_a_call_gives_text_and_structured_content(self):
        result = self.call("job_status", {"job": "web"})
        self.assertFalse(result["isError"])
        self.assertEqual(result["structuredContent"]["result"]["id"], "web")
        self.assertEqual(json.loads(result["content"][0]["text"]), result["structuredContent"]["result"])

    def test_a_refusal_is_an_error_result(self):
        result = self.call("alloc_status", {"alloc": "a1b2"})
        self.assertTrue(result["isError"])
        self.assertEqual(result["structuredContent"]["result"]["refused"], "ambiguous")

    def test_unknown_method_and_bad_json(self):
        status, answer = self.post({"jsonrpc": "2.0", "id": 3, "method": "resources/list"})
        self.assertEqual(answer["error"]["code"], -32601)
        request = urllib.request.Request(self.url, data=b"{nope", method="POST",
                                         headers={"Content-Type": "application/json"})
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request, timeout=10)
        self.assertEqual(caught.exception.code, 400)

    def test_health(self):
        with urllib.request.urlopen(self.base + "/health", timeout=10) as answer:
            body = json.loads(answer.read())
        self.assertEqual(body, {"ok": True, "tools": len(tools.TOOL_SPECS)})
        # Health does not ask Nomad.
        self.assertEqual(self.nomad.requests, [])


class TestStdio(unittest.TestCase):

    def test_one_message_per_line(self):
        nomad_fake = fake.FakeNomad()
        self.addCleanup(nomad_fake.close)
        client = Client(addr=nomad_fake.addr)
        lines = [
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
             "params": {"name": "list_nodes", "arguments": {}}},
        ]
        stdin = io.StringIO("\n".join(json.dumps(line) for line in lines) + "\nnot json\n")
        stdout = io.StringIO()
        mcp.serve_stdio(client, stdin=stdin, stdout=stdout)
        answers = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual([a.get("id") for a in answers], [1, 2, None])
        self.assertEqual(answers[1]["result"]["structuredContent"]["result"]["count"], 2)
        self.assertEqual(answers[2]["error"]["code"], -32700)


if __name__ == "__main__":
    unittest.main()
