"""The tests of the Nomad client: the transports, the settings, and GET only."""

import inspect
import os
import re
import shutil
import tempfile
import unittest

from nomadmcp import nomad, tools
from nomadmcp.nomad import Client, NomadError

from . import fake


class TestSettings(unittest.TestCase):

    def test_from_env(self):
        client = Client.from_env({"NOMAD_ADDR": "https://nomad.example:4646/", "NOMAD_TOKEN": "t",
                                  "NOMAD_NAMESPACE": "apps"})
        self.assertEqual((client.scheme, client.host, client.port), ("https", "nomad.example", 4646))
        self.assertEqual(client.namespace, "apps")
        self.assertEqual(client.token, "t")
        client = Client.from_env({"NOMAD_ADDR": "unix:///secrets/api.sock"})
        self.assertEqual(client.socket_path, "/secrets/api.sock")
        self.assertEqual(client.namespace, "default")
        self.assertIsNone(client.token)
        self.assertEqual(Client.from_env({}).addr, "http://127.0.0.1:4646")

    def test_namespaces_from_env(self):
        client = Client.from_env({"NOMAD_MCP_NAMESPACES": "default, doors", "NOMAD_NAMESPACE": "apps"})
        self.assertEqual((client.namespaces, client.namespace), (["default", "doors"], "default"))
        client = Client.from_env({"NOMAD_NAMESPACE": "apps"})
        self.assertEqual((client.namespaces, client.namespace), (["apps"], "apps"))
        self.assertEqual(Client.from_env({}).namespaces, ["default"])

    def test_bad_addresses(self):
        for addr in ("ftp://x", "http://", "unix://"):
            with self.assertRaises(ValueError):
                Client(addr=addr)


class TestTransports(unittest.TestCase):

    def test_tcp_sends_the_token_and_the_namespace(self):
        nomad_fake = fake.FakeNomad()
        self.addCleanup(nomad_fake.close)
        client = Client(addr=nomad_fake.addr, token=fake.TOKEN, namespace="apps")
        self.assertEqual(client.get("/v1/status/leader"), "192.168.1.40:4647")
        method, path, query, token = nomad_fake.requests[0]
        self.assertEqual((method, path, token), ("GET", "/v1/status/leader", fake.TOKEN))
        self.assertEqual(query, {"namespace": "apps"})

    def test_unix_socket_like_the_task_api(self):
        directory = tempfile.mkdtemp(prefix="nomad-mcp-")
        self.addCleanup(shutil.rmtree, directory, True)
        nomad_fake = fake.FakeNomad(unix_path=fake.unix_path(directory))
        self.addCleanup(nomad_fake.close)
        client = Client(addr=nomad_fake.addr, token="workload-identity-jwt", timeout=5)
        answer, refused = tools.call(client, "job_status", {"job": "web"})
        self.assertFalse(refused, answer)
        self.assertEqual(answer["id"], "web")
        self.assertTrue(all(r[3] == "workload-identity-jwt" for r in nomad_fake.requests))
        self.assertEqual(nomad_fake.methods(), ["GET"])

    def test_a_missing_socket_is_a_nomad_error(self):
        client = Client(addr="unix:///nonexistent/api.sock", token="tok", timeout=2)
        with self.assertRaises(NomadError) as caught:
            client.get("/v1/nodes")
        self.assertIn("cannot reach Nomad", caught.exception.reason)

    def test_a_kept_connection_that_the_server_closed_is_retried(self):
        nomad_fake = fake.FakeNomad()
        self.addCleanup(nomad_fake.close)
        client = Client(addr=nomad_fake.addr)
        client.get("/v1/nodes")
        client._local.connection.sock.close()  # As if the agent dropped the idle connection.
        self.assertEqual(len(client.get("/v1/nodes")), 2)


class TestGetOnly(unittest.TestCase):
    """The server may never change the cluster. These tests hold that line."""

    def test_every_tool_sends_only_get(self):
        nomad_fake = fake.FakeNomad()
        self.addCleanup(nomad_fake.close)
        client = Client(addr=nomad_fake.addr, token=fake.TOKEN)
        calls = [("cluster_overview", {}), ("list_jobs", {}), ("job_status", {"job": "web"}),
                 ("job_versions", {"job": "web"}), ("alloc_status", {"alloc": "a1b2c3"}),
                 ("alloc_logs", {"alloc": "a1b2c3"}), ("list_nodes", {}),
                 ("node_status", {"node": "orangepi5plus"}), ("list_services", {}),
                 ("service", {"name": "web"}), ("list_variables", {}),
                 ("list_deployments", {"active_only": False}), ("evaluation", {"eval": "e7e7e7"})]
        # alloc_exec and job_restart are not reads; test_exec.py holds them.
        self.assertEqual(sorted(name for name, _ in calls),
                         sorted(set(tools.TOOLS) - {"alloc_exec", "job_restart"}))
        for name, args in calls:
            answer, refused = tools.call(client, name, args)
            self.assertFalse(refused, (name, answer))
        self.assertGreater(len(nomad_fake.requests), 30)
        self.assertEqual(nomad_fake.methods(), ["GET"])

    def test_the_client_has_one_write_and_no_way_to_send_another_method(self):
        source = inspect.getsource(nomad)
        # Exactly two request calls: the literal "GET", and the literal "POST" of stop_alloc.
        self.assertEqual(sorted(re.findall(r"\.request\(([^,]+),", source)), ['"GET"', '"POST"'])
        self.assertNotRegex(source, r"\b(PUT|DELETE|PATCH)\b")
        self.assertEqual(list(inspect.signature(Client.stop_alloc).parameters), ["self", "alloc_id"])
        for name in ("post", "put", "delete", "patch", "request", "put_raw"):
            self.assertFalse(hasattr(Client, name), name)
        for parameter in ("method", "body", "data"):
            self.assertNotIn(parameter, inspect.signature(Client.get_raw).parameters)
            self.assertNotIn(parameter, inspect.signature(Client.get).parameters)

    def test_no_tool_module_touches_the_network_itself(self):
        for module in ("tools", "mcp"):
            path = os.path.join(os.path.dirname(nomad.__file__), module + ".py")
            with open(path, encoding="utf-8") as handle:
                source = handle.read()
            self.assertNotIn("http.client", source, module)
            self.assertNotIn("urllib.request", source, module)


if __name__ == "__main__":
    unittest.main()
