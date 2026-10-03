"""The registry check, against a stub registry over TCP."""

import json
import time
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

from nomadmcp import registry, tools
from nomadmcp.nomad import Client

from . import fake


DIGEST = "sha256:" + "ab" * 32


class _Handler(BaseHTTPRequestHandler):

    def log_message(self, *args):
        pass

    def _end(self, status, **headers):
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key.replace("_", "-"), value)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if not self.path.startswith("/token?"):
            return self._end(405)
        body = json.dumps({"token": "anon"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        self.server.seen.append(self.path)
        name, _, tag = self.path[len("/v2/"):].partition("/manifests/")
        if name.endswith("/slow"):
            time.sleep(1)
        if name == "team/locked" and self.headers.get("Authorization") != "Bearer anon":
            realm = "http://%s:%s/token" % self.server.server_address
            self._end(401, WWW_Authenticate='Bearer realm="%s",service="stub"' % realm)
        elif name.endswith("/broken") or tag == "missing":
            self._end(404)
        else:
            self._end(200, Docker_Content_Digest=DIGEST)


class RegistryCase(unittest.TestCase):

    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.server.daemon_threads = True
        self.server.seen = []
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.base = "http://127.0.0.1:%s" % self.server.server_address[1]

    def registry(self, host, **kwargs):
        return registry.Registry(bases={host: self.base}, **kwargs)


class TestCheck(RegistryCase):

    def test_present_tag_is_true_with_its_digest(self):
        answer = self.registry("reg.test").check("reg.test/team/app:v1")
        self.assertEqual(answer, {"image_present": True, "registry_status": 200, "image_digest": DIGEST})
        self.assertEqual(self.server.seen, ["/v2/team/app/manifests/v1"])

    def test_missing_tag_is_false(self):
        answer = self.registry("reg.test").check("reg.test/team/app:missing")
        self.assertEqual(answer, {"image_present": False, "registry_status": 404})

    def test_timeout_is_unknown_and_quick(self):
        started = time.monotonic()
        answer = self.registry("reg.test", timeout=0.3).check("reg.test/team/slow:v1")
        self.assertLess(time.monotonic() - started, 0.9)
        self.assertEqual(answer["image_present"], "unknown")
        self.assertIn("registry_error", answer)

    def test_anonymous_token_is_fetched_on_a_challenge(self):
        answer = self.registry("reg.test").check("reg.test/team/locked:v1")
        self.assertIs(answer["image_present"], True)

    def test_answers_are_cached_per_image(self):
        reg = self.registry("reg.test")
        reg.check("reg.test/team/app:v1")
        reg.check("reg.test/team/app:v1")
        self.assertEqual(len(self.server.seen), 1)

    def test_parse(self):
        self.assertEqual(registry.parse("redis"), (registry.DOCKER_HUB, "library/redis", "latest"))
        self.assertEqual(registry.parse("docker.kopsa.info/github-runner:2.337.0-2-amd64"),
                         ("docker.kopsa.info", "github-runner", "2.337.0-2-amd64"))
        self.assertEqual(registry.parse("localhost:5000/a/b@" + DIGEST), ("localhost:5000", "a/b", DIGEST))
        self.assertIsNone(registry.parse("ghcr.io/x/${NOMAD_META_tag}"))


class TestTools(RegistryCase):

    def setUp(self):
        RegistryCase.setUp(self)
        self.nomad = fake.FakeNomad(forbid=(), down=())
        self.addCleanup(self.nomad.close)
        self.client = Client(addr=self.nomad.addr, token=fake.TOKEN, timeout=5)
        patch = mock.patch.object(tools, "REGISTRY", self.registry("ghcr.io"))
        patch.start()
        self.addCleanup(patch.stop)

    def call(self, tool, **args):
        answer, refused = tools.call(self.client, tool, args)
        self.assertFalse(refused, answer)
        return answer

    def test_job_status_says_the_image_is_present(self):
        task = self.call("job_status", job="web")["groups"]["web"]["tasks"]["server"]
        self.assertIs(task["image_present"], True)
        self.assertEqual(task["registry_status"], 200)

    def test_overview_names_a_dead_job_whose_image_is_missing(self):
        answer = self.call("cluster_overview")
        attention = {j["job"]: j["problems"] for j in answer["jobs"]["needing_attention"]}
        self.assertIn("image not in registry: ghcr.io/example/broken:abc123", attention["broken"])
        self.assertNotIn("/v2/example/web/manifests/abc123", self.server.seen)


if __name__ == "__main__":
    unittest.main()
