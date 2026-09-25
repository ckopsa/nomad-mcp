"""The tests of the tools, against the fake Nomad over TCP."""

import json
import unittest

from nomadmcp import tools
from nomadmcp.nomad import Client

from . import fake


class ToolCase(unittest.TestCase):

    forbid = ()

    def setUp(self):
        self.nomad = fake.FakeNomad(forbid=self.forbid)
        self.addCleanup(self.nomad.close)
        self.client = Client(addr=self.nomad.addr, token=fake.TOKEN, timeout=5)

    def call(self, tool, **args):
        answer, refused = tools.call(self.client, tool, args)
        self.assertFalse(refused, answer)
        return answer

    def refuse(self, tool, **args):
        answer, refused = tools.call(self.client, tool, args)
        self.assertTrue(refused, answer)
        return answer


class TestOverview(ToolCase):

    def test_overview_names_what_needs_attention(self):
        answer = self.call("cluster_overview")
        self.assertEqual(answer["leader"], "192.168.1.40:4647")
        self.assertEqual([n["name"] for n in answer["nodes"]], ["orangepi5plus", "big-colt"])
        self.assertEqual(answer["nodes"][0]["unhealthy_drivers"], ["exec"])
        attention = {j["job"]: j["problems"] for j in answer["jobs"]["needing_attention"]}
        # A dead job whose spec asks for an allocation is broken...
        self.assertIn("broken", attention)
        self.assertIn("spec asks for 1", attention["broken"][0])
        # ...and one parked at count 0 is not.
        self.assertNotIn("parked", attention)
        self.assertEqual(answer["jobs"]["parked_count_0"], ["parked"])
        self.assertIn("node pool 'default' has no ready node", attention["nfs"])
        self.assertIn("1 starting", attention["pair"])
        # Children never appear on their own; the healthy job does not appear.
        self.assertNotIn("web", attention)
        self.assertNotIn("backup/periodic-1790305200", attention)
        stuck = [a["job"] for a in answer["allocations_not_running"]]
        self.assertEqual(stuck, ["pair"])  # A failed batch alloc is not "stuck".
        self.assertEqual(answer["failures_last_24h"]["backup"]["failed"], 1)
        blocked = answer["blocked_evaluations"][0]
        reasons = blocked["placement_failures"]["web"]["reasons"]
        self.assertIn("constraint ${attr.cpu.arch} = amd64 filtered 1 node(s)", reasons)
        self.assertIn("memory exhausted on 1 node(s)", reasons)
        self.assertEqual(blocked["placement_failures"]["web"]["more_failed_the_same_way"], 2)
        self.assertEqual(answer["deployments_active_or_failed_24h"][0]["status"], "failed")
        self.assertTrue(answer["csi_plugins"][0]["unhealthy"])
        self.assertNotIn("unavailable", answer)


class TestJobs(ToolCase):

    def test_list_jobs_hides_children_and_zero_counts(self):
        answer = self.call("list_jobs")
        ids = [j["id"] for j in answer["jobs"]]
        self.assertNotIn("backup/periodic-1790305200", ids)
        web = next(j for j in answer["jobs"] if j["id"] == "web")
        self.assertEqual(web["groups"], {"web": {"running": 1, "failed": 40}})
        answer = self.call("list_jobs", include_children=True, type="batch")
        self.assertEqual([j["id"] for j in answer["jobs"]], ["backup", "backup/periodic-1790305200"])

    def test_job_status_summarizes(self):
        answer = self.call("job_status", job="web")
        self.assertEqual(answer["version"], 3)
        self.assertEqual(answer["groups"]["web"]["tasks"]["server"]["image"], "ghcr.io/example/web:abc123")
        self.assertEqual(answer["constraints"], ["${attr.cpu.arch} = arm64"])
        self.assertEqual([a["id"] for a in answer["allocations"]], ["a1b2c3d4", "a1b2ffff"])
        self.assertEqual(answer["allocations"][0]["tasks"]["server"]["restarts"], 2)
        self.assertEqual(answer["latest_deployment"]["groups"]["web"]["unhealthy"], 1)
        failures = answer["evaluations_with_failures"]
        self.assertEqual(failures[0]["status"], "blocked")
        self.assertEqual(failures[1]["placement_failures"]["web"]["reasons"],
                         ["no nodes in node pool 'default'"])

    def test_unknown_job_names_similar_ones(self):
        answer = self.refuse("job_status", job="we")
        self.assertEqual(answer["refused"], "not_found")
        self.assertEqual(answer["similar"], ["web"])

    def test_job_versions_gives_changed_leaves(self):
        answer = self.call("job_versions", job="web")
        newest = answer["versions"][0]
        self.assertEqual(newest["version"], 3)
        self.assertIn("~ group web/ReschedulePolicy/Delay: 30000000000 (30s) -> 15000000000 (15s)",
                      newest["changes"])
        self.assertIn("~ group web/task server/Config/image: ghcr.io/example/web:old -> "
                      "ghcr.io/example/web:abc123", newest["changes"])
        self.assertFalse(any("Count" in line or "Driver" in line for line in newest["changes"]))
        self.assertIn("resubmission", answer["versions"][1]["changes"][0])
        self.assertIn("oldest", answer["versions"][2]["changes"][0])


class TestAllocations(ToolCase):

    def test_alloc_status_by_prefix(self):
        answer = self.call("alloc_status", alloc="a1b2c3")
        self.assertEqual(answer["id"], fake.ALLOC_WEB)
        server = answer["tasks"]["server"]
        self.assertEqual(server["restarts"], 2)
        terminated = next(e for e in server["events"] if e["type"] == "Terminated")
        self.assertEqual(terminated["exit_code"], 137)
        self.assertIn("OOM", terminated["message"])
        self.assertEqual(answer["ports"]["http"], "192.168.1.40:8080 -> 80")
        self.assertEqual(answer["resources"]["server"]["memory_max_mb"], 512)
        answer = self.call("alloc_status", alloc="a1b2c3d4", events=1)
        self.assertEqual(len(answer["tasks"]["server"]["events"]), 1)

    def test_an_odd_prefix_is_sent_even_and_narrowed_here(self):
        self.call("alloc_status", alloc="a1b2c")
        sent = [q.get("prefix") for m, p, q, t in self.nomad.requests if p == "/v1/allocations"]
        self.assertEqual(sent, ["a1b2"])

    def test_an_ambiguous_prefix_is_refused_with_the_candidates(self):
        answer = self.refuse("alloc_status", alloc="a1b2")
        self.assertEqual(answer["refused"], "ambiguous")
        self.assertEqual(sorted(c["id"] for c in answer["candidates"]),
                         [fake.ALLOC_WEB, fake.ALLOC_WEB_OLD])
        self.assertEqual(answer["candidates"][0]["job"], "web")

    def test_bad_and_missing_ids(self):
        self.assertEqual(self.refuse("alloc_status", alloc="zz")["refused"], "input")
        self.assertEqual(self.refuse("alloc_status", alloc="a")["refused"], "input")
        self.assertEqual(self.refuse("alloc_status", alloc="9999")["refused"], "not_found")
        self.assertEqual(self.refuse("alloc_status")["field"], "alloc")

    def test_alloc_logs_tails_the_single_task(self):
        answer = self.call("alloc_logs", alloc="a1b2c3d4", tail_bytes=500)
        self.assertEqual(answer["task"], "server")
        self.assertEqual(answer["stream"], "stderr")
        self.assertTrue(answer["truncated_to_tail"])
        self.assertLessEqual(answer["bytes"], 500)
        self.assertTrue(answer["log"].endswith("line 399 of the log\n"))
        self.assertNotIn("\x1b", answer["log"])
        # The first line is cut mid-way by the tail, so it is dropped.
        self.assertRegex(answer["log"].split("\n")[0], r"^2026-09-25T05:27:\d\dZ line \d+ of the log$")
        logs = [q for m, p, q, t in self.nomad.requests if p.startswith("/v1/client/fs/logs/")]
        self.assertEqual(logs[0], {"task": "server", "type": "stderr", "origin": "end",
                                   "offset": "500", "plain": "true", "namespace": "default"})

    def test_alloc_logs_caps_the_tail(self):
        self.call("alloc_logs", alloc="a1b2c3d4", tail_bytes=10 ** 9, stream="stdout")
        logs = [q for m, p, q, t in self.nomad.requests if p.startswith("/v1/client/fs/logs/")]
        self.assertEqual(logs[0]["offset"], str(tools.CEILING_TAIL))
        self.assertEqual(logs[0]["type"], "stdout")

    def test_alloc_logs_asks_for_the_task_when_there_are_two(self):
        answer = self.refuse("alloc_logs", alloc="c0ffee")
        self.assertEqual(answer["field"], "task")
        self.assertEqual(answer["tasks"], ["app", "sidecar"])
        answer = self.refuse("alloc_logs", alloc="c0ffee", task="nope")
        self.assertEqual(answer["tasks"], ["app", "sidecar"])
        self.assertEqual(self.refuse("alloc_logs", alloc="c0ffee", task="app",
                                     stream="both")["field"], "stream")


class TestNodesAndTheRest(ToolCase):

    def test_node_status_by_name_never_shows_the_secret(self):
        answer = self.call("node_status", node="orangepi5plus")
        self.assertEqual(answer["id"], fake.NODE_A)
        self.assertEqual(answer["address"], "192.168.1.40")
        self.assertEqual(answer["attributes"]["cpu.arch"], "arm64")
        self.assertNotIn("noise.attr", answer["attributes"])
        self.assertEqual(answer["drivers"]["exec"], "unhealthy: cgroups missing")
        self.assertEqual(answer["csi_node_plugins"]["s3"], "unhealthy: fingerprint failed")
        self.assertEqual(answer["resources"]["capacity"]["memory_mb"], 31785)
        # Running and pending allocations on the node count; the web alloc is one task of 256 MB.
        self.assertEqual(answer["resources"]["allocated"]["memory_mb"], 256)
        self.assertEqual(answer["resources"]["free"]["memory_mb"], 31785 - 1024 - 256)
        self.assertEqual(answer["events"][0]["message"], "Node heartbeat missed")
        self.assertNotIn(fake.NODE_SECRET, json.dumps(answer))
        self.assertEqual(self.call("node_status", node="0e19")["name"], "orangepi5plus")

    def test_list_nodes(self):
        answer = self.call("list_nodes")
        self.assertEqual(answer["count"], 2)
        self.assertEqual(answer["nodes"][1]["pool"], "amd64")
        self.assertEqual(answer["nodes"][0]["drivers"], ["docker"])

    def test_services(self):
        answer = self.call("list_services")
        web = next(s for s in answer["services"] if s["name"] == "web")
        self.assertEqual(web["hosts"], ["web.example.org"])
        answer = self.call("service", name="web")
        self.assertEqual(answer["registrations"][0]["address"], "192.168.1.40:8080")
        self.assertEqual(answer["registrations"][0]["node"], "orangepi5plus")
        self.assertEqual(self.refuse("service", name="ghost")["refused"], "not_found")

    def test_variables_are_paths_only(self):
        answer = self.call("list_variables", prefix="nomad/jobs/web")
        self.assertEqual([v["path"] for v in answer["variables"]],
                         ["nomad/jobs/web", "nomad/jobs/web/deploy"])
        self.assertNotIn("hunter2", json.dumps(answer))
        self.assertFalse([p for p in self.nomad.paths() if p.startswith("/v1/var/")])

    def test_deployments(self):
        self.assertEqual(self.call("list_deployments")["deployments"], [])
        answer = self.call("list_deployments", active_only=False)
        self.assertEqual([d["status"] for d in answer["deployments"]], ["failed", "successful"])
        answer = self.call("list_deployments", active_only=False, job="web", limit=1)
        self.assertEqual(len(answer["deployments"]), 1)

    def test_evaluation(self):
        answer = self.call("evaluation", eval="e7e7e7")
        self.assertEqual(answer["id"], fake.EVAL_BLOCKED)
        self.assertEqual(answer["queued_allocations"], {"web": 1})
        self.assertEqual(answer["related"][0]["triggered_by"], "job-register")
        self.assertEqual(answer["allocations"][0]["id"], "a1b2c3d4")
        self.assertEqual(self.refuse("evaluation", eval="e7e7")["refused"], "ambiguous")

    def test_unknown_tool_and_bad_arguments(self):
        answer, refused = tools.call(self.client, "stop_job", {"job": "web"})
        self.assertTrue(refused)
        self.assertEqual(answer["refused"], "unknown_tool")
        answer, refused = tools.call(self.client, "list_jobs", ["not", "an", "object"])
        self.assertTrue(refused)


class TestForbidden(ToolCase):
    """A token without a capability gets a refusal naming the path, not a crash."""

    forbid = ("/v1/plugins", "/v1/vars", "/v1/client/fs/logs/")

    def test_a_403_is_a_refusal(self):
        answer = self.refuse("list_variables")
        self.assertEqual(answer["refused"], "nomad")
        self.assertEqual(answer["status"], 403)
        self.assertIn("forbidden", answer["reason"])
        self.assertIn("/v1/vars", answer["reason"])
        self.assertIn("client node", self.refuse("alloc_logs", alloc="a1b2c3d4")["hint"])

    def test_the_overview_survives_a_403_in_one_section(self):
        answer = self.call("cluster_overview")
        self.assertNotIn("csi_plugins", answer)
        self.assertTrue(any("csi plugins: forbidden" in n for n in answer["unavailable"]))
        self.assertIn("nodes", answer)


class TestUnreachable(unittest.TestCase):

    def test_no_nomad_is_a_refusal(self):
        client = Client(addr="http://127.0.0.1:9", timeout=2, token="tok-xyz")
        answer, refused = tools.call(client, "list_nodes", {})
        self.assertTrue(refused)
        self.assertIn("cannot reach Nomad", answer["reason"])
        self.assertNotIn("tok-xyz", json.dumps(answer))
        overview, refused = tools.call(client, "cluster_overview", {})
        self.assertTrue(refused)  # Nothing read at all is not an empty cluster.
        self.assertIn("cannot reach Nomad", overview["reason"])


if __name__ == "__main__":
    unittest.main()
