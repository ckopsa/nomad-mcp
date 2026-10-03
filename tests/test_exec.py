"""The tests of the tools that write, alloc_exec against the fake Nomad's
exec websocket, job_restart against its allocation stop and job_revert
against its revert, and of the namespaces a call may name."""

import io
import json
import os
from unittest import mock

from nomadmcp import tools
from nomadmcp.nomad import Client

from . import fake
from .test_tools import ToolCase


class TestAllocExec(ToolCase):

    def setUp(self):
        ToolCase.setUp(self)
        patcher = mock.patch.dict(os.environ, {"NOMAD_MCP_EXEC_JOBS": "web, pair"})
        patcher.start()
        self.addCleanup(patcher.stop)

    def exec(self, **args):
        args.setdefault("job", "web")
        args.setdefault("why", "a test")
        return self.call("alloc_exec", **args)

    def test_a_job_not_on_the_list_is_refused_without_a_nomad_call(self):
        answer = self.refuse("alloc_exec", job="broken", command=["id"], why="a test")
        self.assertEqual(answer, {"refused": "job_not_allowed", "job": "broken"})
        with mock.patch.dict(os.environ, {"NOMAD_MCP_EXEC_JOBS": ""}):
            answer = self.refuse("alloc_exec", job="web", command=["id"], why="a test")
        self.assertEqual(answer["refused"], "job_not_allowed")
        self.assertEqual(self.nomad.requests, [])

    def test_an_alloc_of_another_job_is_refused(self):
        answer = self.refuse("alloc_exec", job="web", alloc=fake.ALLOC_PAIR[:8], command=["id"],
                             why="a test")
        self.assertEqual((answer["refused"], answer["alloc_job"]), ("alloc_not_in_job", "pair"))
        self.assertEqual(self.nomad.execs, [])

    def test_the_output_and_the_exit_code_come_back(self):
        answer = self.exec(command=["fail", "now"])
        self.assertEqual((answer["exit_code"], answer["stdout"], answer["stderr"]),
                         (3, "fail now\n", "boom\n"))
        self.assertFalse(answer["truncated"] or answer["timed_out"])
        alloc, task, argv, _ = self.nomad.execs[0]
        self.assertEqual((alloc, task, argv), (fake.ALLOC_WEB, "server", ["fail", "now"]))
        self.assertEqual(self.nomad.methods(), ["GET"])
        self.assertTrue(all(r[3] == fake.TOKEN for r in self.nomad.requests))

    def test_stdin_is_passed_through_and_never_logged(self):
        with mock.patch("sys.stderr", new_callable=io.StringIO) as log:
            answer = self.exec(command=["cat"], stdin="line one\nline two\n", why="read it back")
        self.assertEqual(answer["stdout"], "line one\nline two\n")
        self.assertEqual(self.nomad.execs[0][3], b"line one\nline two\n")
        self.assertIn('exec job=web alloc=a1b2c3d4 task=server argv=["cat"] why="read it back" '
                      'exit_code=0', log.getvalue())
        self.assertNotIn("line one", log.getvalue())

    def test_a_call_without_a_why_runs_and_logs_why_null(self):
        with mock.patch("sys.stderr", new_callable=io.StringIO) as log:
            answer = self.call("alloc_exec", job="web", command=["fail", "now"])
        self.assertEqual(answer["exit_code"], 3)
        self.assertIn('exec job=web alloc=a1b2c3d4 task=server argv=["fail", "now"] why=null '
                      'exit_code=3', log.getvalue())

    def test_output_past_64_kb_is_cut_to_its_tail(self):
        answer = self.exec(command=["flood"])
        self.assertTrue(answer["truncated"])
        self.assertEqual(len(answer["stdout"]), 64 * 1024)
        self.assertTrue(answer["stdout"].endswith("xEND"))
        self.assertEqual(answer["exit_code"], 0)

    def test_a_timeout_hangs_up_and_says_so(self):
        answer = self.exec(command=["sleep"], timeout_seconds=1)
        self.assertTrue(answer["timed_out"])
        self.assertIsNone(answer["exit_code"])
        self.assertIn("ran past 1s", answer["note"])
        self.assertTrue(self.nomad.hung_up.wait(5))


class NamespacesCase(ToolCase):

    def setUp(self):
        ToolCase.setUp(self)
        self.client = Client(addr=self.nomad.addr, token=fake.TOKEN, timeout=5,
                             namespaces=["default", "doors"])


class TestNamespaces(NamespacesCase):

    def test_a_namespace_not_listed_is_refused_without_a_nomad_call(self):
        for tool, args in (("job_status", {"job": "web"}), ("list_jobs", {}),
                           ("alloc_status", {"alloc": "a1b2c3"}), ("job_restart", {"job": "clone-mcp"})):
            answer = self.refuse(tool, namespace="secret", **args)
            self.assertEqual(answer, {"refused": "namespace_not_allowed", "namespace": "secret",
                                      "allowed": ["default", "doors"]})
        self.assertEqual(self.nomad.requests, [])

    def test_list_jobs_reads_every_listed_namespace(self):
        answer = self.call("list_jobs")
        spaces = {j["id"]: j["namespace"] for j in answer["jobs"]}
        self.assertEqual((spaces["web"], spaces["clone-mcp"]), ("default", "doors"))
        self.assertEqual(sorted(r[2]["namespace"] for r in self.nomad.requests), ["default", "doors"])
        answer = self.call("list_jobs", namespace="doors")
        self.assertEqual([j["id"] for j in answer["jobs"]], ["clone-mcp"])

    def test_a_job_is_read_in_the_namespace_named(self):
        self.assertEqual(self.call("job_status", job="clone-mcp", namespace="doors")["id"], "clone-mcp")
        self.assertEqual(self.refuse("job_status", job="clone-mcp")["refused"], "not_found")

    def test_an_alloc_prefix_is_found_in_any_listed_namespace(self):
        answer = self.call("alloc_status", alloc=fake.ALLOC_CLONE[:8])
        self.assertEqual((answer["id"], answer["job"]), (fake.ALLOC_CLONE, "clone-mcp"))
        reads = {r[2]["namespace"] for r in self.nomad.requests
                 if r[1] == "/v1/allocation/" + fake.ALLOC_CLONE}
        self.assertEqual(reads, {"doors"})


class TestJobRestart(NamespacesCase):

    def setUp(self):
        NamespacesCase.setUp(self)
        patcher = mock.patch.dict(os.environ, {"NOMAD_MCP_RESTART_JOBS": "doors/clone-mcp"})
        patcher.start()
        self.addCleanup(patcher.stop)
        tools._restart_cache.clear()
        self.addCleanup(tools._restart_cache.clear)
        self.variable_path = "/v1/var/" + tools.RESTART_VARIABLE

    def put_variable(self, items):
        patcher = mock.patch.dict(fake.VARIABLES, {tools.RESTART_VARIABLE: {
            "Namespace": "default", "Path": tools.RESTART_VARIABLE, "Items": items}})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_job_not_on_the_list_is_refused_without_a_nomad_call(self):
        answer = self.refuse("job_restart", job="web", why="a test")
        self.assertEqual(answer, {"refused": "job_not_allowed", "job": "web", "namespace": "default",
                                  "allowlist": "env"})
        with mock.patch.dict(os.environ, {"NOMAD_MCP_RESTART_JOBS": ""}):
            answer = self.refuse("job_restart", job="clone-mcp", namespace="doors", why="a test")
        self.assertEqual(answer["refused"], "job_not_allowed")
        # Nothing but the allow-list's own read, once: the cache holds it.
        self.assertEqual(self.nomad.paths(), [self.variable_path])

    def test_a_listed_job_in_the_wrong_namespace_is_refused(self):
        answer = self.refuse("job_restart", job="clone-mcp", why="a test")
        self.assertEqual(answer, {"refused": "job_not_allowed", "job": "clone-mcp",
                                  "namespace": "default", "allowlist": "env"})
        self.assertEqual(self.nomad.paths(), [self.variable_path])

    def test_the_variable_is_used_when_present(self):
        self.put_variable({"default/broken": "", "jobs": "default/other, doors/thing"})
        answer = self.call("restart_allowlist")
        self.assertEqual((answer["source"], answer["jobs"]),
                         ("variable", ["default/broken", "default/other", "doors/thing"]))
        self.assertNotIn("variable_unused", answer)
        with mock.patch("sys.stderr", new_callable=io.StringIO):
            answer = self.call("job_restart", job="broken", why="the runners are gone")
        self.assertEqual((answer["revived"], answer["allowlist"]), (True, "variable"))
        read = self.nomad.requests[0]
        self.assertEqual((read[0], read[1], read[2]["namespace"]), ("GET", self.variable_path, "default"))

    def test_the_env_is_used_when_the_variable_is_missing(self):
        answer = self.call("restart_allowlist")
        self.assertEqual((answer["source"], answer["jobs"]), ("env", ["doors/clone-mcp"]))
        self.assertIn("not found", answer["variable_unused"])
        with mock.patch("sys.stderr", new_callable=io.StringIO):
            answer = self.call("job_restart", job="clone-mcp", namespace="doors", why="a test")
        self.assertEqual(answer["allowlist"], "env")

    def test_a_job_outside_both_lists_is_refused(self):
        self.put_variable({"default/broken": ""})
        answer = self.refuse("job_restart", job="web", why="a test")
        self.assertEqual(answer, {"refused": "job_not_allowed", "job": "web", "namespace": "default",
                                  "allowlist": "variable"})
        self.assertEqual(self.nomad.paths(), [self.variable_path])
        self.assertEqual(self.nomad.stops, [])

    def test_a_change_to_the_variable_is_picked_up_when_the_cache_expires(self):
        now = [1000.0]
        with mock.patch.object(tools, "_clock", lambda: now[0]):
            self.assertEqual(self.call("restart_allowlist")["source"], "env")
            self.put_variable({"default/github-runner": ""})
            now[0] += tools.RESTART_CACHE_SECONDS - 1
            self.assertEqual(self.call("restart_allowlist")["source"], "env")
            now[0] += 2
            answer = self.call("restart_allowlist")
        self.assertEqual((answer["source"], answer["jobs"]), ("variable", ["default/github-runner"]))
        self.assertEqual(self.nomad.paths(), [self.variable_path, self.variable_path])

    def test_the_running_allocations_are_stopped_and_the_answer_comes_at_once(self):
        with mock.patch("sys.stderr", new_callable=io.StringIO) as log:
            answer = self.call("job_restart", job="clone-mcp", namespace="doors", why="a new image")
        self.assertEqual(answer["stopped"], [fake.ALLOC_CLONE[:8], fake.ALLOC_CLONE_B[:8]])
        self.assertEqual((answer["job"], answer["namespace"], len(answer["evals"])),
                         ("clone-mcp", "doors", 2))
        self.assertEqual(self.nomad.stops, [(fake.ALLOC_CLONE, "doors"), (fake.ALLOC_CLONE_B, "doors")])
        # The allow-list, one read, then the two stops: it never waits for the new allocations.
        self.assertEqual([r[0] for r in self.nomad.requests], ["GET", "GET", "POST", "POST"])
        self.assertTrue(all(r[3] == fake.TOKEN for r in self.nomad.requests))
        self.assertIn('restart job=clone-mcp namespace=doors allocs=c10e0000,c10e1111 '
                      'why="a new image"', log.getvalue())

    def test_a_dead_job_not_stopped_is_registered_again_with_its_own_spec(self):
        with mock.patch.dict(os.environ, {"NOMAD_MCP_RESTART_JOBS": "default/broken"}), \
                mock.patch("sys.stderr", new_callable=io.StringIO) as log:
            answer = self.call("job_restart", job="broken", why="the runners are gone")
        self.assertEqual(answer, {"job": "broken", "namespace": "default", "revived": True,
                                  "eval": "e6e6e6e6-0000-0000-0000-000000000001", "allowlist": "env"})
        spec = json.dumps(fake.JOB_SPECS["broken"]).encode("utf-8")
        self.assertEqual(self.nomad.registers, [(b'{"Job":' + spec + b"}", "default")])
        self.assertEqual(self.nomad.stops, [])
        self.assertEqual(self.nomad.requests[-1][:2], ("POST", "/v1/jobs"))
        self.assertIn('restart job=broken namespace=default allocs= why="the runners are gone" '
                      'revived=true', log.getvalue())

    def test_a_job_stopped_on_purpose_is_refused(self):
        with mock.patch.dict(os.environ, {"NOMAD_MCP_RESTART_JOBS": "default/parked"}), \
                mock.patch.dict(fake.JOB_SPECS["parked"], {"Stop": True}):
            answer = self.refuse("job_restart", job="parked", why="a test")
        self.assertEqual(answer, {"refused": "stopped_on_purpose", "job": "parked",
                                  "namespace": "default"})
        self.assertEqual(self.nomad.methods(), ["GET"])
        self.assertEqual(self.nomad.registers, [])


SECRET = "ghp-never-shown-0123456789"
VAR_PATH = "nomad/jobs/waymark-bench"


class TestVarPut(NamespacesCase):

    def setUp(self):
        NamespacesCase.setUp(self)
        for patcher in (mock.patch.dict(fake.VARIABLES),
                        mock.patch.dict(os.environ, {"NOMAD_MCP_VAR_PUT_PATHS": "default/" + VAR_PATH})):
            patcher.start()
            self.addCleanup(patcher.stop)
        tools._restart_cache.clear()
        self.addCleanup(tools._restart_cache.clear)
        fake.VARIABLES[VAR_PATH] = {"Namespace": "default", "Path": VAR_PATH, "ModifyIndex": 41,
                                    "Items": {"BENCH_GITHUB_TOKEN": "kept-as-it-was"}}

    def put(self, **args):
        args.setdefault("path", VAR_PATH)
        args.setdefault("key", "BENCH_SECRETS_TOKEN")
        args.setdefault("value", SECRET)
        with mock.patch("sys.stderr", new_callable=io.StringIO) as log:
            answer, refused = tools.call(self.client, "var_put", args)
        return answer, refused, log.getvalue()

    def test_the_value_is_a_secret_reference(self):
        value = tools.TOOLS["var_put"]["schema"]["properties"]["value"]
        self.assertIs(value["x-secret-ref"], True)

    def test_one_key_is_set_and_the_others_kept(self):
        answer, refused, log = self.put(why="hand over the token")
        self.assertFalse(refused, answer)
        self.assertEqual(answer, {"path": VAR_PATH, "key": "BENCH_SECRETS_TOKEN", "namespace": "default",
                                  "modify_index": 42})
        self.assertEqual(fake.VARIABLES[VAR_PATH]["Items"],
                         {"BENCH_GITHUB_TOKEN": "kept-as-it-was", "BENCH_SECRETS_TOKEN": SECRET})
        put = self.nomad.requests[-1]
        self.assertEqual((put[0], put[1], put[2]["cas"], put[2]["namespace"]),
                         ("POST", "/v1/var/" + VAR_PATH, "41", "default"))
        self.assertEqual(self.nomad.methods(), ["GET", "POST"])
        self.assertIn('var_put namespace=default path=%s key=BENCH_SECRETS_TOKEN '
                      'why="hand over the token" modify_index=42' % VAR_PATH, log)
        self.assertNotIn(SECRET, json.dumps(answer) + log)
        self.assertNotIn("kept-as-it-was", json.dumps(answer) + log)

    def test_a_new_variable_is_made_with_cas_zero(self):
        with mock.patch.dict(os.environ, {"NOMAD_MCP_VAR_PUT_PATHS": "default/nomad/jobs/nomad-mcp/new"}):
            answer, refused, _ = self.put(path="nomad/jobs/nomad-mcp/new")
        self.assertFalse(refused, answer)
        self.assertEqual(answer["modify_index"], 1)
        self.assertEqual(self.nomad.requests[-1][2]["cas"], "0")

    def test_a_stale_modify_index_refuses(self):
        real = self.client.put_variable

        def racing(*args):
            fake.VARIABLES[VAR_PATH] = dict(fake.VARIABLES[VAR_PATH], ModifyIndex=50,
                                            Items={"BENCH_GITHUB_TOKEN": "rotated-meanwhile"})
            return real(*args)

        self.client.put_variable = racing
        answer, refused, log = self.put()
        self.assertTrue(refused)
        self.assertEqual((answer["refused"], answer["status"]), ("conflict", 409))
        self.assertEqual(fake.VARIABLES[VAR_PATH]["Items"], {"BENCH_GITHUB_TOKEN": "rotated-meanwhile"})
        for text in (SECRET, "rotated-meanwhile", "kept-as-it-was"):
            self.assertNotIn(text, json.dumps(answer) + log)

    def test_a_path_outside_the_list_is_refused_before_any_read(self):
        answer, refused, _ = self.put(path="nomad/jobs/web")
        self.assertTrue(refused)
        self.assertEqual(answer, {"refused": "path_not_allowed", "path": "nomad/jobs/web",
                                  "namespace": "default", "allowlist": "env"})
        answer, refused, _ = self.put(namespace="doors")
        self.assertEqual(answer["refused"], "path_not_allowed")
        self.assertEqual(self.nomad.paths(), ["/v1/var/" + tools.VAR_PUT_VARIABLE])
        self.assertNotIn(SECRET, json.dumps(answer))

    def test_the_variable_list_is_used_when_present(self):
        fake.VARIABLES[tools.VAR_PUT_VARIABLE] = {"Items": {"paths": "default/nomad/jobs/clone-mcp"}}
        answer, refused, _ = self.put()
        self.assertEqual((answer["refused"], answer["allowlist"]), ("path_not_allowed", "variable"))
        answer, refused, _ = self.put(path="nomad/jobs/clone-mcp")
        self.assertFalse(refused, answer)


class TestJobRevert(NamespacesCase):

    def setUp(self):
        NamespacesCase.setUp(self)
        patcher = mock.patch.dict(os.environ, {"NOMAD_MCP_RESTART_JOBS": "doors/clone-mcp, default/broken"})
        patcher.start()
        self.addCleanup(patcher.stop)
        tools._restart_cache.clear()
        self.addCleanup(tools._restart_cache.clear)
        patcher = mock.patch("sys.stderr", new_callable=io.StringIO)
        self.log = patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_job_is_reverted_with_the_version_read_enforced(self):
        answer = self.call("job_revert", job="clone-mcp", namespace="doors", version=2, why="a bad image")
        self.assertEqual(answer, {"job": "clone-mcp", "namespace": "doors", "reverted_to": 2, "from": 3,
                                  "version": 4, "eval": "e4e4e4e4-0000-0000-0000-000000000001",
                                  "allowlist": "env"})
        self.assertEqual(self.nomad.reverts, [("clone-mcp", {"JobID": "clone-mcp", "JobVersion": 2,
                                                             "EnforcePriorVersion": 3}, "doors")])
        self.assertEqual(self.nomad.requests[-1][:2], ("POST", "/v1/job/clone-mcp/revert"))
        self.assertTrue(all(r[3] == fake.TOKEN for r in self.nomad.requests))
        self.assertIn('revert job=clone-mcp namespace=doors from=3 to=2 why="a bad image" eval=e4e4e4e4',
                      self.log.getvalue())

    def test_a_change_since_the_read_is_refused_not_overwritten(self):
        self.nomad.moved["broken"] = 4
        answer = self.refuse("job_revert", job="broken", version=2, why="a test")
        self.assertEqual((answer["refused"], answer["read"]), ("version_moved", 3))
        self.assertIn("enforcing version 3", answer["reason"])
        self.assertEqual([r[1]["EnforcePriorVersion"] for r in self.nomad.reverts], [3])
        self.assertIn("refused=version_moved", self.log.getvalue())

    def test_a_job_not_on_the_list_is_refused_without_a_write(self):
        answer = self.refuse("job_revert", job="web", version=2, why="a test")
        self.assertEqual(answer, {"refused": "job_not_allowed", "job": "web", "namespace": "default",
                                  "allowlist": "env"})
        answer = self.refuse("job_revert", job="clone-mcp", version=2, why="a test")
        self.assertEqual(answer["refused"], "job_not_allowed")
        self.assertEqual(self.nomad.paths(), ["/v1/var/" + tools.RESTART_VARIABLE])
        self.assertEqual(self.nomad.reverts, [])

    def test_a_version_too_far_back_or_not_earlier_is_refused(self):
        with mock.patch.dict(fake.JOB_SPECS["broken"], {"Version": 19}):
            answer = self.refuse("job_revert", job="broken", version=8, why="a test")
            self.assertEqual((answer["refused"], answer["oldest"]), ("too_old", 9))
            self.assertEqual(self.refuse("job_revert", job="broken", version=19)["refused"], "not_earlier")
            self.assertEqual(self.refuse("job_revert", job="broken", version="18")["refused"], "input")
            self.assertEqual(self.nomad.methods(), ["GET"])
            answer = self.call("job_revert", job="broken", version=18, why="the image is gone")
        self.assertEqual((answer["reverted_to"], answer["from"], answer["version"]), (18, 19, 20))
        self.assertEqual([r[1]["JobVersion"] for r in self.nomad.reverts], [18])
