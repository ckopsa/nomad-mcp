"""The tests of alloc_exec, against the fake Nomad's exec websocket."""

import io
import os
from unittest import mock

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
