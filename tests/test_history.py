"""The tests of the allocation history: a fake event stream, and a failure
read back after Nomad has collected its allocation."""

import json
import os
import shutil
import tempfile
import threading
import time
import unittest

from nomadmcp import history, tools
from nomadmcp.nomad import Client

from . import fake


GONE = "0dead000-0000-0000-0000-00000000000c"
PULL = "Failed to pull `ghcr.io/example/broken:abc123`: manifest unknown: not found"


def _gone(ident=GONE, ago=fake.HOUR_NS, events=None):
    events = events or [fake._event("Received", "Task received by client", ago),
                        fake._event("Driver Failure", PULL, ago, DriverError=PULL, FailsTask=True)]
    return fake._alloc(ident, "broken", "main", fake.NODE_B, "big-colt", "failed",
                       {"server": fake._state("dead", failed=True, events=events)}, ago)


def _batch(index, alloc):
    return {"Index": index, "Events": [{"Topic": "Allocation", "Type": "AllocationUpdated",
                                        "Key": alloc["ID"], "Namespace": "default",
                                        "Index": index, "Payload": {"Allocation": alloc}}]}


class TestHistory(unittest.TestCase):

    def setUp(self):
        self.nomad = fake.FakeNomad()
        self.addCleanup(self.nomad.close)
        self.client = Client(addr=self.nomad.addr, token=fake.TOKEN, timeout=5)
        self.addCleanup(setattr, tools, "HISTORY", tools.HISTORY)
        tools.HISTORY = history.History()

    def follow(self, *batches):
        """Serves the batches with heartbeats between, and follows until the last is read."""
        self.nomad.stream = b"".join(json.dumps(b).encode() + b"\n{}\n" for b in batches)
        stop = threading.Event()
        thread = threading.Thread(target=tools.HISTORY.follow, args=(self.client, "default", stop),
                                  daemon=True)
        thread.start()
        deadline = time.time() + 5
        while tools.HISTORY.index.get("default") != batches[-1]["Index"] and time.time() < deadline:
            time.sleep(0.02)
        stop.set()
        thread.join(5)

    def call(self, tool, **args):
        answer, refused = tools.call(self.client, tool, args)
        self.assertFalse(refused, answer)
        return answer

    def test_a_failed_pull_is_served_after_the_allocation_is_gone(self):
        self.follow(_batch(7, fake.ALLOCS[0]), _batch(9, _gone()))
        method, _, query, token = next(r for r in self.nomad.requests if r[1] == "/v1/event/stream")
        self.assertEqual((method, query["topic"], query["namespace"], token),
                         ("GET", "Allocation", "default", fake.TOKEN))
        # Nomad has collected it: the job's allocation list is empty.
        answer = self.call("job_status", job="broken")
        self.assertEqual(answer["allocations"], [])
        end = answer["recent_ends"][0]
        self.assertEqual((end["id"], end["client"], end["node"]), ("0dead000", "failed", "big-colt"))
        self.assertEqual(end["events"]["server"][-1]["driver_error"], PULL)
        self.assertTrue(end["events"]["server"][-1]["fails_task"])
        self.assertEqual([e["id"] for e in self.call("alloc_history", job="broken")["ends"]],
                         ["0dead000"])
        # A running allocation is no end, and a job with live allocations shows none.
        self.assertEqual(self.call("alloc_history", job="web")["ends"], [])
        self.assertNotIn("recent_ends", self.call("job_status", job="web"))
        self.assertEqual(self.nomad.methods(), ["GET"])

    def test_the_ring_keeps_the_newest_20_per_job_and_their_last_10_events(self):
        ring = tools.HISTORY
        events = [fake._event("Restarting", "try %s" % i, fake.HOUR_NS) for i in range(15)]
        for i in range(25):
            self.assertTrue(ring.record(_gone("0dead%03d-0000-0000-0000-000000000000" % i,
                                              (30 - i) * fake.HOUR_NS, events)))
        self.assertFalse(ring.record(fake.ALLOCS[0]))  # running
        kept = ring.ends("default", "broken")
        self.assertEqual(len(kept), history.KEEP_PER_JOB)
        self.assertEqual(kept[0]["ID"][:8], "0dead024")
        self.assertEqual([e["DisplayMessage"] for e in kept[0]["TaskStates"]["server"]["Events"]],
                         ["try %s" % i for i in range(5, 15)])
        # Past 7 days an end is let go.
        ring.record(_gone(GONE, 8 * 24 * fake.HOUR_NS))
        self.assertNotIn(GONE, [a["ID"] for a in ring.ends("default", "broken")])

    def test_the_file_in_the_alloc_dir_outlives_the_server(self):
        directory = tempfile.mkdtemp(prefix="nomad-mcp-")
        self.addCleanup(shutil.rmtree, directory, True)
        os.makedirs(os.path.join(directory, "data"))
        history.History.from_env({"NOMAD_ALLOC_DIR": directory}).record(_gone())
        again = history.History.from_env({"NOMAD_ALLOC_DIR": directory})
        self.assertEqual(again.path, os.path.join(directory, "data", history.FILE_NAME))
        self.assertEqual([a["ID"] for a in again.ends("default", "broken")], [GONE])


if __name__ == "__main__":
    unittest.main()
