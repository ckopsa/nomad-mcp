"""The allocation history: what an allocation said as it ended, kept after
Nomad forgets it.

WHY: a client past its GC disk threshold collects every dead allocation at
once, and the task events and logs go with it. On 2026-10-03 a job sat dead
with no allocation at all, its driver's pull error erased before anyone read
it. So the server follows the event stream's Allocation topic and keeps,
for each allocation it sees end failed or lost, its job, group, node, task
states and last 10 task events: the newest 20 per job, for 7 days, in a
small sqlite file. NOMAD_MCP_HISTORY names it; inside a task it is
$NOMAD_ALLOC_DIR/data/alloc-history.sqlite, which outlives a restart of the
task; elsewhere it lives in memory.

It reads through the client's GET alone and writes only its own file.
"""

import json
import os
import sqlite3
import sys
import threading
import time


KEEP_PER_JOB = 20
KEEP_SECONDS = 7 * 24 * 3600
KEEP_EVENTS = 10
ENDED = ("failed", "lost")
RETRY_SECONDS = 5
FILE_NAME = "alloc-history.sqlite"
_ALLOC_KEYS = ("ID", "Namespace", "JobID", "TaskGroup", "NodeID", "NodeName", "DesiredStatus",
               "ClientStatus", "ClientDescription", "JobVersion", "CreateTime", "ModifyTime")
_EVENT_KEYS = ("Time", "Type", "Message", "DisplayMessage", "ExitCode", "Signal", "DriverError",
               "SetupError", "DownloadError", "KillError", "KillReason", "RestartReason",
               "ValidationError", "VaultError", "FailsTask")


class History:
    """The kept ends, in one sqlite table. Every method is safe across threads."""

    def __init__(self, path=":memory:"):
        self.path = path
        self.started = time.time()
        self.index = {}  # namespace -> the last stream index read
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        with self._lock, self._db:
            self._db.execute("CREATE TABLE IF NOT EXISTS ends (alloc TEXT PRIMARY KEY, namespace TEXT,"
                             " job TEXT, ended REAL, record TEXT)")

    @classmethod
    def from_env(cls, environ=None):
        env = os.environ if environ is None else environ
        path = env.get("NOMAD_MCP_HISTORY")
        if not path and env.get("NOMAD_ALLOC_DIR"):
            path = os.path.join(env["NOMAD_ALLOC_DIR"], "data", FILE_NAME)
        try:
            return cls(path or ":memory:")
        except sqlite3.Error as exc:
            print("alloc history: cannot open %s (%s); keeping it in memory" % (path, exc),
                  file=sys.stderr)
            return cls()

    def record(self, alloc, namespace="default", now=None):
        """Keeps one allocation that ended failed or lost. Gives True when kept."""
        if not isinstance(alloc, dict) or alloc.get("ClientStatus") not in ENDED or not alloc.get("ID"):
            return False
        now = time.time() if now is None else now
        kept = {key: alloc[key] for key in _ALLOC_KEYS if alloc.get(key) is not None}
        kept.setdefault("Namespace", namespace)
        kept["TaskStates"] = {}
        for name, state in (alloc.get("TaskStates") or {}).items():
            state = state or {}
            kept["TaskStates"][name] = {
                "State": state.get("State"), "Failed": state.get("Failed"),
                "Restarts": state.get("Restarts", 0), "LastRestart": state.get("LastRestart"),
                "Events": [{key: event[key] for key in _EVENT_KEYS if event.get(key) not in (None, "")}
                           for event in (state.get("Events") or [])[-KEEP_EVENTS:]
                           if isinstance(event, dict)]}
        ended = (alloc.get("ModifyTime") or 0) / 1e9 or now
        where = (kept["Namespace"], kept.get("JobID"))
        with self._lock, self._db:
            self._db.execute("INSERT OR REPLACE INTO ends VALUES (?, ?, ?, ?, ?)",
                             (kept["ID"],) + where + (ended, json.dumps(kept)))
            self._db.execute("DELETE FROM ends WHERE ended < ?", (now - KEEP_SECONDS,))
            self._db.execute("DELETE FROM ends WHERE namespace = ? AND job = ? AND alloc NOT IN"
                             " (SELECT alloc FROM ends WHERE namespace = ? AND job = ?"
                             " ORDER BY ended DESC LIMIT ?)", where + where + (KEEP_PER_JOB,))
        return True

    def ends(self, namespace, job, now=None):
        """The kept ends of one job, newest first, as slim allocations."""
        now = time.time() if now is None else now
        with self._lock:
            rows = self._db.execute("SELECT record FROM ends WHERE namespace = ? AND job = ?"
                                    " AND ended >= ? ORDER BY ended DESC LIMIT ?",
                                    (namespace, job, now - KEEP_SECONDS, KEEP_PER_JOB)).fetchall()
        return [json.loads(row[0]) for row in rows]

    def follow(self, client, namespace, stop):
        """Follows the Allocation topic of one namespace until stop is set,
        keeping each allocation that ends. A broken stream is opened again
        from the last index read after RETRY_SECONDS."""
        while not stop.is_set():
            try:
                for batch in client.events("Allocation", self.index.get(namespace, 0), namespace):
                    for event in batch.get("Events") or []:
                        self.record((event.get("Payload") or {}).get("Allocation"), namespace)
                    if batch.get("Index"):
                        self.index[namespace] = batch["Index"]
                    if stop.is_set():
                        return
            except Exception as exc:  # The follower never dies of one bad stream.
                print("alloc history: %s: %s" % (namespace, getattr(exc, "reason", exc)),
                      file=sys.stderr)
            stop.wait(RETRY_SECONDS)

    def start(self, client):
        """Follows every namespace the client lists, each on a thread of its
        own. Gives the event that stops them."""
        stop = threading.Event()
        for namespace in client.namespaces:
            threading.Thread(target=self.follow, args=(client, namespace, stop), daemon=True,
                             name="alloc-history-%s" % namespace).start()
        return stop
