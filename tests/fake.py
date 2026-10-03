"""A fake Nomad for the tests: a real HTTP server with canned answers.

No test talks to a real cluster. The fake listens on 127.0.0.1 or on a
unix socket, answers the GET paths the tools read, and records every
request it sees, method included, so a test can prove that nothing but
GET ever left the client but an allocation stop or a job register. Any other method or
path is recorded and answered 405. A second namespace, doors, holds one
job of its own.

The shapes are trimmed copies of what a Nomad 2.0 agent answers.
"""

import base64
import json
import os
import socketserver
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


NOW_NS = int(time.time() * 1e9)
HOUR_NS = 3600 * 10 ** 9
TOKEN = "s3cr3t-token-value"

NODE_A = "0e192304-dbfc-e2cf-824a-6dc796884403"
NODE_B = "d61ebfd1-24c9-75ff-821b-9c2d68a56013"
ALLOC_WEB = "a1b2c3d4-0000-0000-0000-000000000001"
ALLOC_WEB_OLD = "a1b2ffff-0000-0000-0000-000000000002"
ALLOC_PAIR = "c0ffee00-0000-0000-0000-000000000003"
ALLOC_BACKUP = "b0b0b0b0-0000-0000-0000-000000000004"
EVAL_BLOCKED = "e7e7e7e7-0000-0000-0000-000000000005"
ALLOC_CLONE = "c10e0000-0000-0000-0000-000000000009"
ALLOC_CLONE_B = "c10e1111-0000-0000-0000-00000000000a"
ALLOC_CLONE_OLD = "c10e2222-0000-0000-0000-00000000000b"
NODE_SECRET = "node-secret-id-never-shown"
# Variables answered by path; a test patches one in. Unset, nomad-mcp's own
# paths answer 404.
VARIABLES = {}


def _node(ident, name, pool, address):
    return {"ID": ident, "Name": name, "NodePool": pool, "Address": address, "Datacenter": "home",
            "Status": "ready", "SchedulingEligibility": "eligible", "Drain": False,
            "Version": "2.0.2", "NodeClass": "",
            "Drivers": {"docker": {"Detected": True, "Healthy": True},
                        "exec": {"Detected": True, "Healthy": False,
                                 "HealthDescription": "cgroups missing"},
                        "qemu": {"Detected": False, "Healthy": False}}}


def _summary(**groups):
    base = {"Complete": 0, "Failed": 0, "Lost": 0, "Queued": 0, "Running": 0, "Starting": 0, "Unknown": 0}
    return {"Summary": {name: dict(base, **counts) for name, counts in groups.items()},
            "Children": {"Dead": 0, "Pending": 0, "Running": 0}}


def _job_stub(ident, kind="service", status="running", pool="arm64", parent="", **groups):
    return {"ID": ident, "Name": ident, "Type": kind, "Status": status, "Priority": 50,
            "NodePool": pool, "ParentID": parent, "Stop": False, "Periodic": False,
            "ParameterizedJob": False, "JobSummary": _summary(**groups), "SubmitTime": NOW_NS}


def _event(kind, message, ago_ns, **extra):
    event = {"Type": kind, "DisplayMessage": message, "Message": "", "Time": NOW_NS - ago_ns,
             "ExitCode": 0, "DriverError": "", "FailsTask": False}
    event.update(extra)
    return event


def _alloc(ident, job, group, node, node_name, client, tasks, created_ago, job_type="service",
           version=3):
    return {"ID": ident, "JobID": job, "JobType": job_type, "TaskGroup": group, "NodeID": node,
            "NodeName": node_name, "DesiredStatus": "run", "ClientStatus": client,
            "JobVersion": version, "Name": "%s.%s[0]" % (job, group),
            "CreateTime": NOW_NS - created_ago, "ModifyTime": NOW_NS - created_ago + 10 ** 9,
            "TaskStates": tasks}


def _state(state, restarts=0, failed=False, events=None):
    return {"State": state, "Restarts": restarts, "Failed": failed,
            "StartedAt": "2026-09-20T23:05:10.35710523Z", "FinishedAt": None,
            "LastRestart": "2026-09-21T07:04:52.604263097+08:00" if restarts else None,
            "Events": events or []}


ALLOCS = [
    _alloc(ALLOC_WEB, "web", "web", NODE_A, "orangepi5plus", "running",
           {"server": _state("running", 2, events=[
               _event("Received", "Task received by client", 3 * HOUR_NS),
               _event("Terminated", "Exit Code: 137, Exit Message: \"OOM Killed\"", 2 * HOUR_NS,
                      ExitCode=137),
               _event("Restarting", "Task restarting in 15s", 2 * HOUR_NS,
                      RestartReason="Restart within policy"),
               _event("Started", "Task started by client", HOUR_NS)])}, 3 * HOUR_NS),
    _alloc(ALLOC_WEB_OLD, "web", "web", NODE_A, "orangepi5plus", "complete",
           {"server": _state("dead")}, 30 * HOUR_NS, version=2),
    _alloc(ALLOC_PAIR, "pair", "main", NODE_B, "big-colt", "pending",
           {"app": _state("pending"), "sidecar": _state("pending")}, HOUR_NS),
    _alloc(ALLOC_BACKUP, "backup/periodic-1790305200", "backup", NODE_A, "orangepi5plus", "failed",
           {"run": _state("dead", failed=True, events=[
               _event("Driver Failure", "Failed to pull image", HOUR_NS,
                      DriverError="unauthorized", FailsTask=True)])},
           2 * HOUR_NS, job_type="batch"),
]
DOORS_ALLOCS = [dict(_alloc(ident, "clone-mcp", "clone", NODE_A, "orangepi5plus", client,
                            {"server": _state("running" if client == "running" else "dead")}, ago),
                     Namespace="doors")
                for ident, client, ago in ((ALLOC_CLONE, "running", HOUR_NS),
                                           (ALLOC_CLONE_B, "running", 2 * HOUR_NS),
                                           (ALLOC_CLONE_OLD, "complete", 30 * HOUR_NS))]
for _a in ALLOCS + DOORS_ALLOCS:
    _a["DesiredStatus"] = "run" if _a["ClientStatus"] != "complete" else "stop"
    _a["AllocatedResources"] = {
        "Tasks": {name: {"Cpu": {"CpuShares": 200}, "Memory": {"MemoryMB": 256, "MemoryMaxMB": 512}}
                  for name in _a["TaskStates"]},
        "Shared": {"DiskMB": 300, "Ports": [{"Label": "http", "Value": 8080, "To": 80,
                                             "HostIP": "192.168.1.40"}]}}

JOBS = [
    _job_stub("web", web={"Running": 1, "Failed": 40}),
    _job_stub("broken", status="dead", main={"Failed": 3}),
    _job_stub("parked", status="dead", main={}),
    _job_stub("pair", pool="amd64", main={"Starting": 1}),
    _job_stub("nfs", kind="system", pool="default", nodes={}),
    _job_stub("backup", kind="batch", backup={"Failed": 5}),
    _job_stub("backup/periodic-1790305200", kind="batch", parent="backup", backup={"Failed": 1}),
]
JOBS[5]["Periodic"] = True
DOORS_JOBS = [dict(_job_stub("clone-mcp", clone={"Running": 2}), Namespace="doors")]


def _job_spec(stub, counts):
    job = {key: stub[key] for key in ("ID", "Type", "Status", "Priority", "NodePool", "Stop", "SubmitTime")}
    job.update({"Version": 3, "Stable": False, "Datacenters": ["home"], "StatusDescription": "",
                "Constraints": [{"LTarget": "${attr.cpu.arch}", "Operand": "=", "RTarget": "arm64"}],
                "TaskGroups": [{"Name": name, "Count": count, "Tasks": [
                    {"Name": "server", "Driver": "docker",
                     "Config": {"image": "ghcr.io/example/%s:abc123" % stub["ID"], "args": ["--port", "80"]},
                     "Resources": {"CPU": 200, "MemoryMB": 256}}]} for name, count in counts.items()]})
    return job


JOB_SPECS = {
    "web": _job_spec(JOBS[0], {"web": 1}),
    "broken": _job_spec(JOBS[1], {"main": 1}),
    "parked": _job_spec(JOBS[2], {"main": 0}),
    "pair": _job_spec(JOBS[3], {"main": 1}),
    "nfs": _job_spec(JOBS[4], {"nodes": 1}),
}
DOORS_SPECS = {"clone-mcp": _job_spec(DOORS_JOBS[0], {"clone": 2})}

EVALS = [
    {"ID": EVAL_BLOCKED, "JobID": "web", "Status": "blocked", "Type": "service", "Priority": 50,
     "TriggeredBy": "queued-allocs", "StatusDescription": "created to place remaining allocations",
     "CreateTime": NOW_NS - HOUR_NS, "ModifyTime": NOW_NS - HOUR_NS, "ModifyIndex": 20,
     "QueuedAllocations": {"web": 1},
     "FailedTGAllocs": {"web": {
         "NodePool": "arm64", "NodesInPool": 1, "NodesEvaluated": 1, "NodesFiltered": 0,
         "NodesAvailable": {"home": 1}, "ClassFiltered": None,
         "ConstraintFiltered": {"${attr.cpu.arch} = amd64": 1},
         "DimensionExhausted": {"memory": 1}, "NodesExhausted": 1, "CoalescedFailures": 2}}},
    {"ID": "e7e70000-0000-0000-0000-000000000006", "JobID": "web", "Status": "complete",
     "Type": "service", "TriggeredBy": "job-register", "ModifyTime": NOW_NS - 3 * HOUR_NS,
     "ModifyIndex": 10, "BlockedEval": EVAL_BLOCKED,
     "FailedTGAllocs": {"web": {"NodePool": "default", "NodesInPool": 0, "NodesEvaluated": 0}}},
]

DEPLOYMENTS = [
    {"ID": "d3d3d3d3-0000-0000-0000-000000000007", "JobID": "web", "JobVersion": 3,
     "Status": "failed", "StatusDescription": "Failed due to progress deadline", "ModifyIndex": 30,
     "ModifyTime": 0, "CreateTime": NOW_NS - HOUR_NS,
     "TaskGroups": {"web": {"DesiredTotal": 1, "PlacedAllocs": 1, "HealthyAllocs": 0,
                            "UnhealthyAllocs": 1, "DesiredCanaries": 0}}},
    {"ID": "d4d4d4d4-0000-0000-0000-000000000008", "JobID": "web", "JobVersion": 2,
     "Status": "successful", "StatusDescription": "Deployment completed successfully",
     "ModifyIndex": 5, "CreateTime": NOW_NS - 40 * HOUR_NS,
     "TaskGroups": {"web": {"DesiredTotal": 1, "PlacedAllocs": 1, "HealthyAllocs": 1,
                            "UnhealthyAllocs": 0}}},
]

VERSIONS = {
    "Versions": [{"Version": 3, "Stable": False, "SubmitTime": NOW_NS - HOUR_NS},
                 {"Version": 2, "Stable": True, "SubmitTime": NOW_NS - 40 * HOUR_NS},
                 {"Version": 1, "Stable": True, "SubmitTime": NOW_NS - 80 * HOUR_NS}],
    "Diffs": [
        {"Type": "Edited", "Fields": None, "Objects": None, "TaskGroups": [{
            "Name": "web", "Type": "Edited", "Fields": [
                {"Name": "Count", "Old": "1", "New": "1", "Type": "None"}],
            "Objects": [{"Name": "ReschedulePolicy", "Type": "Edited", "Fields": [
                {"Name": "Delay", "Old": "30000000000", "New": "15000000000", "Type": "Edited"}],
                "Objects": None}],
            "Tasks": [{"Name": "server", "Type": "Edited", "Fields": [
                {"Name": "Driver", "Old": "docker", "New": "docker", "Type": "None"}],
                "Objects": [{"Name": "Config", "Type": "Edited", "Fields": [
                    {"Name": "image", "Old": "ghcr.io/example/web:old", "New": "ghcr.io/example/web:abc123",
                     "Type": "Edited"}], "Objects": None}]}]}]},
        {"Type": "None", "TaskGroups": []},
    ],
}

NODE_DETAIL = {
    NODE_A: dict(_node(NODE_A, "orangepi5plus", "arm64", "192.168.1.40"),
                 SecretID=NODE_SECRET, HTTPAddr="192.168.1.40:4646",
                 Attributes={"cpu.arch": "arm64", "os.name": "ubuntu", "nomad.version": "2.0.2",
                             "unique.network.ip-address": "192.168.1.40", "noise.attr": "x"},
                 NodeResources={"Cpu": {"CpuShares": 14400}, "Memory": {"MemoryMB": 31785},
                                "Disk": {"DiskMB": 231342}},
                 ReservedResources={"Cpu": {"CpuShares": 400}, "Memory": {"MemoryMB": 1024}},
                 CSINodePlugins={"s3": {"Healthy": False, "HealthDescription": "fingerprint failed"}},
                 HostVolumes={"postgres": {}, "minio-data": {}}, StatusUpdatedAt=int(time.time()) - 60,
                 Meta={"gc_max_allocs": "200"},
                 Events=[{"Timestamp": "2026-09-20T23:00:00Z", "Subsystem": "Cluster",
                          "Message": "Node heartbeat missed", "Details": None}]),
}
del NODE_DETAIL[NODE_A]["Address"]  # The single-node read has no Address field.

MB = 1024 * 1024
_ROOT_DISK = {"Device": "/dev/nvme0n1p2", "Mountpoint": "/", "Size": 238000 * MB,
              "Used": 202300 * MB, "Available": 35700 * MB, "UsedPercent": 85.0,
              "InodesUsedPercent": 12.34}
HOST_STATS = {
    "Memory": {"Total": 31785 * MB, "Used": 11785 * MB, "Available": 20000 * MB, "Free": 1000 * MB},
    "Uptime": 3 * 86400 + 4 * 3600 + 5 * 60 + 7,
    "DiskStats": [_ROOT_DISK,
                  {"Device": "/dev/nvme0n1p1", "Mountpoint": "/boot/firmware", "Size": 512 * MB,
                   "Used": 100 * MB, "Available": 412 * MB, "UsedPercent": 19.53,
                   "InodesUsedPercent": 0.0}],
    "AllocDirStats": dict(_ROOT_DISK, Mountpoint=""),
    "Timestamp": NOW_NS,
}

LOG_TEXT = ("partial line that the tail cuts\n"
            + "".join("\x1b[90m2026-09-25T05:27:%02dZ\x1b[0m line %d of the log\n" % (i % 60, i)
                      for i in range(400)))


def doors_routes(path, query):
    """Gives (status, body) for one GET of a namespaced path in doors, or None."""
    prefix = query.get("prefix", "")
    if path == "/v1/jobs":
        return 200, [j for j in DOORS_JOBS if j["ID"].startswith(prefix)]
    if path == "/v1/allocations":
        return 200, [a for a in DOORS_ALLOCS if a["ID"].startswith(prefix)]
    if path.startswith("/v1/allocation/"):
        found = [a for a in DOORS_ALLOCS if a["ID"] == path.split("/")[3]]
        return (200, found[0]) if found else (404, "alloc not found")
    if path.startswith("/v1/job/"):
        parts = path.split("/")
        job = urllib.parse.unquote(parts[3])
        rest = parts[4] if len(parts) > 4 else ""
        if job not in DOORS_SPECS:
            return 404, "job not found"
        if rest == "":
            return 200, DOORS_SPECS[job]
        if rest == "allocations":
            return 200, [a for a in DOORS_ALLOCS if a["JobID"] == job]
        return 200, None if rest in ("deployment", "summary") else []
    if path in ("/v1/evaluations", "/v1/deployments"):
        return 200, []
    return None


def routes(path, query):
    """Gives (status, body) for one GET."""
    if query.get("namespace") == "doors":
        answer = doors_routes(path, query)
        if answer is not None:
            return answer
    prefix = query.get("prefix", "")
    if prefix and len(prefix.replace("-", "")) % 2 and path in ("/v1/allocations", "/v1/nodes",
                                                                 "/v1/evaluations"):
        # The real agent refuses an odd prefix with a 500.
        return 500, "index error: Input (without hyphens) must be even length"

    def by_prefix(items):
        return [i for i in items if i["ID"].startswith(prefix)]

    if path == "/v1/status/leader":
        return 200, "192.168.1.40:4647"
    if path == "/v1/nodes":
        return 200, by_prefix([_node(NODE_A, "orangepi5plus", "arm64", "192.168.1.40"),
                               _node(NODE_B, "big-colt", "amd64", "192.168.1.231")])
    if path.startswith("/v1/node/") and path.endswith("/allocations"):
        node = path.split("/")[3]
        return 200, [a for a in ALLOCS if a["NodeID"] == node]
    if path.startswith("/v1/node/"):
        node = path.split("/")[3]
        return (200, NODE_DETAIL[node]) if node in NODE_DETAIL else (404, "node not found")
    if path == "/v1/jobs":
        return 200, [j for j in JOBS if j["ID"].startswith(prefix)]
    if path.startswith("/v1/job/"):
        parts = path.split("/")
        job = urllib.parse.unquote(parts[3])
        rest = parts[4] if len(parts) > 4 else ""
        if job not in JOB_SPECS:
            return 404, "job not found"
        if rest == "":
            return 200, JOB_SPECS[job]
        if rest == "allocations":
            return 200, [a for a in ALLOCS if a["JobID"] == job]
        if rest == "evaluations":
            return 200, [e for e in EVALS if e["JobID"] == job]
        if rest == "deployment":
            return 200, next((d for d in DEPLOYMENTS if d["JobID"] == job), None)
        if rest == "deployments":
            return 200, [d for d in DEPLOYMENTS if d["JobID"] == job]
        if rest == "versions":
            return 200, VERSIONS
        if rest == "summary":
            return 200, {"Children": {"Dead": 4, "Running": 0}}
    if path == "/v1/allocations":
        return 200, by_prefix(ALLOCS)
    if path.startswith("/v1/allocation/"):
        ident = path.split("/")[3]
        found = [a for a in ALLOCS if a["ID"] == ident]
        return (200, found[0]) if found else (404, "alloc not found")
    if path.startswith("/v1/client/fs/logs/"):
        ident = path.split("/")[5]
        alloc = next((a for a in ALLOCS if a["ID"] == ident), None)
        if alloc is None or query.get("task") not in alloc["TaskStates"]:
            return 400, 'unknown task name "%s"' % query.get("task")
        data = LOG_TEXT.encode("utf-8")
        if query.get("origin") == "end":
            data = data[-int(query.get("offset", 0)):] if int(query.get("offset", 0)) else data
        return 200, data
    if path == "/v1/evaluations":
        items = by_prefix(EVALS)
        if query.get("filter") == 'Status == "blocked"':
            items = [e for e in items if e["Status"] == "blocked"]
        return 200, items
    if path.startswith("/v1/evaluation/") and path.endswith("/allocations"):
        return 200, [ALLOCS[0]]
    if path.startswith("/v1/evaluation/"):
        ident = path.split("/")[3]
        found = [dict(e, RelatedEvals=[{"ID": EVALS[1]["ID"], "Status": "complete",
                                        "TriggeredBy": "job-register"}])
                 for e in EVALS if e["ID"] == ident]
        return (200, found[0]) if found else (404, "eval not found")
    if path == "/v1/deployments":
        return 200, DEPLOYMENTS
    if path == "/v1/services":
        return 200, [{"Namespace": "default", "Services": [
            {"ServiceName": "web", "Tags": ["traefik.enable=true",
                                            "traefik.http.routers.web.rule=Host(`web.example.org`)"]},
            {"ServiceName": "postgres", "Tags": []}]}]
    if path == "/v1/service/web":
        return 200, [{"ServiceName": "web", "Address": "192.168.1.40", "Port": 8080, "NodeID": NODE_A,
                      "AllocID": ALLOC_WEB, "JobID": "web", "Datacenter": "home", "Tags": ["t1"]}]
    if path.startswith("/v1/service/"):
        return 200, []
    if path == "/v1/vars":
        items = [{"Path": "nomad/jobs/web", "ModifyTime": NOW_NS - HOUR_NS, "Namespace": "default"},
                 {"Path": "nomad/jobs/web/deploy", "ModifyTime": NOW_NS, "Namespace": "default"},
                 {"Path": "other/thing", "ModifyTime": NOW_NS, "Namespace": "default"}]
        return 200, [v for v in items if v["Path"].startswith(prefix)]
    if path[8:] in VARIABLES and path.startswith("/v1/var/"):
        return 200, VARIABLES[path[8:]]
    if path.startswith("/v1/var/nomad/jobs/nomad-mcp/"):
        return 404, "variable not found"
    if path.startswith("/v1/var/"):
        return 200, {"Path": path[8:], "Items": {"password": "hunter2"}}
    if path == "/v1/client/stats":
        if query.get("node_id") != NODE_A:
            return 500, "Unknown node %s" % query.get("node_id")
        return 200, HOST_STATS
    if path == "/v1/plugins":
        return 200, [{"ID": "s3", "ControllerRequired": True, "ControllersHealthy": 1,
                      "ControllersExpected": 1, "NodesHealthy": 1, "NodesExpected": 2}]
    return 404, "no route"


class FakeNomad:
    """Records every request as (method, path, query, token)."""

    def __init__(self, unix_path=None, forbid=(), down=()):
        self.requests = []
        self.execs = []  # (alloc, task, argv, stdin) for each exec that ran
        self.stops = []  # (alloc, namespace) for each allocation stop
        self.registers = []  # (body, namespace) for each job register
        self.hung_up = threading.Event()  # set when a sleep exec saw the client close
        self.forbid = tuple(forbid)
        self.down = tuple(down)  # paths answered as a client the servers cannot reach
        self.stream = b"{}\n"  # the body /v1/event/stream answers, then ends
        handler = self._handler()
        if unix_path:
            class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
                daemon_threads = True
            self.httpd = Server(unix_path, handler)
            self.addr = "unix://" + unix_path
        else:
            self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
            self.httpd.daemon_threads = True
            self.addr = "http://127.0.0.1:%s" % self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, args=(0.05,), daemon=True)
        self.thread.start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    def methods(self):
        return sorted({r[0] for r in self.requests})

    def paths(self):
        return [r[1] for r in self.requests]

    def _handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt, *args):
                pass

            def _answer(self, status, body):
                if isinstance(body, (bytes, bytearray)):
                    data = bytes(body)
                elif isinstance(body, str) and status >= 400:
                    data = body.encode("utf-8")
                else:
                    data = json.dumps(body).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _record(self):
                parsed = urllib.parse.urlsplit(self.path)
                query = dict(urllib.parse.parse_qsl(parsed.query))
                fake.requests.append((self.command, parsed.path, query,
                                      self.headers.get("X-Nomad-Token")))
                return parsed.path, query

            def do_GET(self):
                path, query = self._record()
                if any(path.startswith(p) for p in fake.forbid):
                    self._answer(403, "Permission denied")
                    return
                if any(path.startswith(p) for p in fake.down):
                    self._answer(500, "rpc error: no path to node")
                    return
                if path == "/v1/event/stream":
                    self._answer(200, fake.stream)
                    return
                if path.endswith("/exec") and self.headers.get("Upgrade", "").lower() == "websocket":
                    self._exec(path, query)
                    return
                status, body = routes(path, query)
                self._answer(status, body)

            def _exec(self, path, query):
                """Canned commands: cat echoes stdin, flood writes 100 KB, sleep waits
                for the hang-up, fail exits 3; anything else echoes its argv."""
                self.send_response(101)
                self.send_header("Upgrade", "websocket")
                self.send_header("Connection", "Upgrade")
                self.end_headers()
                self.close_connection = True
                argv = json.loads(query.get("command", "[]"))
                stdin, message = b"", {}
                while not (message.get("stdin") or {}).get("close"):
                    message = self._ws_read()
                    if message is None:
                        return
                    stdin += base64.b64decode((message.get("stdin") or {}).get("data", ""))
                fake.execs.append((path.split("/")[4], query.get("task"), argv, stdin))
                name = argv[0] if argv else ""
                if name == "sleep":
                    while self._ws_read() is not None:
                        pass
                    fake.hung_up.set()
                    return
                out = {"cat": stdin, "flood": b"x" * 100000 + b"END"}.get(
                    name, " ".join(argv).encode() + b"\n")
                for start in range(0, len(out), 30000):
                    self._ws_send({"stdout": {"data": base64.b64encode(out[start:start + 30000]).decode()}})
                if name == "fail":
                    self._ws_send({"stderr": {"data": base64.b64encode(b"boom\n").decode()}})
                self._ws_send({"exited": True, "result": {"exit_code": 3 if name == "fail" else 0}})

            def _ws_send(self, message):
                data = json.dumps(message).encode()
                size = len(data)
                head = bytes([0x81, size]) if size < 126 else bytes([0x81, 126]) + size.to_bytes(2, "big")
                self.wfile.write(head + data)

            def _ws_read(self):
                head = self.rfile.read(2)
                if len(head) < 2 or head[0] & 0x0F == 0x8:
                    return None
                size = head[1] & 0x7F
                if size == 126:
                    size = int.from_bytes(self.rfile.read(2), "big")
                mask = self.rfile.read(4)
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(self.rfile.read(size)))
                return json.loads(data)

            def _refuse(self):
                self._record()
                self._answer(405, "method not allowed")

            def do_POST(self):
                """The writes the client may send: an allocation stop, a job register."""
                path, query = self._record()
                if path == "/v1/jobs":
                    body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                    fake.registers.append((body, query.get("namespace")))
                    self._answer(200, {"EvalID": "e6e6e6e6-0000-0000-0000-%012d" % len(fake.registers),
                                       "Index": 8, "JobModifyIndex": 8})
                    return
                parts = path.split("/")
                if len(parts) == 5 and parts[1:3] == ["v1", "allocation"] and parts[4] == "stop":
                    fake.stops.append((parts[3], query.get("namespace")))
                    self._answer(200, {"EvalID": "e5e5e5e5-0000-0000-0000-%012d" % len(fake.stops),
                                       "Index": 7})
                    return
                self._answer(405, "method not allowed")

            do_PUT = do_DELETE = do_PATCH = _refuse

        return Handler


def unix_path(directory):
    return os.path.join(directory, "api.sock")
