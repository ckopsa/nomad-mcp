"""The tools of nomad-mcp.

Each tool is a function over a nomad.Client and its arguments, and it
gives a dictionary. The answers are summaries for troubleshooting, not
the raw API: the raw JSON of one allocation is tens of kilobytes, and a
model reading it spends its attention on the wrong fields. So every
tool picks the fields that answer "what is wrong and why", caps its
lists, and says when it cut something.

A refusal is a Refusal exception with a name and its data. A failed
read of Nomad (unreachable, 403, 404) is a refusal too. No tool gives a
stack trace.

Nothing here writes but alloc_exec, which runs one command through
execws for a job on its allow-list. The client has no method that could.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import quote

from . import execws
from .nomad import NomadError


DEFAULT_LIST = 50
CEILING_LIST = 500
DEFAULT_TAIL = 8000
CEILING_TAIL = 64000
DEFAULT_EVENTS = 10
CEILING_EVENTS = 50
TEXT_CAP = 300
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
HEXISH = re.compile(r"^[0-9a-f-]+$")
# A deployment still in motion. Everything else (successful, failed,
# cancelled) is history.
ACTIVE_DEPLOYMENTS = ("running", "paused", "pending", "blocked", "unblocking", "initializing")
# Allocation states that mean "this should be running and is not".
TROUBLE_CLIENT = ("pending", "failed", "lost", "unknown")


class Refusal(Exception):
    """A refusal with a name and its data."""

    def __init__(self, name, **data):
        self.data = {"refused": name}
        self.data.update(data)
        Exception.__init__(self, name)


# ---------------------------------------------------------------- helpers


def _int(args, key, default, low, high):
    value = args.get(key, default)
    if value is None:
        value = default
    try:
        value = int(value)
    except (TypeError, ValueError):
        raise Refusal("input", field=key, reason="a whole number is necessary")
    return max(low, min(high, value))


def _text(args, key, required=False, default=None, choices=None):
    value = args.get(key, default)
    if value is None or value == "":
        if required:
            raise Refusal("input", field=key, reason="a value is necessary")
        return default
    if not isinstance(value, str):
        raise Refusal("input", field=key, reason="a text is necessary")
    value = value.strip()
    if choices and value not in choices:
        raise Refusal("input", field=key, reason="use one of: %s" % ", ".join(choices))
    return value


def _bool(args, key, default):
    value = args.get(key, default)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return value.lower() == "true"
    raise Refusal("input", field=key, reason="true or false is necessary")


def short(ident):
    return (ident or "")[:8]


def clip(text, cap=TEXT_CAP):
    if text is None:
        return None
    text = str(text)
    return text if len(text) <= cap else text[:cap] + "..."


def _seconds_ago(seconds):
    seconds = int(seconds)
    if seconds < 0:
        return "in the future"
    if seconds < 90:
        return "%ss ago" % seconds
    minutes = seconds // 60
    if minutes < 90:
        return "%sm ago" % minutes
    hours = minutes // 60
    if hours < 48:
        return "%sh%02dm ago" % (hours, minutes % 60)
    return "%sd ago" % (hours // 24)


def to_datetime(value):
    """Nomad spells time two ways: nanoseconds since the epoch, and RFC 3339
    text in the agent's own zone. Both become an aware UTC datetime."""
    if value in (None, 0, ""):
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1e9, tz=timezone.utc)
    if isinstance(value, str):
        if value.startswith("0001-01-01"):
            return None  # Go's zero time: "never".
        text = value.replace("Z", "+00:00")
        # Python 3.11 reads at most 6 fractional digits; Nomad writes 9.
        text = re.sub(r"(\.\d{6})\d+", r"\1", text)
        try:
            moment = datetime.fromisoformat(text)
        except ValueError:
            return None
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return moment.astimezone(timezone.utc)
    return None


def stamp(value):
    """A time as 'YYYY-MM-DD HH:MM:SSZ (3h12m ago)', or None for never.

    The age is there because "when" in troubleshooting almost always
    means "how long ago", and a model is poor at subtracting timestamps.
    """
    moment = to_datetime(value)
    if moment is None:
        return None
    age = time.time() - moment.timestamp()
    return "%s (%s)" % (moment.strftime("%Y-%m-%d %H:%M:%SZ"), _seconds_ago(age))


def nonzero(counts):
    """Keeps the counts that are not zero, with lowercase names."""
    return {key.lower(): value for key, value in (counts or {}).items()
            if isinstance(value, int) and value}


def capped(items, limit, name="items"):
    """Gives (the first limit items, a note or None)."""
    if len(items) <= limit:
        return items, None
    return items[:limit], "%s of %s %s shown" % (limit, len(items), name)


def _get(client, path, params=None):
    try:
        return client.get(path, params)
    except NomadError as exc:
        raise Refusal("nomad", reason=exc.reason, status=exc.status or None)


def _try(client, path, params=None, notes=None, label=None):
    """A read that may fail without failing the tool: the reason goes in notes."""
    try:
        return client.get(path, params)
    except NomadError as exc:
        if notes is not None:
            notes.append("%s: %s" % (label or path, exc.reason))
        return None


def log_call(name, args, refused=None):
    """Writes one short line for the call to the standard error."""
    fields = ["tool=%s" % name]
    for key in ("job", "alloc", "node", "eval", "name", "prefix"):
        value = args.get(key)
        if isinstance(value, str) and value:
            fields.append("%s=%s" % (key, value[:60]))
    if refused:
        fields.append("refused=%s" % refused)
    print("nomad-mcp call " + " ".join(fields), file=sys.stderr)


# --------------------------------------------------------------- resolving


_ARG = {"allocation": "alloc", "node": "node", "evaluation": "eval"}
_KINDS = {
    "allocation": "/v1/allocations",
    "node": "/v1/nodes",
    "evaluation": "/v1/evaluations",
}


def _describe(kind, item):
    if kind == "allocation":
        return {"id": item.get("ID"), "job": item.get("JobID"), "group": item.get("TaskGroup"),
                "client_status": item.get("ClientStatus"), "node": item.get("NodeName")}
    if kind == "node":
        return {"id": item.get("ID"), "name": item.get("Name"), "status": item.get("Status")}
    return {"id": item.get("ID"), "job": item.get("JobID"), "status": item.get("Status"),
            "triggered_by": item.get("TriggeredBy")}


def resolve(client, kind, ident):
    """Turns a full id, an id prefix, or (for a node) a name into a full id.

    Nomad's ?prefix= wants an even number of hex digits and answers 500
    otherwise, so an odd prefix is sent one digit short and the list is
    narrowed here. Two or more matches is a refusal that names them:
    guessing which allocation the model meant is how you read the wrong
    logs.
    """
    field = _ARG[kind]
    if isinstance(ident, int) and not isinstance(ident, bool):
        ident = str(ident)  # A prefix of digits arrives as a number from some clients.
    if not isinstance(ident, str) or not ident.strip():
        raise Refusal("input", field=field, reason="an id or an id prefix is necessary")
    ident = ident.strip()
    lowered = ident.lower()
    if UUID.match(lowered):
        return lowered
    path = _KINDS[kind]
    if kind == "node":
        # Names are what people say; a name match wins over a prefix match.
        nodes = _get(client, path) or []
        named = [n for n in nodes if n.get("Name") == ident]
        if len(named) == 1:
            return named[0]["ID"]
        matches = named or [n for n in nodes if n.get("ID", "").startswith(lowered)]
    else:
        if not HEXISH.match(lowered):
            raise Refusal("input", field=field, reason="an id prefix is hex digits and dashes")
        if len(lowered.replace("-", "")) < 2:
            raise Refusal("input", field=field, reason="give at least 2 characters of the id")
        query = lowered
        if len(query.replace("-", "")) % 2:
            query = query[:-1]
            if query.endswith("-"):
                query = query[:-1]
        items = _get(client, path, {"prefix": query}) or []
        matches = [item for item in items if item.get("ID", "").startswith(lowered)]
    if not matches:
        raise Refusal("not_found", kind=kind, given=ident)
    if len(matches) > 1:
        shown, note = capped([_describe(kind, m) for m in matches], 20, "candidates")
        answer = {"kind": kind, "given": ident, "candidates": shown}
        if note:
            answer["note"] = note
        raise Refusal("ambiguous", **answer)
    return matches[0]["ID"]


def _job(client, job_id):
    """Reads a job by its exact id; on a 404 names the jobs that start with it."""
    if not isinstance(job_id, str) or not job_id.strip():
        raise Refusal("input", field="job", reason="a job id is necessary")
    job_id = job_id.strip()
    try:
        return client.get("/v1/job/%s" % _quote(job_id))
    except NomadError as exc:
        if exc.status != 404:
            raise Refusal("nomad", reason=exc.reason, status=exc.status or None)
    similar = _try(client, "/v1/jobs", {"prefix": job_id}) or []
    raise Refusal("not_found", kind="job", given=job_id,
                  similar=[j.get("ID") for j in similar[:20]])


def _quote(part):
    return quote(part, safe="")


# ------------------------------------------------------- humanized pieces


def placement_failures(failed_tg_allocs):
    """Turns an evaluation's FailedTGAllocs into sentences per task group.

    The raw AllocMetric is a dozen counters, most of them zero or null.
    The sentences keep the ones that explain the failure: an empty node
    pool, a constraint that filtered nodes out, a resource that ran out.
    """
    answer = {}
    for group, metric in (failed_tg_allocs or {}).items():
        metric = metric or {}
        reasons = []
        pool = metric.get("NodePool")
        in_pool = metric.get("NodesInPool")
        evaluated = metric.get("NodesEvaluated") or 0
        if in_pool == 0:
            reasons.append("no nodes in node pool %r" % pool if pool else "no nodes in the node pool")
        available = metric.get("NodesAvailable") or {}
        if available is not None and in_pool != 0 and not any(available.values()):
            reasons.append("no nodes available in the job's datacenters (%s)"
                           % (", ".join(sorted(available)) or "none match"))
        for klass, count in (metric.get("ClassFiltered") or {}).items():
            reasons.append("node class %r filtered %s node(s)" % (klass, count))
        for constraint, count in (metric.get("ConstraintFiltered") or {}).items():
            reasons.append("constraint %s filtered %s node(s)" % (constraint, count))
        for dimension, count in (metric.get("DimensionExhausted") or {}).items():
            reasons.append("%s exhausted on %s node(s)" % (dimension, count))
        for klass, count in (metric.get("ClassExhausted") or {}).items():
            reasons.append("node class %r exhausted on %s node(s)" % (klass, count))
        for quota in metric.get("QuotaExhausted") or []:
            reasons.append("quota exhausted: %s" % quota)
        if not reasons and metric.get("NodesExhausted"):
            reasons.append("resources exhausted on %s node(s)" % metric["NodesExhausted"])
        if not reasons and metric.get("NodesFiltered"):
            reasons.append("%s node(s) filtered out" % metric["NodesFiltered"])
        if not reasons:
            reasons.append("no reason recorded (evaluated %s node(s))" % evaluated)
        coalesced = metric.get("CoalescedFailures") or 0
        entry = {"nodes_evaluated": evaluated, "reasons": reasons}
        if pool:
            entry["node_pool"] = pool
        if coalesced:
            entry["more_failed_the_same_way"] = coalesced
        answer[group] = entry
    return answer


_DURATION_FIELD = re.compile(r"(Delay|Interval|Timeout|Deadline|Time|Window|Duration|Wait)$")


def _duration(name, value):
    """Nomad diffs print durations as nanoseconds. Adds '(15s)' where the
    field name says it is a duration."""
    if value is None or not _DURATION_FIELD.search(name or ""):
        return value
    try:
        nanos = int(value)
    except (TypeError, ValueError):
        return value
    if nanos < 1000000 or nanos % 1000000:
        return value
    seconds = nanos / 1e9
    if seconds >= 3600 and seconds % 3600 == 0:
        human = "%dh" % (seconds // 3600)
    elif seconds >= 60 and seconds % 60 == 0:
        human = "%dm" % (seconds // 60)
    else:
        human = ("%g" % seconds) + "s"
    return "%s (%s)" % (value, human)


def flatten_diff(diff, limit):
    """Walks one JobDiff and gives its changes as short lines.

    A JobDiff nests Fields and Objects under the job, its task groups and
    their tasks, and marks every node with Type None, Added, Deleted or
    Edited. The lines keep only the changed leaves, with their path.
    """
    lines = []

    def field(path, f):
        if f.get("Type") in (None, "None"):
            return
        name = f.get("Name")
        old = _duration(name, f.get("Old"))
        new = _duration(name, f.get("New"))
        where = "/".join(path + [name])
        if f.get("Type") == "Added":
            lines.append("+ %s = %s" % (where, clip(new, 160)))
        elif f.get("Type") == "Deleted":
            lines.append("- %s (was %s)" % (where, clip(old, 160)))
        else:
            lines.append("~ %s: %s -> %s" % (where, clip(old, 160), clip(new, 160)))

    def obj(path, o):
        if o.get("Type") in (None, "None"):
            return
        here = path + [o.get("Name") or "?"]
        for f in o.get("Fields") or []:
            field(here, f)
        for child in o.get("Objects") or []:
            obj(here, child)

    def node(path, d):
        for f in d.get("Fields") or []:
            field(path, f)
        for o in d.get("Objects") or []:
            obj(path, o)

    node([], diff or {})
    for group in (diff or {}).get("TaskGroups") or []:
        if group.get("Type") in (None, "None"):
            continue
        gpath = ["group %s" % group.get("Name")]
        if group.get("Type") in ("Added", "Deleted"):
            lines.append("%s %s" % ("+" if group["Type"] == "Added" else "-", gpath[0]))
        node(gpath, group)
        for task in group.get("Tasks") or []:
            if task.get("Type") in (None, "None"):
                continue
            tpath = gpath + ["task %s" % task.get("Name")]
            if task.get("Type") in ("Added", "Deleted"):
                lines.append("%s %s" % ("+" if task["Type"] == "Added" else "-", "/".join(tpath)))
            node(tpath, task)
        for update, count in (group.get("Updates") or {}).items():
            lines.append("  update plan: %s %s" % (update, count))
    return capped(lines, limit, "changes")


def task_event(event):
    """One task event, with only the fields that carry information."""
    answer = {"time": stamp(event.get("Time")), "type": event.get("Type")}
    message = event.get("DisplayMessage") or event.get("Message")
    if message:
        answer["message"] = clip(message)
    for key, name in (("DriverError", "driver_error"), ("SetupError", "setup_error"),
                      ("DownloadError", "download_error"), ("KillError", "kill_error"),
                      ("ValidationError", "validation_error"), ("VaultError", "vault_error"),
                      ("KillReason", "kill_reason"), ("RestartReason", "restart_reason")):
        if event.get(key):
            answer[name] = clip(event[key])
    if event.get("Type") == "Terminated" or event.get("ExitCode"):
        answer["exit_code"] = event.get("ExitCode")
    if event.get("Signal"):
        answer["signal"] = event["Signal"]
    if event.get("FailsTask"):
        answer["fails_task"] = True
    return answer


def task_states_brief(states):
    answer = {}
    for name, state in (states or {}).items():
        state = state or {}
        entry = {"state": state.get("State"), "restarts": state.get("Restarts", 0)}
        if state.get("Failed"):
            entry["failed"] = True
        last = state.get("LastRestart")
        if to_datetime(last):
            entry["last_restart"] = stamp(last)
        answer[name] = entry
    return answer


def _task_brief(task):
    config = task.get("Config") or {}
    entry = {"driver": task.get("Driver")}
    for key in ("image", "command"):
        if config.get(key):
            entry[key] = clip(config[key], 200)
    if config.get("args"):
        entry["args"] = clip(" ".join(str(a) for a in config["args"]), 200)
    if task.get("Lifecycle"):
        hook = task["Lifecycle"].get("Hook")
        entry["lifecycle"] = hook + (" (sidecar)" if task["Lifecycle"].get("Sidecar") else "")
    resources = task.get("Resources") or {}
    entry["cpu_mhz"] = resources.get("CPU")
    entry["memory_mb"] = resources.get("MemoryMB")
    if resources.get("MemoryMaxMB"):
        entry["memory_max_mb"] = resources["MemoryMaxMB"]
    return entry


def _deployment_brief(deployment):
    if not deployment:
        return None
    groups = {}
    for name, state in (deployment.get("TaskGroups") or {}).items():
        state = state or {}
        entry = {"desired": state.get("DesiredTotal"), "placed": state.get("PlacedAllocs"),
                 "healthy": state.get("HealthyAllocs"), "unhealthy": state.get("UnhealthyAllocs")}
        if state.get("DesiredCanaries"):
            entry["canaries"] = state.get("DesiredCanaries")
            entry["promoted"] = state.get("Promoted")
        if state.get("RequireProgressBy") and deployment.get("Status") in ACTIVE_DEPLOYMENTS:
            entry["progress_deadline"] = stamp(state["RequireProgressBy"])
        groups[name] = entry
    return {"id": short(deployment.get("ID")), "job": deployment.get("JobID"),
            "job_version": deployment.get("JobVersion"), "status": deployment.get("Status"),
            "description": deployment.get("StatusDescription"), "groups": groups,
            "updated": stamp(deployment.get("ModifyTime")) or stamp(deployment.get("CreateTime"))}


def _alloc_row(alloc):
    return {"id": short(alloc.get("ID")), "node": alloc.get("NodeName"),
            "group": alloc.get("TaskGroup"), "version": alloc.get("JobVersion"),
            "desired": alloc.get("DesiredStatus"), "client": alloc.get("ClientStatus"),
            "tasks": task_states_brief(alloc.get("TaskStates")),
            "created": stamp(alloc.get("CreateTime")), "modified": stamp(alloc.get("ModifyTime"))}


def _eval_row(evaluation):
    row = {"id": short(evaluation.get("ID")), "status": evaluation.get("Status"),
           "triggered_by": evaluation.get("TriggeredBy"), "job": evaluation.get("JobID"),
           "modified": stamp(evaluation.get("ModifyTime"))}
    if evaluation.get("StatusDescription"):
        row["description"] = clip(evaluation["StatusDescription"])
    if evaluation.get("FailedTGAllocs"):
        row["placement_failures"] = placement_failures(evaluation["FailedTGAllocs"])
    if evaluation.get("BlockedEval"):
        row["blocked_eval"] = short(evaluation["BlockedEval"])
    return row


# ------------------------------------------------------------------ tools


def cluster_overview(client, args):
    """What needs attention right now, in one answer."""
    notes = []
    answer = {"now": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")}
    answer["leader"] = _try(client, "/v1/status/leader", notes=notes, label="leader")

    nodes = _try(client, "/v1/nodes", notes=notes, label="nodes")
    if nodes is not None:
        rows = []
        for node in nodes:
            row = {"name": node.get("Name"), "id": short(node.get("ID")),
                   "status": node.get("Status"), "eligibility": node.get("SchedulingEligibility"),
                   "pool": node.get("NodePool")}
            if node.get("Drain"):
                row["draining"] = True
            unhealthy = [d for d, info in (node.get("Drivers") or {}).items()
                         if info and info.get("Detected") and not info.get("Healthy")]
            if unhealthy:
                row["unhealthy_drivers"] = unhealthy
            rows.append(row)
        answer["nodes"] = rows
        pools = sorted({n.get("NodePool") for n in nodes if n.get("Status") == "ready"})
        answer["pools_with_ready_nodes"] = pools

    pools = set(answer.get("pools_with_ready_nodes") or [])
    jobs = _try(client, "/v1/jobs", notes=notes, label="jobs")
    if jobs is not None:
        attention = []
        stopped = []
        parked = []
        fetched = 0
        for job in jobs:
            if job.get("ParentID"):
                continue  # Dispatched and periodic children: their parent speaks for them.
            if job.get("Stop"):
                stopped.append(job.get("ID"))
                continue
            groups = ((job.get("JobSummary") or {}).get("Summary")) or {}
            queued = sum((g or {}).get("Queued", 0) for g in groups.values())
            starting = sum((g or {}).get("Starting", 0) for g in groups.values())
            running = sum((g or {}).get("Running", 0) for g in groups.values())
            long_running = job.get("Type") in ("service", "system")
            launcher = job.get("Periodic") or job.get("ParameterizedJob")
            problems = []
            pool = job.get("NodePool")
            if long_running and pools and pool not in (None, "all") and pool not in pools:
                problems.append("node pool %r has no ready node" % pool)
            if long_running and not launcher and (job.get("Status") != "running" or running == 0):
                # The job list does not carry group counts, and a job parked
                # with count = 0 is dead on purpose. Only a read of the job
                # tells a parked job from a broken one; the cap keeps a
                # cluster full of dead jobs from costing a hundred reads.
                wanted = None
                if fetched < 60:
                    fetched += 1
                    spec = _try(client, "/v1/job/%s" % _quote(job.get("ID")))
                    if spec is not None:
                        wanted = sum(g.get("Count") or 0 for g in spec.get("TaskGroups") or [])
                if wanted == 0 and job.get("Type") == "service":
                    parked.append(job.get("ID"))
                    continue
                if job.get("Status") != "running":
                    problems.append("status %s, not stopped%s" % (
                        job.get("Status"), "" if wanted is None else ", spec asks for %s" % wanted))
                else:
                    problems.append("no allocation running")
            if queued:
                problems.append("%s queued (cannot place)" % queued)
            if starting:
                problems.append("%s starting" % starting)
            if problems:
                attention.append({"job": job.get("ID"), "type": job.get("Type"),
                                  "status": job.get("Status"), "problems": problems})
        answer["jobs"] = {"total": len([j for j in jobs if not j.get("ParentID")]),
                          "needing_attention": attention,
                          "parked_count_0": sorted(parked),
                          "stopped": sorted(stopped)}

    # Allocations that should run and do not, and failures of the last day.
    # The job summary's Failed and Lost counts only ever grow, so they
    # cannot say what is failing now; the allocations can. A failed batch
    # allocation keeps desired "run" forever, so batch failures are only
    # counted under failures_last_24h, not as stuck.
    allocs = _try(client, "/v1/allocations", {"task_states": False, "resources": False},
                  notes=notes, label="allocations")
    if allocs is not None:
        now = time.time()
        stuck = []
        recent = {}
        for alloc in allocs:
            status = alloc.get("ClientStatus")
            batch = alloc.get("JobType") in ("batch", "sysbatch")
            if alloc.get("DesiredStatus") == "run" and status in TROUBLE_CLIENT \
                    and not (batch and status == "failed"):
                stuck.append({"alloc": short(alloc.get("ID")), "job": alloc.get("JobID"),
                              "group": alloc.get("TaskGroup"), "client": status,
                              "node": alloc.get("NodeName"),
                              "since": stamp(alloc.get("ModifyTime"))})
            moment = to_datetime(alloc.get("ModifyTime"))
            if status in ("failed", "lost") and moment and now - moment.timestamp() < 86400:
                # Periodic and dispatched children fold into their parent.
                job_id = re.sub(r"/(periodic|dispatch)-[^/]*$", "", alloc.get("JobID") or "")
                entry = recent.setdefault(job_id, {"failed": 0, "lost": 0, "_t": 0})
                entry[status] += 1
                if moment.timestamp() > entry["_t"]:
                    entry["_t"] = moment.timestamp()
                    entry["latest_alloc"] = short(alloc.get("ID"))
                    entry["latest"] = stamp(alloc.get("ModifyTime"))
        for entry in recent.values():
            entry.pop("_t", None)
            for key in ("failed", "lost"):
                if not entry[key]:
                    entry.pop(key)
        answer["allocations_not_running"], note = capped(stuck, 30, "allocations")
        if note:
            notes.append(note)
        answer["failures_last_24h"] = recent

    # Deployments in motion, and the ones that failed in the last day: a
    # failed deployment leaves the old version running, so the job looks
    # healthy everywhere else.
    deployments = _try(client, "/v1/deployments", notes=notes, label="deployments")
    if deployments is not None:
        now = time.time()
        rows = []
        for deployment in sorted(deployments, key=lambda d: d.get("ModifyIndex") or 0, reverse=True):
            status = deployment.get("Status")
            moment = to_datetime(deployment.get("ModifyTime")) or to_datetime(deployment.get("CreateTime"))
            recent_failure = status == "failed" and moment and now - moment.timestamp() < 86400
            if status in ACTIVE_DEPLOYMENTS or recent_failure:
                rows.append(_deployment_brief(deployment))
        answer["deployments_active_or_failed_24h"] = rows[:20]

    blocked = _try(client, "/v1/evaluations", {"filter": 'Status == "blocked"'},
                   notes=notes, label="blocked evaluations")
    if blocked is not None:
        answer["blocked_evaluations"] = [_eval_row(e) for e in blocked[:20]]

    plugins = _try(client, "/v1/plugins", {"type": "csi"}, notes=notes, label="csi plugins")
    if plugins:
        rows = []
        for plugin in plugins:
            row = {"id": plugin.get("ID"),
                   "nodes": "%s/%s healthy" % (plugin.get("NodesHealthy"), plugin.get("NodesExpected"))}
            if plugin.get("ControllerRequired"):
                row["controllers"] = "%s/%s healthy" % (plugin.get("ControllersHealthy"),
                                                        plugin.get("ControllersExpected"))
            if (plugin.get("NodesHealthy") or 0) < (plugin.get("NodesExpected") or 0) or (
                    plugin.get("ControllerRequired")
                    and (plugin.get("ControllersHealthy") or 0) < (plugin.get("ControllersExpected") or 0)):
                row["unhealthy"] = True
            rows.append(row)
        answer["csi_plugins"] = rows
    if answer["leader"] is None and nodes is None and jobs is None:
        # Nothing at all could be read: that is a refusal, not an empty cluster.
        raise Refusal("nomad", reason=notes[0] if notes else "no answer", unavailable=notes)
    if notes:
        answer["unavailable"] = notes
    return answer


def list_jobs(client, args):
    prefix = _text(args, "prefix")
    kind = _text(args, "type", choices=("service", "batch", "system", "sysbatch"))
    children = _bool(args, "include_children", False)
    limit = _int(args, "limit", 200, 1, CEILING_LIST)
    jobs = _get(client, "/v1/jobs", {"prefix": prefix}) or []
    rows = []
    for job in jobs:
        if kind and job.get("Type") != kind:
            continue
        if job.get("ParentID") and not children:
            continue
        groups = ((job.get("JobSummary") or {}).get("Summary")) or {}
        row = {"id": job.get("ID"), "type": job.get("Type"), "status": job.get("Status"),
               "priority": job.get("Priority"), "pool": job.get("NodePool"),
               "groups": {name: nonzero(counts) for name, counts in groups.items()}}
        if job.get("Stop"):
            row["stopped"] = True
        if job.get("Periodic"):
            row["periodic"] = True
        if job.get("ParameterizedJob"):
            row["parameterized"] = True
        rows.append(row)
    rows, note = capped(rows, limit, "jobs")
    answer = {"jobs": rows, "count": len(rows),
              "counts_note": "group counts are the job summary: failed and lost only ever "
                             "grow over the job's life; use job_status for what runs now"}
    if note:
        answer["note"] = note
    return answer


def job_status(client, args):
    job = _job(client, args.get("job"))
    job_id = job.get("ID")
    notes = []
    answer = {
        "id": job_id, "type": job.get("Type"), "status": job.get("Status"),
        "version": job.get("Version"), "stable": job.get("Stable"), "stopped": job.get("Stop"),
        "datacenters": job.get("Datacenters"), "node_pool": job.get("NodePool"),
        "priority": job.get("Priority"), "submitted": stamp(job.get("SubmitTime")),
    }
    if job.get("StatusDescription"):
        answer["status_description"] = job["StatusDescription"]
    if job.get("Periodic"):
        answer["periodic"] = (job["Periodic"] or {}).get("Spec")
    if job.get("ParameterizedJob"):
        answer["parameterized"] = True
    if job.get("Constraints"):
        answer["constraints"] = ["%s %s %s" % (c.get("LTarget"), c.get("Operand"), c.get("RTarget"))
                                 for c in job["Constraints"]]
    groups = {}
    for group in job.get("TaskGroups") or []:
        groups[group.get("Name")] = {
            "count": group.get("Count"),
            "tasks": {t.get("Name"): _task_brief(t) for t in group.get("Tasks") or []},
        }
    answer["groups"] = groups

    allocs = _try(client, "/v1/job/%s/allocations" % _quote(job_id), notes=notes,
                  label="allocations") or []
    allocs.sort(key=lambda a: a.get("CreateTime") or 0, reverse=True)
    answer["allocations"], note = capped([_alloc_row(a) for a in allocs], 10, "allocations")
    if note:
        notes.append(note + " (newest first)")
    if job.get("Type") in ("service", "system"):
        deployment = _try(client, "/v1/job/%s/deployment" % _quote(job_id), notes=notes,
                          label="deployment")
        answer["latest_deployment"] = _deployment_brief(deployment)
    if job.get("Periodic") or job.get("ParameterizedJob"):
        summary = _try(client, "/v1/job/%s/summary" % _quote(job_id), notes=notes, label="summary")
        if summary:
            answer["children"] = nonzero(summary.get("Children"))

    evals = _try(client, "/v1/job/%s/evaluations" % _quote(job_id), notes=notes,
                 label="evaluations") or []
    evals.sort(key=lambda e: e.get("ModifyIndex") or 0, reverse=True)
    troubled = [e for e in evals if e.get("FailedTGAllocs") or e.get("Status") in ("blocked", "failed")]
    answer["evaluations_with_failures"] = [_eval_row(e) for e in troubled[:5]]
    if evals:
        answer["latest_evaluation"] = _eval_row(evals[0])
    if notes:
        answer["notes"] = notes
    return answer


def job_versions(client, args):
    job = _job(client, args.get("job"))
    limit = _int(args, "limit", 5, 1, 20)
    answer = _get(client, "/v1/job/%s/versions" % _quote(job["ID"]), {"diffs": True}) or {}
    versions = answer.get("Versions") or []
    diffs = answer.get("Diffs") or []
    rows = []
    # Diffs[i] is what changed from Versions[i+1] to Versions[i]; the
    # oldest version has no diff.
    for index, version in enumerate(versions[:limit]):
        row = {"version": version.get("Version"), "stable": version.get("Stable"),
               "submitted": stamp(version.get("SubmitTime"))}
        if version.get("Stop"):
            row["stopped"] = True
        if index < len(diffs):
            changes, note = flatten_diff(diffs[index], 40)
            row["changes"] = changes or ["(no field changes: a resubmission of the same spec)"]
            if note:
                row["note"] = note
        else:
            row["changes"] = ["(oldest version kept: nothing to compare with)"]
        rows.append(row)
    return {"job": job["ID"], "current_version": job.get("Version"), "versions": rows}


def alloc_status(client, args):
    alloc_id = resolve(client, "allocation", args.get("alloc"))
    events = _int(args, "events", DEFAULT_EVENTS, 1, CEILING_EVENTS)
    alloc = _get(client, "/v1/allocation/%s" % alloc_id)
    answer = {
        "id": alloc.get("ID"), "name": alloc.get("Name"), "job": alloc.get("JobID"),
        "job_version": (alloc.get("Job") or {}).get("Version"), "group": alloc.get("TaskGroup"),
        "node": alloc.get("NodeName"), "node_id": short(alloc.get("NodeID")),
        "desired": alloc.get("DesiredStatus"), "client": alloc.get("ClientStatus"),
        "created": stamp(alloc.get("CreateTime")), "modified": stamp(alloc.get("ModifyTime")),
    }
    for key, name in (("DesiredDescription", "desired_description"),
                      ("ClientDescription", "client_description")):
        if alloc.get(key):
            answer[name] = alloc[key]
    health = alloc.get("DeploymentStatus") or {}
    if health:
        answer["deployment_health"] = health.get("Healthy")
    tracker = alloc.get("RescheduleTracker") or {}
    if tracker.get("Events"):
        answer["reschedules"] = len(tracker["Events"])
    for key, name in (("PreviousAllocation", "previous_alloc"), ("NextAllocation", "next_alloc"),
                      ("FollowupEvalID", "followup_eval")):
        if alloc.get(key):
            answer[name] = short(alloc[key])
    tasks = {}
    for name, state in (alloc.get("TaskStates") or {}).items():
        state = state or {}
        entry = {"state": state.get("State"), "failed": bool(state.get("Failed")),
                 "restarts": state.get("Restarts", 0), "started": stamp(state.get("StartedAt")),
                 "finished": stamp(state.get("FinishedAt"))}
        if to_datetime(state.get("LastRestart")):
            entry["last_restart"] = stamp(state.get("LastRestart"))
        entry["events"] = [task_event(e) for e in (state.get("Events") or [])[-events:]]
        tasks[name] = entry
    answer["tasks"] = tasks

    allocated = alloc.get("AllocatedResources") or {}
    resources = {}
    for name, res in (allocated.get("Tasks") or {}).items():
        res = res or {}
        memory = res.get("Memory") or {}
        entry = {"cpu_mhz": (res.get("Cpu") or {}).get("CpuShares"), "memory_mb": memory.get("MemoryMB")}
        if memory.get("MemoryMaxMB"):
            entry["memory_max_mb"] = memory["MemoryMaxMB"]
        cores = (res.get("Cpu") or {}).get("ReservedCores")
        if cores:
            entry["cores"] = cores
        resources[name] = entry
    shared = allocated.get("Shared") or {}
    answer["resources"] = resources
    if shared.get("DiskMB"):
        answer["disk_mb"] = shared["DiskMB"]
    ports = {}
    for port in shared.get("Ports") or []:
        ports[port.get("Label")] = "%s:%s" % (port.get("HostIP"), port.get("Value"))
        if port.get("To") and port.get("To") != port.get("Value"):
            ports[port.get("Label")] += " -> %s" % port["To"]
    if ports:
        answer["ports"] = ports
    return answer


def alloc_logs(client, args):
    alloc_id = resolve(client, "allocation", args.get("alloc"))
    stream = _text(args, "stream", default="stderr", choices=("stderr", "stdout"))
    tail = _int(args, "tail_bytes", DEFAULT_TAIL, 1, CEILING_TAIL)
    task = _text(args, "task")
    alloc = _get(client, "/v1/allocation/%s" % alloc_id)
    names = sorted((alloc.get("TaskStates") or {}).keys())
    if not names:
        group = next((g for g in ((alloc.get("Job") or {}).get("TaskGroups") or [])
                      if g.get("Name") == alloc.get("TaskGroup")), {})
        names = sorted(t.get("Name") for t in group.get("Tasks") or [])
    if task is None:
        if len(names) != 1:
            raise Refusal("input", field="task", reason="the allocation has %s tasks: name one"
                          % len(names), tasks=names)
        task = names[0]
    elif names and task not in names:
        raise Refusal("input", field="task", reason="no such task in the allocation", tasks=names)
    try:
        body = client.get_raw("/v1/client/fs/logs/%s" % alloc_id,
                              {"task": task, "type": stream, "origin": "end",
                               "offset": tail, "plain": True})
    except NomadError as exc:
        raise Refusal("nomad", reason=exc.reason, status=exc.status or None,
                      hint="logs are read from the client node: a node that is down, or an "
                           "allocation that was garbage-collected, has none")
    body = body[-tail:]
    # Colour codes cost bytes of the tail and carry nothing for a reader.
    text = ANSI.sub("", body.decode("utf-8", "replace"))
    truncated = len(body) >= tail
    if truncated and "\n" in text:
        text = text.split("\n", 1)[1]  # The first line is cut mid-way; drop it.
    return {"alloc": short(alloc_id), "job": alloc.get("JobID"), "task": task, "stream": stream,
            "client": alloc.get("ClientStatus"), "bytes": len(text.encode("utf-8")),
            "truncated_to_tail": truncated, "log": text}


def _node_row(node):
    row = {"name": node.get("Name"), "id": short(node.get("ID")), "status": node.get("Status"),
           "eligibility": node.get("SchedulingEligibility"), "pool": node.get("NodePool"),
           "datacenter": node.get("Datacenter"), "address": node.get("Address"),
           "version": node.get("Version")}
    if node.get("NodeClass"):
        row["class"] = node["NodeClass"]
    if node.get("Drain"):
        row["draining"] = True
    drivers = node.get("Drivers") or {}
    row["drivers"] = sorted(d for d, i in drivers.items() if i and i.get("Detected") and i.get("Healthy"))
    unhealthy = sorted(d for d, i in drivers.items() if i and i.get("Detected") and not i.get("Healthy"))
    if unhealthy:
        row["unhealthy_drivers"] = unhealthy
    return row


def list_nodes(client, args):
    nodes = _get(client, "/v1/nodes") or []
    return {"nodes": [_node_row(n) for n in nodes], "count": len(nodes)}


NODE_ATTRIBUTES = ("cpu.arch", "cpu.numcores", "cpu.modelname", "kernel.name", "kernel.version",
                   "os.name", "os.version", "nomad.version", "unique.hostname",
                   "unique.network.ip-address", "driver.docker.version", "memory.totalbytes")


def node_status(client, args):
    node_id = resolve(client, "node", args.get("node"))
    events = _int(args, "events", DEFAULT_EVENTS, 1, CEILING_EVENTS)
    node = _get(client, "/v1/node/%s" % node_id)
    # The answer is built from an allow-list on purpose: the node object
    # carries its SecretID, which must never leave this process.
    answer = _node_row(node)
    answer["id"] = node.get("ID")
    # The single-node read has no Address field; the fingerprint has it.
    answer["address"] = (node.get("Attributes") or {}).get("unique.network.ip-address") \
        or node.get("HTTPAddr")
    answer["status_description"] = node.get("StatusDescription") or None
    answer["status_updated"] = stamp((node.get("StatusUpdatedAt") or 0) * 1000000000)
    attrs = node.get("Attributes") or {}
    answer["attributes"] = {k: attrs[k] for k in NODE_ATTRIBUTES if k in attrs}
    drivers = {}
    for name, info in (node.get("Drivers") or {}).items():
        if info and info.get("Detected"):
            drivers[name] = "healthy" if info.get("Healthy") else (
                "unhealthy: %s" % (info.get("HealthDescription") or "no description"))
    answer["drivers"] = drivers
    if node.get("DrainStrategy"):
        strategy = node["DrainStrategy"]
        answer["drain"] = {"deadline": stamp(strategy.get("ForceDeadline")),
                           "ignore_system_jobs": strategy.get("IgnoreSystemJobs")}
    if node.get("LastDrain"):
        last = node["LastDrain"]
        answer["last_drain"] = {"status": last.get("Status"), "started": stamp(last.get("StartedAt")),
                                "updated": stamp(last.get("UpdatedAt"))}
    plugins = {}
    for name, info in (node.get("CSINodePlugins") or {}).items():
        info = info or {}
        plugins[name] = "healthy" if info.get("Healthy") else (
            "unhealthy: %s" % (info.get("HealthDescription") or "no description"))
    if plugins:
        answer["csi_node_plugins"] = plugins
    if node.get("HostVolumes"):
        answer["host_volumes"] = sorted(node["HostVolumes"])

    capacity = node.get("NodeResources") or {}
    reserved = node.get("ReservedResources") or {}
    total = {"cpu_mhz": (capacity.get("Cpu") or {}).get("CpuShares"),
             "memory_mb": (capacity.get("Memory") or {}).get("MemoryMB"),
             "disk_mb": (capacity.get("Disk") or {}).get("DiskMB")}
    held = {"cpu_mhz": (reserved.get("Cpu") or {}).get("CpuShares") or 0,
            "memory_mb": (reserved.get("Memory") or {}).get("MemoryMB") or 0,
            "disk_mb": (reserved.get("Disk") or {}).get("DiskMB") or 0}
    notes = []
    allocs = _try(client, "/v1/node/%s/allocations" % node_id, notes=notes, label="allocations") or []
    used = {"cpu_mhz": 0, "memory_mb": 0, "memory_max_mb": 0, "disk_mb": 0}
    running = []
    trouble = []
    for alloc in allocs:
        status = alloc.get("ClientStatus")
        if status in ("running", "pending"):
            resources = alloc.get("AllocatedResources") or {}
            for task in (resources.get("Tasks") or {}).values():
                task = task or {}
                memory = task.get("Memory") or {}
                used["cpu_mhz"] += (task.get("Cpu") or {}).get("CpuShares") or 0
                used["memory_mb"] += memory.get("MemoryMB") or 0
                used["memory_max_mb"] += memory.get("MemoryMaxMB") or memory.get("MemoryMB") or 0
            used["disk_mb"] += (resources.get("Shared") or {}).get("DiskMB") or 0
            running.append("%s/%s %s" % (alloc.get("JobID"), alloc.get("TaskGroup"), short(alloc.get("ID"))))
        if alloc.get("DesiredStatus") == "run" and status in TROUBLE_CLIENT:
            trouble.append({"alloc": short(alloc.get("ID")), "job": alloc.get("JobID"), "client": status})
    answer["resources"] = {"capacity": total, "reserved_for_os": held, "allocated": used,
                           "free": {k: (total[k] - held[k] - used[k]) if total[k] is not None else None
                                    for k in ("cpu_mhz", "memory_mb", "disk_mb")}}
    running.sort()
    answer["running_allocations"], note = capped(running, 60, "allocations")
    if note:
        notes.append(note)
    if trouble:
        answer["allocations_not_running"] = trouble
    answer["events"] = [{"time": stamp(e.get("Timestamp")), "subsystem": e.get("Subsystem"),
                         "message": clip(e.get("Message")),
                         **({"details": e["Details"]} if e.get("Details") else {})}
                        for e in (node.get("Events") or [])[-events:]]
    if notes:
        answer["notes"] = notes
    return answer


_HOST_RULE = re.compile(r"Host\(`([^`]+)`\)")


def list_services(client, args):
    prefix = _text(args, "prefix")
    groups = _get(client, "/v1/services") or []
    rows = []
    for group in groups:
        for service in group.get("Services") or []:
            name = service.get("ServiceName") or ""
            if prefix and not name.startswith(prefix):
                continue
            tags = service.get("Tags") or []
            row = {"name": name}
            hosts = sorted({h for tag in tags for h in _HOST_RULE.findall(tag)})
            if hosts:
                row["hosts"] = hosts
            if tags:
                row["tags"] = len(tags)
            rows.append(row)
    rows.sort(key=lambda r: r["name"])
    return {"services": rows, "count": len(rows),
            "note": "Nomad native services; hosts come from Traefik Host() rules in the tags"}


def service(client, args):
    name = _text(args, "name", required=True)
    registrations = _get(client, "/v1/service/%s" % _quote(name)) or []
    if not registrations:
        raise Refusal("not_found", kind="service", given=name,
                      reason="no registrations: nothing healthy-or-not is registered under that name")
    nodes = {n.get("ID"): n.get("Name") for n in (_try(client, "/v1/nodes") or [])}
    rows = []
    tags = None
    for reg in registrations:
        rows.append({"address": "%s:%s" % (reg.get("Address"), reg.get("Port")),
                     "node": nodes.get(reg.get("NodeID")) or short(reg.get("NodeID")),
                     "alloc": short(reg.get("AllocID")), "job": reg.get("JobID"),
                     "datacenter": reg.get("Datacenter")})
        tags = reg.get("Tags") or tags
    return {"service": name, "registrations": rows, "tags": tags or []}


def list_variables(client, args):
    prefix = _text(args, "prefix")
    limit = _int(args, "limit", 200, 1, CEILING_LIST)
    # /v1/vars lists metadata only; this tool never reads /v1/var/<path>,
    # which is where values live.
    items = _get(client, "/v1/vars", {"prefix": prefix}) or []
    rows = [{"path": v.get("Path"), "modified": stamp(v.get("ModifyTime"))} for v in items]
    rows, note = capped(rows, limit, "variables")
    answer = {"variables": rows, "count": len(items),
              "note": "paths and times only: this server never reads variable values"}
    if note:
        answer["cut"] = note
    return answer


def list_deployments(client, args):
    active = _bool(args, "active_only", True)
    job = _text(args, "job")
    limit = _int(args, "limit", 20, 1, 100)
    if job:
        items = _get(client, "/v1/job/%s/deployments" % _quote(job)) or []
    else:
        items = _get(client, "/v1/deployments") or []
    if active:
        items = [d for d in items if d.get("Status") in ACTIVE_DEPLOYMENTS]
    items.sort(key=lambda d: d.get("ModifyIndex") or 0, reverse=True)
    rows, note = capped([_deployment_brief(d) for d in items], limit, "deployments")
    answer = {"deployments": rows, "active_only": active}
    if note:
        answer["note"] = note
    return answer


def evaluation(client, args):
    eval_id = resolve(client, "evaluation", args.get("eval"))
    ev = _get(client, "/v1/evaluation/%s" % eval_id, {"related": True})
    answer = _eval_row(ev)
    answer["id"] = ev.get("ID")
    answer["type"] = ev.get("Type")
    answer["priority"] = ev.get("Priority")
    answer["created"] = stamp(ev.get("CreateTime"))
    for key, name in (("DeploymentID", "deployment"), ("NodeID", "node"),
                      ("PreviousEval", "previous_eval"), ("NextEval", "next_eval")):
        if ev.get(key):
            answer[name] = short(ev[key])
    if ev.get("QueuedAllocations"):
        answer["queued_allocations"] = nonzero(ev["QueuedAllocations"])
    if ev.get("ClassEligibility"):
        answer["class_eligibility"] = ev["ClassEligibility"]
    if ev.get("WaitUntil") and to_datetime(ev["WaitUntil"]):
        answer["wait_until"] = stamp(ev["WaitUntil"])
    related = ev.get("RelatedEvals") or []
    if related:
        answer["related"] = [{"id": short(r.get("ID")), "status": r.get("Status"),
                              "triggered_by": r.get("TriggeredBy")} for r in related[:10]]
    allocs = _try(client, "/v1/evaluation/%s/allocations" % eval_id) or []
    if allocs:
        answer["allocations"] = [{"id": short(a.get("ID")), "node": a.get("NodeName"),
                                  "client": a.get("ClientStatus")} for a in allocs[:10]]
    return answer


# ------------------------------------------------------------------- exec


EXEC_DEFAULT_TIMEOUT = 60
EXEC_CEILING_TIMEOUT = 300


def exec_jobs(environ=None):
    """The jobs alloc_exec may reach, from NOMAD_MCP_EXEC_JOBS. Empty refuses all."""
    env = os.environ if environ is None else environ
    return {job.strip() for job in (env.get("NOMAD_MCP_EXEC_JOBS") or "").split(",") if job.strip()}


def alloc_exec(client, args):
    """The one tool that changes anything. The allow-list is judged before
    Nomad hears a word, and the allocation must be the job's own."""
    job = _text(args, "job", required=True)
    if job not in exec_jobs():
        raise Refusal("job_not_allowed", job=job)
    argv = args.get("command")
    if not isinstance(argv, list) or not argv or not all(isinstance(a, str) for a in argv):
        raise Refusal("input", field="command", reason="an argv array of texts is necessary")
    stdin = args.get("stdin")
    if stdin is not None and not isinstance(stdin, str):
        raise Refusal("input", field="stdin", reason="a text is necessary")
    why = _text(args, "why")
    timeout = _int(args, "timeout_seconds", EXEC_DEFAULT_TIMEOUT, 1, EXEC_CEILING_TIMEOUT)
    if args.get("alloc"):
        alloc_id = resolve(client, "allocation", args.get("alloc"))
    else:
        running = [a for a in _get(client, "/v1/job/%s/allocations" % _quote(job)) or []
                   if a.get("ClientStatus") == "running"]
        if not running:
            raise Refusal("no_running_alloc", job=job)
        alloc_id = max(running, key=lambda a: a.get("CreateTime") or 0)["ID"]
    alloc = _get(client, "/v1/allocation/%s" % alloc_id)
    if alloc.get("JobID") != job:
        raise Refusal("alloc_not_in_job", job=job, alloc=short(alloc_id), alloc_job=alloc.get("JobID"))
    tasks = sorted((alloc.get("TaskStates") or {}).keys())
    task = _text(args, "task") or (tasks[0] if len(tasks) == 1 else None)
    if task not in tasks:
        raise Refusal("input", field="task", reason="name one of the allocation's tasks", tasks=tasks)
    # The log names what ran and why; never its stdin or its output.
    line = "nomad-mcp exec job=%s alloc=%s task=%s argv=%s why=%s" % (
        job, short(alloc_id), task, json.dumps(argv)[:500], json.dumps(why)[:300])
    try:
        result = execws.run(client, alloc_id, task, argv, stdin, timeout)
    except execws.ExecError as exc:
        print(line + " refused=nomad", file=sys.stderr)
        raise Refusal("nomad", reason=exc.reason, status=exc.status or None)
    print(line + " exit_code=%s timed_out=%s" % (result["exit_code"], result["timed_out"]),
          file=sys.stderr)
    return dict({"job": job, "alloc": short(alloc_id), "task": task}, **result)


# ------------------------------------------------------------------ specs


def _schema(properties=None, required=None):
    return {"type": "object", "properties": properties or {}, "required": required or [],
            "additionalProperties": False}


_JOB = {"type": "string", "description": "The job id, exactly (for example traefik)."}
_ALLOC = {"type": "string", "description": "An allocation id or a prefix of it (8 characters is usual)."}
_EVENTS = {"type": "integer", "minimum": 1, "maximum": CEILING_EVENTS,
           "description": "How many recent events to give. The default is %s." % DEFAULT_EVENTS}

TOOL_SPECS = [
    {
        "name": "cluster_overview",
        "function": cluster_overview,
        "description": (
            "Start here. Gives what needs attention now: the leader, each node's status, "
            "eligibility, drain and pool, the jobs that are not running as they should or "
            "have allocations queued, allocations that should run and do not, failures of "
            "the last 24 hours by job, deployments in motion or failed in the last day, "
            "blocked evaluations with the reason they cannot "
            "place, and CSI plugin health."
        ),
        "schema": _schema(),
    },
    {
        "name": "list_jobs",
        "function": list_jobs,
        "description": (
            "Lists jobs: id, type, status, priority, node pool and the non-zero summary "
            "counts per task group. Dispatched and periodic children are hidden unless "
            "include_children is true. Summary failed and lost counts are lifetime totals."
        ),
        "schema": _schema({
            "prefix": {"type": "string", "description": "Only jobs whose id starts with this."},
            "type": {"type": "string", "enum": ["service", "batch", "system", "sysbatch"]},
            "include_children": {"type": "boolean", "description": "Also list dispatched and periodic children."},
            "limit": {"type": "integer", "minimum": 1, "maximum": CEILING_LIST},
        }),
    },
    {
        "name": "job_status",
        "function": job_status,
        "description": (
            "Everything about one job for troubleshooting: version, status, stability, "
            "datacenters, node pool, each group's count and each task's driver, image or "
            "command and resources; the newest allocations with their task states and "
            "restarts; the latest deployment's healthy and unhealthy counts; and the recent "
            "evaluations that failed to place, with the reasons in words."
        ),
        "schema": _schema({"job": _JOB}, ["job"]),
    },
    {
        "name": "job_versions",
        "function": job_versions,
        "description": (
            "The job's recent versions, newest first, each with what changed from the one "
            "before it as old -> new lines. Use it to answer 'what changed right before it "
            "broke'."
        ),
        "schema": _schema({"job": _JOB, "limit": {"type": "integer", "minimum": 1, "maximum": 20,
                                                  "description": "How many versions. The default is 5."}},
                          ["job"]),
    },
    {
        "name": "alloc_status",
        "function": alloc_status,
        "description": (
            "One allocation in full: job, node, desired and client status with their "
            "descriptions, reschedules, each task's state, restarts and last events (exit "
            "codes, driver errors, kill reasons), allocated resources and ports."
        ),
        "schema": _schema({"alloc": _ALLOC, "events": _EVENTS}, ["alloc"]),
    },
    {
        "name": "alloc_logs",
        "function": alloc_logs,
        "description": (
            "The tail of one task's log. stream is stderr (the default) or stdout. task may "
            "be left out when the allocation has one task. tail_bytes is 8000 by default "
            "and %s at most." % CEILING_TAIL
        ),
        "schema": _schema({
            "alloc": _ALLOC,
            "task": {"type": "string", "description": "The task name inside the allocation."},
            "stream": {"type": "string", "enum": ["stderr", "stdout"]},
            "tail_bytes": {"type": "integer", "minimum": 1, "maximum": CEILING_TAIL},
        }, ["alloc"]),
    },
    {
        "name": "alloc_exec",
        "function": alloc_exec,
        "read_only": False,
        "description": (
            "Runs one command inside a running allocation of a job on the exec allow-list and "
            "gives its exit code, stdout and stderr (each capped at 64 KB, the tail kept). "
            "command is an argv array with no shell: pass [\"sh\", \"-c\", \"...\"] for one. "
            "alloc defaults to the job's newest running allocation; task may be left out when "
            "there is one. timeout_seconds is 60 by default and 300 at most. There is no TTY. "
            "why is one optional sentence for the log. A job not on the allow-list is refused."
        ),
        "schema": _schema({
            "job": _JOB,
            "alloc": _ALLOC,
            "task": {"type": "string", "description": "The task name inside the allocation."},
            "command": {"type": "array", "items": {"type": "string"}, "minItems": 1,
                        "description": "The argv to run, for example [\"ls\", \"-la\", \"/local\"]."},
            "stdin": {"type": "string", "description": "Text written to the command's standard input."},
            "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": EXEC_CEILING_TIMEOUT},
            "why": {"type": "string", "description": (
                "One sentence for the log. Optional: a gate in front of this server may hold the why itself.")},
        }, ["job", "command"]),
    },
    {
        "name": "list_nodes",
        "function": list_nodes,
        "description": "Lists the client nodes: name, status, eligibility, drain, pool, address, "
                       "version, and healthy and unhealthy drivers.",
        "schema": _schema(),
    },
    {
        "name": "node_status",
        "function": node_status,
        "description": (
            "One node in full: attributes (arch, os, kernel, Nomad version, address), driver "
            "and CSI plugin health, drain and eligibility, capacity against what running "
            "allocations hold, the allocations on it, and recent node events."
        ),
        "schema": _schema({"node": {"type": "string", "description": "The node name, id, or id prefix."},
                           "events": _EVENTS}, ["node"]),
    },
    {
        "name": "list_services",
        "function": list_services,
        "description": "Lists the Nomad native services, with the hosts their Traefik tags route.",
        "schema": _schema({"prefix": {"type": "string", "description": "Only services whose name starts with this."}}),
    },
    {
        "name": "service",
        "function": service,
        "description": "One service's registrations: address and port, node, allocation, job, and tags.",
        "schema": _schema({"name": {"type": "string", "description": "The service name."}}, ["name"]),
    },
    {
        "name": "list_variables",
        "function": list_variables,
        "description": (
            "Lists Nomad variable paths and when each changed. It never reads or gives "
            "variable values: use it to see that a path exists and when it was last written "
            "(for example nomad/jobs/<job>/deploy after a CI deploy)."
        ),
        "schema": _schema({"prefix": {"type": "string", "description": "Only paths that start with this."},
                           "limit": {"type": "integer", "minimum": 1, "maximum": CEILING_LIST}}),
    },
    {
        "name": "list_deployments",
        "function": list_deployments,
        "description": (
            "Lists deployments, newest first, with per-group desired, placed, healthy and "
            "unhealthy counts. active_only (the default) keeps the ones still in motion; "
            "job narrows to one job."
        ),
        "schema": _schema({"active_only": {"type": "boolean"}, "job": _JOB,
                           "limit": {"type": "integer", "minimum": 1, "maximum": 100}}),
    },
    {
        "name": "evaluation",
        "function": evaluation,
        "description": (
            "One evaluation: why it ran, its status, the placement failures per group in "
            "words (empty node pool, constraint filtered, memory exhausted), queued "
            "allocations, the blocked follow-up, related evaluations and the allocations it made. "
            "Use it to answer 'why didn't it place'."
        ),
        "schema": _schema({"eval": {"type": "string", "description": "An evaluation id or prefix."}},
                          ["eval"]),
    },
]

TOOLS = {spec["name"]: spec for spec in TOOL_SPECS}


def call(client, name, args):
    """Calls one tool by name. Gives (answer, refused)."""
    if args is None:
        args = {}
    if not isinstance(args, dict):
        return {"refused": "input", "reason": "the arguments must be an object"}, True
    spec = TOOLS.get(name)
    if spec is None:
        answer = {"refused": "unknown_tool", "tool": name, "known": sorted(TOOLS)}
    else:
        try:
            answer = spec["function"](client, args)
            log_call(name, args)
            return answer, False
        except Refusal as exc:
            answer = exc.data
        except NomadError as exc:
            answer = {"refused": "nomad", "reason": exc.reason}
        except Exception as exc:  # A fault is an answer, and never a stack trace.
            answer = {"refused": "error", "reason": clip("%s: %s" % (type(exc).__name__, exc), 600)}
    log_call(name, args, answer["refused"])
    return answer, True
