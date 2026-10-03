# nomad-mcp

A small MCP server for troubleshooting a HashiCorp Nomad cluster. It is
**read-only but for three tools**: every read goes through a client that sends
nothing but `GET` (the lines that write a request hard-code the method, and
the tests fail if that changes). The exceptions are `alloc_exec`, which runs
one command inside an allocation of a job on an allow-list, and `job_restart`,
which stops the running allocations of a job on another allow-list, or registers
a dead one again with its own current spec, and `job_revert`, which rolls a job
on that same allow-list back to an earlier version, through the client's `POST`s
(below). Changes to
the cluster go through commits to
[ckopsa/home-infrastructure](https://github.com/ckopsa/home-infrastructure),
which CI plans and applies on merge.

It needs Python 3.11 and nothing else: no dependency outside the standard
library.

```
NOMAD_ADDR=http://orangepi5plus.lan:4646 NOMAD_TOKEN=... python3 -m nomadmcp --http 8111
python3 -m nomadmcp --stdio                     # for a local MCP client
python3 -m nomadmcp call job_status job=traefik # poke a running server
```

The transport is waymark-bench's: JSON-RPC 2.0 on `POST /mcp/`, `GET /health`,
or one message per line on stdio; protocol `2025-06-18`. Each `tools/call`
answer carries the JSON as a text part and as `structuredContent.result`. A
refusal is a normal answer with `isError: true` and a `refused` field; a failed
read of Nomad (unreachable, 403, 404) is a refusal too, never a stack trace.

## Settings

The same environment the `nomad` CLI reads:

| Variable | Meaning |
| --- | --- |
| `NOMAD_ADDR` | `http://…`, `https://…`, or `unix:///path/to/api.sock` |
| `NOMAD_TOKEN` | sent as `X-Nomad-Token` |
| `NOMAD_NAMESPACE` | the namespace to read when `NOMAD_MCP_NAMESPACES` is unset; `default` when unset |
| `NOMAD_MCP_NAMESPACES` | the namespaces a tool may name, comma-separated; the first is the default. Unset, the one of `NOMAD_NAMESPACE` |
| `NOMAD_CACERT` | a CA bundle for an https address (optional) |
| `NOMAD_MCP_HOST` | the address `--http` listens on; `127.0.0.1` when unset |
| `NOMAD_MCP_URL` | where `call` sends; `http://127.0.0.1:8111/mcp/` when unset |
| `NOMAD_MCP_EXEC_JOBS` | the jobs `alloc_exec` may reach, comma-separated; empty or unset refuses every call |
| `NOMAD_MCP_RESTART_JOBS` | the jobs `job_restart` may restart, comma-separated `namespace/job` entries (e.g. `doors/clone-mcp`), used only when the variable `nomad/jobs/nomad-mcp/restart_jobs` is missing or unreadable (see `restart_allowlist`); empty or unset then refuses every call |

In production the server runs as a Nomad task and talks to the **Task API**:
the task's `identity { env = true }` puts its workload identity in
`NOMAD_TOKEN`, and `NOMAD_ADDR=unix:///secrets/api.sock` reaches the agent over
the socket Nomad mounts into the task. No long-lived token exists anywhere.

## Tools

Answers are summaries for troubleshooting, not raw API JSON: lists are capped,
log tails are capped, times read `2026-09-20 23:05:10Z (4d ago)`. Allocation,
node and evaluation ids may be given as prefixes (a node also by name); a prefix
that matches more than one is refused with the candidates listed.

Every tool that names a job or an allocation takes an optional `namespace`,
one of `NOMAD_MCP_NAMESPACES`; another is refused (`namespace_not_allowed`)
before Nomad is asked anything. Left out, it is the first listed, an id prefix
is looked up in every listed namespace, and `list_jobs` lists them all and names
each job's namespace.

| Tool | What it answers |
| --- | --- |
| `cluster_overview` | Start here. Leader, nodes, jobs not running as they should (a dead job parked at `count = 0` is listed apart), allocations stuck, failures of the last 24 h, deployments in motion or failed today, blocked evaluations with reasons, CSI plugin health. |
| `list_jobs` | Jobs with type, status, pool and non-zero summary counts per group. |
| `job_status` | One job: spec basics, groups and tasks (driver, image), newest allocations with task states, the latest deployment, evaluations that failed to place and why. |
| `job_versions` | Recent versions with the diff from each one's predecessor as `old -> new` lines. |
| `alloc_status` | One allocation: statuses and descriptions, each task's state, restarts and last events (exit codes, driver errors), resources, ports. |
| `alloc_logs` | The tail of a task's stderr or stdout, 8000 bytes by default, 64000 at most. |
| `alloc_exec` | Runs one command (an argv, no shell, no TTY, optional stdin) in the job's newest running allocation or a named one of it, and gives the exit code, stdout and stderr, each capped at 64 KB with the tail kept. The timeout is 60 s by default and 300 s at most; past it the exec is closed, which ends the command, and the answer says so. A job not in `NOMAD_MCP_EXEC_JOBS` is refused (`job_not_allowed`) before Nomad is asked anything; an allocation of another job is refused (`alloc_not_in_job`). Each call is logged on stderr (job, alloc, task, argv, why, exit code), never its stdin or output. `why` is optional, because a gate in front of this server may hold the why itself and strip it before forwarding; without one the log writes `why=null`. |
| `job_restart` | Stops each running allocation of a job (`POST /v1/allocation/:id/stop`), so the scheduler places fresh ones: a fresh allocation pulls its image again and re-renders its templates, which an in-place task restart may not. It answers at once with `{job, namespace, stopped, evals}` and never waits for the new allocations; `job_status` follows them. A job with no running allocation whose status is `dead` and whose `Stop` is false is registered again: its own current spec is read (`GET /v1/job/:id`) and posted back to `POST /v1/jobs` unchanged, so nothing about the job can be altered here, and the answer is `{job, namespace, revived: true, eval}`. A job stopped on purpose is refused (`stopped_on_purpose`). A job whose `namespace/job` is not on the allow-list is refused (`job_not_allowed`) before Nomad is asked anything but the list itself. The list is the Nomad variable `nomad/jobs/nomad-mcp/restart_jobs` in `default` (items `namespace/job`, as keys or comma-separated values), read live and cached 60 s, so adding a job never waits on a tofu apply; when it is missing or unreadable, `NOMAD_MCP_RESTART_JOBS`. Each answer says which in `allowlist` (`variable` or `env`). Each call is logged on stderr (job, namespace, allocs, why); `why` is optional, as for `alloc_exec`. |
| `job_revert` | Rolls a job on the restart allow-list back to an earlier `version` with Nomad's revert (`POST /v1/job/:id/revert`), which registers that version's spec as a new version. It reads the job's current version first and sends it as `EnforcePriorVersion`, so a change that lands in between is refused (`version_moved`) instead of overwritten. A target not earlier than the current version is refused (`not_earlier`), one more than 10 versions back too (`too_old`), and a job off the allow-list (`job_not_allowed`), all before Nomad hears a write. It answers at once with `{job, namespace, reverted_to, from, version, eval}`, `version` being the new one. Each call is logged on stderr (job, namespace, from, to, why); `why` is optional, as for `alloc_exec`. |
| `restart_allowlist` | The entries `job_restart` may restart now and their `source` (`variable` or `env`), with the reason the variable was not used when it was not. |
| `list_nodes`, `node_status` | Nodes; one node's attributes, driver and CSI health, capacity against allocations, drain, events. |
| `node_host` | One node's host from its client's stats: each disk's device, mountpoint, size, use and inode use, the alloc dir's disk, memory and uptime; the client's GC thresholds and `gc_max_allocs` (from node meta of the same name, else Nomad's defaults, named as such: no API carries a client's agent config); the allocations it holds, running and terminal; and `gc_pressure`, what makes the client collect dead allocations, logs included, at once. A client the servers cannot reach is named under `unreadable`. |
| `list_services`, `service` | Nomad native services (with Traefik `Host()` rules); one service's registrations. |
| `list_variables` | Variable paths and modify times. **Never values.** |
| `list_deployments`, `evaluation` | Deployments (active by default); one evaluation's placement failures in words. |

The job summary's failed and lost counts only grow over a job's life, so
the overview judges "failing now" from the allocations, not from them.

## The ACL policy

Every endpoint the server reads, and the capability it needs:

| Endpoint | Tools | Capability |
| --- | --- | --- |
| `/v1/status/leader` | overview | none |
| `/v1/jobs`, `/v1/job/:id` and its `allocations`, `evaluations`, `deployment(s)`, `versions`, `summary` | overview, jobs, job_status, job_versions, list_deployments | namespace `list-jobs`, `read-job` |
| `/v1/allocations`, `/v1/allocation/:id` | overview, alloc_*, prefix lookup | namespace `read-job` |
| `/v1/evaluations`, `/v1/evaluation/:id` (+ `/allocations`) | overview, evaluation | namespace `read-job` |
| `/v1/deployments` | overview, list_deployments | namespace `read-job` |
| `/v1/services`, `/v1/service/:name` | list_services, service | namespace `read-job` |
| `/v1/client/fs/logs/:alloc` | alloc_logs | namespace `read-logs` |
| `/v1/vars` (list only) | list_variables | namespace `variables` path `list` |
| `/v1/var/nomad/jobs/nomad-mcp/restart_jobs` (`default`) | job_restart, restart_allowlist | namespace `variables` path `read`, on that path alone |
| `/v1/nodes`, `/v1/node/:id`, `/v1/node/:id/allocations` | overview, nodes, node_status, node_host, service | `node` read |
| `/v1/client/stats` | node_host | `node` read |
| `/v1/plugins?type=csi` | overview | `plugin` read |
| `/v1/client/allocation/:id/exec` (websocket) | alloc_exec | namespace `alloc-exec` |
| `/v1/allocation/:id/stop` (POST) | job_restart | namespace `alloc-lifecycle` |
| `/v1/jobs` (POST, the job's own spec) | job_restart, for a dead job | namespace `submit-job` |
| `/v1/job/:id/revert` (POST) | job_revert | namespace `submit-job` |

```hcl
namespace "default" {
  capabilities = ["list-jobs", "read-job", "read-logs"]
  variables {
    # list shows paths and times; without read, the values stay unreadable
    # even to a server that tried.
    path "*" { capabilities = ["list"] }
    # The one value it reads: job_restart's allow-list.
    path "nomad/jobs/nomad-mcp/restart_jobs" { capabilities = ["list", "read"] }
  }
}
node   { policy = "read" }
plugin { policy = "read" }
```

Attach it to the job's workload identity:
`nomad acl policy apply -namespace default -job nomad-mcp nomad-mcp-read nomad-mcp.policy.hcl`.
A section the token may not read shows up in `cluster_overview` under
`unavailable`; any other tool refuses with the path that was forbidden.

`alloc_exec` also needs `alloc-exec`, which this policy leaves out: a separate
policy in ckopsa/home-infrastructure grants it. `job_restart` needs
`alloc-lifecycle` in each namespace its allow-list names, and `submit-job`
beside it to register a dead job again or revert one (`job_revert`), left out the same way. Each namespace in `NOMAD_MCP_NAMESPACES` needs the read capabilities above
in a `namespace` block of its own.

## Development

```
make test     # python3 -m unittest -v: a fake Nomad on 127.0.0.1 and on a unix socket
make run      # serve on :8111 against $NOMAD_ADDR
make image    # buildx arm64 → ghcr.io/ckopsa/nomad-mcp:<short sha>
make deploy   # the image, then nomad var put nomad/jobs/nomad-mcp/deploy image_tag=<sha>
```

CI (`.github/workflows/image.yml`) builds on an arm64 runner, pushes
`ghcr.io/ckopsa/nomad-mcp:<short sha>` and `latest` from `main`, and points the
job at the new tag by writing `nomad/jobs/nomad-mcp/deploy`. That write is CI's,
with CI's own token; the server's token only reads.

## License

Copyright (C) 2026 Colton Kopsa. Licensed under the GNU Affero General Public
License v3.0 or later; see [LICENSE](LICENSE).
