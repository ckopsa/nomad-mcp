# nomad-mcp

A small MCP server for troubleshooting a HashiCorp Nomad cluster. It is
**read-only**: it sends nothing but `GET` to the Nomad API, and there is no
code path that could send anything else (the one line that writes a request
hard-codes the method, and the tests fail if that changes). Changes to the
cluster go through commits to
[ckopsa/home-infrastructure](https://github.com/ckopsa/home-infrastructure),
which CI plans and applies on merge after approval; this server only looks.

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
| `NOMAD_NAMESPACE` | the namespace to read; `default` when unset |
| `NOMAD_CACERT` | a CA bundle for an https address (optional) |
| `NOMAD_MCP_HOST` | the address `--http` listens on; `127.0.0.1` when unset |
| `NOMAD_MCP_URL` | where `call` sends; `http://127.0.0.1:8111/mcp/` when unset |

In production the server runs as a Nomad task and talks to the **Task API**:
the task's `identity { env = true }` puts its workload identity in
`NOMAD_TOKEN`, and `NOMAD_ADDR=unix:///secrets/api.sock` reaches the agent over
the socket Nomad mounts into the task. No long-lived token exists anywhere.

## Tools

Answers are summaries for troubleshooting, not raw API JSON: lists are capped,
log tails are capped, times read `2026-09-20 23:05:10Z (4d ago)`. Allocation,
node and evaluation ids may be given as prefixes (a node also by name); a prefix
that matches more than one is refused with the candidates listed.

| Tool | What it answers |
| --- | --- |
| `cluster_overview` | Start here. Leader, nodes, jobs not running as they should (a dead job parked at `count = 0` is listed apart), allocations stuck, failures of the last 24 h, deployments in motion or failed today, blocked evaluations with reasons, CSI plugin health. |
| `list_jobs` | Jobs with type, status, pool and non-zero summary counts per group. |
| `job_status` | One job: spec basics, groups and tasks (driver, image), newest allocations with task states, the latest deployment, evaluations that failed to place and why. |
| `job_versions` | Recent versions with the diff from each one's predecessor as `old -> new` lines. |
| `alloc_status` | One allocation: statuses and descriptions, each task's state, restarts and last events (exit codes, driver errors), resources, ports. |
| `alloc_logs` | The tail of a task's stderr or stdout, 8000 bytes by default, 64000 at most. |
| `list_nodes`, `node_status` | Nodes; one node's attributes, driver and CSI health, capacity against allocations, drain, events. |
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
| `/v1/nodes`, `/v1/node/:id`, `/v1/node/:id/allocations` | overview, nodes, node_status, service | `node` read |
| `/v1/plugins?type=csi` | overview | `plugin` read |

```hcl
namespace "default" {
  capabilities = ["list-jobs", "read-job", "read-logs"]
  variables {
    # list shows paths and times; without read, the values stay unreadable
    # even to a server that tried.
    path "*" { capabilities = ["list"] }
  }
}
node   { policy = "read" }
plugin { policy = "read" }
```

Attach it to the job's workload identity:
`nomad acl policy apply -namespace default -job nomad-mcp nomad-mcp-read nomad-mcp.policy.hcl`.
A section the token may not read shows up in `cluster_overview` under
`unavailable`; any other tool refuses with the path that was forbidden.

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
