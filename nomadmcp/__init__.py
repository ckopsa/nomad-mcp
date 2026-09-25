"""nomad-mcp: a small, read-only MCP server for troubleshooting a Nomad cluster.

It answers questions about the cluster (what is failing, why an
allocation will not place, what a task wrote to stderr) and it changes
nothing. Changes go through commits to ckopsa/home-infrastructure,
which CI plans and applies. The server has no dependency outside the
standard library.
"""

__version__ = "0.1.0"
