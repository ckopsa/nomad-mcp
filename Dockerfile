# Serves nomad-mcp over HTTP: a read-only view of the Nomad cluster over
# MCP. Build: make image (buildx arm64 → ghcr.io/ckopsa/nomad-mcp:<tag>).
FROM python:3.11-slim-bookworm

WORKDIR /app

# The server is the standard library and nothing else, so there is no
# dependency layer: the source and the one install step.
COPY pyproject.toml README.md ./
COPY nomadmcp/ nomadmcp/
RUN pip install --no-cache-dir --no-deps .

# The server binds every interface inside the container; the job
# publishes the port. The Nomad address and the token arrive from the
# job: inside a Nomad task that is the Task API socket and the task's
# workload identity,
#
#   NOMAD_ADDR=unix:///secrets/api.sock   (from the task's identity block)
#   NOMAD_TOKEN=<workload identity JWT>   (identity { env = true })
#
# so no long-lived token is baked into the image or the job.
ENV NOMAD_MCP_HOST=0.0.0.0 \
    PYTHONUNBUFFERED=1
EXPOSE 8111

# The server runs as root, as the bench does: the Task API socket lives
# in the task's secrets directory, whose owner and mode are Nomad's
# choice, and a non-root user there is a permission error waiting for
# the first deploy. The container holds no secret beyond its own
# short-lived identity, and every request it makes is a GET.

# The health check asks the server, not Nomad: a Nomad outage must not
# get the one tool for looking at the outage restarted in a loop.
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8111/health', timeout=4).status == 200 else 1)"

CMD ["nomad-mcp", "--http", "8111", "--host", "0.0.0.0"]
