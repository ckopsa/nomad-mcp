"""The entry point of nomad-mcp.

  nomad-mcp --http 8111
  nomad-mcp --stdio
  nomad-mcp call job_status job=traefik --url http://127.0.0.1:8111/mcp/

The server reads NOMAD_ADDR, NOMAD_TOKEN and NOMAD_NAMESPACE, as the
nomad CLI does. The call form drives one tool of a running server from
the shell. A value that parses as JSON is JSON (true, 3, ["a"]); any
other value is a text.
"""

import argparse
import json
import os
import sys
import urllib.request

from . import mcp
from .nomad import Client


DEFAULT_PORT = 8111


def parse_args(argv):
    parser = argparse.ArgumentParser(
        prog="nomad-mcp", description="A read-only MCP server for troubleshooting Nomad.")
    parser.add_argument("--http", type=int, metavar="PORT",
                        help="Serve MCP over HTTP at /mcp/ on this port (%s in the image)." % DEFAULT_PORT)
    parser.add_argument("--host", default=os.environ.get("NOMAD_MCP_HOST", "127.0.0.1"),
                        help="The address to listen on. The default is 127.0.0.1. "
                             "The setting is NOMAD_MCP_HOST.")
    parser.add_argument("--stdio", action="store_true", help="Serve MCP over stdin and stdout.")
    return parser.parse_args(argv)


def parse_call_args(argv):
    parser = argparse.ArgumentParser(prog="nomad-mcp call",
                                     description="Call one tool of a running server.")
    parser.add_argument("tool", help="The tool name, for example cluster_overview or job_status.")
    parser.add_argument("pairs", nargs="*", metavar="key=value", help="The arguments of the tool.")
    parser.add_argument("--url", default=os.environ.get(
        "NOMAD_MCP_URL", "http://127.0.0.1:%s/mcp/" % DEFAULT_PORT),
        help="The address of the server. The setting is NOMAD_MCP_URL.")
    parser.add_argument("--timeout", type=int, default=60, help="The seconds to wait for the answer.")
    return parser.parse_args(argv)


def call(argv):
    """Runs one tool over HTTP and prints its JSON. Exits 1 on a refusal."""
    args = parse_call_args(argv)
    arguments = {}
    for pair in args.pairs:
        if "=" not in pair:
            print("give key=value, not %r" % pair, file=sys.stderr)
            return 2
        key, value = pair.split("=", 1)
        try:
            arguments[key] = json.loads(value)
        except ValueError:
            arguments[key] = value
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                       "params": {"name": args.tool, "arguments": arguments}}).encode("utf-8")
    # This POST goes to the MCP server, not to Nomad.
    request = urllib.request.Request(args.url, data=body, method="POST",
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=args.timeout) as answer:
            message = json.load(answer)
    except OSError as exc:
        print("no answer from %s: %s" % (args.url, exc), file=sys.stderr)
        return 2
    if "error" in message:
        print(json.dumps(message["error"], indent=1), file=sys.stderr)
        return 2
    result = message["result"]
    print(json.dumps(result["structuredContent"]["result"], indent=1, sort_keys=True))
    return 1 if result.get("isError") else 0


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "call":
        return call(argv[1:])
    args = parse_args(argv)
    if args.http is None and not args.stdio:
        print("give --http PORT or --stdio", file=sys.stderr)
        return 2
    try:
        client = Client.from_env()
    except ValueError as exc:
        print("configuration: %s" % exc, file=sys.stderr)
        return 2
    if args.stdio:
        mcp.serve_stdio(client)
        return 0
    httpd = mcp.serve_http(client, args.http, host=args.host)
    print("nomad-mcp on http://%s:%s/mcp/ reading %s (namespace %s, token %s)"
          % (args.host, httpd.server_address[1], client.describe(), ",".join(client.namespaces),
             "set" if client.token else "none"), file=sys.stderr)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
