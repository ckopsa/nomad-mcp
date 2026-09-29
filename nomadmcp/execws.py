"""Runs one command inside an allocation, over Nomad's exec websocket.

The one place nomad-mcp can change anything, kept apart from nomad.Client,
whose only method stays GET. Each frame carries one of Nomad's exec messages
as JSON; there is no TTY. Closing the connection is how Nomad is told to end
the command.
"""

import base64
import json
import os
import socket
import ssl
import struct
import time
import urllib.parse


OUTPUT_CAP = 64 * 1024


class ExecError(Exception):

    def __init__(self, reason, status=0):
        Exception.__init__(self, reason)
        self.reason = reason
        self.status = status


def _open(client, timeout):
    if client.scheme == "unix":
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect(client.socket_path)
        return sock, "localhost"
    sock = socket.create_connection((client.host, client.port), timeout=timeout)
    if client.scheme == "https":
        context = ssl.create_default_context(cafile=client.cacert)
        sock = context.wrap_socket(sock, server_hostname=client.host)
    return sock, "%s:%s" % (client.host, client.port)


class _WebSocket:

    def __init__(self, sock):
        self.sock = sock
        self.buffer = b""
        self.closed_with = None

    def _read(self, count):
        while len(self.buffer) < count:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ExecError("Nomad closed the exec connection")
            self.buffer += chunk
        data, self.buffer = self.buffer[:count], self.buffer[count:]
        return data

    def handshake(self, host, target, token):
        lines = ["GET %s HTTP/1.1" % target, "Host: " + host, "Upgrade: websocket",
                 "Connection: Upgrade", "Sec-WebSocket-Version: 13", "User-Agent: nomad-mcp",
                 "Sec-WebSocket-Key: " + base64.b64encode(os.urandom(16)).decode("ascii")]
        if token:
            lines.append("X-Nomad-Token: " + token)
        self.sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("utf-8"))
        while b"\r\n\r\n" not in self.buffer:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ExecError("Nomad closed the exec connection before answering")
            self.buffer += chunk
        head, self.buffer = self.buffer.split(b"\r\n\r\n", 1)
        first = head.split(b"\r\n", 1)[0].decode("latin-1")
        parts = first.split(" ", 2)
        status = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        if status != 101:
            detail = self.buffer[:300].decode("utf-8", "replace").strip() or first
            if status == 403:
                detail += "; the token needs the alloc-exec capability"
            raise ExecError("Nomad answered %s to the exec: %s" % (status, detail), status)

    def send(self, message):
        payload, mask = json.dumps(message).encode("utf-8"), os.urandom(4)
        size = len(payload)
        if size < 126:
            head = struct.pack("!BB", 0x81, 0x80 | size)
        elif size < 65536:
            head = struct.pack("!BBH", 0x81, 0xFE, size)
        else:
            head = struct.pack("!BBQ", 0x81, 0xFF, size)
        key = (mask * (size // 4 + 1))[:size]
        masked = (int.from_bytes(payload, "big") ^ int.from_bytes(key, "big")).to_bytes(size, "big")
        self.sock.sendall(head + mask + masked)

    def receive(self):
        """The next message as a dict, or None when Nomad closed the socket."""
        message = b""
        while True:
            first, second = self._read(2)
            size = second & 0x7F
            if size == 126:
                size = struct.unpack("!H", self._read(2))[0]
            elif size == 127:
                size = struct.unpack("!Q", self._read(8))[0]
            payload = self._read(size)  # A server never masks its frames.
            if first & 0x0F == 0x8:
                self.closed_with = payload[2:].decode("utf-8", "replace").strip()
                return None
            if first & 0x0F in (0x0, 0x1, 0x2):
                message += payload
                if first & 0x80:
                    return json.loads(message.decode("utf-8"))


def run(client, alloc_id, task, argv, stdin, timeout):
    """Runs argv in the task for up to timeout seconds. Raises ExecError."""
    query = urllib.parse.urlencode({"namespace": client.namespace, "task": task, "tty": "false",
                                    "command": json.dumps(argv)})
    target = "/v1/client/allocation/%s/exec?%s" % (urllib.parse.quote(alloc_id, safe=""), query)
    deadline = time.monotonic() + timeout
    streams = {"stdout": bytearray(), "stderr": bytearray()}
    answer = {"exit_code": None, "truncated": False, "timed_out": False}
    try:
        sock, host = _open(client, min(timeout, client.timeout))
    except OSError as exc:
        raise ExecError(client._scrub("cannot reach Nomad at %s: %s: %s"
                                      % (client.addr, type(exc).__name__, exc)))
    try:
        ws = _WebSocket(sock)
        ws.handshake(host, target, client.token)
        if stdin:
            ws.send({"stdin": {"data": base64.b64encode(stdin.encode("utf-8")).decode("ascii")}})
        ws.send({"stdin": {"close": True}})
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise socket.timeout()
            sock.settimeout(remaining)
            message = ws.receive()
            if message is None:
                raise ExecError(client._scrub("Nomad closed the exec before the command exited: %s"
                                              % (ws.closed_with or "no reason given")))
            if message.get("exited"):
                answer["exit_code"] = (message.get("result") or {}).get("exit_code", 0)
                break
            for name, buffer in streams.items():
                data = (message.get(name) or {}).get("data")
                if data:
                    buffer += base64.b64decode(data)
                    if len(buffer) > OUTPUT_CAP:
                        del buffer[:-OUTPUT_CAP]  # The tail is what says how it ended.
                        answer["truncated"] = True
    except socket.timeout:
        answer["timed_out"] = True
        answer["note"] = "the command ran past %ss: the exec was closed, which ends it" % timeout
    except (OSError, ValueError) as exc:
        raise ExecError(client._scrub("the exec failed: %s: %s" % (type(exc).__name__, exc)))
    finally:
        sock.close()
    for name, buffer in streams.items():
        answer[name] = buffer.decode("utf-8", "replace")
    return answer
