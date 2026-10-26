"""Minimal stdlib async HTTP layer (no dependencies).

Server: single request per connection, JSON body, CORS enabled so the static web
UI can talk to every node directly.
Client: best-effort, returns None on any network error (Raft must tolerate that).
"""

import asyncio
import json
from urllib.parse import urlsplit

KIND_PATH = {
    "vote": "/raft/vote",
    "vote_ack": "/raft/vote",          # responses come back on the same path
    "append": "/raft/append",
    "append_ack": "/raft/append",
    "snapshot": "/raft/snapshot",
    "snapshot_ack": "/raft/snapshot",
    "check": "/raft/check",
    "check_ack": "/raft/check",
}

PATH_KIND = {KIND_PATH[k]: k for k in ("vote", "append", "snapshot", "check")}


def url_for(base, kind):
    return KIND_PATH[kind]


def base_url(host, port):
    return f"http://{host}:{port}"


async def read_json_request(reader):
    line = await reader.readline()
    if not line:
        return None
    parts = line.decode().split()
    if len(parts) < 2:
        return None
    method, path = parts[0], parts[1]
    headers = {}
    while True:
        h = await reader.readline()
        if h in (b"\r\n", b"\n", b""):
            break
        k, _, v = h.decode().partition(":")
        headers[k.strip().lower()] = v.strip()
    body = b""
    n = int(headers.get("content-length", "0") or 0)
    if n:
        body = await reader.readexactly(n)
    payload = json.loads(body.decode()) if body else {}
    return method, path, headers, payload


async def write_json_response(writer, obj, status=200):
    body = json.dumps(obj, sort_keys=True, default=str).encode()
    await write_raw(writer, body, "application/json", status)


CORS_HEADERS = (
    b"Access-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: content-type\r\n"
    b"Access-Control-Allow-Methods: GET,POST,OPTIONS\r\n"
)


async def write_raw(writer, body, content_type="text/html; charset=utf-8", status=200):
    writer.write(b"HTTP/1.1 %d OK\r\nContent-Type: %s\r\nContent-Length: %d\r\n%s"
                 b"Connection: close\r\n\r\n" % (status, content_type.encode(), len(body), CORS_HEADERS))
    writer.write(body)
    await writer.drain()


async def start_server(host, port, handler):
    async def on_conn(reader, writer):
        try:
            req = await read_json_request(reader)
            if req is None:
                await writer.drain()
                writer.close()
                return
            method, path, headers, payload = req
            if method == "OPTIONS":
                await write_json_response(writer, {"ok": True})
            else:
                result = await handler(path, payload)
                if isinstance(result, tuple):
                    content_type, body = result
                    await write_raw(writer, body, content_type)
                else:
                    await write_json_response(writer, result if result is not None else {"ok": True})
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            pass
        except Exception as e:  # a node must never die because of one bad request
            try:
                await write_json_response(writer, {"ok": False, "error": str(e)}, status=500)
            except Exception:
                pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    server = await asyncio.start_server(on_conn, host, port)
    actual = server.sockets[0].getsockname()[1]
    return server, actual


async def post(base, path, payload, timeout=1.0, host="127.0.0.1"):
    return await _request(base, path, payload, "POST", timeout)


async def get(base, path, payload=None, timeout=1.0):
    return await _request(base, path, payload, "GET", timeout)


async def _request(base, path, payload, method, timeout):
    u = urlsplit(base)
    host, port = u.hostname or "127.0.0.1", u.port or 80
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
    except Exception:
        return None
    try:
        body = json.dumps(payload or {}, sort_keys=True, default=str).encode()
        writer.write(b"%s %s HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\n"
                     b"Content-Length: %d\r\nConnection: close\r\n\r\n" % (method.encode(), path.encode(),
                                            f"{host}:{port}".encode(), len(body)))
        writer.write(body)
        await writer.drain()
        status_line = await asyncio.wait_for(reader.readline(), timeout)
        if not status_line:
            return None
        headers = {}
        while True:
            h = await asyncio.wait_for(reader.readline(), timeout)
            if h in (b"\r\n", b"\n", b""):
                break
            k, _, v = h.decode().partition(":")
            headers[k.strip().lower()] = v.strip()
        n = int(headers.get("content-length", "0") or 0)
        data = await asyncio.wait_for(reader.readexactly(n), timeout) if n else b""
        if not data:
            return None
        return json.loads(data.decode())
    except Exception:
        return None
    finally:
        try:
            writer.close()
        except Exception:
            pass
