"""Protobuf-over-Unix-socket IPC client for nuefsd."""

import os
import pathlib
import shutil
import socket
import struct
import subprocess
import time

from nuefs._proto.nuefs import Request, Response


def default_socket_path() -> pathlib.Path:
    env = os.environ.get("NUEFSD_SOCKET")
    if env:
        return pathlib.Path(env)
    base = os.environ.get("XDG_RUNTIME_DIR") or "/tmp"
    uid = os.getuid()
    return pathlib.Path(base) / f"nuefsd-{uid}.sock"


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            msg = "connection closed"
            raise ConnectionError(msg)
        buf.extend(chunk)
    return bytes(buf)


def _send_recv(sock: socket.socket, request: Request) -> Response:
    body = bytes(request)
    sock.sendall(struct.pack(">I", len(body)) + body)
    length = struct.unpack(">I", _recv_exact(sock, 4))[0]
    return Response.parse(_recv_exact(sock, length))


def _find_nuefsd() -> pathlib.Path | None:
    env = os.environ.get("NUEFSD_BIN")
    if env:
        p = pathlib.Path(env)
        if p.exists():
            return p
    found = shutil.which("nuefsd")
    if found:
        return pathlib.Path(found)
    return None


def _ensure_daemon(socket_path: pathlib.Path) -> None:
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(str(socket_path))
        s.close()
        return
    except (FileNotFoundError, ConnectionRefusedError, OSError):
        pass

    nuefsd = _find_nuefsd()
    if nuefsd is None:
        msg = "nuefsd not found; install it or set NUEFSD_BIN"
        raise FileNotFoundError(msg)

    subprocess.Popen(
        [str(nuefsd), "--socket", str(socket_path)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd="/",
    )

    for _ in range(40):
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.connect(str(socket_path))
            s.close()
            return
        except (FileNotFoundError, ConnectionRefusedError, OSError):
            time.sleep(0.05)

    msg = "nuefsd started but did not become ready"
    raise TimeoutError(msg)


def call(request: Request) -> Response:
    socket_path = default_socket_path()
    _ensure_daemon(socket_path)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(str(socket_path))
    try:
        resp = _send_recv(sock, request)
        if resp.error is not None:
            raise RuntimeError(resp.error)
        return resp
    finally:
        sock.close()
