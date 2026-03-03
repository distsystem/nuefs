"""NueFS core: gRPC client objects."""

import collections.abc
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import time

import grpc

from nuefs._proto.nuefs import (
    DaemonInfo,
    DaemonInfoReq,
    ManifestEntry,
    MountReq,
    MountRoot,
    NueFsStub,
    ResolveReq,
    ShutdownReq,
    StatusReq,
    UnmountReq,
    UpdateReq,
)


def default_socket_path() -> pathlib.Path:
    env = os.environ.get("NUEFSD_SOCKET")
    if env:
        return pathlib.Path(env)
    base = os.environ.get("XDG_RUNTIME_DIR") or "/tmp"
    uid = os.getuid()
    return pathlib.Path(base) / f"nuefsd-{uid}.sock"


def daemon_running(socket_path: pathlib.Path | None = None) -> bool:
    sp = socket_path or default_socket_path()
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(str(sp))
        return True
    except OSError:
        return False


def _find_nuefsd() -> pathlib.Path | None:
    env = os.environ.get("NUEFSD_BIN")
    if env:
        p = pathlib.Path(env)
        if p.exists():
            return p
    found = shutil.which("nuefsd")
    if found:
        return pathlib.Path(found)
    prefix_bin = pathlib.Path(sys.prefix) / "bin" / "nuefsd"
    if prefix_bin.exists():
        return prefix_bin
    return None


def _ensure_daemon(socket_path: pathlib.Path) -> None:
    if daemon_running(socket_path):
        return

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
        if daemon_running(socket_path):
            return
        time.sleep(0.05)

    msg = "nuefsd started but did not become ready"
    raise TimeoutError(msg)


class Mount:
    __slots__ = ("_stub", "_root", "_mount_id")

    def __init__(self, stub: NueFsStub, root: str, mount_id: int) -> None:
        self._stub = stub
        self._root = root
        self._mount_id = mount_id

    @property
    def root(self) -> str:
        return self._root

    def update(
        self,
        entries: collections.abc.Sequence[ManifestEntry],
        mount_roots: collections.abc.Sequence[MountRoot] | None = None,
    ) -> None:
        self._stub.update(
            UpdateReq(
                mount_id=self._mount_id,
                entries=list(entries),
                mount_roots=list(mount_roots or []),
            )
        )

    def unmount(self) -> None:
        self._stub.unmount(UnmountReq(mount_id=self._mount_id))


class NueFs:
    """gRPC client for the NueFS daemon."""

    __slots__ = ("_channel", "_stub")

    def __init__(self, socket_path: pathlib.Path | None = None) -> None:
        sp = socket_path or default_socket_path()
        _ensure_daemon(sp)
        self._channel = grpc.insecure_channel(
            f"unix:{sp}",
            options=[("grpc.default_authority", "localhost")],
        )
        self._stub = NueFsStub(self._channel)

    def connect(self, root: str | os.PathLike[str]) -> Mount:
        root_str = str(pathlib.Path(root).expanduser().resolve())
        resp = self._stub.resolve(ResolveReq(root=root_str))
        mount_id = resp.mount_id
        if mount_id is None:
            mount_id = self._stub.mount(MountReq(root=root_str)).mount_id
        return Mount(self._stub, root_str, mount_id)

    def status(self) -> list[Mount]:
        resp = self._stub.status(StatusReq())
        return [Mount(self._stub, m.root, m.mount_id) for m in resp.mounts]

    def info(self) -> DaemonInfo:
        return self._stub.get_daemon_info(DaemonInfoReq()).info

    def shutdown(self) -> None:
        try:
            self._stub.shutdown(ShutdownReq())
        except grpc.RpcError:
            pass
