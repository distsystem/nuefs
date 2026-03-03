"""NueFS core implementation."""

import collections.abc
import dataclasses
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import time
import typing

import grpc

from nuefs._proto.nuefs import (
    DaemonInfoReq,
    ManifestEntry as ProtoEntry,
    MountReq,
    MountRoot as ProtoMount,
    NueFsStub,
    ResolveReq,
    ShutdownReq,
    StatusReq,
    UnmountReq,
    UpdateReq,
    WhichReq,
)


def default_socket_path() -> pathlib.Path:
    """Get the default socket path for the daemon."""
    env = os.environ.get("NUEFSD_SOCKET")
    if env:
        return pathlib.Path(env)
    base = os.environ.get("XDG_RUNTIME_DIR") or "/tmp"
    uid = os.getuid()
    return pathlib.Path(base) / f"nuefsd-{uid}.sock"


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


def _stub() -> NueFsStub:
    socket_path = default_socket_path()
    _ensure_daemon(socket_path)
    channel = grpc.insecure_channel(
        f"unix:{socket_path}",
        options=[("grpc.default_authority", "localhost")],
    )
    return NueFsStub(channel)


@dataclasses.dataclass
class ManifestEntry:
    virtual_path: pathlib.PurePosixPath
    backend_path: pathlib.Path
    is_dir: bool

    def _to_proto(self) -> ProtoEntry:
        return ProtoEntry(
            virtual_path=str(self.virtual_path),
            backend_path=str(self.backend_path),
            is_dir=self.is_dir,
        )


@dataclasses.dataclass
class MountRoot:
    virtual_prefix: pathlib.PurePosixPath
    backend_path: pathlib.Path

    def _to_proto(self) -> ProtoMount:
        return ProtoMount(
            virtual_prefix=str(self.virtual_prefix),
            backend_path=str(self.backend_path),
        )


@dataclasses.dataclass
class OwnerInfo:
    owner: str
    backend_path: pathlib.Path


@dataclasses.dataclass
class DaemonInfo:
    pid: int
    socket: pathlib.Path
    started_at: int


class Handle:
    """Handle to a mounted NueFS filesystem."""

    __slots__ = ("_root", "_mount_id")

    def __init__(self, root: str, mount_id: int) -> None:
        self._root = root
        self._mount_id = mount_id

    @property
    def root(self) -> str:
        """Mount root path (read-only)."""
        return self._root

    def update(
        self,
        entries: collections.abc.Sequence[ManifestEntry],
        mount_roots: collections.abc.Sequence[MountRoot] | None = None,
    ) -> None:
        """Update the mount manifest."""
        _stub().update(
            UpdateReq(
                mount_id=self._mount_id,
                entries=[e._to_proto() for e in entries],
                mount_roots=[m._to_proto() for m in (mount_roots or [])],
            )
        )

    def which(self, path: str) -> OwnerInfo | None:
        """Query which backend owns a path."""
        resp = _stub().which(
            WhichReq(mount_id=self._mount_id, path=path)
        )
        info = resp.owner_info
        if info is None or not info.owner:
            return None
        return OwnerInfo(owner=info.owner, backend_path=pathlib.Path(info.backend_path))

    def unmount(self) -> None:
        """Unmount the filesystem."""
        _stub().unmount(UnmountReq(mount_id=self._mount_id))

    def close(self) -> None:
        """Release the client handle (mount stays alive in daemon)."""

    def __enter__(self) -> typing.Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: typing.Any,
    ) -> bool:
        self.close()
        return False


def open(root: str | os.PathLike[str] | pathlib.Path) -> Handle:
    """Open a NueFS mount, creating an empty one if it doesn't exist."""
    root_path = pathlib.Path(root).expanduser().resolve()

    resp = _stub().resolve(ResolveReq(root=str(root_path)))
    if resp.mount_id is not None:
        return Handle(str(root_path), resp.mount_id)

    resp = _stub().mount(MountReq(root=str(root_path)))
    return Handle(str(root_path), resp.mount_id)


def status() -> list[Handle]:
    """List all active mounts."""
    resp = _stub().status(StatusReq())
    return [Handle(m.root, m.mount_id) for m in resp.mounts]


def daemon_info() -> DaemonInfo:
    """Get information about the daemon process."""
    resp = _stub().get_daemon_info(DaemonInfoReq())
    info = resp.info
    return DaemonInfo(
        pid=info.pid,
        socket=pathlib.Path(info.socket),
        started_at=info.started_at,
    )


def shutdown() -> None:
    """Shutdown the daemon gracefully."""
    try:
        _stub().shutdown(ShutdownReq())
    except grpc.RpcError:
        pass
