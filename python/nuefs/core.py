"""NueFS core implementation."""

import collections.abc
import dataclasses
import os
import pathlib
import typing

from nuefs import _ipc
from nuefs._proto.nuefs import (
    DaemonInfoReq,
    ManifestEntry as ProtoEntry,
    MountReq,
    MountRoot as ProtoMount,
    ResolveReq,
    Request,
    ShutdownReq,
    StatusReq,
    UnmountReq,
    UpdateReq,
    WhichReq,
)


@dataclasses.dataclass
class ManifestEntry:
    virtual_path: str
    backend_path: pathlib.Path
    is_dir: bool

    def _to_proto(self) -> ProtoEntry:
        return ProtoEntry(
            virtual_path=self.virtual_path,
            backend_path=str(self.backend_path),
            is_dir=self.is_dir,
        )


@dataclasses.dataclass
class MountRoot:
    virtual_prefix: str
    backend_path: pathlib.Path

    def _to_proto(self) -> ProtoMount:
        return ProtoMount(
            virtual_prefix=self.virtual_prefix,
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
        _ipc.call(
            Request(
                update=UpdateReq(
                    mount_id=self._mount_id,
                    entries=[e._to_proto() for e in entries],
                    mount_roots=[m._to_proto() for m in (mount_roots or [])],
                )
            )
        )

    def which(self, path: str) -> OwnerInfo | None:
        """Query which backend owns a path."""
        resp = _ipc.call(
            Request(which=WhichReq(mount_id=self._mount_id, path=path))
        )
        ok = resp.ok
        if ok is None:
            return None
        info = ok.owner_info
        if info is None or not info.owner:
            return None
        return OwnerInfo(owner=info.owner, backend_path=pathlib.Path(info.backend_path))

    def unmount(self) -> None:
        """Unmount the filesystem."""
        _ipc.call(Request(unmount=UnmountReq(mount_id=self._mount_id)))

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

    resp = _ipc.call(Request(resolve=ResolveReq(root=str(root_path))))
    ok = resp.ok
    if ok is not None and ok.resolve is not None and ok.resolve.mount_id is not None:
        return Handle(str(root_path), ok.resolve.mount_id)

    resp = _ipc.call(Request(mount=MountReq(root=str(root_path))))
    return Handle(str(root_path), resp.ok.mount_id)


def status() -> list[Handle]:
    """List all active mounts."""
    resp = _ipc.call(Request(status=StatusReq()))
    ok = resp.ok
    if ok is None or ok.status is None:
        return []
    return [Handle(m.root, m.mount_id) for m in ok.status.mounts]


def daemon_info() -> DaemonInfo:
    """Get information about the daemon process."""
    resp = _ipc.call(Request(daemon_info=DaemonInfoReq()))
    info = resp.ok.daemon_info
    return DaemonInfo(
        pid=info.pid,
        socket=pathlib.Path(info.socket),
        started_at=info.started_at,
    )


def shutdown() -> None:
    """Shutdown the daemon gracefully."""
    _ipc.call(Request(shutdown=ShutdownReq()))


def default_socket_path() -> pathlib.Path:
    """Get the default socket path for the daemon."""
    return _ipc.default_socket_path()
