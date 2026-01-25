"""NueFS core implementation."""

import collections.abc
import os
import pathlib
import typing

import nuefs._nuefs as _ext

Mapping = _ext.Mapping
OwnerInfo = _ext.OwnerInfo
DaemonInfo = _ext.DaemonInfo


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

    @property
    def manifest(self) -> list[Mapping]:
        """Get the current mount manifest."""
        return _ext._get_manifest(self._mount_id)

    def mount(self, mounts: collections.abc.Sequence[Mapping]) -> None:
        """Set/update the mount configuration."""
        _ext._update(self._mount_id, list(mounts))

    def which(self, path: str) -> OwnerInfo | None:
        """Query which backend owns a path."""
        return _ext._which(self._mount_id, path)

    def close(self) -> None:
        """Close the mount."""
        _ext._unmount(self._mount_id)

    def __enter__(self) -> typing.Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: typing.Any,
    ) -> bool:
        return False


def open(root: str | os.PathLike[str] | pathlib.Path) -> Handle:
    """Open a NueFS mount, creating an empty one if it doesn't exist."""
    root_path = pathlib.Path(root).expanduser().resolve()

    mount_id = _ext._resolve(root_path)
    if mount_id is not None:
        return Handle(str(root_path), mount_id)

    raw = _ext._mount(root_path, [])
    return Handle(str(raw.root), raw.mount_id)


def status() -> list[Handle]:
    """List all active mounts."""
    return [Handle(str(h.root), h.mount_id) for h in _ext._status()]


def daemon_info() -> DaemonInfo:
    """Get information about the daemon process."""
    return _ext._daemon_info()
