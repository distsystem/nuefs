"""NueFS core implementation."""

import collections.abc
import os
import pathlib
import typing

import nuefs._nuefs as _ext

from nuefs.builder import ManifestBuilder

Mapping = _ext.Mapping
ManifestEntry = _ext.ManifestEntry
OwnerInfo = _ext.OwnerInfo
DaemonInfo = _ext.DaemonInfo


class Handle:
    """Handle to a mounted NueFS filesystem."""

    __slots__ = ("_root", "_mount_id", "_mounts")

    def __init__(self, root: str, mount_id: int) -> None:
        self._root = root
        self._mount_id = mount_id
        self._mounts: list[Mapping] = []

    @property
    def root(self) -> str:
        """Mount root path (read-only)."""
        return self._root

    def mount(self, mounts: collections.abc.Sequence[Mapping]) -> None:
        """Set/update the mount configuration."""
        self._mounts = list(mounts)

        builder = ManifestBuilder(pathlib.Path(self._root))
        builder.scan_real()

        for m in mounts:
            builder.apply_layer(m.source, str(m.target))

        entries = builder.build()
        _ext._update(self._mount_id, entries)

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

    builder = ManifestBuilder(root_path)
    builder.scan_real()
    entries = builder.build()

    raw = _ext._mount(root_path, entries)
    return Handle(str(raw.root), raw.mount_id)


def status() -> list[Handle]:
    """List all active mounts."""
    return [Handle(str(h.root), h.mount_id) for h in _ext._status()]


def daemon_info() -> DaemonInfo:
    """Get information about the daemon process."""
    return _ext._daemon_info()
