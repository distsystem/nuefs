"""NueFS - FUSE-based layered filesystem for Python."""

import collections.abc
import os
import pathlib
import typing

from nuefs._nuefs import Mapping, OwnerInfo
from nuefs._nuefs import _get_manifest, _mount, _status, _unmount, _update, _which


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
        return _get_manifest(self._mount_id)

    def update(self, mounts: collections.abc.Sequence[Mapping]) -> None:
        """Update the mount manifest."""
        _update(self._mount_id, list(mounts))

    def which(self, path: str) -> OwnerInfo | None:
        """Query which backend owns a path."""
        try:
            return _which(self._mount_id, path)
        except RuntimeError:
            return None

    def close(self) -> None:
        """Close the mount."""
        _unmount(self._mount_id)

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


def open(
    root: str | os.PathLike[str] | pathlib.Path,
    mounts: collections.abc.Sequence[Mapping] | None = None,
) -> Handle:
    """Open a NueFS mount.

    If mounts is provided, creates a new mount; otherwise resolves existing.
    """
    root_path = pathlib.Path(root).expanduser().resolve()

    if mounts is not None:
        raw = _mount(root_path, list(mounts))
        return Handle(str(raw.root), raw.mount_id)

    for h in _status():
        if pathlib.Path(h.root).resolve() == root_path:
            return Handle(str(h.root), h.mount_id)
    raise RuntimeError("Mount not found")


def status() -> list[Handle]:
    """List all active mounts."""
    return [Handle(str(h.root), h.mount_id) for h in _status()]


__all__ = ["Handle", "Mapping", "OwnerInfo", "open", "status"]
