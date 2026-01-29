"""NueFS lockfile (nue.lock).

This is the compiled, machine-oriented snapshot of a mount's resolved union view.
The lockfile is the single source of truth for what gets sent to the daemon.
"""

import hashlib
import os
import pathlib
import time
import typing

import pydantic
from sheaves.sheaf import Sheaf
from sheaves.typing import Pathspec

import nuefs._nuefs as _ext

# Directories to skip during scan (caches, build artifacts)
SKIP_DIRS = {".git", ".pixi", "node_modules", "__pycache__", ".venv", "target"}


class MountLayer(typing.NamedTuple):
    """A mount layer with source, target, and filtering rules."""

    source: pathlib.Path
    target: str
    exclude: Pathspec
    include: Pathspec


class LockMeta(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    generated_at: int
    nuefs_version: str | None = None


class Lock(Sheaf):
    """Compiled NueFS lockfile (nue.lock)."""

    model_config = pydantic.ConfigDict(extra="forbid")

    apiVersion: typing.Literal["nue/lock/v1"] = "nue/lock/v1"
    meta: LockMeta
    entries: list[_ext.ManifestEntry] = pydantic.Field(default_factory=list)

    manifest_path: str = "nue.yaml"
    manifest_sha256: str | None = None

    @classmethod
    def compile(
        cls,
        root: pathlib.Path,
        mounts: typing.Iterable[MountLayer | tuple[pathlib.Path, str]] = (),
        *,
        nuefs_version: str | None = None,
        manifest_path: str = "nue.yaml",
    ) -> "Lock":
        root = root.expanduser().resolve()

        entries: dict[str, _ext.ManifestEntry] = {}

        # Minimal cover: only register top-level entries, not recursive.
        # FUSE layer will use openat to read subdirectory contents dynamically.
        if root.exists():
            for item in root.iterdir():
                name = item.name
                try:
                    is_dir = item.is_dir() and not item.is_symlink()
                except (PermissionError, OSError):
                    continue
                entries[name] = _ext.ManifestEntry(
                    virtual_path=name,
                    backend_path=item.resolve() if not item.is_symlink() else item,
                    is_dir=is_dir,
                )

        for mount in mounts:
            if isinstance(mount, MountLayer):
                cls._apply_layer(
                    entries, mount.source, mount.target, mount.exclude, mount.include
                )
            else:
                source, target = mount
                cls._apply_layer(entries, source, target, Pathspec(), Pathspec())

        manifest_sha256 = None
        manifest_file = root / manifest_path
        if manifest_file.exists():
            manifest_sha256 = hashlib.sha256(manifest_file.read_bytes()).hexdigest()

        return cls(
            meta=LockMeta(
                generated_at=int(time.time()),
                nuefs_version=nuefs_version,
            ),
            entries=list(entries.values()),
            manifest_path=manifest_path,
            manifest_sha256=manifest_sha256,
        )

    @staticmethod
    def _apply_layer(
        entries: dict[str, _ext.ManifestEntry],
        source: pathlib.Path,
        target: str,
        exclude: Pathspec | list[str],
        include: Pathspec | list[str],
    ) -> None:
        """Apply a layer mount with exclude/include filtering."""
        source = source.expanduser().resolve()

        target = target.strip() if target else "."
        if target == "":
            target = "."

        # Convert list to Pathspec if needed
        exclude_spec = Pathspec(exclude) if isinstance(exclude, list) else exclude
        include_spec = Pathspec(include) if isinstance(include, list) else include

        def is_excluded(rel_path: str, is_dir: bool = False) -> bool:
            """Check if path matches exclude patterns (and not include)."""
            if not exclude_spec or not exclude_spec.root:
                return False
            # For directories, also try matching with trailing slash (gitignore convention)
            paths_to_check = [rel_path]
            if is_dir:
                paths_to_check.append(f"{rel_path}/")
            for p in paths_to_check:
                if exclude_spec.match(p):
                    if include_spec and include_spec.root:
                        if any(include_spec.match(ip) for ip in paths_to_check):
                            return False
                    return True
            return False

        if source.is_file():
            virtual = target if target != "." else source.name
            if is_excluded(source.name):
                return
            entries[virtual] = _ext.ManifestEntry(
                virtual_path=virtual,
                backend_path=source,
                is_dir=False,
            )
            return

        if not source.exists():
            return

        # Minimal cover: only register the target directory (or top-level items if target=".").
        # FUSE layer will read contents dynamically via readdir_from_backend.
        if target != ".":
            entries[target] = _ext.ManifestEntry(
                virtual_path=target,
                backend_path=source,
                is_dir=True,
            )
        else:
            # Merge into root: register top-level items from source
            for item in source.iterdir():
                name = item.name
                if name in SKIP_DIRS or is_excluded(name, is_dir=item.is_dir()):
                    continue
                entries[name] = _ext.ManifestEntry(
                    virtual_path=name,
                    backend_path=item,
                    is_dir=item.is_dir() and not item.is_symlink(),
                )
