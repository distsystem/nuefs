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

import nuefs._nuefs as _ext

# Directories to skip during scan (caches, build artifacts)
SKIP_DIRS = {".git", ".pixi", "node_modules", "__pycache__", ".venv", "target"}


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
        mounts: typing.Iterable[tuple[pathlib.Path, str]] = (),
        *,
        include_real: bool = True,
        nuefs_version: str | None = None,
        manifest_path: str = "nue.yaml",
    ) -> "Lock":
        root = root.expanduser().resolve()

        entries: dict[str, _ext.ManifestEntry] = {}

        if include_real and root.exists():
            for dirpath, dirnames, filenames in os.walk(root):
                # Prune skip dirs in-place to avoid descending
                dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]

                dp = pathlib.Path(dirpath)
                for name in dirnames:
                    path = dp / name
                    rel = str(path.relative_to(root))
                    entries[rel] = _ext.ManifestEntry(
                        virtual_path=rel,
                        backend_path=path,
                        is_dir=True,
                    )
                for name in filenames:
                    path = dp / name
                    try:
                        is_dir = path.is_dir()
                    except (PermissionError, OSError):
                        continue
                    rel = str(path.relative_to(root))
                    entries[rel] = _ext.ManifestEntry(
                        virtual_path=rel,
                        backend_path=path,
                        is_dir=is_dir,
                    )

        for source, target in mounts:
            cls._apply_layer(entries, source, target)

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
    ) -> None:
        source = source.expanduser().resolve()

        target = target.strip() if target else "."
        if target == "":
            target = "."

        include_git = target == ".git" or target.startswith(".git/")

        if source.is_file():
            virtual = target if target != "." else source.name
            entries[virtual] = _ext.ManifestEntry(
                virtual_path=virtual,
                backend_path=source,
                is_dir=False,
            )
            return

        if not source.exists():
            return

        if target != ".":
            if target not in entries or not entries[target].is_dir:
                entries[target] = _ext.ManifestEntry(
                    virtual_path=target,
                    backend_path=source,
                    is_dir=True,
                )

        skip_dirs = SKIP_DIRS if not include_git else SKIP_DIRS - {".git"}

        for dirpath, dirnames, filenames in os.walk(source):
            # Prune skip dirs in-place
            dirnames[:] = [d for d in dirnames if d not in skip_dirs]

            dp = pathlib.Path(dirpath)
            rel_base = dp.relative_to(source)

            for name in dirnames:
                path = dp / name
                rel = rel_base / name
                virtual = (
                    str(rel) if target == "." else str(pathlib.PurePosixPath(target) / rel)
                )
                if virtual not in entries or not entries[virtual].is_dir:
                    entries[virtual] = _ext.ManifestEntry(
                        virtual_path=virtual,
                        backend_path=path,
                        is_dir=True,
                    )

            for name in filenames:
                path = dp / name
                try:
                    is_dir = path.is_dir()
                except (PermissionError, OSError):
                    continue
                rel = rel_base / name
                virtual = (
                    str(rel) if target == "." else str(pathlib.PurePosixPath(target) / rel)
                )
                if virtual in entries and entries[virtual].is_dir and is_dir:
                    continue
                entries[virtual] = _ext.ManifestEntry(
                    virtual_path=virtual,
                    backend_path=path,
                    is_dir=is_dir,
                )
