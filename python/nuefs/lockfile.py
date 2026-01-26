"""NueFS lockfile (nue.lock).

This is the compiled, machine-oriented snapshot of a mount's resolved union view.
The lockfile is the single source of truth for what gets sent to the daemon.
"""

import hashlib
import pathlib
import time
import typing

import pydantic
from sheaves.sheaf import Sheaf

import nuefs._nuefs as _ext


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
            for path in root.rglob("*"):
                if ".git" in path.parts:
                    continue
                rel = str(path.relative_to(root))
                entries[rel] = _ext.ManifestEntry(
                    virtual_path=rel,
                    backend_path=path,
                    is_dir=path.is_dir(),
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

        for path in source.rglob("*"):
            if not include_git and ".git" in path.parts:
                continue

            rel = path.relative_to(source)
            virtual = (
                str(rel) if target == "." else str(pathlib.PurePosixPath(target) / rel)
            )

            if not include_git and (virtual == ".git" or virtual.startswith(".git/")):
                continue

            if virtual in entries:
                if entries[virtual].is_dir and path.is_dir():
                    continue

            entries[virtual] = _ext.ManifestEntry(
                virtual_path=virtual,
                backend_path=path,
                is_dir=path.is_dir(),
            )
