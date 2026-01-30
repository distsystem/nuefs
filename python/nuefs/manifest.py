"""NueFS manifest models (nue.yaml)."""

import collections.abc
import pathlib
from typing import Literal

import pydantic
from sheaves.sheaf import Sheaf
from sheaves.typing import Pathspec

import nuefs._nuefs as _ext

# Default excludes: caches, build artifacts, VCS directories
DEFAULT_EXCLUDE = Pathspec(
    [".git", ".pixi", "node_modules", "__pycache__", ".venv", "target"]
)


def _collapse_single_child_dirs(
    dir_path: pathlib.Path,
    rel_name: str,
    is_excluded: collections.abc.Callable[[str], bool],
) -> tuple[pathlib.Path, str]:
    """Collapse single-child directory chains into minimal cover prefix.

    Walks down directories that have exactly one non-excluded subdirectory
    (and no non-excluded files), accumulating the relative path.
    """
    while True:
        dirs: list[pathlib.Path] = []
        has_files = False
        for item in dir_path.iterdir():
            is_dir = item.is_dir() and not item.is_symlink()
            if is_excluded(item.name, is_dir=is_dir):
                continue
            if is_dir:
                dirs.append(item)
            else:
                has_files = True
                break  # any file → stop collapsing
        if has_files or len(dirs) != 1:
            break
        dir_path = dirs[0]
        rel_name = f"{rel_name}/{dirs[0].name}"
    return dir_path, rel_name


class MountEntry(pydantic.BaseModel):
    """A single mount entry in the manifest."""

    model_config = pydantic.ConfigDict(extra="forbid")

    source: str
    dest: str = ""
    exclude: Pathspec = pydantic.Field(default=DEFAULT_EXCLUDE)
    include: Pathspec = pydantic.Field(default_factory=Pathspec)

    def resolve(self, root: pathlib.Path) -> dict[str, _ext.ManifestEntry]:
        """Resolve this mount entry into ManifestEntry mappings."""
        raw = self.source.strip()
        expand_contents = raw.endswith("/") or raw in (".", "./")

        source = pathlib.Path(raw).expanduser()
        if not source.is_absolute():
            source = (root / source).resolve()
        else:
            source = source.resolve()

        # rsync-style dest: explicit > auto-derived
        if self.dest:
            prefix = self.dest.strip().strip("/")
        elif expand_contents or source.is_file():
            prefix = ""
        else:
            prefix = source.name

        exclude_spec = self.exclude
        include_spec = self.include

        def is_excluded(rel_path: str, *, is_dir: bool = False) -> bool:
            path = f"{rel_path}/" if is_dir else rel_path
            return exclude_spec.match(path) and not include_spec.match(path)

        entries: dict[str, _ext.ManifestEntry] = {}

        if not source.exists():
            return entries

        # Single file: dest acts as full virtual path (rename)
        if source.is_file():
            vpath = prefix if prefix else source.name
            if not is_excluded(vpath):
                entries[vpath] = _ext.ManifestEntry(
                    virtual_path=vpath,
                    backend_path=source,
                    is_dir=False,
                )
            return entries

        # Directory without trailing slash: register as single entry
        if not expand_contents:
            vpath = prefix  # always non-empty (auto-derived = basename)
            entries[vpath] = _ext.ManifestEntry(
                virtual_path=vpath,
                backend_path=source,
                is_dir=True,
            )
            return entries

        # Trailing slash: expand contents into dest prefix
        items = [
            (item.name, item, item.is_dir() and not item.is_symlink())
            for item in source.iterdir()
        ]

        for name, path, is_dir in items:
            if is_excluded(name, is_dir=is_dir):
                continue
            if is_dir:
                path, name = _collapse_single_child_dirs(
                    path, name, is_excluded
                )
            vpath = f"{prefix}/{name}" if prefix else name
            entries[vpath] = _ext.ManifestEntry(
                virtual_path=vpath,
                backend_path=path,
                is_dir=is_dir,
            )

        return entries


class Manifest(Sheaf, app_name="nue"):
    """NueFS manifest (nue.yaml)."""

    apiVersion: Literal["nue/v1"] = "nue/v1"
    mounts: list[MountEntry] = pydantic.Field(default_factory=list)
