"""NueFS manifest models (nue.yaml)."""

import collections.abc
import os
import pathlib
import typing
from typing import Any, Literal

import pathspec as _pathspec
import pygit2
import pydantic
import yaml

from nuefs.core import ManifestEntry, MountRoot

type Pathable = str | os.PathLike[str]


class Pathspec(pydantic.RootModel[list[str]]):
    """Gitignore-style pattern list with matching capabilities."""

    root: list[str] = pydantic.Field(default_factory=list)
    _spec: _pathspec.PathSpec = pydantic.PrivateAttr()

    def model_post_init(self, __context: Any) -> None:
        self._spec = _pathspec.PathSpec.from_lines("gitignore", self.root)

    def __len__(self) -> int:
        return len(self.root)

    def match(self, path: Pathable) -> bool:
        if not self.root:
            return False
        return bool(self._spec.match_file(str(path)))

# Default excludes: caches, build artifacts, VCS directories
DEFAULT_EXCLUDE = Pathspec(
    [".git", ".pixi", "node_modules", "__pycache__", ".venv", "target"]
)


class MountEntry(pydantic.BaseModel):
    """A single mount entry in the manifest."""

    model_config = pydantic.ConfigDict(extra="forbid")

    source: str
    dest: str = ""
    exclude: Pathspec = pydantic.Field(default=DEFAULT_EXCLUDE)
    include: Pathspec = pydantic.Field(default_factory=Pathspec)
    gitignore: bool = True

    def resolve(self, root: pathlib.Path) -> dict[str, ManifestEntry]:
        """Resolve this mount entry into ManifestEntry mappings."""
        return {
            str(pathlib.PurePosixPath(vpath)): ManifestEntry(
                virtual_path=pathlib.PurePosixPath(vpath),
                backend_path=path,
                is_dir=is_dir,
            )
            for vpath, path, is_dir in self._iter_entries(root)
        }

    def mount_root(self, root: pathlib.Path) -> MountRoot:
        """Return the MountRoot for this entry resolved against *root*."""
        source, prefix, _ = self._resolve_source(root)
        return MountRoot(virtual_prefix=pathlib.PurePosixPath(prefix), backend_path=source)

    def _is_excluded(self, name: str, *, is_dir: bool = False) -> bool:
        path = f"{name}/" if is_dir else name
        return self.exclude.match(path) and not self.include.match(path)

    def _resolve_source(
        self,
        root: pathlib.Path,
    ) -> tuple[pathlib.Path, str, bool]:
        """Return (resolved_source, prefix, expand_contents)."""
        raw = self.source.strip()
        expand_contents = raw.endswith("/") or raw in (".", "./")

        source = pathlib.Path(raw).expanduser()
        if not source.is_absolute():
            source = (root / source).resolve()
        else:
            source = source.resolve()

        if self.dest:
            prefix = self.dest.strip().strip("/")
        elif expand_contents or source.is_file():
            prefix = ""
        else:
            prefix = source.name

        return source, prefix, expand_contents

    def _list_items(
        self, source: pathlib.Path,
    ) -> list[tuple[pathlib.Path, str, bool]]:
        """List non-excluded top-level items under *source*, respecting gitignore when gitignore=True."""
        repo: pygit2.Repository | None = None
        if self.gitignore:
            try:
                repo = pygit2.Repository(source)
            except pygit2.GitError:
                pass

        items: list[tuple[pathlib.Path, str, bool]] = []
        for item in source.iterdir():
            is_dir = item.is_dir() and not item.is_symlink()
            path_arg = f"{item.name}/" if is_dir else item.name
            if repo is not None and repo.path_is_ignored(path_arg):
                continue
            if not self._is_excluded(item.name, is_dir=is_dir):
                items.append((item, item.name, is_dir))
        return items

    def _iter_entries(
        self,
        root: pathlib.Path,
    ) -> collections.abc.Iterator[tuple[str, pathlib.Path, bool]]:
        """Yield (vpath, backend_path, is_dir) for all resolved entries."""
        source, prefix, expand_contents = self._resolve_source(root)

        if not source.exists():
            return

        if source.is_file():
            vpath = prefix if prefix else source.name
            if not self._is_excluded(vpath):
                yield vpath, source, False
            return

        if not expand_contents:
            yield prefix, source, True
            return

        for path, name, is_dir in self._list_items(source):
            vpath = f"{prefix}/{name}" if prefix else name
            yield vpath, path, is_dir


class Manifest(pydantic.BaseModel):
    """NueFS manifest (nue.yaml)."""

    apiVersion: Literal["nue/v1"] = "nue/v1"
    mounts: list[MountEntry] = pydantic.Field(default_factory=list)

    @classmethod
    def load(
        cls, path: pathlib.Path = pathlib.Path("nue.yaml"),
    ) -> tuple[typing.Self, pathlib.Path]:
        resolved = path.expanduser().resolve()
        if resolved.is_dir():
            resolved = resolved / "nue.yaml"
        data = yaml.safe_load(resolved.read_text()) or {}
        return cls.model_validate(data), resolved.parent

    def resolve_mounts(
        self, root: pathlib.Path,
    ) -> collections.abc.Iterator[tuple[MountEntry, dict[str, ManifestEntry]]]:
        root = root.expanduser().resolve()
        for mount in self.mounts:
            resolved = mount.resolve(root)
            if resolved:
                yield mount, resolved
