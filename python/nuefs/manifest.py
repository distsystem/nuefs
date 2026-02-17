"""NueFS manifest models (nue.yaml)."""

import collections.abc
import os
import pathlib
import typing
from collections.abc import Iterable, Iterator
from typing import Any, Literal

import pathspec as _pathspec
import pygit2
import pydantic
import rich
import yaml
from rich.tree import Tree

from nuefs.core import ManifestEntry, MountRoot

console = rich.get_console()

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

    def include[P: Pathable](self, paths: Iterable[P]) -> Iterator[P]:
        if not self.root:
            return iter([])
        return (p for p in paths if self.match(p))

    def exclude[P: Pathable](self, paths: Iterable[P]) -> Iterator[P]:
        if not self.root:
            return iter(paths)
        return (p for p in paths if not self.match(p))

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
    vcs: bool = True

    def resolve(self, root: pathlib.Path) -> dict[str, ManifestEntry]:
        """Resolve this mount entry into ManifestEntry mappings."""
        return {
            vpath: ManifestEntry(
                virtual_path=vpath,
                backend_path=path,
                is_dir=is_dir,
            )
            for vpath, path, is_dir in self._iter_entries(root)
        }

    def _is_excluded(self, name: str, *, is_dir: bool = False) -> bool:
        path = f"{name}/" if is_dir else name
        return self.exclude.match(path) and not self.include.match(path)

    def _collapse_chain(
        self,
        dir_path: pathlib.Path,
        rel_name: str,
    ) -> tuple[pathlib.Path, str]:
        """Collapse single-child directory chains into minimal cover prefix."""
        while True:
            dirs: list[pathlib.Path] = []
            has_files = False
            for item in dir_path.iterdir():
                is_dir = item.is_dir() and not item.is_symlink()
                if self._is_excluded(item.name, is_dir=is_dir):
                    continue
                if is_dir:
                    dirs.append(item)
                else:
                    has_files = True
                    break
            if has_files or len(dirs) != 1:
                break
            dir_path = dirs[0]
            rel_name = f"{rel_name}/{dirs[0].name}"
        return dir_path, rel_name

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

    def _git_toplevel_items(
        self, source: pathlib.Path,
    ) -> list[tuple[pathlib.Path, str, bool]] | None:
        """Use gitignore rules to enumerate non-ignored top-level items under *source*.

        Returns None if *source* is not a git repo (caller falls back to
        iterdir).
        """
        try:
            repo = pygit2.Repository(source)
        except pygit2.GitError:
            return None

        items: list[tuple[pathlib.Path, str, bool]] = []
        for item in source.iterdir():
            is_dir = item.is_dir() and not item.is_symlink()
            path_arg = f"{item.name}/" if is_dir else item.name
            if repo.path_is_ignored(path_arg):
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

        # Single file
        if source.is_file():
            vpath = prefix if prefix else source.name
            if not self._is_excluded(vpath):
                yield vpath, source, False
            return

        # Directory without trailing slash: single entry
        if not expand_contents:
            yield prefix, source, True
            return

        # Trailing slash: expand contents – use git index when vcs=True
        git_items = self._git_toplevel_items(source) if self.vcs else None
        if git_items is not None:
            items = git_items
        else:
            items = [
                (item, item.name, item.is_dir() and not item.is_symlink())
                for item in source.iterdir()
                if not self._is_excluded(
                    item.name, is_dir=item.is_dir() and not item.is_symlink(),
                )
            ]

        for path, name, is_dir in items:
            if is_dir:
                path, name = self._collapse_chain(path, name)
            vpath = f"{prefix}/{name}" if prefix else name
            yield vpath, path, is_dir


def ensure_ancestors(
    entries: dict[str, ManifestEntry],
) -> dict[str, ManifestEntry]:
    """Fill in missing ancestor directory entries produced by _collapse_chain."""
    for vpath, entry in list(entries.items()):
        parts = vpath.split("/")
        backend = entry.backend_path
        for depth in range(len(parts) - 1, 0, -1):
            ancestor = "/".join(parts[:depth])
            backend = backend.parent
            if ancestor not in entries:
                entries[ancestor] = ManifestEntry(
                    virtual_path=ancestor, backend_path=backend, is_dir=True,
                )
    return entries


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


def print_tree(
    root: pathlib.Path,
    sources: list[tuple[MountEntry, dict[str, ManifestEntry]]],
    entries: dict[str, ManifestEntry],
) -> None:
    source_tree = Tree(f"[bold blue]{root}[/] [dim](sources)[/]")
    for mount_entry, resolved in sources:
        branch = source_tree.add(f"[bold yellow]{mount_entry.source}[/]")
        _render_entries(branch, resolved)
    console.print(source_tree)
    console.print()

    merged_tree = Tree(f"[bold blue]{root}[/] [dim](merged)[/]")
    _render_entries(merged_tree, entries)
    console.print(merged_tree)


def _render_entries(root_node: Tree, entries: dict[str, ManifestEntry]) -> None:
    nodes: dict[str, Tree] = {"": root_node}

    def _ensure_parent(path: str) -> Tree:
        if path in nodes:
            return nodes[path]
        parent, _, name = path.rpartition("/")
        node = _ensure_parent(parent).add(f"[bold cyan]{name}/[/]")
        nodes[path] = node
        return node

    for entry in sorted(entries.values(), key=lambda e: e.virtual_path):
        parent, _, name = entry.virtual_path.rpartition("/")
        if entry.is_dir:
            label = f"[bold cyan]{name}/[/] [dim]→ {entry.backend_path}[/]"
        else:
            label = f"{name} [dim]→ {entry.backend_path}[/]"
        nodes[entry.virtual_path] = _ensure_parent(parent).add(label)
