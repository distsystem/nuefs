"""NueFS manifest models (nue.yaml)."""

import collections.abc
import pathlib
from typing import Literal

import pydantic
from rich.tree import Tree
from sheaves.console import console
from sheaves.sheaf import Sheaf
from sheaves.typing import Pathspec

import nuefs._nuefs as _ext

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

    def resolve(self, root: pathlib.Path) -> dict[str, _ext.ManifestEntry]:
        """Resolve this mount entry into ManifestEntry mappings."""
        return {
            vpath: _ext.ManifestEntry(
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

        # Trailing slash: expand contents
        for item in source.iterdir():
            is_dir = item.is_dir() and not item.is_symlink()
            name = item.name
            if self._is_excluded(name, is_dir=is_dir):
                continue
            path = item
            if is_dir:
                path, name = self._collapse_chain(path, name)
            vpath = f"{prefix}/{name}" if prefix else name
            yield vpath, path, is_dir


class Manifest(Sheaf, app_name="nue"):
    """NueFS manifest (nue.yaml)."""

    apiVersion: Literal["nue/v1"] = "nue/v1"
    mounts: list[MountEntry] = pydantic.Field(default_factory=list)

    @property
    def root(self) -> pathlib.Path:
        return self.sheaf_source.parent

    def resolve_mounts(
        self,
    ) -> collections.abc.Iterator[tuple[MountEntry, dict[str, _ext.ManifestEntry]]]:
        root = self.root.expanduser().resolve()
        for mount in self.mounts:
            resolved = mount.resolve(root)
            if resolved:
                yield mount, resolved


def print_tree(
    root: pathlib.Path,
    sources: list[tuple[MountEntry, dict[str, _ext.ManifestEntry]]],
    entries: dict[str, _ext.ManifestEntry],
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


def _render_entries(root_node: Tree, entries: dict[str, _ext.ManifestEntry]) -> None:
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
