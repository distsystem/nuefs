"""NueFS manifest models (.gitnue)."""

import collections.abc
import logging
import os
import pathlib
import typing
from typing import Annotated, Any, Literal, Self

import pathspec as _pathspec
import pydantic
import pygit2
import yaml
from pydantic import Discriminator, Field, Tag

from nuefs.core import ManifestEntry, MountRoot
from nuefs.gitconfig import NueGitConfig

logger = logging.getLogger(__name__)

class Pathspec(pydantic.RootModel[list[str]]):
    """Gitignore-style pattern list with matching capabilities."""

    root: list[str] = pydantic.Field(default_factory=list)
    _spec: _pathspec.PathSpec = pydantic.PrivateAttr()

    def model_post_init(self, __context: Any) -> None:
        self._spec = _pathspec.PathSpec.from_lines("gitignore", self.root)

    def match(self, path: str | os.PathLike[str]) -> bool:
        if not self.root:
            return False
        return bool(self._spec.match_file(str(path)))

# Default excludes: caches, build artifacts, VCS directories
DEFAULT_EXCLUDE = Pathspec(
    [".git", ".pixi", "node_modules", "__pycache__", ".venv", "target"]
)


# ---------------------------------------------------------------------------
# Source types (discriminated union)
# ---------------------------------------------------------------------------


class PathSource(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    path: pathlib.Path

    def resolve(self, repo_root: pathlib.Path) -> pathlib.Path:
        p = self.path.expanduser()
        if not p.is_absolute():
            return (repo_root / p).resolve()
        return p.resolve()


class GitSource(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    url: str
    ref: str = "HEAD"

    def resolve(self, name: str, cache_dir: pathlib.Path) -> pathlib.Path:
        clone_path = cache_dir / name
        if clone_path.is_dir():
            self._checkout(clone_path)
            logger.info("source %s: cached at %s", name, clone_path)
            return clone_path
        return self._clone(name, cache_dir)

    def _checkout(self, repo_path: pathlib.Path) -> None:
        if self.ref == "HEAD":
            return
        repo = pygit2.Repository(str(repo_path))
        target = repo.revparse_single(self.ref)
        if target.type == pygit2.GIT_OBJECT_TAG:
            target = target.peel(pygit2.Commit)
        repo.checkout_tree(target)
        repo.set_head(target.id)

    def _clone(self, name: str, cache_dir: pathlib.Path) -> pathlib.Path:
        dest = cache_dir / name
        cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info("source %s: cloning %s", name, self.url)
        pygit2.clone_repository(self.url, str(dest))
        self._checkout(dest)
        return dest


def _source_discriminator(v: Any) -> str:
    if isinstance(v, dict):
        return "path" if "path" in v else "git"
    return "path" if isinstance(v, PathSource) else "git"


type Source = Annotated[
    Annotated[PathSource, Tag("path")] | Annotated[GitSource, Tag("git")],
    Discriminator(_source_discriminator),
]


# ---------------------------------------------------------------------------
# MountEntry
# ---------------------------------------------------------------------------


class MountEntry(pydantic.BaseModel):
    """A single mount entry in the manifest."""

    model_config = pydantic.ConfigDict(extra="forbid")

    source: str
    root: str = ""
    prefix: str = ""
    exclude: Pathspec = Field(default=DEFAULT_EXCLUDE)
    include: Pathspec = Field(default_factory=Pathspec)
    gitignore: bool = True

    def resolve(self, source_path: pathlib.Path) -> dict[str, ManifestEntry]:
        source, prefix = self._apply_transform(source_path)

        if not source.exists():
            return {}

        # Single file
        if source.is_file():
            vpath = prefix if prefix else source.name
            if self.exclude.match(vpath) and not self.include.match(vpath):
                return {}
            vp = pathlib.PurePosixPath(vpath)
            return {str(vp): ManifestEntry(virtual_path=vp, backend_path=source, is_dir=False)}

        # Directory
        repo: pygit2.Repository | None = None
        if self.gitignore:
            try:
                repo = pygit2.Repository(source)
            except pygit2.GitError:
                pass

        entries: dict[str, ManifestEntry] = {}
        for item in source.iterdir():
            is_dir = item.is_dir() and not item.is_symlink()
            path_arg = f"{item.name}/" if is_dir else item.name
            if repo is not None and repo.path_is_ignored(path_arg):
                continue
            if self.exclude.match(path_arg) and not self.include.match(path_arg):
                continue
            vpath = f"{prefix}/{item.name}" if prefix else item.name
            vp = pathlib.PurePosixPath(vpath)
            entries[str(vp)] = ManifestEntry(virtual_path=vp, backend_path=item, is_dir=is_dir)
        return entries

    def mount_root(self, source_path: pathlib.Path) -> MountRoot:
        source, prefix = self._apply_transform(source_path)
        return MountRoot(virtual_prefix=pathlib.PurePosixPath(prefix), backend_path=source)

    def _apply_transform(
        self, source_path: pathlib.Path,
    ) -> tuple[pathlib.Path, str]:
        source = source_path
        if self.root:
            source = source / self.root.strip().strip("/")
        prefix = self.prefix.strip().strip("/") if self.prefix else ""
        return source, prefix


# ---------------------------------------------------------------------------
# Gitnue (top-level manifest)
# ---------------------------------------------------------------------------


class Gitnue(pydantic.BaseModel):
    """NueFS manifest (.gitnue)."""

    version: Literal[1] = 1
    sources: dict[str, Source] = Field(default_factory=dict)
    mounts: list[MountEntry] = Field(default_factory=list)

    @pydantic.model_validator(mode="after")
    def _validate_mount_sources(self) -> Self:
        known = set(self.sources)
        for mount in self.mounts:
            if mount.source not in known:
                msg = f"unknown source: {mount.source!r}"
                raise ValueError(msg)
        return self

    @classmethod
    def load(
        cls, path: pathlib.Path = pathlib.Path(".gitnue"),
    ) -> tuple[typing.Self, pathlib.Path]:
        resolved = path.expanduser().resolve()
        if resolved.is_dir():
            resolved = resolved / ".gitnue"
        data = yaml.safe_load(resolved.read_text()) or {}
        return cls.model_validate(data), resolved.parent

    def resolve_sources(
        self,
        repo_root: pathlib.Path,
        config: NueGitConfig | None = None,
    ) -> dict[str, pathlib.Path]:
        """Resolve all named sources: dev path -> env override -> cached clone -> git clone."""
        cache_dir = repo_root / ".git" / "nue" / "sources"
        result: dict[str, pathlib.Path] = {}

        for name, source in self.sources.items():
            # 1. Dev path override (lazy.nvim-style)
            if config and config.dev:
                dev_dir = config.dev.path / name
                if dev_dir.is_dir():
                    logger.info("source %s: dev override %s", name, dev_dir)
                    result[name] = dev_dir
                    continue
                if not config.dev.fallback:
                    msg = f"dev source {name!r} not found at {dev_dir} and fallback=false"
                    raise FileNotFoundError(msg)

            # 2. Env var override: NUE_<UPPER_NAME>=/local/path
            env_key = f"NUE_{name.upper().replace('-', '_')}"
            env_val = os.environ.get(env_key)
            if env_val:
                path = pathlib.Path(env_val).expanduser().resolve()
                logger.info("source %s: env override %s=%s", name, env_key, path)
                result[name] = path
                continue

            # 3. Resolve by source type
            match source:
                case GitSource():
                    result[name] = source.resolve(name, cache_dir)
                case PathSource():
                    result[name] = source.resolve(repo_root)

        return result

    def resolve_mounts(
        self,
        resolved: dict[str, pathlib.Path],
    ) -> collections.abc.Iterator[tuple[MountEntry, dict[str, ManifestEntry]]]:
        for mount in self.mounts:
            source_path = resolved[mount.source]
            entries = mount.resolve(source_path)
            if entries:
                yield mount, entries
