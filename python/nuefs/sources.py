"""Named source resolution: env override → cached clone → git clone."""

import logging
import os
import pathlib

import pygit2

from nuefs.manifest import Gitnue, Source

logger = logging.getLogger(__name__)


def source_cache_dir(repo_root: pathlib.Path) -> pathlib.Path:
    """Return .git/nue/sources/ path."""
    return repo_root / ".git" / "nue" / "sources"


def resolve_sources(gitnue: Gitnue, repo_root: pathlib.Path) -> dict[str, pathlib.Path]:
    """Resolve all named sources to local paths."""
    result: dict[str, pathlib.Path] = {}
    cache_dir = source_cache_dir(repo_root)

    for name, source in gitnue.sources.items():
        # 1. Env var override: NUE_<UPPER_NAME>=/local/path
        env_key = f"NUE_{name.upper().replace('-', '_')}"
        env_val = os.environ.get(env_key)
        if env_val:
            path = pathlib.Path(env_val).expanduser().resolve()
            logger.info("source %s: env override %s=%s", name, env_key, path)
            result[name] = path
            continue

        # 2. Already cloned
        clone_path = cache_dir / name
        if clone_path.is_dir():
            checkout_ref(clone_path, source.ref)
            logger.info("source %s: cached at %s", name, clone_path)
            result[name] = clone_path
            continue

        # 3. Clone
        result[name] = clone_source(name, source, cache_dir)

    return result


def clone_source(name: str, source: Source, cache_dir: pathlib.Path) -> pathlib.Path:
    """Clone a git source into cache_dir/name."""
    dest = cache_dir / name
    cache_dir.mkdir(parents=True, exist_ok=True)

    logger.info("source %s: cloning %s", name, source.url)
    pygit2.clone_repository(source.url, str(dest))
    checkout_ref(dest, source.ref)
    return dest


def checkout_ref(repo_path: pathlib.Path, ref: str) -> None:
    """Checkout a specific ref."""
    if ref == "HEAD":
        return
    repo = pygit2.Repository(str(repo_path))
    target = repo.revparse_single(ref)
    if target.type == pygit2.GIT_OBJECT_TAG:
        target = target.peel(pygit2.Commit)
    repo.checkout_tree(target)
    repo.set_head(target.id)
