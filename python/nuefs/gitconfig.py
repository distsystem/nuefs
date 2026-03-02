"""Git config reader for nue.* settings."""

import logging
import pathlib

import pydantic
import pygit2

logger = logging.getLogger(__name__)


class DevConfig(pydantic.BaseModel):
    """nue.dev.* settings (lazy.nvim-style local dev override)."""

    path: pathlib.Path
    fallback: bool = True


class NueGitConfig(pydantic.BaseModel):
    """Parsed nue.* git config section."""

    dev: DevConfig | None = None

    @classmethod
    def from_repo(cls, repo_root: pathlib.Path) -> "NueGitConfig":
        raw = _load_gitconfig(repo_root)
        if not raw:
            return cls()
        return cls.model_validate(raw)


def _load_gitconfig(repo_root: pathlib.Path) -> dict:
    """One-pass scan of nue.* git config entries → nested dict."""
    try:
        config = pygit2.Repository(str(repo_root)).config
    except pygit2.GitError:
        return {}

    nested: dict = {}
    for entry in config:
        if not entry.name.startswith("nue."):
            continue
        parts = entry.name.split(".")[1:]  # strip leading "nue"
        d = nested
        for part in parts[:-1]:
            d = d.setdefault(part, {})
        d[parts[-1]] = entry.value

    return nested
