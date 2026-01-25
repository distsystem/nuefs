"""Git worktree helpers.

This module implements the minimal logic needed to keep Git's heavy `.git/**` IO
outside the FUSE mountpoint by using Git's `gitdir:` indirection.
"""

import hashlib
import os
import pathlib
import shutil


_DEFAULT_GITDIR_ROOT = pathlib.Path("~/.local/share/nuefs/gitdirs")


def default_gitdir_root() -> pathlib.Path:
    """Return the default external gitdir root.

    Controlled by `NUEFS_GITDIR_ROOT` if set.
    """
    env = os.environ.get("NUEFS_GITDIR_ROOT")
    if env:
        return pathlib.Path(env).expanduser().resolve()
    return _DEFAULT_GITDIR_ROOT.expanduser().resolve()


def _stable_id(worktree: pathlib.Path) -> str:
    worktree = worktree.expanduser().resolve()
    digest = hashlib.sha256(str(worktree).encode("utf-8")).hexdigest()
    return digest[:16]


def _parse_gitdir_file(path: pathlib.Path) -> pathlib.Path:
    data = path.read_text(encoding="utf-8", errors="strict").strip()
    prefix = "gitdir:"
    if not data.startswith(prefix):
        raise ValueError(".git file does not start with 'gitdir:'")
    raw = data[len(prefix) :].strip()
    if not raw:
        raise ValueError(".git file has empty gitdir target")
    gitdir = pathlib.Path(raw)
    if not gitdir.is_absolute():
        gitdir = (path.parent / gitdir).resolve()
    return gitdir


def ensure_external_gitdir(
    worktree: pathlib.Path, gitdir_root: pathlib.Path
) -> pathlib.Path:
    """Ensure worktree/.git is a gitdir file pointing outside worktree.

    If worktree/.git is a directory, it is moved to an external location and
    replaced by a `gitdir:` file.

    Returns the resolved external gitdir path.
    """
    worktree = worktree.expanduser().resolve()
    gitdir_root = gitdir_root.expanduser().resolve()

    if os.path.ismount(worktree):
        raise RuntimeError("refusing to rewrite .git under a mountpoint")

    git_path = worktree / ".git"
    if not git_path.exists():
        raise FileNotFoundError("worktree has no .git")

    if git_path.is_file():
        external = _parse_gitdir_file(git_path)
        try:
            external.relative_to(worktree)
        except ValueError:
            return external
        raise ValueError(".git points inside the worktree; expected external gitdir")

    if not git_path.is_dir():
        raise RuntimeError(".git is neither file nor directory")

    external = gitdir_root / _stable_id(worktree)
    external_parent = external.parent
    external_parent.mkdir(parents=True, exist_ok=True)

    if external.exists():
        # If the external path already exists, do not guess intent.
        raise FileExistsError(f"external gitdir already exists: {external}")

    shutil.move(str(git_path), str(external))
    git_path.write_text(f"gitdir: {external}\n", encoding="utf-8")
    return external
