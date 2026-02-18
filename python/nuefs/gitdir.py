"""Git worktree helpers for managing .git/info/exclude."""

import pathlib


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


_MARKER_BEGIN = "# --- BEGIN nuefs ---\n"
_MARKER_END = "# --- END nuefs ---\n"


def resolve_gitdir(worktree: pathlib.Path) -> pathlib.Path | None:
    """Resolve .git to the actual gitdir path, handling gitdir: indirection.

    Returns None if worktree has no .git.
    """
    git_path = worktree / ".git"
    if not git_path.exists():
        return None
    if git_path.is_file():
        return _parse_gitdir_file(git_path)
    return git_path


def sync_git_exclude(worktree: pathlib.Path, vpaths: set[str]) -> None:
    """Manage nuefs marker block in .git/info/exclude."""
    gitdir = resolve_gitdir(worktree)
    if gitdir is None:
        return

    info_dir = gitdir / "info"
    exclude_path = info_dir / "exclude"

    if exclude_path.is_file():
        lines = exclude_path.read_text(encoding="utf-8").splitlines(keepends=True)
    else:
        lines = []

    cleaned: list[str] = []
    inside_block = False
    for line in lines:
        if line == _MARKER_BEGIN:
            inside_block = True
            continue
        if line == _MARKER_END:
            inside_block = False
            continue
        if not inside_block:
            cleaned.append(line)

    while cleaned and cleaned[-1].strip() == "":
        cleaned.pop()

    if vpaths:
        if cleaned:
            cleaned.append("\n")
        cleaned.append(_MARKER_BEGIN)
        for vp in sorted(vpaths):
            cleaned.append(f"/{vp}\n")
        cleaned.append(_MARKER_END)

    info_dir.mkdir(parents=True, exist_ok=True)
    exclude_path.write_text("".join(cleaned), encoding="utf-8")
