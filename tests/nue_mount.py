"""Shared nuefs mount lifecycle for tests and benchmarks."""

import contextlib
import os
import pathlib
import subprocess
import time
from collections.abc import Iterator

from nuefs.cli import _lazy_unmount as lazy_unmount


def wait_for_mount(path: pathlib.Path, *, timeout_s: float = 5.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = subprocess.run(
            ["findmnt", "-T", os.fspath(path)],
            check=False, capture_output=True, text=True,
        )
        if "nuefs" in r.stdout and "fuse" in r.stdout:
            return
        time.sleep(0.05)
    msg = f"mount did not become ready within {timeout_s}s: {path}"
    raise RuntimeError(msg)


@contextlib.contextmanager
def mount(root: pathlib.Path) -> Iterator[pathlib.Path]:
    """Mount nuefs at *root* (expects .gitnue), unmount on exit."""
    subprocess.run(
        ["git-nue", "mount", "-m", str(root / ".gitnue")],
        check=True, timeout=15,
    )
    wait_for_mount(root)
    try:
        yield root
    finally:
        lazy_unmount(root)
