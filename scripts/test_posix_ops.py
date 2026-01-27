"""POSIX smoke tests for NueFS mount behavior.

This script is meant for manual/CI-style validation of common operations
through the mounted union view (create, read/write, rename, unlink, mkdir,
rmdir, chmod, utime).
"""

import os
import pathlib
import subprocess
import sys
import time


REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
WORKSPACE_ROOT = pathlib.Path("/tmp/nue-test")
WORKSPACE = WORKSPACE_ROOT / "workspace"
SOURCES = WORKSPACE_ROOT / "sources"


def _run(argv: list[str], *, cwd: pathlib.Path | None = None) -> None:
    subprocess.run(argv, cwd=cwd, check=True)


def _run_capture(argv: list[str]) -> str:
    p = subprocess.run(
        argv, check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    return p.stdout


def _wait_for_mount(path: pathlib.Path, *, timeout_s: float = 5.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        out = _run_capture(["findmnt", "-T", os.fspath(path)])
        if "nuefs" in out and "fuse" in out:
            return
        time.sleep(0.05)
    msg = f"mount did not become ready: {path}"
    raise RuntimeError(msg)


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def main() -> int:
    _run(["pixi", "run", "nue", "stop"], cwd=REPO_ROOT)

    try:
        _run(
            [
                sys.executable,
                "scripts/setup_test_workspace.py",
                "--fixture",
                "nue.yaml",
            ],
            cwd=REPO_ROOT,
        )

        _run(["pixi", "run", "nue", "mount"], cwd=WORKSPACE)
        _wait_for_mount(WORKSPACE)

        # If we mounted while already inside the mountpoint, re-enter it so the
        # process sees the mounted view.
        os.chdir("/")
        os.chdir(WORKSPACE)

        # Sanity: overlay directories exist.
        _assert((WORKSPACE / "src").is_dir(), "missing src/ in mounted view")
        _assert((WORKSPACE / "vendor").is_dir(), "missing vendor/ in mounted view")

        # 1) touch create + stat through mount (layer-backed directory).
        union_new = WORKSPACE / "vendor" / "posix_touch.txt"
        src_new = SOURCES / "libs" / "posix_touch.txt"
        src_new.unlink(missing_ok=True)
        union_new.unlink(missing_ok=True)

        _run(["touch", os.fspath(union_new)])
        _assert(
            union_new.exists(), "touch created file but it is not visible through mount"
        )
        _assert(src_new.exists(), "touch created file but it did not land in sources")
        os.stat(union_new)

        # 2) write-through
        union_new.write_text("hello\n", encoding="utf-8")
        _assert(
            src_new.read_text(encoding="utf-8") == "hello\n", "write-through mismatch"
        )

        # 3) utime (touch existing file should update mtime)
        before = union_new.stat().st_mtime_ns
        time.sleep(0.01)
        _run(["touch", os.fspath(union_new)])
        after = union_new.stat().st_mtime_ns
        _assert(after > before, "touch did not update mtime")

        # 4) rename
        union_renamed = WORKSPACE / "vendor" / "posix_renamed.txt"
        src_renamed = SOURCES / "libs" / "posix_renamed.txt"
        union_renamed.unlink(missing_ok=True)
        src_renamed.unlink(missing_ok=True)
        os.rename(union_new, union_renamed)
        _assert(union_renamed.exists(), "rename result missing in mount")
        _assert(src_renamed.exists(), "rename result missing in sources")
        _assert(not src_new.exists(), "rename left old source path behind")

        # 5) chmod
        os.chmod(union_renamed, 0o600)
        _assert(
            (src_renamed.stat().st_mode & 0o777) == 0o600, "chmod did not propagate"
        )

        # 6) unlink
        os.unlink(union_renamed)
        _assert(not src_renamed.exists(), "unlink did not remove source file")

        # 7) mkdir + rmdir (layer-backed directory)
        union_dir = WORKSPACE / "src" / "posix_dir"
        src_dir = SOURCES / "project-a" / "src" / "posix_dir"
        if src_dir.exists():
            for p in sorted(src_dir.rglob("*"), reverse=True):
                if p.is_file():
                    p.unlink()
                else:
                    p.rmdir()
            src_dir.rmdir()
        if union_dir.exists():
            union_dir.rmdir()

        os.mkdir(union_dir)
        _assert(union_dir.is_dir(), "mkdir result missing in mount")
        _assert(src_dir.is_dir(), "mkdir result missing in sources")
        os.rmdir(union_dir)
        _assert(not src_dir.exists(), "rmdir did not remove source dir")

        print("OK: posix operations behave as expected")
        return 0
    finally:
        _run(["pixi", "run", "nue", "stop"], cwd=REPO_ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
