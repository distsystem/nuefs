"""E2E test: CLI mount + uposixtest POSIX conformance."""

import os
import pathlib
import shutil
import subprocess
import time

import pytest

import uposixtest

pytestmark = [
    pytest.mark.skipif(not pathlib.Path("/dev/fuse").exists(), reason="no /dev/fuse"),
    pytest.mark.skipif(shutil.which("nuefsd") is None, reason="nuefsd not in PATH"),
]

YAML_CONTENT = """\
apiVersion: nue/v1
mounts:
- source: ./project-a/
  exclude:
    - __pycache__
  vcs: false
- source: ./libs/
  dest: vendor
  vcs: false
"""


def setup_test_dirs(root: pathlib.Path) -> None:
    (root / "project-a" / "lib" / "deep").mkdir(parents=True)
    (root / "project-a" / "lib" / "deep" / "mod.py").write_text("# mod")
    (root / "project-a" / "main.py").write_text("# main")
    (root / "project-a" / "__pycache__").mkdir()
    (root / "project-a" / "__pycache__" / "main.cpython-312.pyc").write_bytes(b"\x00")

    (root / "libs" / "helpers").mkdir(parents=True)
    (root / "libs" / "utils.py").write_text("# utils")
    (root / "libs" / "helpers" / "fmt.py").write_text("# fmt")


def _wait_for_mount(path: pathlib.Path, *, timeout_s: float = 5.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        result = subprocess.run(
            ["findmnt", "-T", os.fspath(path)],
            check=False,
            capture_output=True,
            text=True,
        )
        if "nuefs" in result.stdout and "fuse" in result.stdout:
            return
        time.sleep(0.05)
    msg = f"mount did not become ready within {timeout_s}s: {path}"
    raise RuntimeError(msg)


def _lazy_unmount(root: pathlib.Path) -> None:
    for cmd in ("fusermount3", "fusermount"):
        try:
            subprocess.run(
                [cmd, "-uz", str(root)],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return
        except (FileNotFoundError, subprocess.CalledProcessError):
            continue


@pytest.fixture(scope="session")
def fuse_workspace(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    root = tmp_path_factory.mktemp("e2e_cli")
    setup_test_dirs(root)
    (root / "nue.yaml").write_text(YAML_CONTENT)

    subprocess.run(
        ["nue", "mount", "-c", str(root / "nue.yaml")],
        check=True,
        timeout=15,
    )
    _wait_for_mount(root)

    yield root

    _lazy_unmount(root)


class TestCLIMount:
    def test_uposixtest(self, fuse_workspace: pathlib.Path) -> None:
        rc = uposixtest.run(str(fuse_workspace))
        assert rc == 0
