"""E2E test: CLI mount + uposixtest POSIX conformance."""

import pathlib
import shutil

import pytest

import uposixtest

from tests.nue_mount import mount

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


@pytest.fixture(scope="session")
def fuse_workspace(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    root = tmp_path_factory.mktemp("e2e_cli")
    setup_test_dirs(root)
    (root / "nue.yaml").write_text(YAML_CONTENT)

    with mount(root) as workspace:
        yield workspace


class TestCLIMount:
    def test_uposixtest(self, fuse_workspace: pathlib.Path) -> None:
        rc = uposixtest.run(str(fuse_workspace))
        assert rc == 0
