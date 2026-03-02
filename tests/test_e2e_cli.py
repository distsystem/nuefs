"""E2E test: CLI mount + uposixtest POSIX conformance + git-on-FUSE."""

import pathlib
import shutil

import pygit2
import pytest

import uposixtest

from tests.nue_mount import mount

pytestmark = [
    pytest.mark.skipif(not pathlib.Path("/dev/fuse").exists(), reason="no /dev/fuse"),
    pytest.mark.skipif(shutil.which("nuefsd") is None, reason="nuefsd not in PATH"),
]

GITNUE_CONTENT = """\
version: 1
sources:
  project-a:
    path: ./project-a/
  libs:
    path: ./libs/
mounts:
- source: project-a
  exclude:
    - __pycache__
  gitignore: false
- source: libs
  to: vendor
  gitignore: false
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
    (root / ".gitnue").write_text(GITNUE_CONTENT)

    with mount(root) as workspace:
        yield workspace


class TestCLIMount:
    def test_uposixtest(self, fuse_workspace: pathlib.Path) -> None:
        rc = uposixtest.run(str(fuse_workspace))
        assert rc == 0


# ---------------------------------------------------------------------------
# Git-on-FUSE tests
# ---------------------------------------------------------------------------

GIT_GITNUE = """\
version: 1
sources:
  external:
    path: ./external/
mounts:
- source: external
  to: vendor
  gitignore: false
"""

_SIG = pygit2.Signature("Test", "test@test.com")


def _setup_git_workspace(root: pathlib.Path) -> None:
    (root / "external").mkdir()
    (root / "external" / "lib.py").write_text("# lib")

    repo = pygit2.init_repository(str(root))
    (root / "README.md").write_text("# test project")
    repo.index.add("README.md")
    repo.index.write()
    tree = repo.index.write_tree()
    repo.create_commit("HEAD", _SIG, _SIG, "initial", tree, [])


@pytest.fixture(scope="session")
def git_workspace(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    root = tmp_path_factory.mktemp("git_fuse")
    _setup_git_workspace(root)
    (root / ".gitnue").write_text(GIT_GITNUE)
    with mount(root) as ws:
        yield ws


class TestGitOnFuse:
    def test_open_and_read_head(self, git_workspace: pathlib.Path) -> None:
        repo = pygit2.Repository(str(git_workspace))
        assert not repo.is_bare
        commit = repo.head.peel(pygit2.Commit)
        assert commit.message == "initial"

    def test_virtual_files_readable_but_git_excluded(
        self, git_workspace: pathlib.Path,
    ) -> None:
        # Virtual file is accessible through FUSE
        assert (git_workspace / "vendor" / "lib.py").read_text() == "# lib"
        # But excluded from git status by sync_git_exclude
        repo = pygit2.Repository(str(git_workspace))
        st = repo.status()
        assert not any(p.startswith("vendor/") for p in st)

    def test_modify_commit_read(self, git_workspace: pathlib.Path) -> None:
        repo = pygit2.Repository(str(git_workspace))
        (git_workspace / "README.md").write_text("# updated")

        st = repo.status()
        assert st["README.md"] & pygit2.GIT_STATUS_WT_MODIFIED

        repo.index.add("README.md")
        repo.index.write()
        tree = repo.index.write_tree()
        parent = repo.head.peel(pygit2.Commit)
        repo.create_commit("HEAD", _SIG, _SIG, "update readme", tree, [parent.id])

        blob = repo[repo.head.peel(pygit2.Commit).tree["README.md"].id]
        assert blob.data == b"# updated"

    def test_remove_commit(self, git_workspace: pathlib.Path) -> None:
        repo = pygit2.Repository(str(git_workspace))
        # Create and commit a file first
        (git_workspace / "tmp.txt").write_text("delete me")
        repo.index.add("tmp.txt")
        repo.index.write()
        tree = repo.index.write_tree()
        parent = repo.head.peel(pygit2.Commit)
        repo.create_commit("HEAD", _SIG, _SIG, "add tmp", tree, [parent.id])

        # Delete through FUSE and commit the removal
        (git_workspace / "tmp.txt").unlink()
        repo.index.remove("tmp.txt")
        repo.index.write()
        tree = repo.index.write_tree()
        parent = repo.head.peel(pygit2.Commit)
        repo.create_commit("HEAD", _SIG, _SIG, "remove tmp", tree, [parent.id])

        commit = repo.head.peel(pygit2.Commit)
        assert commit.message == "remove tmp"
        assert "tmp.txt" not in [e.name for e in commit.tree]

    def test_add_commit_read(self, git_workspace: pathlib.Path) -> None:
        repo = pygit2.Repository(str(git_workspace))
        (git_workspace / "hello.py").write_text("print('hello')")

        repo.index.add("hello.py")
        repo.index.write()
        tree = repo.index.write_tree()
        parent = repo.head.peel(pygit2.Commit)
        repo.create_commit("HEAD", _SIG, _SIG, "add hello", tree, [parent.id])

        commit = repo.head.peel(pygit2.Commit)
        assert commit.message == "add hello"
        blob = repo[commit.tree["hello.py"].id]
        assert blob.data == b"print('hello')"
