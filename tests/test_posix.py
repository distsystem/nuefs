"""POSIX semantics compatibility tests for NueFS FUSE mount."""

import os
import pathlib
import shutil
import stat
import subprocess
import time

import pytest

import nuefs
import nuefs._nuefs as _ext
from nuefs.manifest import Manifest, MountEntry, ensure_ancestors

from conftest import YAML_CONTENT, setup_test_dirs

pytestmark = [
    pytest.mark.skipif(not pathlib.Path("/dev/fuse").exists(), reason="no /dev/fuse"),
    pytest.mark.skipif(shutil.which("nuefsd") is None, reason="nuefsd not in PATH"),
]


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


class TestPosixSemantics:
    """POSIX operations on a live FUSE mount."""

    @pytest.fixture(scope="class")
    def fuse_mount(
        self,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> (
        tuple[pathlib.Path, dict[str, _ext.ManifestEntry], list[_ext.MountRoot], nuefs.Handle]
    ):
        root = setup_test_dirs(tmp_path_factory.mktemp("posix"))
        (root / "nue.yaml").write_text(YAML_CONTENT)

        manifest, _ = Manifest.load(path=root / "nue.yaml")
        sources = list(manifest.resolve_mounts(root))

        entries: dict[str, _ext.ManifestEntry] = {}
        mount_roots: list[_ext.MountRoot] = []
        for mount_entry, resolved in sources:
            entries.update(resolved)
            source, prefix, _ = mount_entry._resolve_source(root)
            mount_roots.append(
                _ext.MountRoot(virtual_prefix=prefix, backend_path=source)
            )
        entries = ensure_ancestors(entries)

        handle = nuefs.open(root)
        handle.update(list(entries.values()), mount_roots)
        _wait_for_mount(root)

        yield root, entries, mount_roots, handle

        handle.unmount()

    # -- getattr --

    def test_stat_file(self, fuse_mount: tuple) -> None:
        root, entries, _, _ = fuse_mount
        st = os.stat(root / "main.py")
        assert stat.S_ISREG(st.st_mode)
        assert st.st_size == len("# main")

    def test_stat_directory(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        st = os.stat(root / "vendor")
        assert stat.S_ISDIR(st.st_mode)

    # -- read --

    def test_read_through_mount(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        content = (root / "main.py").read_text()
        backend = root / "project-a" / "main.py"
        assert content == backend.read_text()

    # -- readdir --

    def test_readdir_root(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        names = set(os.listdir(root))
        assert "main.py" in names
        assert "lib" in names
        assert "vendor" in names

    def test_readdir_subdir(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        names = set(os.listdir(root / "vendor"))
        assert "utils.py" in names
        assert "helpers" in names

    # -- exclude visibility --

    def test_exclude_not_in_manifest(self, fuse_mount: tuple) -> None:
        _, entries, *_ = fuse_mount
        assert "__pycache__" not in entries
        assert "__pycache__/main.cpython-312.pyc" not in entries

    # -- write --

    def test_write_through(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        target = root / "main.py"
        backend = root / "project-a" / "main.py"
        try:
            target.write_text("# modified")
            assert backend.read_text() == "# modified"
        finally:
            backend.write_text("# main")

    # -- create --

    def test_create_file(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        new_file = root / "posix_created.txt"
        # prefix="" → first mount root (project-a/)
        backend = mount_roots[0].backend_path / "posix_created.txt"
        try:
            new_file.write_text("hello")
            assert backend.exists()
            assert backend.read_text() == "hello"
        finally:
            backend.unlink(missing_ok=True)

    # -- unlink --

    def test_unlink(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_unlink.txt"
        backend.write_text("doomed")
        target = root / "posix_unlink.txt"
        try:
            assert target.exists()
            os.unlink(target)
            assert not backend.exists()
        finally:
            backend.unlink(missing_ok=True)

    # -- mkdir + rmdir --

    def test_mkdir_rmdir(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        target = root / "posix_dir"
        try:
            os.mkdir(target)
            assert target.is_dir()
            os.rmdir(target)
            assert not target.exists()
        finally:
            # Clean up via mount if rmdir assertion failed
            if target.exists():
                os.rmdir(target)

    # -- rename --

    def test_rename(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend_src = mount_roots[0].backend_path / "posix_ren_src.txt"
        backend_dst = mount_roots[0].backend_path / "posix_ren_dst.txt"
        backend_src.write_text("rename me")
        try:
            os.rename(root / "posix_ren_src.txt", root / "posix_ren_dst.txt")
            assert not backend_src.exists()
            assert backend_dst.read_text() == "rename me"
        finally:
            backend_src.unlink(missing_ok=True)
            backend_dst.unlink(missing_ok=True)

    # -- chmod --

    def test_chmod(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_chmod.txt"
        backend.write_text("chmod test")
        target = root / "posix_chmod.txt"
        try:
            os.chmod(target, 0o600)
            assert (backend.stat().st_mode & 0o777) == 0o600
        finally:
            backend.unlink(missing_ok=True)

    # -- utime --

    def test_utime(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_utime.txt"
        backend.write_text("utime test")
        target = root / "posix_utime.txt"
        try:
            before = target.stat().st_mtime_ns
            # Explicit future timestamp to avoid resolution issues
            future_ns = before + 1_000_000_000
            future_s = future_ns / 1e9
            os.utime(target, (future_s, future_s))
            after = target.stat().st_mtime_ns
            assert after > before
        finally:
            backend.unlink(missing_ok=True)

    # -- symlink + readlink --

    def test_symlink_readlink(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        link = root / "posix_link"
        backend_link = mount_roots[0].backend_path / "posix_link"
        try:
            os.symlink("main.py", link)
            assert os.readlink(link) == "main.py"
            assert backend_link.is_symlink()
        finally:
            backend_link.unlink(missing_ok=True)
