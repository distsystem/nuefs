"""POSIX semantics compatibility tests for NueFS FUSE mount."""

import errno
import os
import pathlib
import shutil
import stat
import subprocess
import time

import pytest

import nuefs
import nuefs._nuefs as _ext
from nuefs.manifest import Manifest, ensure_ancestors

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

    # ── getattr ──

    def test_stat_file(self, fuse_mount: tuple) -> None:
        root, entries, _, _ = fuse_mount
        st = os.stat(root / "main.py")
        assert stat.S_ISREG(st.st_mode)
        assert st.st_size == len("# main")

    def test_stat_directory(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        st = os.stat(root / "vendor")
        assert stat.S_ISDIR(st.st_mode)

    def test_stat_symlink_lstat(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        link = root / "posix_lstat_link"
        backend = mount_roots[0].backend_path / "posix_lstat_link"
        try:
            os.symlink("main.py", link)
            st = os.lstat(link)
            assert stat.S_ISLNK(st.st_mode)
            # stat (follow) should give regular file
            st2 = os.stat(link)
            assert stat.S_ISREG(st2.st_mode)
        finally:
            backend.unlink(missing_ok=True)

    # ── lookup ──

    def test_lookup_nonexistent(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        assert not (root / "no_such_file_xyz").exists()
        with pytest.raises(FileNotFoundError):
            os.stat(root / "no_such_file_xyz")

    # ── read ──

    def test_read_through_mount(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        content = (root / "main.py").read_text()
        backend = root / "project-a" / "main.py"
        assert content == backend.read_text()

    def test_read_at_offset(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_pread.txt"
        backend.write_text("abcdefghij")
        target = root / "posix_pread.txt"
        try:
            fd = os.open(target, os.O_RDONLY)
            try:
                data = os.pread(fd, 3, 4)
                assert data == b"efg"
            finally:
                os.close(fd)
        finally:
            backend.unlink(missing_ok=True)

    def test_read_large_file(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_large.bin"
        size = 1024 * 1024  # 1 MiB
        data = os.urandom(size)
        backend.write_bytes(data)
        target = root / "posix_large.bin"
        try:
            assert target.read_bytes() == data
        finally:
            backend.unlink(missing_ok=True)

    # ── readdir ──

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

    def test_readdir_dot_entries(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            entries = list(os.scandir(fd))
            # scandir does not include . and .. but the FUSE readdir does;
            # verify at least that scandir works without error
            names = {e.name for e in entries}
            assert "main.py" in names
        finally:
            os.close(fd)

    # ── exclude ──

    def test_exclude_not_in_manifest(self, fuse_mount: tuple) -> None:
        _, entries, *_ = fuse_mount
        assert "__pycache__" not in entries
        assert "__pycache__/main.cpython-312.pyc" not in entries

    # ── write ──

    def test_write_through(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        target = root / "main.py"
        backend = root / "project-a" / "main.py"
        try:
            target.write_text("# modified")
            assert backend.read_text() == "# modified"
        finally:
            backend.write_text("# main")

    def test_write_append(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_append.txt"
        backend.write_text("line1\n")
        target = root / "posix_append.txt"
        try:
            fd = os.open(target, os.O_WRONLY | os.O_APPEND)
            try:
                os.write(fd, b"line2\n")
            finally:
                os.close(fd)
            assert backend.read_text() == "line1\nline2\n"
        finally:
            backend.unlink(missing_ok=True)

    def test_write_at_offset(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_pwrite.txt"
        backend.write_text("aaaaaaaaaa")
        target = root / "posix_pwrite.txt"
        try:
            fd = os.open(target, os.O_WRONLY)
            try:
                os.pwrite(fd, b"bbb", 3)
            finally:
                os.close(fd)
            assert backend.read_text() == "aaabbbaaaa"
        finally:
            backend.unlink(missing_ok=True)

    # ── create ──

    def test_create_file(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        new_file = root / "posix_created.txt"
        backend = mount_roots[0].backend_path / "posix_created.txt"
        try:
            new_file.write_text("hello")
            assert backend.exists()
            assert backend.read_text() == "hello"
        finally:
            backend.unlink(missing_ok=True)

    def test_create_exclusive(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_excl.txt"
        backend.write_text("exists")
        target = root / "posix_excl.txt"
        try:
            with pytest.raises(FileExistsError):
                os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        finally:
            backend.unlink(missing_ok=True)

    def test_create_in_vendor(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        new_file = root / "vendor" / "posix_vendor_new.txt"
        backend = mount_roots[1].backend_path / "posix_vendor_new.txt"
        try:
            new_file.write_text("vendor file")
            assert backend.exists()
            assert backend.read_text() == "vendor file"
        finally:
            backend.unlink(missing_ok=True)

    # ── truncate ──

    def test_truncate(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_trunc.txt"
        backend.write_text("hello world")
        target = root / "posix_trunc.txt"
        try:
            os.truncate(target, 5)
            assert backend.read_text() == "hello"
        finally:
            backend.unlink(missing_ok=True)

    def test_ftruncate(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_ftrunc.txt"
        backend.write_text("hello world")
        target = root / "posix_ftrunc.txt"
        try:
            fd = os.open(target, os.O_WRONLY)
            try:
                os.ftruncate(fd, 5)
            finally:
                os.close(fd)
            assert backend.read_text() == "hello"
        finally:
            backend.unlink(missing_ok=True)

    def test_truncate_extend(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_trunc_ext.txt"
        backend.write_text("hi")
        target = root / "posix_trunc_ext.txt"
        try:
            os.truncate(target, 10)
            data = backend.read_bytes()
            assert len(data) == 10
            assert data[:2] == b"hi"
            assert data[2:] == b"\x00" * 8
        finally:
            backend.unlink(missing_ok=True)

    # ── unlink ──

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

    def test_unlink_nonexistent(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        with pytest.raises(FileNotFoundError):
            os.unlink(root / "posix_no_such_file.txt")

    def test_open_unlink_fd_survives(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_fd_unlink.txt"
        backend.write_text("still readable")
        target = root / "posix_fd_unlink.txt"
        try:
            fd = os.open(target, os.O_RDONLY)
            try:
                os.unlink(target)
                assert not backend.exists()
                # fd should still be readable after unlink
                data = os.read(fd, 100)
                assert data == b"still readable"
            finally:
                os.close(fd)
        finally:
            backend.unlink(missing_ok=True)

    # ── mkdir + rmdir ──

    def test_mkdir_rmdir(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        target = root / "posix_dir"
        try:
            os.mkdir(target)
            assert target.is_dir()
            os.rmdir(target)
            assert not target.exists()
        finally:
            if target.exists():
                os.rmdir(target)

    def test_mkdir_with_mode(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        target = root / "posix_dir_mode"
        try:
            os.mkdir(target, 0o700)
            st = os.stat(target)
            assert stat.S_ISDIR(st.st_mode)
            # mode may be masked by umask, just check it's a directory
            assert st.st_mode & 0o700 == 0o700
        finally:
            if target.exists():
                os.rmdir(target)

    def test_rmdir_notempty(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        target_dir = root / "posix_notempty"
        backend_dir = mount_roots[0].backend_path / "posix_notempty"
        try:
            os.mkdir(target_dir)
            (target_dir / "child.txt").write_text("x")
            with pytest.raises(OSError) as exc_info:
                os.rmdir(target_dir)
            assert exc_info.value.errno == errno.ENOTEMPTY
        finally:
            (backend_dir / "child.txt").unlink(missing_ok=True)
            if backend_dir.exists():
                backend_dir.rmdir()

    # ── rename ──

    def test_rename_same_dir(self, fuse_mount: tuple) -> None:
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

    def test_rename_cross_mount_root(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        # vendor/ → libs/ (mount_roots[1]), root → project-a/ (mount_roots[0])
        backend_vendor = mount_roots[1].backend_path / "posix_cross.txt"
        backend_root = mount_roots[0].backend_path / "posix_cross.txt"
        backend_vendor.write_text("cross mount")
        try:
            os.rename(root / "vendor" / "posix_cross.txt", root / "posix_cross.txt")
            assert not backend_vendor.exists()
            assert backend_root.read_text() == "cross mount"
        finally:
            backend_vendor.unlink(missing_ok=True)
            backend_root.unlink(missing_ok=True)

    def test_rename_overwrite(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend_a = mount_roots[0].backend_path / "posix_ren_ow_a.txt"
        backend_b = mount_roots[0].backend_path / "posix_ren_ow_b.txt"
        backend_a.write_text("old")
        backend_b.write_text("new")
        try:
            os.rename(root / "posix_ren_ow_b.txt", root / "posix_ren_ow_a.txt")
            assert not backend_b.exists()
            assert backend_a.read_text() == "new"
        finally:
            backend_a.unlink(missing_ok=True)
            backend_b.unlink(missing_ok=True)

    def test_rename_directory(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        src = root / "posix_ren_dir_src"
        dst = root / "posix_ren_dir_dst"
        backend_src = mount_roots[0].backend_path / "posix_ren_dir_src"
        backend_dst = mount_roots[0].backend_path / "posix_ren_dir_dst"
        try:
            os.mkdir(src)
            (src / "inner.txt").write_text("inside")
            os.rename(src, dst)
            assert not backend_src.exists()
            assert backend_dst.is_dir()
            assert (backend_dst / "inner.txt").read_text() == "inside"
        finally:
            shutil.rmtree(backend_src, ignore_errors=True)
            shutil.rmtree(backend_dst, ignore_errors=True)

    # ── hard link ──

    def test_hard_link(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend_orig = mount_roots[0].backend_path / "posix_link_orig.txt"
        backend_link = mount_roots[0].backend_path / "posix_link_hard.txt"
        backend_orig.write_text("linkable")
        try:
            os.link(root / "posix_link_orig.txt", root / "posix_link_hard.txt")
            assert backend_link.exists()
            assert backend_link.read_text() == "linkable"
            # Backend nlink should be 2
            assert backend_link.stat().st_nlink == 2
        finally:
            backend_orig.unlink(missing_ok=True)
            backend_link.unlink(missing_ok=True)

    def test_hard_link_unlink_one(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend_orig = mount_roots[0].backend_path / "posix_hl_a.txt"
        backend_link = mount_roots[0].backend_path / "posix_hl_b.txt"
        backend_orig.write_text("shared")
        try:
            os.link(root / "posix_hl_a.txt", root / "posix_hl_b.txt")
            os.unlink(root / "posix_hl_a.txt")
            assert not backend_orig.exists()
            assert backend_link.read_text() == "shared"
        finally:
            backend_orig.unlink(missing_ok=True)
            backend_link.unlink(missing_ok=True)

    # ── chmod ──

    def test_chmod(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_chmod.txt"
        backend.write_text("chmod test")
        target = root / "posix_chmod.txt"
        try:
            os.chmod(target, 0o600)
            assert (os.stat(target).st_mode & 0o777) == 0o600
            os.chmod(target, 0o755)
            assert (os.stat(target).st_mode & 0o777) == 0o755
        finally:
            backend.unlink(missing_ok=True)

    def test_chmod_directory(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        target = root / "posix_chmod_dir"
        backend = mount_roots[0].backend_path / "posix_chmod_dir"
        try:
            os.mkdir(target, 0o755)
            os.chmod(target, 0o700)
            assert (backend.stat().st_mode & 0o777) == 0o700
        finally:
            if backend.exists():
                backend.rmdir()

    # ── chown ──

    def test_chown_to_self(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_chown.txt"
        backend.write_text("chown test")
        target = root / "posix_chown.txt"
        try:
            uid = os.getuid()
            gid = os.getgid()
            # chown to self should always succeed
            os.chown(target, uid, gid)
            st = os.stat(target)
            assert st.st_uid == uid
            assert st.st_gid == gid
        finally:
            backend.unlink(missing_ok=True)

    # ── utime ──

    def test_utime(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_utime.txt"
        backend.write_text("utime test")
        target = root / "posix_utime.txt"
        try:
            before = target.stat().st_mtime_ns
            future_ns = before + 1_000_000_000
            future_s = future_ns / 1e9
            os.utime(target, (future_s, future_s))
            after = target.stat().st_mtime_ns
            assert after > before
        finally:
            backend.unlink(missing_ok=True)

    def test_utime_explicit_times(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_utime2.txt"
        backend.write_text("utime2")
        target = root / "posix_utime2.txt"
        try:
            atime = 1_700_000_000.0
            mtime = 1_700_000_500.0
            os.utime(target, (atime, mtime))
            st = os.stat(target)
            assert abs(st.st_atime - atime) < 1.0
            assert abs(st.st_mtime - mtime) < 1.0
        finally:
            backend.unlink(missing_ok=True)

    # ── symlink + readlink ──

    def test_symlink_readlink(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        link = root / "posix_sym"
        backend_link = mount_roots[0].backend_path / "posix_sym"
        try:
            os.symlink("main.py", link)
            assert os.readlink(link) == "main.py"
            assert backend_link.is_symlink()
        finally:
            backend_link.unlink(missing_ok=True)

    def test_symlink_read_through(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        link = root / "posix_sym_read"
        backend_link = mount_roots[0].backend_path / "posix_sym_read"
        try:
            os.symlink("main.py", link)
            content = link.read_text()
            assert content == "# main"
        finally:
            backend_link.unlink(missing_ok=True)

    def test_symlink_dangling(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        link = root / "posix_sym_dangle"
        backend_link = mount_roots[0].backend_path / "posix_sym_dangle"
        try:
            os.symlink("no_such_target", link)
            assert os.readlink(link) == "no_such_target"
            # exists() follows the link and returns False for dangling
            assert not link.exists()
            # lstat should succeed (the symlink itself exists)
            st = os.lstat(link)
            assert stat.S_ISLNK(st.st_mode)
        finally:
            backend_link.unlink(missing_ok=True)

    # ── fsync ──

    def test_fsync(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_fsync.txt"
        target = root / "posix_fsync.txt"
        try:
            fd = os.open(target, os.O_CREAT | os.O_WRONLY, 0o644)
            try:
                os.write(fd, b"sync me")
                os.fsync(fd)
            finally:
                os.close(fd)
            assert backend.read_text() == "sync me"
        finally:
            backend.unlink(missing_ok=True)

    def test_fdatasync(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_fdsync.txt"
        target = root / "posix_fdsync.txt"
        try:
            fd = os.open(target, os.O_CREAT | os.O_WRONLY, 0o644)
            try:
                os.write(fd, b"datasync me")
                os.fdatasync(fd)
            finally:
                os.close(fd)
            assert backend.read_text() == "datasync me"
        finally:
            backend.unlink(missing_ok=True)

    # ── statfs ──

    def test_statvfs(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        st = os.statvfs(root)
        assert st.f_bsize > 0
        assert st.f_namemax > 0

    # ── access ──

    def test_access_existing(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        assert os.access(root / "main.py", os.R_OK)
        assert os.access(root / "main.py", os.F_OK)

    def test_access_nonexistent(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        assert not os.access(root / "no_such_file_xyz", os.F_OK)

    def test_access_directory(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        assert os.access(root / "vendor", os.R_OK | os.X_OK)

    # ── multiple fd ──

    def test_concurrent_open(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_concurrent.txt"
        backend.write_text("concurrent")
        target = root / "posix_concurrent.txt"
        try:
            fd1 = os.open(target, os.O_RDONLY)
            fd2 = os.open(target, os.O_RDONLY)
            try:
                assert os.read(fd1, 100) == b"concurrent"
                assert os.read(fd2, 100) == b"concurrent"
            finally:
                os.close(fd1)
                os.close(fd2)
        finally:
            backend.unlink(missing_ok=True)

    # ── file mode on create ──

    def test_create_with_mode(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        target = root / "posix_mode_create.txt"
        backend = mount_roots[0].backend_path / "posix_mode_create.txt"
        try:
            fd = os.open(target, os.O_CREAT | os.O_WRONLY, 0o600)
            os.close(fd)
            st = os.stat(target)
            assert (st.st_mode & 0o777) == 0o600
        finally:
            backend.unlink(missing_ok=True)

    # ── empty file ──

    def test_read_empty_file(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend = mount_roots[0].backend_path / "posix_empty.txt"
        backend.write_bytes(b"")
        target = root / "posix_empty.txt"
        try:
            assert target.read_bytes() == b""
            assert target.stat().st_size == 0
        finally:
            backend.unlink(missing_ok=True)

    # ── nested operations ──

    def test_nested_mkdir_create_cleanup(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        d = root / "posix_nest"
        backend_d = mount_roots[0].backend_path / "posix_nest"
        try:
            os.mkdir(d)
            (d / "a.txt").write_text("a")
            (d / "b.txt").write_text("b")
            names = set(os.listdir(d))
            assert names == {"a.txt", "b.txt"}
            os.unlink(d / "a.txt")
            os.unlink(d / "b.txt")
            os.rmdir(d)
            assert not d.exists()
        finally:
            shutil.rmtree(backend_d, ignore_errors=True)
