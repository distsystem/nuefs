"""End-to-end tests for NueFS FUSE mount behavior.

Validates NueFS-specific semantics that generic POSIX tests cannot cover:
multi-source merging, exclude filtering, write-through routing, and
cross-mount-root operations.
"""

import os
import pathlib
import shutil
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


class TestFuseMount:
    """NueFS-specific behavior on a live FUSE mount."""

    @pytest.fixture(scope="class")
    def fuse_mount(
        self,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> tuple[
        pathlib.Path, dict[str, _ext.ManifestEntry], list[_ext.MountRoot], nuefs.Handle
    ]:
        root = setup_test_dirs(tmp_path_factory.mktemp("e2e_fuse"))
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

    # ── merged view ──

    def test_readdir_merged_root(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        names = set(os.listdir(root))
        assert "main.py" in names, "project-a/main.py should appear at root"
        assert "vendor" in names, "libs/ should appear as vendor/"

    def test_readdir_vendor_subdir(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        names = set(os.listdir(root / "vendor"))
        assert "utils.py" in names
        assert "helpers" in names

    # ── exclude ──

    def test_exclude_hides_pycache(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        names = set(os.listdir(root))
        assert "__pycache__" not in names

    # ── read-through ──

    def test_read_matches_backend(self, fuse_mount: tuple) -> None:
        root, *_ = fuse_mount
        mount_content = (root / "main.py").read_text()
        backend_content = (root / "project-a" / "main.py").read_text()
        assert mount_content == backend_content

    # ── write-through routing ──

    def test_write_routes_to_root_backend(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        target = root / "wt_root.txt"
        backend = mount_roots[0].backend_path / "wt_root.txt"
        try:
            target.write_text("root write")
            assert backend.read_text() == "root write"
        finally:
            backend.unlink(missing_ok=True)

    def test_write_routes_to_vendor_backend(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        target = root / "vendor" / "wt_vendor.txt"
        backend = mount_roots[1].backend_path / "wt_vendor.txt"
        try:
            target.write_text("vendor write")
            assert backend.read_text() == "vendor write"
        finally:
            backend.unlink(missing_ok=True)

    # ── cross-mount-root rename ──

    def test_rename_cross_mount_root(self, fuse_mount: tuple) -> None:
        root, _, mount_roots, _ = fuse_mount
        backend_vendor = mount_roots[1].backend_path / "cross.txt"
        backend_root = mount_roots[0].backend_path / "cross.txt"
        backend_vendor.write_text("cross mount")
        try:
            os.rename(root / "vendor" / "cross.txt", root / "cross.txt")
            assert not backend_vendor.exists()
            assert backend_root.read_text() == "cross mount"
        finally:
            backend_vendor.unlink(missing_ok=True)
            backend_root.unlink(missing_ok=True)
