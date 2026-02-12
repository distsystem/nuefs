"""End-to-end tests for manifest workflow: YAML → load → resolve → merge."""

import pathlib

import pytest

import nuefs._nuefs as _ext
from nuefs.manifest import Manifest, MountEntry, ensure_ancestors


def _setup_dirs(tmp_path: pathlib.Path) -> pathlib.Path:
    """Create test directory structure under tmp_path.

    tmp_path/
    ├── nue.yaml
    ├── project-a/
    │   ├── lib/
    │   │   └── deep/
    │   │       └── mod.py
    │   ├── main.py
    │   └── __pycache__/
    │       └── main.cpython-312.pyc
    └── libs/
        ├── utils.py
        └── helpers/
            └── fmt.py
    """
    (tmp_path / "project-a" / "lib" / "deep").mkdir(parents=True)
    (tmp_path / "project-a" / "lib" / "deep" / "mod.py").write_text("# mod")
    (tmp_path / "project-a" / "main.py").write_text("# main")
    (tmp_path / "project-a" / "__pycache__").mkdir()
    (tmp_path / "project-a" / "__pycache__" / "main.cpython-312.pyc").write_bytes(b"\x00")

    (tmp_path / "libs" / "helpers").mkdir(parents=True)
    (tmp_path / "libs" / "utils.py").write_text("# utils")
    (tmp_path / "libs" / "helpers" / "fmt.py").write_text("# fmt")

    return tmp_path


class TestEndToEndManifest:
    """Full pipeline: YAML → Manifest.load() → resolve_mounts() → ensure_ancestors()."""

    def test_multi_mount_merge(self, tmp_path: pathlib.Path) -> None:
        """Two trailing-slash sources merge into a single virtual namespace."""
        root = _setup_dirs(tmp_path)
        yaml_content = """\
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
        (root / "nue.yaml").write_text(yaml_content)

        manifest, cfg_root = Manifest.load(path=root / "nue.yaml")
        assert cfg_root == root

        sources = list(manifest.resolve_mounts(root))
        assert len(sources) == 2

        entries: dict[str, _ext.ManifestEntry] = {}
        for _, resolved in sources:
            entries.update(resolved)
        entries = ensure_ancestors(entries)

        # project-a contents (trailing slash → expand)
        assert "main.py" in entries
        assert not entries["main.py"].is_dir

        # __pycache__ excluded
        assert "__pycache__" not in entries
        assert "__pycache__/main.cpython-312.pyc" not in entries

        # libs contents under vendor/ prefix
        assert "vendor/utils.py" in entries
        assert not entries["vendor/utils.py"].is_dir

        assert "vendor/helpers" in entries
        assert entries["vendor/helpers"].is_dir

    def test_exclude_filtering(self, tmp_path: pathlib.Path) -> None:
        """Excluded patterns are absent from resolved entries."""
        root = _setup_dirs(tmp_path)

        entry = MountEntry(source="./project-a/", exclude=["__pycache__", "*.pyc"], vcs=False)
        resolved = entry.resolve(root)

        assert "main.py" in resolved
        for vpath in resolved:
            assert "__pycache__" not in vpath
            assert not vpath.endswith(".pyc")

    def test_dest_prefix(self, tmp_path: pathlib.Path) -> None:
        """dest: vendor prepends prefix to all virtual paths."""
        root = _setup_dirs(tmp_path)

        entry = MountEntry(source="./libs/", dest="vendor", exclude=[], vcs=False)
        resolved = entry.resolve(root)

        for vpath in resolved:
            assert vpath.startswith("vendor/"), f"{vpath} missing vendor/ prefix"

        assert "vendor/utils.py" in resolved
        assert "vendor/helpers" in resolved

    def test_single_child_chain_collapse(self, tmp_path: pathlib.Path) -> None:
        """lib/deep/ (single-child chain) collapses to lib/deep as dir entry."""
        root = _setup_dirs(tmp_path)

        entry = MountEntry(source="./project-a/", exclude=["__pycache__"], vcs=False)
        resolved = entry.resolve(root)

        # lib → deep is a single-child chain, collapsed to lib/deep
        assert "lib/deep" in resolved
        assert resolved["lib/deep"].is_dir
        # intermediate "lib" should NOT appear (collapsed)
        assert "lib" not in resolved

    def test_ensure_ancestors_fills_gaps(self, tmp_path: pathlib.Path) -> None:
        """ensure_ancestors adds missing ancestor dirs after collapse."""
        root = _setup_dirs(tmp_path)

        entry = MountEntry(source="./project-a/", exclude=["__pycache__"], vcs=False)
        resolved = entry.resolve(root)

        # Before: "lib" is missing because of collapse
        assert "lib" not in resolved

        filled = ensure_ancestors(resolved)

        # After: "lib" ancestor is filled in
        assert "lib" in filled
        assert filled["lib"].is_dir
        assert filled["lib"].backend_path == root / "project-a" / "lib"

    def test_mount_override(self, tmp_path: pathlib.Path) -> None:
        """Later mount overrides earlier mount for same virtual path."""
        root = tmp_path

        # Two sources with a conflicting file
        (root / "base").mkdir()
        (root / "base" / "config.py").write_text("# base")
        (root / "override").mkdir()
        (root / "override" / "config.py").write_text("# override")

        yaml_content = """\
apiVersion: nue/v1
mounts:
- source: ./base/
  vcs: false
- source: ./override/
  vcs: false
"""
        (root / "nue.yaml").write_text(yaml_content)

        manifest, _ = Manifest.load(path=root / "nue.yaml")
        sources = list(manifest.resolve_mounts(root))

        entries: dict[str, _ext.ManifestEntry] = {}
        for _, resolved in sources:
            entries.update(resolved)

        # override wins (dict.update with later mount)
        assert entries["config.py"].backend_path == root / "override" / "config.py"

    def test_backend_paths_are_absolute(self, tmp_path: pathlib.Path) -> None:
        """All backend_path values must be absolute paths."""
        root = _setup_dirs(tmp_path)

        entry = MountEntry(source="./project-a/", exclude=["__pycache__"], vcs=False)
        resolved = entry.resolve(root)
        filled = ensure_ancestors(resolved)

        for vpath, me in filled.items():
            assert me.backend_path.is_absolute(), f"{vpath} → {me.backend_path} is not absolute"
