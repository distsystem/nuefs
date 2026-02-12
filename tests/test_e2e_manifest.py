"""End-to-end tests for manifest workflow: YAML → load → resolve → merge."""

import pathlib

import nuefs._nuefs as _ext
from nuefs.manifest import Manifest, MountEntry, ensure_ancestors


class TestEndToEndManifest:
    """Full pipeline: YAML → Manifest.load() → resolve_mounts() → ensure_ancestors()."""

    def test_multi_mount_merge(
        self,
        resolved_manifest: tuple[
            pathlib.Path,
            dict[str, _ext.ManifestEntry],
            list[_ext.MountRoot],
            list[tuple[MountEntry, dict[str, _ext.ManifestEntry]]],
        ],
    ) -> None:
        """Two trailing-slash sources merge into a single virtual namespace."""
        root, entries, _, _ = resolved_manifest

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

    def test_exclude_filtering(self, setup_dirs: pathlib.Path) -> None:
        """Excluded patterns are absent from resolved entries."""
        entry = MountEntry(source="./project-a/", exclude=["__pycache__", "*.pyc"], vcs=False)
        resolved = entry.resolve(setup_dirs)

        assert "main.py" in resolved
        for vpath in resolved:
            assert "__pycache__" not in vpath
            assert not vpath.endswith(".pyc")

    def test_dest_prefix(self, setup_dirs: pathlib.Path) -> None:
        """dest: vendor prepends prefix to all virtual paths."""
        entry = MountEntry(source="./libs/", dest="vendor", exclude=[], vcs=False)
        resolved = entry.resolve(setup_dirs)

        for vpath in resolved:
            assert vpath.startswith("vendor/"), f"{vpath} missing vendor/ prefix"

        assert "vendor/utils.py" in resolved
        assert "vendor/helpers" in resolved

    def test_single_child_chain_collapse(self, setup_dirs: pathlib.Path) -> None:
        """lib/deep/ (single-child chain) collapses to lib/deep as dir entry."""
        entry = MountEntry(source="./project-a/", exclude=["__pycache__"], vcs=False)
        resolved = entry.resolve(setup_dirs)

        # lib → deep is a single-child chain, collapsed to lib/deep
        assert "lib/deep" in resolved
        assert resolved["lib/deep"].is_dir
        # intermediate "lib" should NOT appear (collapsed)
        assert "lib" not in resolved

    def test_ensure_ancestors_fills_gaps(self, setup_dirs: pathlib.Path) -> None:
        """ensure_ancestors adds missing ancestor dirs after collapse."""
        entry = MountEntry(source="./project-a/", exclude=["__pycache__"], vcs=False)
        resolved = entry.resolve(setup_dirs)

        # Before: "lib" is missing because of collapse
        assert "lib" not in resolved

        filled = ensure_ancestors(resolved)

        # After: "lib" ancestor is filled in
        assert "lib" in filled
        assert filled["lib"].is_dir
        assert filled["lib"].backend_path == setup_dirs / "project-a" / "lib"

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

    def test_backend_paths_are_absolute(self, setup_dirs: pathlib.Path) -> None:
        """All backend_path values must be absolute paths."""
        entry = MountEntry(source="./project-a/", exclude=["__pycache__"], vcs=False)
        resolved = entry.resolve(setup_dirs)
        filled = ensure_ancestors(resolved)

        for vpath, me in filled.items():
            assert me.backend_path.is_absolute(), f"{vpath} → {me.backend_path} is not absolute"
