"""Tests for NueFS manifest (nue.yaml) parsing and validation."""

import pathlib

import pytest
from sheaves.typing import Pathspec

from nuefs.manifest import DEFAULT_EXCLUDE, Manifest, MountEntry


class TestMountEntry:
    """Tests for MountEntry model."""

    def test_minimal_entry(self) -> None:
        entry = MountEntry(source="./src")
        assert entry.source == "./src"
        assert entry.dest == ""
        assert entry.exclude == DEFAULT_EXCLUDE
        assert len(entry.include) == 0

    def test_entry_with_dest(self) -> None:
        entry = MountEntry(source="./libs", dest="vendor")
        assert entry.source == "./libs"
        assert entry.dest == "vendor"

    def test_entry_with_exclude(self) -> None:
        entry = MountEntry(
            source="./src",
            exclude=["*.pyc", "__pycache__/", ".git/"],
        )
        assert len(entry.exclude) == 3
        assert entry.exclude.match("foo.pyc")
        assert entry.exclude.match("__pycache__/bar")
        assert not entry.exclude.match("foo.py")

    def test_entry_with_include(self) -> None:
        entry = MountEntry(
            source="./src",
            include=["*.py", "*.pyi"],
        )
        assert len(entry.include) == 2
        assert entry.include.match("foo.py")
        assert entry.include.match("types.pyi")
        assert not entry.include.match("data.json")


class TestTrailingSlashSemantics:
    """Tests for rsync-style trailing-slash dest derivation."""

    def test_trailing_slash_expands_contents(self) -> None:
        entry = MountEntry(source="./src/")
        assert entry.source == "./src/"

    def test_no_trailing_slash_preserves_dirname(self) -> None:
        entry = MountEntry(source="./src")
        assert entry.source == "./src"

    def test_dot_expands_contents(self) -> None:
        entry = MountEntry(source=".")
        assert entry.source == "."

    def test_dot_slash_expands_contents(self) -> None:
        entry = MountEntry(source="./")
        assert entry.source == "./"

    def test_resolve_trailing_slash_no_prefix(self, tmp_path: pathlib.Path) -> None:
        """source with trailing slash → expand contents to root (prefix='')."""
        src = tmp_path / "mydir"
        src.mkdir()
        (src / "a.txt").touch()
        (src / "b.txt").touch()

        entry = MountEntry(source="mydir/", exclude=[])
        result = entry.resolve(tmp_path)
        assert "a.txt" in result
        assert "b.txt" in result

    def test_resolve_no_trailing_slash_single_entry(
        self, tmp_path: pathlib.Path
    ) -> None:
        """source without trailing slash → single directory entry."""
        src = tmp_path / "mydir"
        src.mkdir()
        (src / "a.txt").touch()

        entry = MountEntry(source="mydir", exclude=[])
        result = entry.resolve(tmp_path)
        assert len(result) == 1
        assert "mydir" in result
        assert result["mydir"].is_dir

    def test_resolve_explicit_dest_overrides(self, tmp_path: pathlib.Path) -> None:
        """Explicit dest overrides auto-derived prefix."""
        src = tmp_path / "mydir"
        src.mkdir()
        (src / "a.txt").touch()

        entry = MountEntry(source="mydir/", dest="libs", exclude=[])
        result = entry.resolve(tmp_path)
        assert "libs/a.txt" in result

    def test_resolve_dot_expands_to_root(self, tmp_path: pathlib.Path) -> None:
        """source='.' expands contents to root."""
        (tmp_path / "file.txt").touch()

        entry = MountEntry(source=".", exclude=[])
        result = entry.resolve(tmp_path)
        assert "file.txt" in result

    def test_resolve_file_no_prefix(self, tmp_path: pathlib.Path) -> None:
        """Single file source → no prefix."""
        (tmp_path / "foo.txt").touch()

        entry = MountEntry(source="foo.txt", exclude=[])
        result = entry.resolve(tmp_path)
        assert "foo.txt" in result

    def test_resolve_file_with_dest_rename(self, tmp_path: pathlib.Path) -> None:
        """Single file with dest → rename."""
        (tmp_path / "foo.txt").touch()

        entry = MountEntry(source="foo.txt", dest="bar.txt", exclude=[])
        result = entry.resolve(tmp_path)
        assert "bar.txt" in result


class TestCollapseSingleChildDirs:
    """Tests for minimal cover prefix via single-child directory collapsing."""

    def test_single_child_chain_collapses(self, tmp_path: pathlib.Path) -> None:
        """a/b/c (file) with no siblings → register a/b (dir)."""
        (tmp_path / "a" / "b").mkdir(parents=True)
        (tmp_path / "a" / "b" / "c").touch()

        entry = MountEntry(source="./", exclude=[])
        result = entry.resolve(tmp_path)
        assert "a/b" in result
        assert result["a/b"].is_dir

    def test_no_collapse_with_multiple_children(
        self, tmp_path: pathlib.Path
    ) -> None:
        """a/ has two children → no collapsing."""
        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "x").mkdir()
        (tmp_path / "a" / "y").mkdir()
        (tmp_path / "a" / "x" / "f.txt").touch()
        (tmp_path / "a" / "y" / "g.txt").touch()

        entry = MountEntry(source="./", exclude=[])
        result = entry.resolve(tmp_path)
        assert "a" in result
        assert result["a"].is_dir

    def test_no_collapse_with_file_sibling(
        self, tmp_path: pathlib.Path
    ) -> None:
        """a/ has a dir and a file → no collapsing."""
        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "sub").mkdir()
        (tmp_path / "a" / "readme.txt").touch()

        entry = MountEntry(source="./", exclude=[])
        result = entry.resolve(tmp_path)
        assert "a" in result

    def test_deep_chain_collapses(self, tmp_path: pathlib.Path) -> None:
        """a/b/c/d/ with single-child chain → collapse to a/b/c/d."""
        (tmp_path / "a" / "b" / "c" / "d").mkdir(parents=True)
        (tmp_path / "a" / "b" / "c" / "d" / "file.txt").touch()

        entry = MountEntry(source="./", exclude=[])
        result = entry.resolve(tmp_path)
        assert "a/b/c/d" in result
        assert result["a/b/c/d"].is_dir

    def test_collapse_respects_exclude(self, tmp_path: pathlib.Path) -> None:
        """Excluded siblings don't prevent collapsing."""
        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "real").mkdir()
        (tmp_path / "a" / "__pycache__").mkdir()
        (tmp_path / "a" / "real" / "f.txt").touch()

        entry = MountEntry(source="./", exclude=["__pycache__"])
        result = entry.resolve(tmp_path)
        assert "a/real" in result
        assert result["a/real"].is_dir

    def test_collapse_with_prefix(self, tmp_path: pathlib.Path) -> None:
        """Collapsing works with non-empty dest prefix."""
        (tmp_path / "a" / "b").mkdir(parents=True)
        (tmp_path / "a" / "b" / "f.txt").touch()

        entry = MountEntry(source="./", dest="libs", exclude=[])
        result = entry.resolve(tmp_path)
        assert "libs/a/b" in result


class TestManifest:
    """Tests for Manifest model."""

    def test_empty_manifest(self) -> None:
        manifest = Manifest()
        assert manifest.apiVersion == "nue/v1"
        assert manifest.mounts == []

    def test_manifest_with_mounts(self) -> None:
        manifest = Manifest(
            mounts=[
                MountEntry(source="./src/"),
                MountEntry(source="./libs", dest="vendor"),
            ]
        )
        assert len(manifest.mounts) == 2
        assert manifest.mounts[0].source == "./src/"
        assert manifest.mounts[1].dest == "vendor"


class TestManifestLoad:
    """Tests for loading Manifest from YAML files."""

    def test_load_basic_manifest(self, tmp_path: pathlib.Path) -> None:
        yaml_content = """\
apiVersion: nue/v1
mounts:
- source: ./sources/project-a/
  exclude:
    - '*.pyc'
    - __pycache__/
    - .git/
- source: ./sources/libs
  dest: vendor
"""
        (tmp_path / "nue.yaml").write_text(yaml_content)

        manifest = Manifest.load(config_path=tmp_path / "nue.yaml")

        assert manifest.apiVersion == "nue/v1"
        assert len(manifest.mounts) == 2

        first = manifest.mounts[0]
        assert first.source == "./sources/project-a/"
        assert first.dest == ""
        assert len(first.exclude) == 3
        assert first.exclude.match("test.pyc")

        second = manifest.mounts[1]
        assert second.source == "./sources/libs"
        assert second.dest == "vendor"

    def test_load_multi_mount_manifest(self, tmp_path: pathlib.Path) -> None:
        yaml_content = """\
apiVersion: nue/v1
mounts:
- source: ./sources/base/
- source: ./sources/override/
"""
        (tmp_path / "nue.yaml").write_text(yaml_content)

        manifest = Manifest.load(config_path=tmp_path / "nue.yaml")

        assert len(manifest.mounts) == 2

    def test_load_nonexistent_manifest(self, tmp_path: pathlib.Path) -> None:
        manifest = Manifest.load(config_path=tmp_path / "nue.yaml")
        assert manifest.mounts == []

    def test_load_from_fixtures(self) -> None:
        fixtures = pathlib.Path(__file__).parent / "fixtures"

        manifest = Manifest.load(config_path=fixtures / "nue.yaml")
        assert len(manifest.mounts) == 2
        assert manifest.mounts[0].source == "./sources/project-a/"
        assert len(manifest.mounts[0].exclude) == 3

        multi = Manifest.load(config_path=fixtures / "nue-multi.yaml")
        assert len(multi.mounts) == 2
