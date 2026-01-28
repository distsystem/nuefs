"""Tests for NueFS manifest (nue.yaml) parsing and validation."""

import pathlib
import tempfile

import pytest
from sheaves.typing import LocalPath, Pathspec

from nuefs.manifest import CreatePolicy, Manifest, MountConfig, MountEntry


class TestMountConfig:
    """Tests for MountConfig model."""

    def test_default_create_policy(self) -> None:
        config = MountConfig()
        assert config.create_policy == CreatePolicy.ROOT

    def test_custom_create_policy(self) -> None:
        config = MountConfig(create_policy=CreatePolicy.LAST)
        assert config.create_policy == CreatePolicy.LAST

    def test_create_policy_from_string(self) -> None:
        config = MountConfig(create_policy="first")  # type: ignore[arg-type]
        assert config.create_policy == CreatePolicy.FIRST


class TestMountEntry:
    """Tests for MountEntry model."""

    def test_minimal_entry(self) -> None:
        entry = MountEntry(source="./src")
        assert str(entry.source) == "src"
        assert str(entry.target) == "."
        assert len(entry.exclude) == 0
        assert len(entry.include) == 0
        assert entry.config.create_policy == CreatePolicy.ROOT

    def test_entry_with_target(self) -> None:
        entry = MountEntry(source="./libs", target="vendor/")
        assert str(entry.source) == "libs"
        assert str(entry.target) == "vendor"

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

    def test_entry_with_config(self) -> None:
        entry = MountEntry(
            source="./override",
            target=".",
            config=MountConfig(create_policy=CreatePolicy.LAST),
        )
        assert entry.config.create_policy == CreatePolicy.LAST


class TestManifest:
    """Tests for Manifest model."""

    def test_empty_manifest(self) -> None:
        manifest = Manifest()
        assert manifest.apiVersion == "nue/v1"
        assert manifest.mounts == []
        assert manifest.defaults.create_policy == CreatePolicy.ROOT

    def test_manifest_with_defaults(self) -> None:
        manifest = Manifest(
            defaults=MountConfig(create_policy=CreatePolicy.ERROR),
        )
        assert manifest.defaults.create_policy == CreatePolicy.ERROR

    def test_manifest_with_mounts(self) -> None:
        manifest = Manifest(
            mounts=[
                MountEntry(source="./src", target="."),
                MountEntry(source="./libs", target="vendor/"),
            ]
        )
        assert len(manifest.mounts) == 2
        assert str(manifest.mounts[0].source) == "src"
        assert str(manifest.mounts[1].target) == "vendor"


class TestManifestLoad:
    """Tests for loading Manifest from YAML files."""

    def test_load_basic_manifest(self, tmp_path: pathlib.Path) -> None:
        yaml_content = """\
apiVersion: nue/v1
mounts:
- source: ./sources/project-a
  target: .
  exclude:
    - '*.pyc'
    - __pycache__/
    - .git/
- source: ./sources/libs
  target: vendor/
"""
        (tmp_path / "nue.yaml").write_text(yaml_content)

        manifest = Manifest.load(config_path=tmp_path / "nue.yaml")

        assert manifest.apiVersion == "nue/v1"
        assert len(manifest.mounts) == 2

        first = manifest.mounts[0]
        assert str(first.source) == "sources/project-a"
        assert str(first.target) == "."
        assert len(first.exclude) == 3
        assert first.exclude.match("test.pyc")

        second = manifest.mounts[1]
        assert str(second.source) == "sources/libs"
        assert str(second.target) == "vendor"

    def test_load_multi_mount_manifest(self, tmp_path: pathlib.Path) -> None:
        yaml_content = """\
apiVersion: nue/v1
mounts:
- source: ./sources/base
  target: .
- source: ./sources/override
  target: .
  config:
    create_policy: last
"""
        (tmp_path / "nue.yaml").write_text(yaml_content)

        manifest = Manifest.load(config_path=tmp_path / "nue.yaml")

        assert len(manifest.mounts) == 2
        assert manifest.mounts[0].config.create_policy == CreatePolicy.ROOT
        assert manifest.mounts[1].config.create_policy == CreatePolicy.LAST

    def test_load_manifest_with_global_defaults(self, tmp_path: pathlib.Path) -> None:
        yaml_content = """\
apiVersion: nue/v1
defaults:
  create_policy: first
mounts:
- source: ./src
  target: .
"""
        (tmp_path / "nue.yaml").write_text(yaml_content)

        manifest = Manifest.load(config_path=tmp_path / "nue.yaml")

        assert manifest.defaults.create_policy == CreatePolicy.FIRST

    def test_load_nonexistent_manifest(self, tmp_path: pathlib.Path) -> None:
        manifest = Manifest.load(config_path=tmp_path / "nue.yaml")
        assert manifest.mounts == []

    def test_load_from_fixtures(self) -> None:
        fixtures = pathlib.Path(__file__).parent / "fixtures"

        manifest = Manifest.load(config_path=fixtures / "nue.yaml")
        assert len(manifest.mounts) == 2
        assert str(manifest.mounts[0].source) == "sources/project-a"
        assert len(manifest.mounts[0].exclude) == 3

        multi = Manifest.load(config_path=fixtures / "nue-multi.yaml")
        assert len(multi.mounts) == 2
        assert multi.mounts[1].config.create_policy == CreatePolicy.LAST


class TestCreatePolicy:
    """Tests for CreatePolicy enum."""

    def test_all_policies(self) -> None:
        assert CreatePolicy.ROOT == "root"
        assert CreatePolicy.FIRST == "first"
        assert CreatePolicy.LAST == "last"
        assert CreatePolicy.ERROR == "error"

    def test_policy_from_string(self) -> None:
        assert CreatePolicy("root") == CreatePolicy.ROOT
        assert CreatePolicy("first") == CreatePolicy.FIRST
        assert CreatePolicy("last") == CreatePolicy.LAST
        assert CreatePolicy("error") == CreatePolicy.ERROR
