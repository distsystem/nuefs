"""Shared fixtures for NueFS tests."""

import pathlib

import pytest

import nuefs._nuefs as _ext
from nuefs.manifest import Manifest, MountEntry, ensure_ancestors

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


def setup_test_dirs(root: pathlib.Path) -> pathlib.Path:
    """Create test directory structure under *root*.

    root/
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
    (root / "project-a" / "lib" / "deep").mkdir(parents=True)
    (root / "project-a" / "lib" / "deep" / "mod.py").write_text("# mod")
    (root / "project-a" / "main.py").write_text("# main")
    (root / "project-a" / "__pycache__").mkdir()
    (root / "project-a" / "__pycache__" / "main.cpython-312.pyc").write_bytes(b"\x00")

    (root / "libs" / "helpers").mkdir(parents=True)
    (root / "libs" / "utils.py").write_text("# utils")
    (root / "libs" / "helpers" / "fmt.py").write_text("# fmt")

    return root


@pytest.fixture
def setup_dirs(tmp_path: pathlib.Path) -> pathlib.Path:
    return setup_test_dirs(tmp_path)


@pytest.fixture
def resolved_manifest(
    tmp_path: pathlib.Path,
) -> tuple[
    pathlib.Path,
    dict[str, _ext.ManifestEntry],
    list[_ext.MountRoot],
    list[tuple[MountEntry, dict[str, _ext.ManifestEntry]]],
]:
    root = setup_test_dirs(tmp_path)
    (root / "nue.yaml").write_text(YAML_CONTENT)

    manifest, cfg_root = Manifest.load(path=root / "nue.yaml")
    sources = list(manifest.resolve_mounts(root))

    entries: dict[str, _ext.ManifestEntry] = {}
    mount_roots: list[_ext.MountRoot] = []
    for mount_entry, resolved in sources:
        entries.update(resolved)
        source, prefix, _ = mount_entry._resolve_source(root)
        mount_roots.append(_ext.MountRoot(virtual_prefix=prefix, backend_path=source))
    entries = ensure_ancestors(entries)

    return root, entries, mount_roots, sources
