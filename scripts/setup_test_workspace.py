"""Set up test workspace at /tmp/nue-test for manual testing."""

import shutil
from pathlib import Path

import yaml

WORKSPACE_ROOT = Path("/tmp/nue-test")
FIXTURES_DIR = Path(__file__).parent.parent / "tests" / "fixtures"


def create_source_files(sources_dir: Path) -> None:
    """Create mock source files for testing."""
    # project-a: main project source
    project_a = sources_dir / "project-a"
    (project_a / "src").mkdir(parents=True)
    (project_a / "tests").mkdir(parents=True)

    (project_a / "src" / "main.py").write_text("""\
\"\"\"Main module for project-a.\"\"\"


def main() -> None:
    print("Hello from project-a!")


if __name__ == "__main__":
    main()
""")

    (project_a / "tests" / "test_main.py").write_text("""\
\"\"\"Tests for main module.\"\"\"


def test_main() -> None:
    assert True
""")

    # libs: vendor libraries
    libs = sources_dir / "libs"
    libs.mkdir(parents=True)

    (libs / "utils.py").write_text("""\
\"\"\"Utility functions.\"\"\"


def helper() -> str:
    return "helper from libs"
""")

    # base: base layer for overlay test
    base = sources_dir / "base"
    base.mkdir(parents=True)

    (base / "config.txt").write_text("base config\n")
    (base / "base_only.txt").write_text("only in base\n")

    # override: override layer for overlay test
    override = sources_dir / "override"
    override.mkdir(parents=True)

    (override / "config.txt").write_text("override config\n")
    (override / "override_only.txt").write_text("only in override\n")


def setup_workspace(fixture: str = ".gitnue") -> Path:
    """Set up test workspace with the specified fixture."""
    # Clean up existing workspace
    if WORKSPACE_ROOT.exists():
        shutil.rmtree(WORKSPACE_ROOT)

    workspace = WORKSPACE_ROOT / "workspace"
    sources = WORKSPACE_ROOT / "sources"

    workspace.mkdir(parents=True)
    sources.mkdir(parents=True)

    # Create source files
    create_source_files(sources)

    # Copy manifest fixture
    fixture_path = FIXTURES_DIR / fixture
    if not fixture_path.exists():
        msg = f"Fixture not found: {fixture_path}"
        raise FileNotFoundError(msg)

    shutil.copy(fixture_path, workspace / ".gitnue")

    # The fixture manifests use ./sources/* paths, but this helper creates sources
    # as a sibling of workspace (../sources/*). Rewrite path sources at YAML level.
    manifest_path = workspace / ".gitnue"
    data = yaml.safe_load(manifest_path.read_text()) or {}
    for src_def in (data.get("sources") or {}).values():
        if "path" in src_def:
            src_def["path"] = src_def["path"].replace("./sources/", "../sources/")
    manifest_path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))

    return workspace


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Set up NueFS test workspace")
    parser.add_argument(
        "--fixture",
        default=".gitnue",
        help="Fixture file to use (default: .gitnue)",
    )
    args = parser.parse_args()

    workspace = setup_workspace(args.fixture)

    print(f"Test workspace created at: {WORKSPACE_ROOT}")
    print(f"Workspace directory: {workspace}")
    print(f"Using fixture: {args.fixture}")
    print()
    print("Directory structure:")
    print(f"  {WORKSPACE_ROOT}/")
    print(f"  ├── workspace/")
    print(f"  │   └── .gitnue")
    print(f"  └── sources/")
    print(f"      ├── project-a/")
    print(f"      │   ├── src/main.py")
    print(f"      │   └── tests/test_main.py")
    print(f"      ├── libs/utils.py")
    print(f"      ├── base/")
    print(f"      │   ├── config.txt")
    print(f"      │   └── base_only.txt")
    print(f"      └── override/")
    print(f"          ├── config.txt")
    print(f"          └── override_only.txt")
    print()
    print("To test mounting:")
    print(f"  cd {workspace} && pixi run git-nue mount .")


if __name__ == "__main__":
    main()
