"""Generate default .gitnue manifest."""

from pathlib import Path

import yaml

from nuefs.manifest import Gitnue, MountEntry, PathSource


def create_example_gitnue() -> Gitnue:
    """Create an example manifest with common defaults."""
    return Gitnue(
        sources={
            "example": PathSource(path=Path("~/repos/example")),
            "lib": PathSource(path=Path("~/local/lib")),
        },
        mounts=[
            MountEntry(
                source="example",
                to=".",
                exclude=["*.pyc", "__pycache__/", ".git/"],
                include=["src/", "tests/"],
            ),
            MountEntry(
                source="lib",
                to="vendor/lib",
            ),
        ],
    )


def main() -> None:
    output = Path(".gitnue")
    gitnue = create_example_gitnue()
    data = gitnue.model_dump(mode="json", by_alias=True, exclude_defaults=True)
    data["version"] = 1
    output.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
    print(f"Generated {output}")


if __name__ == "__main__":
    main()
