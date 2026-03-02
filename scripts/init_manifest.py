"""Generate default .gitnue manifest."""

from pathlib import Path

import yaml

from nuefs.manifest import Gitnue, MountEntry


def create_example_gitnue() -> Gitnue:
    """Create an example manifest with common defaults."""
    return Gitnue(
        mounts=[
            MountEntry(
                source="~/repos/example",
                to=".",
                exclude=["*.pyc", "__pycache__/", ".git/"],
                include=["src/", "tests/"],
            ),
            MountEntry(
                source="~/local/lib",
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
