"""Generate default nue.yaml manifest."""

from pathlib import Path

from nuefs.manifest import Manifest, MountEntry


def create_example_manifest() -> Manifest:
    """Create an example manifest with common defaults."""
    return Manifest(
        mounts=[
            MountEntry(
                source="~/repos/example",
                dest=".",
                exclude=["*.pyc", "__pycache__/", ".git/"],
                include=["src/", "tests/"],
            ),
            MountEntry(
                source="~/local/lib",
                dest="vendor/lib",
            ),
        ],
    )


def main() -> None:
    output = Path("nue.yaml")
    manifest = create_example_manifest()
    manifest.apiVersion = "nue/v1"
    manifest.save(output)
    print(f"Generated {output}")


if __name__ == "__main__":
    main()
