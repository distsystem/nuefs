"""NueFS CLI - layered filesystem management."""

import sys
from pathlib import Path
from typing import Annotated

import tyro
from sheaves.cli import Command, cli

import nuefs


class NueFSSheaf(Command, app_name="nuefs"):
    """Base command for NueFS layered filesystem."""


class Mount(NueFSSheaf):
    """Mount NueFS filesystem."""

    root: Annotated[Path, tyro.conf.Positional]
    config: Path | None = None
    foreground: bool = False

    def run(self) -> None:
        ...


class Unmount(NueFSSheaf):
    """Unmount NueFS filesystem."""

    root: Annotated[Path, tyro.conf.Positional]

    def run(self) -> None:
        ...


class Which(NueFSSheaf):
    """Query path ownership in mounted NueFS."""

    root: Annotated[Path, tyro.conf.Positional]
    path: Annotated[str, tyro.conf.Positional]

    def run(self) -> None:
        ...


class Status(NueFSSheaf):
    """Show NueFS mount status."""

    root: Annotated[Path | None, tyro.conf.Positional] = None

    def run(self) -> None:
        ...


def main() -> int:
    cli(Mount | Unmount | Which | Status).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
