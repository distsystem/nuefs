"""NueFS CLI - layered filesystem management."""

import json
import pathlib
import sys
import time
from typing import Annotated

import sheaves.cli

import nuefs


def _load_mounts(config_path: pathlib.Path) -> list[nuefs.Mapping]:
    data = json.loads(config_path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        mounts = data.get("mounts")
    else:
        mounts = data

    if not isinstance(mounts, list):
        raise ValueError("Invalid config: expected a list or an object with 'mounts' list")

    result: list[nuefs.Mapping] = []
    for item in mounts:
        if not isinstance(item, dict):
            raise ValueError("Invalid mount entry: expected an object")

        target = pathlib.Path(str(item.get("target", "")))
        source = pathlib.Path(str(item.get("source", ""))).expanduser()
        result.append(nuefs.Mapping(target=target, source=source))

    return result


class NueFSSheaf(sheaves.cli.Command, app_name="nuefs"):
    """Base command for NueFS layered filesystem."""


class Mount(NueFSSheaf):
    """Mount NueFS filesystem."""

    root: Annotated[pathlib.Path, sheaves.cli.Positional("Mount root directory")]
    config: Annotated[pathlib.Path | None, sheaves.cli.Option(help="JSON config file with mounts")] = None
    foreground: Annotated[bool, sheaves.cli.Flag(help="Run in foreground", short="-f")] = False

    def run(self) -> None:
        root = self.root.expanduser()
        if self.config is None:
            raise ValueError("--config is required (JSON file with mounts)")

        mounts = _load_mounts(self.config.expanduser())
        handle = nuefs.open(root, mounts)
        if not self.foreground:
            return

        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            handle.close()


class Unmount(NueFSSheaf):
    """Unmount NueFS filesystem."""

    root: Annotated[pathlib.Path, sheaves.cli.Positional("Mount root directory")]

    def run(self) -> None:
        nuefs.open(self.root.expanduser()).close()


class Which(NueFSSheaf):
    """Query path ownership in mounted NueFS."""

    root: Annotated[pathlib.Path, sheaves.cli.Positional("Mount root directory")]
    path: Annotated[str, sheaves.cli.Positional("Path to query")]

    def run(self) -> None:
        info = nuefs.open(self.root.expanduser()).which(self.path)
        if info is None:
            print("not found")
            return
        print(f"owner={info.owner} backend_path={info.backend_path}")


class Status(NueFSSheaf):
    """Show NueFS mount status."""

    def run(self) -> None:
        for h in nuefs.status():
            print(f"{h.root}")


def main() -> int:
    sheaves.cli.cli(Mount | Unmount | Which | Status).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
