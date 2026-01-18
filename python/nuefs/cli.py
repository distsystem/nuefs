"""NueFS CLI - layered filesystem management."""

import json
import pathlib
import sys
import time
import typing

import tyro

import sheaves.cli

import nuefs


def _load_mounts(config_path: pathlib.Path) -> list[nuefs.Mount]:
    data = json.loads(config_path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        mounts = data.get("mounts")
    else:
        mounts = data

    if not isinstance(mounts, list):
        raise ValueError("Invalid config: expected a list or an object with 'mounts' list")

    result: list[nuefs.Mount] = []
    for item in mounts:
        if not isinstance(item, dict):
            raise ValueError("Invalid mount entry: expected an object")

        target = pathlib.Path(str(item.get("target", "")))
        source = pathlib.Path(str(item.get("source", ""))).expanduser()
        result.append(nuefs.Mount(target=target, source=source))

    return result


class NueFSSheaf(sheaves.cli.Command, app_name="nuefs"):
    """Base command for NueFS layered filesystem."""


class Mount(NueFSSheaf):
    """Mount NueFS filesystem."""

    root: typing.Annotated[pathlib.Path, tyro.conf.Positional]
    config: pathlib.Path | None = None
    foreground: bool = False

    def run(self) -> None:
        root = self.root.expanduser()
        if self.config is None:
            raise ValueError("--config is required (JSON file with mounts)")

        mounts = _load_mounts(self.config.expanduser())
        handle = nuefs.mount(root, mounts)
        if not self.foreground:
            return

        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            nuefs.unmount(handle)


class Unmount(NueFSSheaf):
    """Unmount NueFS filesystem."""

    root: typing.Annotated[pathlib.Path, tyro.conf.Positional]

    def run(self) -> None:
        nuefs.unmount_root(self.root.expanduser())


class Which(NueFSSheaf):
    """Query path ownership in mounted NueFS."""

    root: typing.Annotated[pathlib.Path, tyro.conf.Positional]
    path: typing.Annotated[str, tyro.conf.Positional]

    def run(self) -> None:
        info = nuefs.which_root(self.root.expanduser(), self.path)
        if info is None:
            print("not found")
            return
        print(f"owner={info.owner} backend_path={info.backend_path}")


class Status(NueFSSheaf):
    """Show NueFS mount status."""

    root: typing.Annotated[pathlib.Path | None, tyro.conf.Positional] = None

    def run(self) -> None:
        root = self.root.expanduser() if self.root is not None else None
        mounts = nuefs.status(root)
        for m in mounts:
            print(f"{m.mount_id}\t{m.root}")


def main() -> int:
    sheaves.cli.cli(Mount | Unmount | Which | Status).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())

