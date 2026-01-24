import json
import pathlib
import sys
import time
from typing import Annotated

import humanize
import sheaves.cli
from rich.panel import Panel
from sheaves.console import console

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


class NueBaseCommand(sheaves.cli.Command, app_name="nue"):
    pass


class Mount(NueBaseCommand):

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


class Unmount(NueBaseCommand):

    root: Annotated[pathlib.Path, sheaves.cli.Positional("Mount root directory")]

    def run(self) -> None:
        nuefs.open(self.root.expanduser()).close()


class Status(NueBaseCommand):

    def run(self) -> None:
        info = nuefs.daemon_info()
        uptime = int(time.time()) - info.started_at
        mounts = nuefs.status()

        lines = [
            f"[bold]pid:[/] {info.pid}",
            f"[bold]socket:[/] {info.socket}",
            f"[bold]uptime:[/] {humanize.naturaldelta(uptime)}",
            f"[bold]mounts:[/] {len(mounts)}",
        ]
        for h in mounts:
            lines.append(f"  {h.root}")

        console.print(Panel("\n".join(lines), title="nuefsd", border_style="dim"))


def main() -> int:
    sheaves.cli.cli(Mount | Unmount | Status).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
