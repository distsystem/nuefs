import os
import pathlib
import signal
import sys
import time

from rich.panel import Panel
from sheaves.cli import Command, cli
from sheaves.console import console

import nuefs

from . import gitdir as gitdir_mod
from .manifest import Manifest


class NueBaseCommand(Manifest, Command, app_name="nue"):
    @property
    def root(self) -> pathlib.Path:
        return self.sheaf_source.parent


class Mount(NueBaseCommand):
    def run(self) -> None:
        git_path = self.root / ".git"
        if git_path.exists():
            gitdir_mod.ensure_external_gitdir(
                self.root, gitdir_mod.default_gitdir_root()
            )

        mounts = [
            nuefs.Mapping(
                target=pathlib.Path(str(entry.target)),
                source=pathlib.Path(str(entry.source)).expanduser(),
            )
            for entry in self.mounts
        ]
        with nuefs.open(self.root) as handle:
            handle.mount(mounts)


class Unmount(NueBaseCommand):
    def run(self) -> None:
        nuefs.open(self.root).close()


class Status(NueBaseCommand):
    def run(self) -> None:
        import humanize

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


class Stop(NueBaseCommand):
    def run(self) -> None:
        os.chdir("/")

        mounts = nuefs.status()
        for h in mounts:
            h.close()

        info = nuefs.daemon_info()
        pid = int(info.pid)

        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return

        for _ in range(40):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

        try:
            pathlib.Path(str(info.socket)).unlink(missing_ok=True)
        except OSError:
            pass


def main() -> int:
    cli(Mount | Unmount | Status | Stop).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
