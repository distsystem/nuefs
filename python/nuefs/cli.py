import os
import pathlib
import signal
import subprocess
import sys
import time

from rich.panel import Panel
from sheaves.cli import Command, cli
from sheaves.console import console

import nuefs
from nuefs.lockfile import Lock

from . import gitdir as gitdir_mod
from .manifest import Manifest


def _lazy_unmount(root: pathlib.Path) -> None:
    for cmd in ("fusermount3", "fusermount"):
        try:
            subprocess.run(
                [cmd, "-uz", str(root)],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return
        except FileNotFoundError:
            continue
        except subprocess.CalledProcessError:
            continue

    msg = "failed to lazy-unmount; fusermount3/fusermount not available or mount is still busy"
    raise RuntimeError(msg)


class NueBaseCommand(Manifest, Command, app_name="nue"):
    @property
    def root(self) -> pathlib.Path:
        return self.sheaf_source.parent


class Mount(NueBaseCommand):
    def run(self) -> None:
        cwd = os.getcwd()
        root = os.fspath(self.root)

        git_path = self.root / ".git"
        if git_path.exists():
            gitdir_mod.ensure_external_gitdir(
                self.root, gitdir_mod.default_gitdir_root()
            )

        mounts = []
        for entry in self.mounts:
            source = pathlib.Path(str(entry.source)).expanduser()
            if not source.is_absolute():
                source = (self.root / source).resolve()
            mounts.append((source, str(entry.target)))
        lock = Lock.compile(self.root, mounts)
        lock.save(self.root / "nue.lock")

        os.chdir("/")

        nuefs.mount(self.root, lock.entries)

        if cwd == root or cwd.startswith(f"{root}{os.sep}"):
            console.print(
                Panel(
                    "Mount created, but your current shell is already inside the directory.\n"
                    "Re-enter it to see the mounted view:\n\n"
                    f"  cd .. && cd {root}\n",
                    title="nue mount",
                    border_style="yellow",
                )
            )


class Unmount(NueBaseCommand):
    def run(self) -> None:
        cwd = os.getcwd()
        root = os.fspath(self.root)
        os.chdir("/")

        # If the caller is currently inside the mountpoint, a normal unmount will be EBUSY.
        if cwd == root or cwd.startswith(f"{root}{os.sep}"):
            _lazy_unmount(pathlib.Path(root))
            return

        nuefs.open(pathlib.Path(root)).close()


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
        cwd = os.getcwd()
        os.chdir("/")

        mounts = nuefs.status()
        failures: list[str] = []
        for h in mounts:
            try:
                h.close()
            except (OSError, RuntimeError) as e:
                try:
                    root = pathlib.Path(h.root)
                    root_s = os.fspath(root)
                    if cwd == root_s or cwd.startswith(f"{root_s}{os.sep}"):
                        _lazy_unmount(root)
                    else:
                        failures.append(f"{h.root}: {e}")
                except (OSError, RuntimeError) as e2:
                    failures.append(f"{h.root}: {e} (lazy unmount failed: {e2})")

        if failures:
            console.print(
                Panel(
                    "\n".join(["failed to unmount:", *failures]),
                    title="nue stop",
                    border_style="red",
                )
            )
            raise SystemExit(1)

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
