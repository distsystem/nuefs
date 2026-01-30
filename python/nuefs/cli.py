import os
import pathlib
import subprocess
import sys
import time
from typing import Annotated

import pydantic
from rich.panel import Panel
from rich.tree import Tree
from sheaves.annotations import Commands, Flag, Option, Readonly
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
    @pydantic.computed_field
    @property
    def root(self) -> Annotated[pathlib.Path, Readonly]:
        return self.sheaf_source.parent


class Mount(NueBaseCommand):
    dry_run: Annotated[
        bool, Flag(help="Show virtual tree without mounting", short="-n")
    ] = False

    def run(self) -> None:
        cwd = os.getcwd()
        root = os.fspath(self.root)

        git_path = self.root / ".git"
        if git_path.exists() and not self.dry_run:
            gitdir_mod.ensure_external_gitdir(
                self.root, gitdir_mod.default_gitdir_root()
            )

        lock = Lock.compile(self.root, self.mounts)

        if self.dry_run:
            self._print_tree(lock)
            return

        lock.save(self.root / "nue.lock")

        os.chdir("/")

        handle = nuefs.open(self.root)
        handle.update(lock.entries)

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

    def _print_tree(self, lock: Lock) -> None:
        """Print the virtual file tree without actually mounting."""
        tree = Tree(f"[bold blue]{self.root}[/]")
        nodes: dict[str, Tree] = {"": tree}

        entries = sorted(lock.entries, key=lambda e: e.virtual_path)

        for entry in entries:
            parts = pathlib.PurePosixPath(entry.virtual_path).parts
            parent_path = ""

            for i, part in enumerate(parts):
                current_path = "/".join(parts[: i + 1])
                if current_path in nodes:
                    parent_path = current_path
                    continue

                parent_node = nodes[parent_path]
                is_last = i == len(parts) - 1

                if is_last:
                    if entry.is_dir:
                        label = f"[bold cyan]{part}/[/] [dim]→ {entry.backend_path}[/]"
                    else:
                        label = f"{part} [dim]→ {entry.backend_path}[/]"
                else:
                    label = f"[bold cyan]{part}/[/]"

                nodes[current_path] = parent_node.add(label)
                parent_path = current_path

        console.print(tree)


class Unmount(Command, app_name="nue"):
    root: Annotated[
        pathlib.Path,
        Option(help="Mount root path to unmount", short="-r", metavar="PATH"),
    ] = pathlib.Path(".")

    def run(self) -> None:
        root_path = self.root.expanduser()
        root = os.path.normpath(os.path.abspath(os.fspath(root_path)))
        os.chdir("/")

        socket_path = nuefs.default_socket_path()
        if not _daemon_running(socket_path):
            try:
                _lazy_unmount(pathlib.Path(root))
            except RuntimeError:
                pass
            return

        for h in nuefs.status():
            if os.path.normpath(h.root) == root:
                h.close()
                return


class Umount(Unmount):
    pass


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


class Stop(Command, app_name="nue"):
    def run(self) -> None:
        socket_path = nuefs.default_socket_path()
        if not _daemon_running(socket_path):
            console.print("[dim]daemon not running[/]")
            return

        nuefs.shutdown()
        console.print("[green]daemon stopped[/]")


class Start(Command, app_name="nue"):
    """Start the daemon in the foreground (for debugging)."""

    def run(self) -> None:
        socket_path = nuefs.default_socket_path()

        if _daemon_running(socket_path):
            info = nuefs.daemon_info()
            console.print(
                Panel(
                    f"[bold]pid:[/] {info.pid}\n[bold]socket:[/] {info.socket}",
                    title="nuefsd already running",
                    border_style="yellow",
                )
            )
            return

        daemon_bin = os.environ.get("NUEFSD_BIN") or _find_nuefsd()
        cmd = [daemon_bin, "--socket", str(socket_path), "--log", "-"]
        console.print(f"[dim]Starting: {' '.join(cmd)}[/]")
        os.chdir("/")
        os.execvp(cmd[0], cmd)


def _daemon_running(socket_path: pathlib.Path) -> bool:
    import socket as sock

    try:
        s = sock.socket(sock.AF_UNIX, sock.SOCK_STREAM)
        s.connect(str(socket_path))
        s.close()
        return True
    except (FileNotFoundError, ConnectionRefusedError, OSError):
        return False


def _find_nuefsd() -> str:
    import shutil

    if found := shutil.which("nuefsd"):
        return found

    bin_dir = pathlib.Path(sys.executable).parent
    candidate = bin_dir / "nuefsd"
    if candidate.exists():
        return str(candidate)

    raise FileNotFoundError(
        "nuefsd not found; install it or set NUEFSD_BIN environment variable"
    )


def main() -> int:
    cli(Annotated[Mount | Unmount | Umount | Status | Start | Stop, Commands()]).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
