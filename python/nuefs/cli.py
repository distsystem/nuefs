import os
import pathlib
import subprocess
import sys
import time

import rich
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, CliSubCommand, SettingsConfigDict, get_subcommand
from rich.panel import Panel

import nuefs

console = rich.get_console()

from . import gitdir as gitdir_mod
from .manifest import Manifest, ensure_ancestors, print_tree


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


class Mount(BaseSettings):
    manifest: pathlib.Path = Field(
        default=pathlib.Path("nue.yaml"),
        validation_alias=AliasChoices("m", "manifest"),
    )
    dry_run: bool = Field(
        default=False,
        validation_alias=AliasChoices("n", "dry_run"),
    )

    def run(self) -> None:
        manifest, root = Manifest.load(path=self.manifest)

        if not self.dry_run:
            git_path = root / ".git"
            if git_path.exists():
                gitdir_mod.ensure_external_gitdir(
                    root, gitdir_mod.default_gitdir_root()
                )

        sources = list(manifest.resolve_mounts(root))
        entries: dict[str, nuefs.ManifestEntry] = {}
        mount_roots: list[nuefs.MountRoot] = []
        external_vpaths: set[str] = set()
        for mount_entry, resolved in sources:
            entries.update(resolved)
            source, prefix, _ = mount_entry._resolve_source(root)
            mount_roots.append(
                nuefs.MountRoot(virtual_prefix=prefix, backend_path=source)
            )
            if source != root:
                for vpath in resolved:
                    external_vpaths.add(vpath.split("/", 1)[0])

        entries = ensure_ancestors(entries)

        print_tree(root, sources, entries)

        if self.dry_run:
            return

        if external_vpaths:
            gitdir_mod.sync_git_exclude(root, external_vpaths)

        with nuefs.open(root) as h:
            h.update(list(entries.values()), mount_roots)
            console.print(
                Panel(
                    "Mount created, but your current shell is already inside the directory.\n"
                    "Re-enter it to see the mounted view:\n\n"
                    "  cd .. && cd -\n",
                    title="nue mount",
                    border_style="yellow",
                )
            )


class Unmount(BaseSettings):
    root: pathlib.Path = Field(
        default=pathlib.Path("."),
        validation_alias=AliasChoices("r", "root"),
    )

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
                h.unmount()
                return


class Status(BaseSettings):
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


class Stop(BaseSettings):
    def run(self) -> None:
        socket_path = nuefs.default_socket_path()
        if not _daemon_running(socket_path):
            console.print("[dim]daemon not running[/]")
            return

        nuefs.shutdown()
        console.print("[green]daemon stopped[/]")


def _daemon_running(socket_path: pathlib.Path) -> bool:
    import socket as sock

    try:
        s = sock.socket(sock.AF_UNIX, sock.SOCK_STREAM)
        s.connect(str(socket_path))
        s.close()
        return True
    except (FileNotFoundError, ConnectionRefusedError, OSError):
        return False


class Nue(BaseSettings):
    model_config = SettingsConfigDict(cli_parse_args=True, cli_implicit_flags=True)

    mount: CliSubCommand[Mount]
    unmount: CliSubCommand[Unmount]
    status: CliSubCommand[Status]
    stop: CliSubCommand[Stop]


def main() -> int:
    cmd = get_subcommand(Nue())
    cmd.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
