import os
import pathlib
import subprocess
import sys
import time

import rich
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, CliSubCommand, SettingsConfigDict, get_subcommand
from rich.panel import Panel
from rich.tree import Tree

import nuefs
from nuefs._proto.nuefs import ManifestEntry

console = rich.get_console()

from nuefs import gitdir as gitdir_mod
from nuefs.gitconfig import NueGitConfig
from nuefs.manifest import Gitnue, MountEntry


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
        default=pathlib.Path(".gitnue"),
        validation_alias=AliasChoices("m", "manifest"),
    )
    dry_run: bool = Field(
        default=False,
        validation_alias=AliasChoices("n", "dry_run"),
    )

    def run(self) -> None:
        gitnue, root = Gitnue.load(path=self.manifest)

        config = NueGitConfig.from_repo(root)
        resolved_src = gitnue.resolve_sources(root, config)

        sources = list(gitnue.resolve_mounts(resolved_src))
        entries: dict[str, nuefs.ManifestEntry] = {}
        mount_roots: list[nuefs.MountRoot] = []
        external_vpaths: set[str] = set()
        for mount_entry, resolved in sources:
            entries.update(resolved)
            mr = mount_entry.mount_root(resolved_src[mount_entry.source])
            mount_roots.append(mr)
            if mr.backend_path != str(root):
                for vpath in resolved:
                    external_vpaths.add(vpath.split("/", 1)[0])

        _print_tree(root, sources, entries)

        if self.dry_run:
            return

        if external_vpaths:
            gitdir_mod.sync_git_exclude(root, external_vpaths)

        client = nuefs.NueFs()
        mount = client.connect(root)
        mount.update(list(entries.values()), mount_roots)
        console.print(
            Panel(
                "Mount created, but your current shell is already inside the directory.\n"
                "Re-enter it to see the mounted view:\n\n"
                "  cd .. && cd -\n",
                title="git nue mount",
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
        if not nuefs.daemon_running(socket_path):
            try:
                _lazy_unmount(pathlib.Path(root))
            except RuntimeError:
                pass
            return

        client = nuefs.NueFs()
        for mount in client.status():
            if os.path.normpath(mount.root) == root:
                mount.unmount()
                return


class Status(BaseSettings):
    def run(self) -> None:
        import humanize

        client = nuefs.NueFs()
        info = client.info()
        uptime = int(time.time()) - info.started_at
        mounts = client.status()

        lines = [
            f"[bold]pid:[/] {info.pid}",
            f"[bold]socket:[/] {info.socket}",
            f"[bold]uptime:[/] {humanize.naturaldelta(uptime)}",
            f"[bold]mounts:[/] {len(mounts)}",
        ]
        for mount in mounts:
            lines.append(f"  {mount.root}")

        console.print(Panel("\n".join(lines), title="nuefsd", border_style="dim"))


class Stop(BaseSettings):
    def run(self) -> None:
        socket_path = nuefs.default_socket_path()
        if not nuefs.daemon_running(socket_path):
            console.print("[dim]daemon not running[/]")
            return

        nuefs.NueFs().shutdown()
        console.print("[green]daemon stopped[/]")


def _print_tree(
    root: pathlib.Path,
    sources: list[tuple[MountEntry, dict[str, ManifestEntry]]],
    entries: dict[str, ManifestEntry],
) -> None:
    source_tree = Tree(f"[bold blue]{root}[/] [dim](sources)[/]")
    for mount_entry, resolved in sources:
        branch = source_tree.add(f"[bold yellow]{mount_entry.source}[/]")
        _render_entries(branch, resolved)
    console.print(source_tree)
    console.print()

    merged_tree = Tree(f"[bold blue]{root}[/] [dim](merged)[/]")
    _render_entries(merged_tree, entries)
    console.print(merged_tree)


def _render_entries(root_node: Tree, entries: dict[str, ManifestEntry]) -> None:
    for entry in sorted(entries.values(), key=lambda e: e.virtual_path):
        vpath = entry.virtual_path
        if entry.is_dir:
            label = f"[bold cyan]{vpath}/[/] [dim]→ {entry.backend_path}[/]"
        else:
            label = f"{vpath} [dim]→ {entry.backend_path}[/]"
        root_node.add(label)


class GitNue(BaseSettings):
    model_config = SettingsConfigDict(cli_parse_args=True, cli_implicit_flags=True)

    mount: CliSubCommand[Mount]
    unmount: CliSubCommand[Unmount]
    status: CliSubCommand[Status]
    stop: CliSubCommand[Stop]


def main() -> int:
    cmd = get_subcommand(GitNue())
    cmd.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
