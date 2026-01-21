"""NueFS CLI - layered filesystem management."""

import json
import pathlib
import sys
import time
from typing import Annotated

import sheaves.cli

import nuefs
from nuefs import workspace


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
    """Base command for NueFS layered filesystem."""


class Mount(NueBaseCommand):
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


class Unmount(NueBaseCommand):
    """Unmount NueFS filesystem."""

    root: Annotated[pathlib.Path, sheaves.cli.Positional("Mount root directory")]

    def run(self) -> None:
        nuefs.open(self.root.expanduser()).close()


class Which(NueBaseCommand):
    """Query path ownership in mounted NueFS."""

    root: Annotated[pathlib.Path | None, sheaves.cli.Positional("Mount root directory")] = None
    path: Annotated[str, sheaves.cli.Positional("Path to query")]

    def run(self) -> None:
        if self.root is not None:
            root = self.root.expanduser()
        else:
            root = workspace.find_workspace()
        info = nuefs.open(root).which(self.path)
        if info is None:
            print("not found")
            return
        print(f"owner={info.owner} backend_path={info.backend_path}")


class Status(NueBaseCommand):
    """Show NueFS mount status."""

    def run(self) -> None:
        for h in nuefs.status():
            print(f"{h.root}")


class Lock(NueBaseCommand):
    """Generate nue.lock from nue.yaml manifest."""

    def run(self) -> None:
        ws = workspace.find_workspace()
        manifest = workspace.load_manifest(ws)
        lock = workspace.generate_lock(manifest, ws)
        workspace.write_lock(lock, ws)
        print(f"Generated {workspace.LOCK_NAME} with {len(lock.mappings)} mappings")


class Apply(NueBaseCommand):
    """Apply nue.lock to mount filesystem (idempotent)."""

    def run(self) -> None:
        ws = workspace.find_workspace()
        manifest = workspace.load_manifest(ws)
        lock = workspace.load_lock(ws)

        if not workspace.validate_lock(lock, manifest):
            raise workspace.LockOutdatedError(
                f"{workspace.LOCK_NAME} is outdated. Run 'nue lock' first."
            )

        mounts = workspace.manifest_to_mounts(manifest)

        # Idempotent: update if already mounted, create otherwise
        try:
            handle = nuefs.open(ws)
            handle.update(mounts)
            print(f"Updated mount at {ws}")
        except RuntimeError:
            nuefs.open(ws, mounts)
            print(f"Created mount at {ws}")


class Sync(NueBaseCommand):
    """Lock and apply in one step (lock + apply)."""

    def run(self) -> None:
        ws = workspace.find_workspace()
        manifest = workspace.load_manifest(ws)
        lock = workspace.generate_lock(manifest, ws)
        workspace.write_lock(lock, ws)
        print(f"Generated {workspace.LOCK_NAME} with {len(lock.mappings)} mappings")

        mounts = workspace.manifest_to_mounts(manifest)

        try:
            handle = nuefs.open(ws)
            handle.update(mounts)
            print(f"Updated mount at {ws}")
        except RuntimeError:
            nuefs.open(ws, mounts)
            print(f"Created mount at {ws}")


def main() -> int:
    sheaves.cli.cli(Mount | Unmount | Which | Status | Init | Lock | Apply | Sync).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
