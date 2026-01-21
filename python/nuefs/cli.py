import functools
import json
import pathlib
import sys
import time
import typing
from typing import Annotated

import pathspec
import platformdirs
import sheaves.cli
import yaml
from sheaves.resource import FileURL, Resource

import nuefs
from nuefs.manifest import LockMapping, MountEntry, NueLock, NueManifest

STATE_DIR = pathlib.Path(platformdirs.user_state_dir("nue"))
MANIFEST_NAME = "nue.yaml"
LOCK_NAME = "nue.lock"


class WorkspaceNotFoundError(Exception):
    pass


class LockNotFoundError(Exception):
    pass


class Workspace:
    root: pathlib.Path
    manifest: NueManifest

    def __init__(self, root: pathlib.Path, manifest: NueManifest) -> None:
        self.root = root
        self.manifest = manifest

    @classmethod
    def find(cls, start: pathlib.Path | None = None) -> typing.Self:
        current = (start or pathlib.Path.cwd()).resolve()

        while current != current.parent:
            if (current / MANIFEST_NAME).is_file():
                break
            current = current.parent
        else:
            if not (current / MANIFEST_NAME).is_file():
                raise WorkspaceNotFoundError(f"No {MANIFEST_NAME} found")

        data = yaml.safe_load((current / MANIFEST_NAME).read_text(encoding="utf-8"))
        return cls(current, NueManifest.model_validate(data))

    @property
    def mounts(self) -> list[nuefs.Mapping]:
        return [
            nuefs.Mapping(target=e.target, source=_resolve_source(e.source))
            for e in self.manifest.mounts
        ]

    def lock(self) -> NueLock:
        mappings: list[LockMapping] = []
        for entry in self.manifest.mounts:
            mappings.extend(_resolve_mappings(entry))

        lock = NueLock(apiVersion="nue/v1", mappings=mappings)
        data = lock.model_dump(mode="json")
        (self.root / LOCK_NAME).write_text(
            yaml.safe_dump(data, sort_keys=False), encoding="utf-8"
        )
        return lock

    def apply(self) -> nuefs.Handle:
        lock_path = self.root / LOCK_NAME
        if not lock_path.exists():
            raise LockNotFoundError(f"No {LOCK_NAME} found, run lock() first")

        data = yaml.safe_load(lock_path.read_text(encoding="utf-8"))
        lock = NueLock.model_validate(data)
        mounts = [
            nuefs.Mapping(target=m.target, source=m.source)
            for m in lock.mappings
        ]

        try:
            handle = nuefs.open(self.root)
            handle.update(mounts)
        except RuntimeError:
            handle = nuefs.open(self.root, mounts)
        return handle

    def sync(self) -> nuefs.Handle:
        self.lock()
        return self.apply()


def _resolve_source(source: Resource) -> pathlib.Path:
    if isinstance(source.root, FileURL):
        return source.root.to_path().expanduser().resolve()
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return source.materialize(STATE_DIR)


def _resolve_mappings(entry: MountEntry) -> list[LockMapping]:
    source = _resolve_source(entry.source)
    target = entry.target

    if not source.exists():
        return []

    if source.is_file():
        return [LockMapping(target=target, source=source)]

    all_files = [p.relative_to(source) for p in source.rglob("*") if p.is_file()]

    if entry.include:
        spec = pathspec.PathSpec.from_lines("gitwildmatch", entry.include)
        filtered = [f for f in all_files if spec.match_file(str(f))]
    elif entry.exclude:
        spec = pathspec.PathSpec.from_lines("gitwildmatch", entry.exclude)
        filtered = [f for f in all_files if not spec.match_file(str(f))]
    else:
        filtered = all_files

    return [
        LockMapping(target=target / rel, source=source / rel)
        for rel in sorted(filtered)
    ]


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

    @functools.cached_property
    def workspace(self) -> Workspace:
        return Workspace.find()


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


class Which(NueBaseCommand):

    root: Annotated[pathlib.Path | None, sheaves.cli.Positional("Mount root directory")] = None
    path: Annotated[str, sheaves.cli.Positional("Path to query")]

    def run(self) -> None:
        if self.root is not None:
            root = self.root.expanduser()
        else:
            root = self.workspace.root
        info = nuefs.open(root).which(self.path)
        if info is None:
            print("not found")
            return
        print(f"owner={info.owner} backend_path={info.backend_path}")


class Status(NueBaseCommand):

    def run(self) -> None:
        for h in nuefs.status():
            print(f"{h.root}")


class Lock(NueBaseCommand):

    def run(self) -> None:
        lock = self.workspace.lock()
        print(f"Generated {LOCK_NAME} with {len(lock.mappings)} mappings")


class Apply(NueBaseCommand):

    def run(self) -> None:
        self.workspace.apply()
        print(f"Applied mount at {self.workspace.root}")


class Sync(NueBaseCommand):

    def run(self) -> None:
        lock = self.workspace.lock()
        print(f"Generated {LOCK_NAME} with {len(lock.mappings)} mappings")
        self.workspace.apply()
        print(f"Applied mount at {self.workspace.root}")


def main() -> int:
    sheaves.cli.cli(Mount | Unmount | Which | Status | Lock | Apply | Sync).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
