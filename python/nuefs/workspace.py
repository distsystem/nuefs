"""NueFS workspace management - manifest parsing and lock generation."""

import datetime
import hashlib
import pathlib

import pathspec
import pydantic
import yaml

import nuefs
from nuefs.manifest import LockMapping, MountEntry, NueLock, NueManifest

MANIFEST_NAME = "nue.yaml"
LOCK_NAME = "nue.lock"

class WorkspaceNotFoundError(Exception):
    """Raised when no workspace (nue.yaml) is found."""


class LockNotFoundError(Exception):
    """Raised when no lock file (nue.lock) is found."""


class LockOutdatedError(Exception):
    """Raised when lock file doesn't match manifest."""


def find_workspace(start: pathlib.Path | None = None) -> pathlib.Path:
    """Find workspace root by searching upward for nue.yaml."""
    current = (start or pathlib.Path.cwd()).resolve()

    while current != current.parent:
        if (current / MANIFEST_NAME).is_file():
            return current
        current = current.parent

    # Check root
    if (current / MANIFEST_NAME).is_file():
        return current

    raise WorkspaceNotFoundError(f"No {MANIFEST_NAME} found in {start or 'cwd'} or parents")


def load_manifest(workspace: pathlib.Path) -> NueManifest:
    """Load and parse nue.yaml from workspace."""
    manifest_path = workspace / MANIFEST_NAME
    data = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    return NueManifest.model_validate(data)


def hash_manifest(manifest: NueManifest) -> str:
    """Compute deterministic hash of manifest for change detection."""
    # Serialize to JSON with sorted keys for deterministic output
    content = manifest.model_dump_json(indent=None)
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def resolve_mappings(entry: MountEntry, workspace: pathlib.Path) -> list[LockMapping]:
    """Resolve MountEntry to concrete LockMappings, applying gitignore patterns."""
    source = entry.source.expanduser().resolve()
    target = entry.target

    if not source.exists():
        return []

    # Single file mapping
    if source.is_file():
        return [LockMapping(target=target, source=source)]

    # Directory: collect all files
    all_files: list[pathlib.Path] = []
    for path in source.rglob("*"):
        if path.is_file():
            all_files.append(path.relative_to(source))

    # Apply include/exclude filters
    if entry.include:
        spec = pathspec.PathSpec.from_lines("gitwildmatch", entry.include)
        filtered = [f for f in all_files if spec.match_file(str(f))]
    elif entry.exclude:
        spec = pathspec.PathSpec.from_lines("gitwildmatch", entry.exclude)
        filtered = [f for f in all_files if not spec.match_file(str(f))]
    else:
        filtered = all_files

    # Generate mappings
    return [
        LockMapping(target=target / rel, source=source / rel)
        for rel in sorted(filtered)
    ]


def generate_lock(manifest: NueManifest, workspace: pathlib.Path) -> NueLock:
    """Generate lock file from manifest."""
    mappings: list[LockMapping] = []
    for entry in manifest.mounts:
        mappings.extend(resolve_mappings(entry, workspace))

    return NueLock(
        apiVersion="nue/v1",
        generated_at=datetime.datetime.now(datetime.UTC),
        manifest_hash=hash_manifest(manifest),
        mappings=mappings,
    )


def write_lock(lock: NueLock, workspace: pathlib.Path) -> None:
    """Write lock file to workspace."""
    lock_path = workspace / LOCK_NAME
    # Convert to dict and dump as YAML
    data = lock.model_dump(mode="json")
    lock_path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def load_lock(workspace: pathlib.Path) -> NueLock:
    """Load lock file from workspace."""
    lock_path = workspace / LOCK_NAME
    if not lock_path.exists():
        raise LockNotFoundError(f"No {LOCK_NAME} found in {workspace}")
    data = yaml.safe_load(lock_path.read_text(encoding="utf-8"))
    return NueLock.model_validate(data)


def validate_lock(lock: NueLock, manifest: NueManifest) -> bool:
    """Check if lock matches manifest (hash comparison)."""
    return lock.manifest_hash == hash_manifest(manifest)


def manifest_to_mounts(manifest: NueManifest) -> list[nuefs.Mapping]:
    """Convert manifest entries to nuefs.Mapping list (directory-level)."""
    return [
        nuefs.Mapping(
            target=entry.target,
            source=entry.source.expanduser().resolve(),
        )
        for entry in manifest.mounts
    ]
