"""Manifest builder for NueFS."""

import pathlib

import nuefs._nuefs as _ext


class ManifestBuilder:
    """Builds a manifest from real directory and mount layers."""

    def __init__(self, root: pathlib.Path) -> None:
        self.root = root.resolve()
        self.entries: dict[str, _ext.ManifestEntry] = {}

    def scan_real(self) -> None:
        """Scan real directory, skipping .git."""
        if not self.root.exists():
            return
        for path in self.root.rglob("*"):
            if ".git" in path.parts:
                continue
            rel = str(path.relative_to(self.root))
            self.entries[rel] = _ext.ManifestEntry(
                virtual_path=rel,
                backend_path=path,
                is_dir=path.is_dir(),
            )

    def apply_layer(self, source: pathlib.Path, target: str = ".") -> None:
        """Apply a mount layer."""
        source = source.resolve()

        # Auto-enable git inclusion when explicitly mounting .git
        include_git = target == ".git" or target.startswith(".git/")

        # Single file mount
        if source.is_file():
            virtual = target if target != "." else source.name
            self.entries[virtual] = _ext.ManifestEntry(
                virtual_path=virtual,
                backend_path=source,
                is_dir=False,
            )
            return

        if not source.exists():
            return

        # Add target directory entry when mounting to non-root
        if target != ".":
            if target not in self.entries or not self.entries[target].is_dir:
                self.entries[target] = _ext.ManifestEntry(
                    virtual_path=target,
                    backend_path=source,
                    is_dir=True,
                )

        for path in source.rglob("*"):
            # Skip .git unless include_git or target is .git itself
            if not include_git and ".git" in path.parts:
                continue
            rel = path.relative_to(source)
            if target == ".":
                virtual = str(rel)
            else:
                virtual = str(pathlib.PurePosixPath(target) / rel)

            # Skip .git at union root unless explicitly requested
            if not include_git and (virtual == ".git" or virtual.startswith(".git/")):
                continue

            # Merge rule: directories don't override directories
            if virtual in self.entries:
                if self.entries[virtual].is_dir and path.is_dir():
                    continue

            self.entries[virtual] = _ext.ManifestEntry(
                virtual_path=virtual,
                backend_path=path,
                is_dir=path.is_dir(),
            )

    def build(self) -> list[_ext.ManifestEntry]:
        """Return the manifest entries."""
        return list(self.entries.values())
