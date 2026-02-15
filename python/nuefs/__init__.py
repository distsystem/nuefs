"""NueFS - FUSE-based layered filesystem for Python."""

from nuefs.core import (
    DaemonInfo,
    Handle,
    ManifestEntry,
    MountRoot,
    OwnerInfo,
    daemon_info,
    default_socket_path,
    open,
    shutdown,
    status,
)
from nuefs.gitdir import ensure_external_gitdir

__all__ = [
    "DaemonInfo",
    "Handle",
    "ManifestEntry",
    "MountRoot",
    "OwnerInfo",
    "daemon_info",
    "default_socket_path",
    "ensure_external_gitdir",
    "open",
    "shutdown",
    "status",
]
