"""NueFS - FUSE-based layered filesystem for Python."""

import nuefs._nuefs as _ext

type Mapping = _ext.Mapping
type MountHandle = _ext.MountHandle
type MountStatus = _ext.MountStatus
type OwnerInfo = _ext.OwnerInfo

get_manifest = _ext.get_manifest
mount = _ext.mount
status = _ext.status
unmount = _ext.unmount
unmount_root = _ext.unmount_root
update = _ext.update
which = _ext.which
which_root = _ext.which_root

__all__ = [
    "Mapping",
    "MountHandle",
    "MountStatus",
    "OwnerInfo",
    "get_manifest",
    "mount",
    "status",
    "unmount",
    "unmount_root",
    "update",
    "which",
    "which_root",
]
