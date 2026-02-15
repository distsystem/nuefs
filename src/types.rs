use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub virtual_path: String,
    pub backend_path: PathBuf,
    pub is_dir: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MountRoot {
    pub virtual_prefix: String,
    pub backend_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnerInfoWire {
    pub owner: String,
    pub backend_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MountStatus {
    pub mount_id: u64,
    pub root: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaemonInfo {
    pub pid: u32,
    pub socket: PathBuf,
    pub started_at: u64,
}

// Proto → internal type conversions

impl From<crate::proto::ManifestEntry> for ManifestEntry {
    fn from(p: crate::proto::ManifestEntry) -> Self {
        Self {
            virtual_path: p.virtual_path,
            backend_path: PathBuf::from(p.backend_path),
            is_dir: p.is_dir,
        }
    }
}

impl From<&ManifestEntry> for crate::proto::ManifestEntry {
    fn from(m: &ManifestEntry) -> Self {
        Self {
            virtual_path: m.virtual_path.clone(),
            backend_path: m.backend_path.to_string_lossy().into(),
            is_dir: m.is_dir,
        }
    }
}

impl From<crate::proto::MountRoot> for MountRoot {
    fn from(p: crate::proto::MountRoot) -> Self {
        Self {
            virtual_prefix: p.virtual_prefix,
            backend_path: PathBuf::from(p.backend_path),
        }
    }
}

impl From<&MountRoot> for crate::proto::MountRoot {
    fn from(m: &MountRoot) -> Self {
        Self {
            virtual_prefix: m.virtual_prefix.clone(),
            backend_path: m.backend_path.to_string_lossy().into(),
        }
    }
}

impl From<&OwnerInfoWire> for crate::proto::OwnerInfo {
    fn from(o: &OwnerInfoWire) -> Self {
        Self {
            owner: o.owner.clone(),
            backend_path: o.backend_path.to_string_lossy().into(),
        }
    }
}

impl From<&MountStatus> for crate::proto::MountStatus {
    fn from(m: &MountStatus) -> Self {
        Self {
            mount_id: m.mount_id,
            root: m.root.to_string_lossy().into(),
        }
    }
}

impl From<&DaemonInfo> for crate::proto::DaemonInfo {
    fn from(d: &DaemonInfo) -> Self {
        Self {
            pid: d.pid,
            socket: d.socket.to_string_lossy().into(),
            started_at: d.started_at,
        }
    }
}
