use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub virtual_path: Utf8PathBuf,
    pub backend_path: Utf8PathBuf,
    pub is_dir: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MountRoot {
    pub virtual_prefix: Utf8PathBuf,
    pub backend_path: Utf8PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnerInfoWire {
    pub owner: String,
    pub backend_path: Utf8PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MountStatus {
    pub mount_id: u64,
    pub root: Utf8PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaemonInfo {
    pub pid: u32,
    pub socket: Utf8PathBuf,
    pub started_at: u64,
}

// Proto ↔ internal type conversions
//
// Python sends `PurePosixPath` strings over proto: root = `"."`, others = `"foo/bar"`.
// Internally Rust uses `""` for root. Normalize at the boundary.

fn normalize_vpath(s: &str) -> Utf8PathBuf {
    let s = s.trim_matches('/');
    if s == "." || s.is_empty() {
        Utf8PathBuf::new()
    } else {
        Utf8PathBuf::from(s)
    }
}

fn vpath_to_proto(s: &str) -> String {
    if s.is_empty() { ".".to_string() } else { s.to_string() }
}

impl From<crate::proto::ManifestEntry> for ManifestEntry {
    fn from(p: crate::proto::ManifestEntry) -> Self {
        Self {
            virtual_path: normalize_vpath(&p.virtual_path),
            backend_path: Utf8PathBuf::from(p.backend_path),
            is_dir: p.is_dir,
        }
    }
}

impl From<&ManifestEntry> for crate::proto::ManifestEntry {
    fn from(m: &ManifestEntry) -> Self {
        Self {
            virtual_path: vpath_to_proto(m.virtual_path.as_str()),
            backend_path: m.backend_path.as_str().to_owned(),
            is_dir: m.is_dir,
        }
    }
}

impl From<crate::proto::MountRoot> for MountRoot {
    fn from(p: crate::proto::MountRoot) -> Self {
        Self {
            virtual_prefix: normalize_vpath(&p.virtual_prefix),
            backend_path: Utf8PathBuf::from(p.backend_path),
        }
    }
}

impl From<&MountRoot> for crate::proto::MountRoot {
    fn from(m: &MountRoot) -> Self {
        Self {
            virtual_prefix: vpath_to_proto(m.virtual_prefix.as_str()),
            backend_path: m.backend_path.as_str().to_owned(),
        }
    }
}

impl From<&OwnerInfoWire> for crate::proto::OwnerInfo {
    fn from(o: &OwnerInfoWire) -> Self {
        Self {
            owner: o.owner.clone(),
            backend_path: o.backend_path.as_str().to_owned(),
        }
    }
}

impl From<&MountStatus> for crate::proto::MountStatus {
    fn from(m: &MountStatus) -> Self {
        Self {
            mount_id: m.mount_id,
            root: m.root.as_str().to_owned(),
        }
    }
}

impl From<&DaemonInfo> for crate::proto::DaemonInfo {
    fn from(d: &DaemonInfo) -> Self {
        Self {
            pid: d.pid,
            socket: d.socket.as_str().to_owned(),
            started_at: d.started_at,
        }
    }
}
