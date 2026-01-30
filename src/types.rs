use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub virtual_path: String,
    pub backend_path: PathBuf,
    pub is_dir: bool,
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

#[tarpc::service]
pub trait NuefsService {
    async fn mount(root: PathBuf, entries: Vec<ManifestEntry>) -> Result<u64, String>;
    async fn unmount(mount_id: u64) -> Result<(), String>;
    async fn which(mount_id: u64, path: String) -> Result<Option<OwnerInfoWire>, String>;
    async fn status() -> Vec<MountStatus>;
    async fn daemon_info() -> DaemonInfo;
    async fn update(mount_id: u64, entries: Vec<ManifestEntry>) -> Result<(), String>;
    async fn resolve(root: PathBuf) -> Option<u64>;
    async fn shutdown() -> Result<(), String>;
}
