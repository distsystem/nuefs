use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MountSpec {
    pub target: PathBuf,
    pub source: PathBuf,
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

#[tarpc::service]
pub trait NuefsService {
    async fn mount(root: PathBuf, mounts: Vec<MountSpec>) -> Result<u64, String>;
    async fn unmount(mount_id: u64) -> Result<(), String>;
    async fn which(mount_id: u64, path: String) -> Result<Option<OwnerInfoWire>, String>;
    async fn status() -> Vec<MountStatus>;
    async fn update(mount_id: u64, mounts: Vec<MountSpec>) -> Result<(), String>;
    async fn get_manifest(mount_id: u64) -> Result<Vec<MountSpec>, String>;
    async fn resolve(root: PathBuf) -> Option<u64>;
}
