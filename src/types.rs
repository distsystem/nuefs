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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Mount { root: PathBuf, mounts: Vec<MountSpec> },
    Unmount { mount_id: u64 },
    Which { mount_id: u64, path: String },
    Status,
    Resolve { root: PathBuf },
    Update { mount_id: u64, mounts: Vec<MountSpec> },
    GetManifest { mount_id: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    Ok { data: ResponseData },
    Err { message: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseData {
    Mounted { mount_id: u64 },
    Unmounted,
    Which { info: Option<OwnerInfoWire> },
    Status { mounts: Vec<MountStatus> },
    Resolved { mount_id: Option<u64> },
    Updated,
    Manifest { mounts: Vec<MountSpec> },
}
