use std::path::PathBuf;
use std::sync::Arc;

use futures::prelude::*;
use tarpc::serde_transport;
use tarpc::server;
use tarpc::server::incoming;
use tarpc::server::incoming::Incoming;
use tarpc::tokio_serde::formats::Bincode;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::types::{DaemonInfo, MountSpec, MountStatus, NuefsService, OwnerInfoWire};

use super::manager::{Manager, ManagerError};

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("failed to bind unix socket {socket}: {source}")]
    Bind {
        socket: PathBuf,
        source: std::io::Error,
    },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone)]
struct NuefsServer {
    manager: Arc<Mutex<Manager>>,
    socket_path: PathBuf,
    started_at: u64,
}

impl NuefsServer {
    async fn manager_call<T>(
        &self,
        f: impl FnOnce(&mut Manager) -> Result<T, ManagerError>,
    ) -> Result<T, String> {
        let mut manager = self.manager.lock().await;
        f(&mut manager).map_err(|e| e.to_string())
    }
}

impl NuefsService for NuefsServer {
    async fn mount(
        self,
        _: tarpc::context::Context,
        root: PathBuf,
        mounts: Vec<MountSpec>,
    ) -> Result<u64, String> {
        self.manager_call(|m| m.mount(root, mounts)).await
    }

    async fn unmount(self, _: tarpc::context::Context, mount_id: u64) -> Result<(), String> {
        self.manager_call(|m| m.unmount(mount_id)).await
    }

    async fn which(
        self,
        _: tarpc::context::Context,
        mount_id: u64,
        path: String,
    ) -> Result<Option<OwnerInfoWire>, String> {
        self.manager_call(|m| m.which(mount_id, &path)).await
    }

    async fn status(self, _: tarpc::context::Context) -> Vec<MountStatus> {
        self.manager.lock().await.status()
    }

    async fn daemon_info(self, _: tarpc::context::Context) -> DaemonInfo {
        DaemonInfo {
            pid: std::process::id(),
            socket: self.socket_path.clone(),
            started_at: self.started_at,
        }
    }

    async fn update(
        self,
        _: tarpc::context::Context,
        mount_id: u64,
        mounts: Vec<MountSpec>,
    ) -> Result<(), String> {
        self.manager_call(|m| m.update(mount_id, mounts)).await
    }

    async fn get_manifest(
        self,
        _: tarpc::context::Context,
        mount_id: u64,
    ) -> Result<Vec<MountSpec>, String> {
        self.manager_call(|m| m.get_manifest(mount_id)).await
    }

    async fn resolve(self, _: tarpc::context::Context, root: PathBuf) -> Option<u64> {
        self.manager_call(|m| m.resolve(root)).await.ok().flatten()
    }
}

pub async fn serve(socket_path: PathBuf) -> Result<(), ServerError> {
    let _ = std::fs::remove_file(&socket_path);

    let started_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let server = NuefsServer {
        manager: Arc::new(Mutex::new(Manager::new())),
        socket_path: socket_path.clone(),
        started_at,
    };

    let listener = serde_transport::unix::listen(&socket_path, Bincode::default)
        .await
        .map_err(|e| ServerError::Bind {
            socket: socket_path.clone(),
            source: e,
        })?;

    let incoming = listener
        .filter_map(|result| async move { result.ok() })
        .map(server::BaseChannel::with_defaults)
        .execute(server.serve());
    incoming::spawn_incoming(incoming).await;

    Ok(())
}
