use std::os::unix::net::UnixStream as StdUnixStream;
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
use tracing::{debug, info, warn};

use crate::types::{DaemonInfo, ManifestEntry, MountStatus, NuefsService, OwnerInfoWire};

use super::manager::{Manager, ManagerError};

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("failed to bind unix socket {socket}: {source}")]
    Bind {
        socket: PathBuf,
        source: std::io::Error,
    },

    #[error("another daemon is already running on {0}")]
    AlreadyRunning(PathBuf),

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
        entries: Vec<ManifestEntry>,
    ) -> Result<u64, String> {
        let entry_count = entries.len();
        info!(root = %root.display(), entries = entry_count, "RPC mount");
        let result = self.manager_call(|m| m.mount(root, entries)).await;
        match &result {
            Ok(mount_id) => info!(mount_id, "mount succeeded"),
            Err(e) => warn!(error = %e, "mount failed"),
        }
        result
    }

    async fn unmount(self, _: tarpc::context::Context, mount_id: u64) -> Result<(), String> {
        info!(mount_id, "RPC unmount");
        let result = self.manager_call(|m| m.unmount(mount_id)).await;
        match &result {
            Ok(()) => info!(mount_id, "unmount succeeded"),
            Err(e) => warn!(mount_id, error = %e, "unmount failed"),
        }
        result
    }

    async fn which(
        self,
        _: tarpc::context::Context,
        mount_id: u64,
        path: String,
    ) -> Result<Option<OwnerInfoWire>, String> {
        debug!(mount_id, path = %path, "RPC which");
        self.manager_call(|m| m.which(mount_id, &path)).await
    }

    async fn status(self, _: tarpc::context::Context) -> Vec<MountStatus> {
        debug!("RPC status");
        self.manager.lock().await.status()
    }

    async fn daemon_info(self, _: tarpc::context::Context) -> DaemonInfo {
        debug!("RPC daemon_info");
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
        entries: Vec<ManifestEntry>,
    ) -> Result<(), String> {
        let entry_count = entries.len();
        info!(mount_id, entries = entry_count, "RPC update");
        let result = self.manager_call(|m| m.update(mount_id, entries)).await;
        match &result {
            Ok(()) => info!(mount_id, "update succeeded"),
            Err(e) => warn!(mount_id, error = %e, "update failed"),
        }
        result
    }

    async fn resolve(self, _: tarpc::context::Context, root: PathBuf) -> Option<u64> {
        debug!(root = %root.display(), "RPC resolve");
        self.manager_call(|m| m.resolve(root)).await.ok().flatten()
    }

    async fn shutdown(self, _: tarpc::context::Context) -> Result<(), String> {
        info!("RPC shutdown");
        let mount_ids: Vec<u64> = {
            let manager = self.manager.lock().await;
            manager.status().iter().map(|m| m.mount_id).collect()
        };
        for id in &mount_ids {
            let mut manager = self.manager.lock().await;
            if let Err(e) = manager.unmount(*id) {
                warn!(mount_id = id, error = %e, "failed to unmount during shutdown");
            }
        }
        info!(unmounted = mount_ids.len(), "shutdown complete, exiting");
        let socket = self.socket_path.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let _ = std::fs::remove_file(&socket);
            std::process::exit(0);
        });
        Ok(())
    }
}

pub async fn serve(socket_path: PathBuf) -> Result<(), ServerError> {
    if StdUnixStream::connect(&socket_path).is_ok() {
        return Err(ServerError::AlreadyRunning(socket_path));
    }
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

    info!(socket = %socket_path.display(), "server listening");

    let incoming = listener
        .filter_map(|result| async move { result.ok() })
        .map(server::BaseChannel::with_defaults)
        .execute(server.serve());
    incoming::spawn_incoming(incoming).await;

    Ok(())
}
