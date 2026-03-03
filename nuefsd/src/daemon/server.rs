use std::path::PathBuf;
use std::sync::Arc;

use camino::Utf8PathBuf;
use thiserror::Error;
use tokio::net::UnixListener;
use tokio::sync::{oneshot, Mutex};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::proto;
use crate::proto::nue_fs_server::{NueFs, NueFsServer};
use crate::types::{DaemonInfo, ManifestEntry, MountRoot};

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

    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}

fn to_status(e: ManagerError) -> Status {
    match &e {
        ManagerError::UnknownMountId(_) => Status::not_found(e.to_string()),
        ManagerError::AlreadyMounted(_) => Status::already_exists(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

pub struct NueFsService {
    manager: Arc<Mutex<Manager>>,
    socket_path: PathBuf,
    started_at: u64,
    shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

#[tonic::async_trait]
impl NueFs for NueFsService {
    async fn mount(
        &self,
        req: Request<proto::MountReq>,
    ) -> Result<Response<proto::MountResp>, Status> {
        let r = req.into_inner();
        let entry_count = r.entries.len();
        info!(root = %r.root, entries = entry_count, "RPC mount");
        let root = PathBuf::from(r.root);
        let entries: Vec<ManifestEntry> = r.entries.into_iter().map(Into::into).collect();
        let mount_roots: Vec<MountRoot> = r.mount_roots.into_iter().map(Into::into).collect();
        let mut m = self.manager.lock().await;
        let mount_id = m.mount(root, entries, mount_roots).map_err(to_status)?;
        info!(mount_id, "mount succeeded");
        Ok(Response::new(proto::MountResp { mount_id }))
    }

    async fn unmount(
        &self,
        req: Request<proto::UnmountReq>,
    ) -> Result<Response<proto::UnmountResp>, Status> {
        let r = req.into_inner();
        info!(mount_id = r.mount_id, "RPC unmount");
        let mut m = self.manager.lock().await;
        m.unmount(r.mount_id).map_err(to_status)?;
        info!(mount_id = r.mount_id, "unmount succeeded");
        Ok(Response::new(proto::UnmountResp {}))
    }

    async fn which(
        &self,
        req: Request<proto::WhichReq>,
    ) -> Result<Response<proto::WhichResp>, Status> {
        let r = req.into_inner();
        debug!(mount_id = r.mount_id, path = %r.path, "RPC which");
        let m = self.manager.lock().await;
        let owner_info = match m.which(r.mount_id, &r.path).map_err(to_status)? {
            Some(info) => Some(proto::OwnerInfo::from(&info)),
            None => None,
        };
        Ok(Response::new(proto::WhichResp { owner_info }))
    }

    async fn status(
        &self,
        _req: Request<proto::StatusReq>,
    ) -> Result<Response<proto::StatusResp>, Status> {
        debug!("RPC status");
        let m = self.manager.lock().await;
        let mounts: Vec<proto::MountStatus> =
            m.status().iter().map(proto::MountStatus::from).collect();
        Ok(Response::new(proto::StatusResp { mounts }))
    }

    async fn get_daemon_info(
        &self,
        _req: Request<proto::DaemonInfoReq>,
    ) -> Result<Response<proto::DaemonInfoResp>, Status> {
        debug!("RPC daemon_info");
        let info = DaemonInfo {
            pid: std::process::id(),
            socket: Utf8PathBuf::from_path_buf(self.socket_path.clone())
                .expect("socket path is not valid UTF-8"),
            started_at: self.started_at,
        };
        Ok(Response::new(proto::DaemonInfoResp {
            info: Some(proto::DaemonInfo::from(&info)),
        }))
    }

    async fn update(
        &self,
        req: Request<proto::UpdateReq>,
    ) -> Result<Response<proto::UpdateResp>, Status> {
        let r = req.into_inner();
        let entry_count = r.entries.len();
        info!(mount_id = r.mount_id, entries = entry_count, "RPC update");
        let entries: Vec<ManifestEntry> = r.entries.into_iter().map(Into::into).collect();
        let mount_roots: Vec<MountRoot> = r.mount_roots.into_iter().map(Into::into).collect();
        let mut m = self.manager.lock().await;
        m.update(r.mount_id, entries, mount_roots).map_err(to_status)?;
        info!(mount_id = r.mount_id, "update succeeded");
        Ok(Response::new(proto::UpdateResp {}))
    }

    async fn resolve(
        &self,
        req: Request<proto::ResolveReq>,
    ) -> Result<Response<proto::ResolveResp>, Status> {
        let r = req.into_inner();
        debug!(root = %r.root, "RPC resolve");
        let m = self.manager.lock().await;
        let mount_id = m.resolve(PathBuf::from(r.root)).ok().flatten();
        Ok(Response::new(proto::ResolveResp { mount_id }))
    }

    async fn shutdown(
        &self,
        _req: Request<proto::ShutdownReq>,
    ) -> Result<Response<proto::ShutdownResp>, Status> {
        info!("RPC shutdown");
        let mount_ids: Vec<u64> = {
            let m = self.manager.lock().await;
            m.status().iter().map(|s| s.mount_id).collect()
        };
        for id in &mount_ids {
            let mut m = self.manager.lock().await;
            if let Err(e) = m.unmount(*id) {
                warn!(mount_id = id, error = %e, "failed to unmount during shutdown");
            }
        }
        info!(unmounted = mount_ids.len(), "shutdown complete");
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        Ok(Response::new(proto::ShutdownResp {}))
    }
}

pub async fn serve(socket_path: PathBuf) -> Result<(), ServerError> {
    if std::os::unix::net::UnixStream::connect(&socket_path).is_ok() {
        return Err(ServerError::AlreadyRunning(socket_path));
    }
    let _ = std::fs::remove_file(&socket_path);

    let started_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let service = NueFsService {
        manager: Arc::new(Mutex::new(Manager::new())),
        socket_path: socket_path.clone(),
        started_at,
        shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
    };

    let listener = UnixListener::bind(&socket_path).map_err(|e| ServerError::Bind {
        socket: socket_path.clone(),
        source: e,
    })?;
    let uds_stream = UnixListenerStream::new(listener);

    info!(socket = %socket_path.display(), "server listening");

    tonic::transport::Server::builder()
        .add_service(NueFsServer::new(service))
        .serve_with_incoming_shutdown(uds_stream, async {
            shutdown_rx.await.ok();
        })
        .await?;

    let _ = std::fs::remove_file(&socket_path);
    info!("server stopped");
    Ok(())
}
