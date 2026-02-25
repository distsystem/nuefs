use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::PathBuf;

use camino::Utf8PathBuf;
use std::sync::Arc;

use prost::Message;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::proto::{
    self, request, response, ok_payload, Request, Response, OkPayload,
    StatusList, ResolveResult, Empty,
};
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
}

fn ok_response(value: ok_payload::Value) -> Response {
    Response {
        result: Some(response::Result::Ok(OkPayload {
            value: Some(value),
        })),
    }
}

fn err_response(msg: impl Into<String>) -> Response {
    Response {
        result: Some(response::Result::Error(msg.into())),
    }
}

fn manager_err(e: ManagerError) -> Response {
    err_response(e.to_string())
}

async fn handle_request(
    req: Request,
    manager: &Arc<Mutex<Manager>>,
    socket_path: &PathBuf,
    started_at: u64,
) -> Response {
    let Some(method) = req.method else {
        return err_response("empty request");
    };

    match method {
        request::Method::Mount(r) => {
            let entry_count = r.entries.len();
            info!(root = %r.root, entries = entry_count, "RPC mount");
            let root = PathBuf::from(r.root);
            let entries: Vec<ManifestEntry> = r.entries.into_iter().map(Into::into).collect();
            let mount_roots: Vec<MountRoot> = r.mount_roots.into_iter().map(Into::into).collect();
            let mut m = manager.lock().await;
            match m.mount(root, entries, mount_roots) {
                Ok(mount_id) => {
                    info!(mount_id, "mount succeeded");
                    ok_response(ok_payload::Value::MountId(mount_id))
                }
                Err(e) => {
                    warn!(error = %e, "mount failed");
                    manager_err(e)
                }
            }
        }

        request::Method::Unmount(r) => {
            info!(mount_id = r.mount_id, "RPC unmount");
            let mut m = manager.lock().await;
            match m.unmount(r.mount_id) {
                Ok(()) => {
                    info!(mount_id = r.mount_id, "unmount succeeded");
                    ok_response(ok_payload::Value::Empty(Empty {}))
                }
                Err(e) => {
                    warn!(mount_id = r.mount_id, error = %e, "unmount failed");
                    manager_err(e)
                }
            }
        }

        request::Method::Which(r) => {
            debug!(mount_id = r.mount_id, path = %r.path, "RPC which");
            let m = manager.lock().await;
            match m.which(r.mount_id, &r.path) {
                Ok(Some(info)) => ok_response(ok_payload::Value::OwnerInfo(
                    proto::OwnerInfo::from(&info),
                )),
                Ok(None) => ok_response(ok_payload::Value::OwnerInfo(proto::OwnerInfo {
                    owner: String::new(),
                    backend_path: String::new(),
                })),
                Err(e) => manager_err(e),
            }
        }

        request::Method::Status(_) => {
            debug!("RPC status");
            let m = manager.lock().await;
            let mounts: Vec<proto::MountStatus> =
                m.status().iter().map(|s| proto::MountStatus::from(s)).collect();
            ok_response(ok_payload::Value::Status(StatusList { mounts }))
        }

        request::Method::DaemonInfo(_) => {
            debug!("RPC daemon_info");
            let info = DaemonInfo {
                pid: std::process::id(),
                socket: Utf8PathBuf::from_path_buf(socket_path.clone())
                    .expect("socket path is not valid UTF-8"),
                started_at,
            };
            ok_response(ok_payload::Value::DaemonInfo(proto::DaemonInfo::from(&info)))
        }

        request::Method::Update(r) => {
            let entry_count = r.entries.len();
            info!(mount_id = r.mount_id, entries = entry_count, "RPC update");
            let entries: Vec<ManifestEntry> = r.entries.into_iter().map(Into::into).collect();
            let mount_roots: Vec<MountRoot> = r.mount_roots.into_iter().map(Into::into).collect();
            let mut m = manager.lock().await;
            match m.update(r.mount_id, entries, mount_roots) {
                Ok(()) => {
                    info!(mount_id = r.mount_id, "update succeeded");
                    ok_response(ok_payload::Value::Empty(Empty {}))
                }
                Err(e) => {
                    warn!(mount_id = r.mount_id, error = %e, "update failed");
                    manager_err(e)
                }
            }
        }

        request::Method::Resolve(r) => {
            debug!(root = %r.root, "RPC resolve");
            let m = manager.lock().await;
            let mount_id = m.resolve(PathBuf::from(r.root)).ok().flatten();
            ok_response(ok_payload::Value::Resolve(ResolveResult { mount_id }))
        }

        request::Method::Shutdown(_) => {
            info!("RPC shutdown");
            let mount_ids: Vec<u64> = {
                let m = manager.lock().await;
                m.status().iter().map(|s| s.mount_id).collect()
            };
            for id in &mount_ids {
                let mut m = manager.lock().await;
                if let Err(e) = m.unmount(*id) {
                    warn!(mount_id = id, error = %e, "failed to unmount during shutdown");
                }
            }
            info!(unmounted = mount_ids.len(), "shutdown complete, exiting");
            let socket = socket_path.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                let _ = std::fs::remove_file(&socket);
                std::process::exit(0);
            });
            ok_response(ok_payload::Value::Empty(Empty {}))
        }
    }
}

async fn read_frame(stream: &mut UnixStream) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn write_frame(stream: &mut UnixStream, data: &[u8]) -> std::io::Result<()> {
    stream
        .write_all(&(data.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(data).await?;
    stream.flush().await
}

async fn handle_connection(
    mut stream: UnixStream,
    manager: Arc<Mutex<Manager>>,
    socket_path: PathBuf,
    started_at: u64,
) {
    loop {
        let frame = match read_frame(&mut stream).await {
            Ok(f) => f,
            Err(_) => return,
        };

        let req = match Request::decode(frame.as_slice()) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "failed to decode request");
                let resp = err_response(format!("decode error: {e}"));
                let buf = resp.encode_to_vec();
                let _ = write_frame(&mut stream, &buf).await;
                return;
            }
        };

        let resp = handle_request(req, &manager, &socket_path, started_at).await;
        let buf = resp.encode_to_vec();
        if write_frame(&mut stream, &buf).await.is_err() {
            return;
        }
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

    let manager = Arc::new(Mutex::new(Manager::new()));

    let listener = UnixListener::bind(&socket_path).map_err(|e| ServerError::Bind {
        socket: socket_path.clone(),
        source: e,
    })?;

    info!(socket = %socket_path.display(), "server listening");

    loop {
        let (stream, _) = listener.accept().await?;
        let mgr = manager.clone();
        let sp = socket_path.clone();
        tokio::spawn(handle_connection(stream, mgr, sp, started_at));
    }
}
