use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::PathBuf;
use std::time::Duration;

use tarpc::client;
use tarpc::serde_transport;
use tarpc::tokio_serde::formats::Bincode;
use thiserror::Error;

use crate::types::{DaemonInfo, ManifestEntry, MountStatus, NuefsServiceClient, OwnerInfoWire};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("failed to connect to nuefsd at {socket}: {source}")]
    Connect {
        socket: PathBuf,
        source: std::io::Error,
    },

    #[error("daemon returned an error: {0}")]
    Daemon(String),

    #[error("rpc error: {0}")]
    Rpc(#[from] tarpc::client::RpcError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct Client {
    rt: tokio::runtime::Runtime,
    inner: NuefsServiceClient,
}

impl Client {
    pub fn new() -> Result<Self, ClientError> {
        let socket_path = crate::runtime::default_socket_path();

        ensure_daemon(&socket_path)?;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;

        let inner = rt.block_on(async {
            let transport = serde_transport::unix::connect(&socket_path, Bincode::default)
                .await
                .map_err(|e| ClientError::Connect {
                    socket: socket_path.clone(),
                    source: e,
                })?;
            Ok::<_, ClientError>(
                NuefsServiceClient::new(client::Config::default(), transport).spawn(),
            )
        })?;

        Ok(Self { rt, inner })
    }

    fn call<T, Fut>(&self, f: impl FnOnce(tarpc::context::Context) -> Fut) -> Result<T, ClientError>
    where
        Fut: std::future::Future<Output = Result<T, tarpc::client::RpcError>>,
    {
        self.rt.block_on(async {
            let mut ctx = tarpc::context::current();
            ctx.deadline = std::time::Instant::now() + Duration::from_secs(10);
            Ok::<_, ClientError>(f(ctx).await?)
        })
    }

    fn call_daemon<T, Fut>(
        &self,
        f: impl FnOnce(tarpc::context::Context) -> Fut,
    ) -> Result<T, ClientError>
    where
        Fut: std::future::Future<Output = Result<Result<T, String>, tarpc::client::RpcError>>,
    {
        self.call(f)?.map_err(ClientError::Daemon)
    }

    pub fn mount(&self, root: PathBuf, entries: Vec<ManifestEntry>) -> Result<u64, ClientError> {
        self.call_daemon(|ctx| self.inner.mount(ctx, root, entries))
    }

    pub fn unmount(&self, mount_id: u64) -> Result<(), ClientError> {
        self.call_daemon(|ctx| self.inner.unmount(ctx, mount_id))
    }

    pub fn which(&self, mount_id: u64, path: String) -> Result<Option<OwnerInfoWire>, ClientError> {
        self.call_daemon(|ctx| self.inner.which(ctx, mount_id, path))
    }

    pub fn status(&self) -> Result<Vec<MountStatus>, ClientError> {
        self.call(|ctx| self.inner.status(ctx))
    }

    pub fn daemon_info(&self) -> Result<DaemonInfo, ClientError> {
        self.call(|ctx| self.inner.daemon_info(ctx))
    }

    pub fn update(&self, mount_id: u64, entries: Vec<ManifestEntry>) -> Result<(), ClientError> {
        self.call_daemon(|ctx| self.inner.update(ctx, mount_id, entries))
    }

    pub fn resolve(&self, root: PathBuf) -> Result<Option<u64>, ClientError> {
        self.call(|ctx| self.inner.resolve(ctx, root))
    }

    pub fn shutdown(&self) -> Result<(), ClientError> {
        self.call_daemon(|ctx| self.inner.shutdown(ctx))
    }
}

fn ensure_daemon(socket_path: &PathBuf) -> Result<(), ClientError> {
    if StdUnixStream::connect(socket_path).is_ok() {
        return Ok(());
    }

    Err(ClientError::Connect {
        socket: socket_path.clone(),
        source: std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "daemon not running; start it with `nue start`",
        ),
    })
}
