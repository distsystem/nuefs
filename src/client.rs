use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use thiserror::Error;

use crate::types::{MountSpec, MountStatus, OwnerInfoWire, Request, Response, ResponseData};

const SOCKET_ENV: &str = "NUEFSD_SOCKET";
const BIN_ENV: &str = "NUEFSD_BIN";

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("failed to connect to nuefsd at {socket}: {source}")]
    Connect { socket: PathBuf, source: std::io::Error },

    #[error("failed to spawn nuefsd: {0}")]
    Spawn(std::io::Error),

    #[error("daemon returned an error: {0}")]
    Daemon(String),

    #[error("failed to decode daemon response: {0}")]
    Decode(serde_json::Error),

    #[error("invalid daemon response: {0}")]
    InvalidResponse(String),

    #[error("io error: {0}")]
    Io(std::io::Error),
}

pub struct Client {
    socket_path: PathBuf,
    daemon_bin: String,
}

impl Client {
    pub fn new() -> Self {
        Self {
            socket_path: default_socket_path(),
            daemon_bin: std::env::var(BIN_ENV).unwrap_or_else(|_| "nuefsd".to_string()),
        }
    }

    pub fn mount(&self, root: PathBuf, mounts: Vec<MountSpec>) -> Result<u64, ClientError> {
        let response = self.request(&Request::Mount { root, mounts })?;
        match response {
            Response::Ok {
                data: ResponseData::Mounted { mount_id },
            } => Ok(mount_id),
            other => Err(ClientError::InvalidResponse(format!("{other:?}"))),
        }
    }

    pub fn unmount(&self, mount_id: u64) -> Result<(), ClientError> {
        let response = self.request(&Request::Unmount { mount_id })?;
        match response {
            Response::Ok {
                data: ResponseData::Unmounted,
            } => Ok(()),
            other => Err(ClientError::InvalidResponse(format!("{other:?}"))),
        }
    }

    pub fn which(&self, mount_id: u64, path: String) -> Result<Option<OwnerInfoWire>, ClientError> {
        let response = self.request(&Request::Which { mount_id, path })?;
        match response {
            Response::Ok {
                data: ResponseData::Which { info },
            } => Ok(info),
            other => Err(ClientError::InvalidResponse(format!("{other:?}"))),
        }
    }

    pub fn status(&self) -> Result<Vec<MountStatus>, ClientError> {
        let response = self.request(&Request::Status)?;
        match response {
            Response::Ok {
                data: ResponseData::Status { mounts },
            } => Ok(mounts),
            other => Err(ClientError::InvalidResponse(format!("{other:?}"))),
        }
    }

    pub fn resolve(&self, root: PathBuf) -> Result<Option<u64>, ClientError> {
        let response = self.request(&Request::Resolve { root })?;
        match response {
            Response::Ok {
                data: ResponseData::Resolved { mount_id },
            } => Ok(mount_id),
            other => Err(ClientError::InvalidResponse(format!("{other:?}"))),
        }
    }

    fn request(&self, request: &Request) -> Result<Response, ClientError> {
        self.ensure_daemon()?;

        let mut stream = UnixStream::connect(&self.socket_path)
            .map_err(|e| ClientError::Connect {
                socket: self.socket_path.clone(),
                source: e,
            })?;

        let json = serde_json::to_vec(request).map_err(ClientError::Decode)?;
        stream.write_all(&json).map_err(ClientError::Io)?;
        stream.write_all(b"\n").map_err(ClientError::Io)?;
        stream.flush().map_err(ClientError::Io)?;

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).map_err(ClientError::Io)?;
        let response: Response = serde_json::from_str(&line).map_err(ClientError::Decode)?;

        match response {
            Response::Ok { .. } => Ok(response),
            Response::Err { message } => Err(ClientError::Daemon(message)),
        }
    }

    fn ensure_daemon(&self) -> Result<(), ClientError> {
        if UnixStream::connect(&self.socket_path).is_ok() {
            return Ok(());
        }

        if let Err(e) = spawn_daemon(&self.daemon_bin, &self.socket_path) {
            return Err(ClientError::Spawn(e));
        }

        for _ in 0..40 {
            if UnixStream::connect(&self.socket_path).is_ok() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(50));
        }

        Err(ClientError::Connect {
            socket: self.socket_path.clone(),
            source: std::io::Error::new(std::io::ErrorKind::TimedOut, "daemon did not become ready"),
        })
    }
}

pub(crate) fn default_socket_path() -> PathBuf {
    if let Some(path) = std::env::var_os(SOCKET_ENV) {
        return PathBuf::from(path);
    }

    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"));

    let uid = unsafe { libc::geteuid() };
    base.join(format!("nuefsd-{uid}.sock"))
}

fn spawn_daemon(daemon_bin: &str, socket_path: &PathBuf) -> Result<(), std::io::Error> {
    try_spawn_daemon_bin(daemon_bin, socket_path)
}

fn try_spawn_daemon_bin(daemon_bin: &str, socket_path: &PathBuf) -> Result<(), std::io::Error> {
    let mut cmd = Command::new(daemon_bin);
    cmd.arg("--socket").arg(socket_path);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.spawn().map(|_| ())
}
