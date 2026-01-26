mod client;
pub mod daemon;
pub mod runtime;
mod types;

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3_stub_gen::define_stub_info_gatherer;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};

use crate::client::Client;

define_stub_info_gatherer!(stub_info);

/// Pre-computed manifest entry for IPC.
#[gen_stub_pyclass]
#[pyclass]
#[derive(Clone, Debug)]
pub struct ManifestEntry {
    /// Relative path within mount root.
    #[pyo3(get, set)]
    pub virtual_path: String,
    /// Absolute path to backend file.
    #[pyo3(get, set)]
    pub backend_path: PathBuf,
    /// Whether this entry is a directory.
    #[pyo3(get, set)]
    pub is_dir: bool,
}

#[gen_stub_pymethods]
#[pymethods]
impl ManifestEntry {
    #[new]
    fn new(virtual_path: String, backend_path: PathBuf, is_dir: bool) -> Self {
        Self {
            virtual_path,
            backend_path,
            is_dir,
        }
    }
}

impl From<ManifestEntry> for types::ManifestEntry {
    fn from(e: ManifestEntry) -> Self {
        Self {
            virtual_path: e.virtual_path,
            backend_path: e.backend_path,
            is_dir: e.is_dir,
        }
    }
}

/// Information about which backend owns a path.
#[gen_stub_pyclass]
#[pyclass]
#[derive(Clone, Debug)]
pub struct OwnerInfo {
    #[pyo3(get)]
    pub owner: String,
    #[pyo3(get)]
    pub backend_path: PathBuf,
}

/// Information about the running daemon.
#[gen_stub_pyclass]
#[pyclass]
#[derive(Clone, Debug)]
pub struct DaemonInfo {
    #[pyo3(get)]
    pub pid: u32,
    #[pyo3(get)]
    pub socket: PathBuf,
    #[pyo3(get)]
    pub started_at: u64,
}

/// Raw handle data from NueFS daemon.
#[gen_stub_pyclass]
#[pyclass]
#[derive(Clone, Debug)]
pub struct RawHandle {
    #[pyo3(get)]
    pub root: PathBuf,
    #[pyo3(get)]
    pub mount_id: u64,
}

/// Create a new mount.
#[gen_stub_pyfunction]
#[pyfunction]
fn _mount(root: PathBuf, entries: Vec<ManifestEntry>) -> PyResult<RawHandle> {
    let root = root.canonicalize().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Invalid root path: {e}"))
    })?;

    let entries: Vec<types::ManifestEntry> = entries.into_iter().map(Into::into).collect();

    let client = Client::new().map_err(to_pyerr)?;
    let mount_id = client.mount(root.clone(), entries).map_err(to_pyerr)?;

    Ok(RawHandle { root, mount_id })
}

/// Unmount by mount_id.
#[gen_stub_pyfunction]
#[pyfunction]
fn _unmount(mount_id: u64) -> PyResult<()> {
    let client = Client::new().map_err(to_pyerr)?;
    client.unmount(mount_id).map_err(to_pyerr)
}

/// List all active mounts.
#[gen_stub_pyfunction]
#[pyfunction]
fn _status() -> PyResult<Vec<RawHandle>> {
    let client = Client::new().map_err(to_pyerr)?;
    let mounts = client.status().map_err(to_pyerr)?;
    Ok(mounts
        .into_iter()
        .map(|m| RawHandle {
            root: m.root,
            mount_id: m.mount_id,
        })
        .collect())
}

/// Get daemon info.
#[gen_stub_pyfunction]
#[pyfunction]
fn _daemon_info() -> PyResult<DaemonInfo> {
    let client = Client::new().map_err(to_pyerr)?;
    let info = client.daemon_info().map_err(to_pyerr)?;
    Ok(DaemonInfo {
        pid: info.pid,
        socket: info.socket,
        started_at: info.started_at,
    })
}

/// Query path owner. Returns None if not found.
#[gen_stub_pyfunction]
#[pyfunction]
fn _which(mount_id: u64, path: String) -> PyResult<Option<OwnerInfo>> {
    let client = Client::new().map_err(to_pyerr)?;
    let info = client.which(mount_id, path).map_err(to_pyerr)?;
    Ok(info.map(|info| OwnerInfo {
        owner: info.owner,
        backend_path: info.backend_path,
    }))
}

/// Update mount manifest.
#[gen_stub_pyfunction]
#[pyfunction]
fn _update(mount_id: u64, entries: Vec<ManifestEntry>) -> PyResult<()> {
    let entries: Vec<types::ManifestEntry> = entries.into_iter().map(Into::into).collect();

    let client = Client::new().map_err(to_pyerr)?;
    client.update(mount_id, entries).map_err(to_pyerr)
}

/// Resolve an existing mount by root. Returns mount_id if found.
#[gen_stub_pyfunction]
#[pyfunction]
fn _resolve(root: PathBuf) -> PyResult<Option<u64>> {
    let client = Client::new().map_err(to_pyerr)?;
    client.resolve(root).map_err(to_pyerr)
}

fn to_pyerr(err: crate::client::ClientError) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string())
}

#[pymodule]
fn _nuefs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ManifestEntry>()?;
    m.add_class::<RawHandle>()?;
    m.add_class::<OwnerInfo>()?;
    m.add_class::<DaemonInfo>()?;
    m.add_function(wrap_pyfunction!(_mount, m)?)?;
    m.add_function(wrap_pyfunction!(_unmount, m)?)?;
    m.add_function(wrap_pyfunction!(_status, m)?)?;
    m.add_function(wrap_pyfunction!(_daemon_info, m)?)?;
    m.add_function(wrap_pyfunction!(_which, m)?)?;
    m.add_function(wrap_pyfunction!(_update, m)?)?;
    m.add_function(wrap_pyfunction!(_resolve, m)?)?;
    Ok(())
}
