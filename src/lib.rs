mod client;
pub mod daemon;
mod types;

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3_stub_gen::define_stub_info_gatherer;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};

use crate::client::Client;
use crate::types::MountSpec;

define_stub_info_gatherer!(stub_info);

/// Single path mapping: source directory -> target path within mount root.
#[gen_stub_pyclass]
#[pyclass]
#[derive(Clone, Debug)]
pub struct Mapping {
    /// Relative path within the mount root (e.g., ".config/nvim").
    #[pyo3(get, set)]
    pub target: PathBuf,
    /// Absolute path to source directory.
    #[pyo3(get, set)]
    pub source: PathBuf,
}

#[gen_stub_pymethods]
#[pymethods]
impl Mapping {
    #[new]
    fn new(target: PathBuf, source: PathBuf) -> Self {
        Self { target, source }
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
fn _mount(root: PathBuf, mounts: Vec<Mapping>) -> PyResult<RawHandle> {
    let root = root.canonicalize().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Invalid root path: {e}"))
    })?;

    let mounts = mounts
        .into_iter()
        .map(|m| MountSpec {
            target: m.target,
            source: m.source,
        })
        .collect();

    let client = Client::new();
    let mount_id = client.mount(root.clone(), mounts).map_err(to_pyerr)?;

    Ok(RawHandle { root, mount_id })
}

/// Unmount by mount_id.
#[gen_stub_pyfunction]
#[pyfunction]
fn _unmount(mount_id: u64) -> PyResult<()> {
    let client = Client::new();
    client.unmount(mount_id).map_err(to_pyerr)
}

/// List all active mounts.
#[gen_stub_pyfunction]
#[pyfunction]
fn _status() -> PyResult<Vec<RawHandle>> {
    let client = Client::new();
    let mounts = client.status().map_err(to_pyerr)?;
    Ok(mounts
        .into_iter()
        .map(|m| RawHandle {
            root: m.root,
            mount_id: m.mount_id,
        })
        .collect())
}

/// Query path owner. Raises RuntimeError if not found.
#[gen_stub_pyfunction]
#[pyfunction]
fn _which(mount_id: u64, path: String) -> PyResult<OwnerInfo> {
    let client = Client::new();
    let info = client
        .which(mount_id, path)
        .map_err(to_pyerr)?
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Path not found"))?;

    Ok(OwnerInfo {
        owner: info.owner,
        backend_path: info.backend_path,
    })
}

/// Update mount manifest.
#[gen_stub_pyfunction]
#[pyfunction]
fn _update(mount_id: u64, mounts: Vec<Mapping>) -> PyResult<()> {
    let mounts = mounts
        .into_iter()
        .map(|m| MountSpec {
            target: m.target,
            source: m.source,
        })
        .collect();

    let client = Client::new();
    client.update(mount_id, mounts).map_err(to_pyerr)
}

/// Get current mount manifest.
#[gen_stub_pyfunction]
#[pyfunction]
fn _get_manifest(mount_id: u64) -> PyResult<Vec<Mapping>> {
    let client = Client::new();
    let mounts = client.get_manifest(mount_id).map_err(to_pyerr)?;

    Ok(mounts
        .into_iter()
        .map(|m| Mapping {
            target: m.target,
            source: m.source,
        })
        .collect())
}

fn to_pyerr(err: crate::client::ClientError) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string())
}

#[pymodule]
fn _nuefs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Mapping>()?;
    m.add_class::<RawHandle>()?;
    m.add_class::<OwnerInfo>()?;
    m.add_function(wrap_pyfunction!(_mount, m)?)?;
    m.add_function(wrap_pyfunction!(_unmount, m)?)?;
    m.add_function(wrap_pyfunction!(_status, m)?)?;
    m.add_function(wrap_pyfunction!(_which, m)?)?;
    m.add_function(wrap_pyfunction!(_update, m)?)?;
    m.add_function(wrap_pyfunction!(_get_manifest, m)?)?;
    Ok(())
}
