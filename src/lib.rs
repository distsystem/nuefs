mod client;
pub mod daemon;
mod types;

use std::path::PathBuf;

use pyo3::prelude::*;

use crate::client::Client;
use crate::types::MountSpec;

/// Single path mapping: source directory -> target path within mount root.
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

#[pymethods]
impl Mapping {
    #[new]
    fn new(target: PathBuf, source: PathBuf) -> Self {
        Self { target, source }
    }
}

/// Information about which backend owns a path.
#[pyclass]
#[derive(Clone, Debug)]
pub struct OwnerInfo {
    #[pyo3(get)]
    pub owner: String,
    #[pyo3(get)]
    pub backend_path: PathBuf,
}

/// Handle to a mounted NueFS filesystem.
#[pyclass]
#[derive(Clone, Debug)]
pub struct MountHandle {
    #[pyo3(get)]
    root: PathBuf,
    mount_id: u64,
}

#[pymethods]
impl MountHandle {
    /// Check if the mount is still tracked by the daemon.
    fn is_mounted(&self) -> PyResult<bool> {
        let client = Client::new();
        let mounts = client.status().map_err(to_pyerr)?;
        Ok(mounts.iter().any(|m| m.mount_id == self.mount_id))
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct MountStatus {
    #[pyo3(get)]
    pub mount_id: u64,
    #[pyo3(get)]
    pub root: PathBuf,
}

#[pyfunction]
fn mount(root: PathBuf, mounts: Vec<Mapping>) -> PyResult<MountHandle> {
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

    Ok(MountHandle { root, mount_id })
}

#[pyfunction]
fn unmount(handle: &MountHandle) -> PyResult<()> {
    let client = Client::new();
    client.unmount(handle.mount_id).map_err(to_pyerr)?;
    Ok(())
}

#[pyfunction]
fn which(handle: &MountHandle, path: &str) -> PyResult<Option<OwnerInfo>> {
    let client = Client::new();
    let info = client
        .which(handle.mount_id, path.to_string())
        .map_err(to_pyerr)?;

    Ok(info.map(|i| OwnerInfo {
        owner: i.owner,
        backend_path: i.backend_path,
    }))
}

#[pyfunction]
fn status(root: Option<PathBuf>) -> PyResult<Vec<MountStatus>> {
    let filter_root = match root {
        Some(r) => Some(r.canonicalize().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Invalid root path: {e}"))
        })?),
        None => None,
    };

    let client = Client::new();
    let mut mounts = client.status().map_err(to_pyerr)?;

    if let Some(root) = filter_root {
        mounts.retain(|m| m.root == root);
    }

    Ok(mounts
        .into_iter()
        .map(|m| MountStatus {
            mount_id: m.mount_id,
            root: m.root,
        })
        .collect())
}

#[pyfunction]
fn unmount_root(root: PathBuf) -> PyResult<()> {
    let root = root.canonicalize().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Invalid root path: {e}"))
    })?;

    let client = Client::new();
    let mount_id = client.resolve(root).map_err(to_pyerr)?;
    let Some(mount_id) = mount_id else {
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Mount not found",
        ));
    };

    client.unmount(mount_id).map_err(to_pyerr)?;
    Ok(())
}

#[pyfunction]
fn which_root(root: PathBuf, path: &str) -> PyResult<Option<OwnerInfo>> {
    let root = root.canonicalize().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Invalid root path: {e}"))
    })?;

    let client = Client::new();
    let mount_id = client.resolve(root).map_err(to_pyerr)?;
    let Some(mount_id) = mount_id else {
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Mount not found",
        ));
    };

    let info = client
        .which(mount_id, path.to_string())
        .map_err(to_pyerr)?;

    Ok(info.map(|i| OwnerInfo {
        owner: i.owner,
        backend_path: i.backend_path,
    }))
}

#[pyfunction]
fn update(handle: &MountHandle, mounts: Vec<Mapping>) -> PyResult<()> {
    let mounts = mounts
        .into_iter()
        .map(|m| MountSpec {
            target: m.target,
            source: m.source,
        })
        .collect();

    let client = Client::new();
    client.update(handle.mount_id, mounts).map_err(to_pyerr)
}

#[pyfunction]
fn get_manifest(handle: &MountHandle) -> PyResult<Vec<Mapping>> {
    let client = Client::new();
    let mounts = client.get_manifest(handle.mount_id).map_err(to_pyerr)?;

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
    m.add_class::<MountHandle>()?;
    m.add_class::<OwnerInfo>()?;
    m.add_class::<MountStatus>()?;
    m.add_function(wrap_pyfunction!(mount, m)?)?;
    m.add_function(wrap_pyfunction!(unmount, m)?)?;
    m.add_function(wrap_pyfunction!(which, m)?)?;
    m.add_function(wrap_pyfunction!(status, m)?)?;
    m.add_function(wrap_pyfunction!(unmount_root, m)?)?;
    m.add_function(wrap_pyfunction!(which_root, m)?)?;
    m.add_function(wrap_pyfunction!(update, m)?)?;
    m.add_function(wrap_pyfunction!(get_manifest, m)?)?;
    Ok(())
}
