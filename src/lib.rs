mod fuse;
mod manifest;

use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::prelude::*;

/// Single mount configuration: layer source -> target path
#[pyclass]
#[derive(Clone, Debug)]
pub struct Mount {
    /// Relative path within the mount root (e.g., ".config/nvim")
    #[pyo3(get, set)]
    pub target: PathBuf,
    /// Absolute path to layer source directory
    #[pyo3(get, set)]
    pub source: PathBuf,
}

#[pymethods]
impl Mount {
    #[new]
    fn new(target: PathBuf, source: PathBuf) -> Self {
        Self { target, source }
    }
}

/// Information about which backend owns a path
#[pyclass]
#[derive(Clone, Debug)]
pub struct OwnerInfo {
    /// Owner name: "real" or layer source path
    #[pyo3(get)]
    pub owner: String,
    /// Actual backend path
    #[pyo3(get)]
    pub backend_path: PathBuf,
}

/// Handle to a mounted NueFS filesystem
#[pyclass]
pub struct MountHandle {
    #[allow(dead_code)]
    root: PathBuf,
    session: Arc<Mutex<Option<fuser::BackgroundSession>>>,
    manifest: Arc<manifest::Manifest>,
}

#[pymethods]
impl MountHandle {
    /// Check if the filesystem is still mounted
    fn is_mounted(&self) -> bool {
        self.session.lock().is_some()
    }
}

/// Mount NueFS filesystem (non-blocking, spawns background thread)
///
/// # Arguments
/// * `root` - The directory to mount (becomes FUSE mount point)
/// * `mounts` - List of layer mounts
///
/// # Returns
/// A handle that can be used to unmount or query the filesystem
#[pyfunction]
fn mount(root: PathBuf, mounts: Vec<Mount>) -> PyResult<MountHandle> {
    let root = root
        .canonicalize()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Invalid root path: {e}")))?;

    // Build manifest from mounts
    let manifest = Arc::new(
        manifest::Manifest::build(&root, &mounts)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to build manifest: {e}")))?,
    );

    // Create FUSE filesystem
    let fs = fuse::NueFs::new(root.clone(), manifest.clone());

    // Mount options (AutoUnmount requires user_allow_other in /etc/fuse.conf)
    let options = vec![
        fuser::MountOption::FSName("nuefs".to_string()),
    ];

    // Spawn background session
    let session = fuser::spawn_mount2(fs, &root, &options)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to mount: {e}")))?;

    Ok(MountHandle {
        root,
        session: Arc::new(Mutex::new(Some(session))),
        manifest,
    })
}

/// Unmount the NueFS filesystem
#[pyfunction]
fn unmount(handle: &MountHandle) -> PyResult<()> {
    let mut guard = handle.session.lock();
    if let Some(session) = guard.take() {
        session.join();
    }
    Ok(())
}

/// Query which backend owns a path
///
/// # Arguments
/// * `handle` - Mount handle
/// * `path` - Relative path within the mount root
///
/// # Returns
/// Owner information, or None if path doesn't exist
#[pyfunction]
fn which(handle: &MountHandle, path: &str) -> PyResult<Option<OwnerInfo>> {
    Ok(handle.manifest.which(path))
}

/// Python module
#[pymodule]
fn _nuefs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Mount>()?;
    m.add_class::<MountHandle>()?;
    m.add_class::<OwnerInfo>()?;
    m.add_function(wrap_pyfunction!(mount, m)?)?;
    m.add_function(wrap_pyfunction!(unmount, m)?)?;
    m.add_function(wrap_pyfunction!(which, m)?)?;
    Ok(())
}
