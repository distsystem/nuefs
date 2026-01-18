use std::collections::HashMap;
use std::fs::{self, File};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

use crate::types::{MountSpec, MountStatus, OwnerInfoWire, Request, Response, ResponseData};

use super::fuse::NueFs;

#[derive(Debug, Error)]
pub enum ManagerError {
    #[error("invalid root path: {0}")]
    InvalidRoot(String),

    #[error("root already mounted: {0}")]
    AlreadyMounted(PathBuf),

    #[error("unknown mount id: {0}")]
    UnknownMountId(u64),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Manifest(#[from] ManifestError),
}

pub struct Manager {
    next_mount_id: u64,
    mounts: HashMap<u64, MountSession>,
    mounts_by_root: HashMap<PathBuf, u64>,
}

struct MountSession {
    root: PathBuf,
    #[allow(dead_code)]
    real_root_fd: File,
    manifest: Arc<Manifest>,
    session: Option<fuser::BackgroundSession>,
}

impl Manager {
    pub fn new() -> Self {
        Self {
            next_mount_id: 1,
            mounts: HashMap::new(),
            mounts_by_root: HashMap::new(),
        }
    }

    pub fn handle(&mut self, request: Request) -> Response {
        let result = match request {
            Request::Ping => Ok(ResponseData::Pong),
            Request::Mount { root, mounts } => self
                .mount(root, mounts)
                .map(|mount_id| ResponseData::Mounted { mount_id }),
            Request::Unmount { mount_id } => self.unmount(mount_id).map(|()| ResponseData::Unmounted),
            Request::Which { mount_id, path } => self
                .which(mount_id, &path)
                .map(|info| ResponseData::Which { info }),
            Request::Status => Ok(ResponseData::Status {
                mounts: self.status(),
            }),
            Request::Resolve { root } => self
                .resolve(root)
                .map(|mount_id| ResponseData::Resolved { mount_id }),
        };

        match result {
            Ok(data) => Response::Ok { data },
            Err(e) => Response::Err {
                message: e.to_string(),
            },
        }
    }

    fn mount(&mut self, root: PathBuf, mounts: Vec<MountSpec>) -> Result<u64, ManagerError> {
        let root = root
            .canonicalize()
            .map_err(|e| ManagerError::InvalidRoot(e.to_string()))?;

        if self.mounts_by_root.contains_key(&root) {
            return Err(ManagerError::AlreadyMounted(root));
        }

        let real_root_fd = File::open(&root)?;
        let access_root = PathBuf::from(format!("/proc/self/fd/{}", real_root_fd.as_raw_fd()));

        let manifest = Arc::new(Manifest::build(&root, &access_root, &mounts)?);
        let fs = NueFs::new(access_root, manifest.clone());

        let options = vec![fuser::MountOption::FSName("nuefs".to_string())];
        let session = fuser::spawn_mount2(fs, &root, &options)?;

        let mount_id = self.next_mount_id;
        self.next_mount_id += 1;

        self.mounts.insert(
            mount_id,
            MountSession {
                root: root.clone(),
                real_root_fd,
                manifest,
                session: Some(session),
            },
        );
        self.mounts_by_root.insert(root, mount_id);

        Ok(mount_id)
    }

    fn unmount(&mut self, mount_id: u64) -> Result<(), ManagerError> {
        let mut session = self
            .mounts
            .remove(&mount_id)
            .ok_or(ManagerError::UnknownMountId(mount_id))?;

        self.mounts_by_root.remove(&session.root);

        if let Some(bg) = session.session.take() {
            bg.join();
        }

        Ok(())
    }

    fn which(&self, mount_id: u64, path: &str) -> Result<Option<OwnerInfoWire>, ManagerError> {
        let session = self
            .mounts
            .get(&mount_id)
            .ok_or(ManagerError::UnknownMountId(mount_id))?;
        Ok(session.manifest.which(path))
    }

    fn status(&self) -> Vec<MountStatus> {
        let mut mounts: Vec<MountStatus> = self
            .mounts
            .iter()
            .map(|(mount_id, session)| MountStatus {
                mount_id: *mount_id,
                root: session.root.clone(),
            })
            .collect();
        mounts.sort_by_key(|m| m.mount_id);
        mounts
    }

    fn resolve(&self, root: PathBuf) -> Result<Option<u64>, ManagerError> {
        let root = root
            .canonicalize()
            .map_err(|e| ManagerError::InvalidRoot(e.to_string()))?;
        Ok(self.mounts_by_root.get(&root).copied())
    }
}

/// Source of a file/directory.
#[derive(Clone, Debug)]
enum Source {
    Real,
    Layer { source_root: PathBuf, backend_path: PathBuf },
}

#[derive(Clone, Debug)]
struct Entry {
    source: Source,
    is_dir: bool,
}

/// Manifest: maps virtual paths to their backend sources.
pub(crate) struct Manifest {
    display_root: PathBuf,
    access_root: PathBuf,
    #[allow(dead_code)]
    mounts: Vec<MountSpec>,
    entries: HashMap<String, Entry>,
}

impl Manifest {
    pub(crate) fn build(
        display_root: &Path,
        access_root: &Path,
        mounts: &[MountSpec],
    ) -> Result<Self, ManifestError> {
        let mut manifest = Self {
            display_root: display_root.to_path_buf(),
            access_root: access_root.to_path_buf(),
            mounts: mounts.to_vec(),
            entries: HashMap::new(),
        };

        manifest.scan_real(access_root, "")?;

        for mount in mounts.iter().rev() {
            manifest.scan_layer(mount)?;
        }

        Ok(manifest)
    }

    fn scan_real(&mut self, base: &Path, prefix: &str) -> Result<(), ManifestError> {
        let dir = if prefix.is_empty() {
            base.to_path_buf()
        } else {
            base.join(prefix)
        };

        if !dir.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(&dir).map_err(|e| ManifestError::Io(dir.clone(), e))? {
            let entry = entry.map_err(|e| ManifestError::Io(dir.clone(), e))?;
            let name = entry.file_name().to_string_lossy().to_string();
            let rel_path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };

            let metadata = entry
                .metadata()
                .map_err(|e| ManifestError::Io(entry.path(), e))?;
            let is_dir = metadata.is_dir();

            self.entries.insert(
                rel_path.clone(),
                Entry {
                    source: Source::Real,
                    is_dir,
                },
            );

            if is_dir {
                self.scan_real(base, &rel_path)?;
            }
        }

        Ok(())
    }

    fn scan_layer(&mut self, mount: &MountSpec) -> Result<(), ManifestError> {
        let target = mount.target.to_string_lossy();
        let target = target.trim_start_matches("./");
        let target = if target == "." { "" } else { target };
        self.scan_layer_dir(&mount.source, target, mount)
    }

    fn scan_layer_dir(
        &mut self,
        dir: &Path,
        prefix: &str,
        mount: &MountSpec,
    ) -> Result<(), ManifestError> {
        if !dir.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(dir).map_err(|e| ManifestError::Io(dir.to_path_buf(), e))? {
            let entry = entry.map_err(|e| ManifestError::Io(dir.to_path_buf(), e))?;
            let name = entry.file_name().to_string_lossy().to_string();
            let rel_path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };

            let metadata = entry
                .metadata()
                .map_err(|e| ManifestError::Io(entry.path(), e))?;
            let is_dir = metadata.is_dir();

            let existing = self.entries.get(&rel_path);
            let should_insert = match existing {
                None => true,
                Some(e) if e.is_dir && is_dir => false,
                Some(_) => true,
            };

            if should_insert {
                self.entries.insert(
                    rel_path.clone(),
                    Entry {
                        source: Source::Layer {
                            source_root: mount.source.clone(),
                            backend_path: entry.path(),
                        },
                        is_dir,
                    },
                );
            }

            if is_dir {
                self.scan_layer_dir(&entry.path(), &rel_path, mount)?;
            }
        }

        Ok(())
    }

    pub(crate) fn which(&self, path: &str) -> Option<OwnerInfoWire> {
        let path = path.trim_start_matches('/');
        self.entries.get(path).map(|entry| match &entry.source {
            Source::Real => OwnerInfoWire {
                owner: "real".to_string(),
                backend_path: self.display_root.join(path),
            },
            Source::Layer {
                source_root,
                backend_path,
            } => OwnerInfoWire {
                owner: source_root.to_string_lossy().to_string(),
                backend_path: backend_path.clone(),
            },
        })
    }

    pub(crate) fn resolve(&self, path: &str) -> Option<PathBuf> {
        let path = path.trim_start_matches('/');
        self.entries.get(path).map(|e| match &e.source {
            Source::Real => self.access_root.join(path),
            Source::Layer { backend_path, .. } => backend_path.clone(),
        })
    }

    pub(crate) fn readdir(&self, path: &str) -> Vec<(String, bool)> {
        let prefix = path.trim_start_matches('/');
        let prefix_with_slash = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };

        let mut children = HashMap::new();

        for (rel_path, entry) in &self.entries {
            if prefix.is_empty() {
                if !rel_path.contains('/') {
                    children.insert(rel_path.clone(), entry.is_dir);
                }
            } else if let Some(rest) = rel_path.strip_prefix(&prefix_with_slash) {
                if !rest.contains('/') {
                    children.insert(rest.to_string(), entry.is_dir);
                }
            }
        }

        children.into_iter().collect()
    }

    pub(crate) fn create_target(&self, parent_path: &str) -> PathBuf {
        let parent_path = parent_path.trim_start_matches('/');

        if let Some(entry) = self.entries.get(parent_path) {
            if let Source::Layer { backend_path, .. } = &entry.source {
                if entry.is_dir {
                    return backend_path.clone();
                }
            }
        }

        if parent_path.is_empty() {
            self.access_root.clone()
        } else {
            self.access_root.join(parent_path)
        }
    }
}

#[derive(Debug)]
pub enum ManifestError {
    Io(PathBuf, std::io::Error),
}

impl std::fmt::Display for ManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestError::Io(path, e) => write!(f, "IO error at {}: {}", path.display(), e),
        }
    }
}

impl std::error::Error for ManifestError {}
