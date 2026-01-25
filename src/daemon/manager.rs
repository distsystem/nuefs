use std::collections::HashMap;
use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use thiserror::Error;

use crate::types::{ManifestEntry, MountStatus, OwnerInfoWire};

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
}

pub struct Manager {
    next_mount_id: u64,
    mounts: HashMap<u64, MountSession>,
    mounts_by_root: HashMap<PathBuf, u64>,
}

struct MountSession {
    root: PathBuf,
    real_root_fd: File,
    manifest: Arc<RwLock<Manifest>>,
    notifier: Arc<RwLock<Option<fuser::Notifier>>>,
    /// FUSE session handle. Dropped on unmount to trigger automatic unmount.
    #[allow(dead_code)]
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

    pub fn mount(
        &mut self,
        root: PathBuf,
        entries: Vec<ManifestEntry>,
    ) -> Result<u64, ManagerError> {
        let root = root
            .canonicalize()
            .map_err(|e| ManagerError::InvalidRoot(e.to_string()))?;

        if self.mounts_by_root.contains_key(&root) {
            return Err(ManagerError::AlreadyMounted(root));
        }

        let real_root_fd = File::open(&root)?;
        let access_root = PathBuf::from(format!("/proc/self/fd/{}", real_root_fd.as_raw_fd()));

        let manifest = Arc::new(RwLock::new(Manifest::from_entries(
            root.clone(),
            access_root.clone(),
            entries,
        )));
        let notifier = Arc::new(RwLock::new(None));
        let fs = NueFs::new(access_root, manifest.clone(), notifier.clone());

        let options = vec![fuser::MountOption::FSName("nuefs".to_string())];
        let session = fuser::spawn_mount2(fs, &root, &options)?;

        // Set notifier after session is created
        *notifier.write() = Some(session.notifier());

        let mount_id = self.next_mount_id;
        self.next_mount_id += 1;

        self.mounts.insert(
            mount_id,
            MountSession {
                root: root.clone(),
                real_root_fd,
                manifest,
                notifier,
                session: Some(session),
            },
        );
        self.mounts_by_root.insert(root, mount_id);

        Ok(mount_id)
    }

    pub fn unmount(&mut self, mount_id: u64) -> Result<(), ManagerError> {
        let session = self
            .mounts
            .remove(&mount_id)
            .ok_or(ManagerError::UnknownMountId(mount_id))?;

        self.mounts_by_root.remove(&session.root);

        // Use lazy unmount to avoid blocking when processes are using the mount.
        // fusermount3 -u -z does MNT_DETACH which detaches immediately.
        let _ = std::process::Command::new("fusermount3")
            .args(["-u", "-z"])
            .arg(&session.root)
            .status();

        // Drop session after unmount command.
        drop(session);

        Ok(())
    }

    pub fn which(&self, mount_id: u64, path: &str) -> Result<Option<OwnerInfoWire>, ManagerError> {
        let session = self
            .mounts
            .get(&mount_id)
            .ok_or(ManagerError::UnknownMountId(mount_id))?;
        Ok(session.manifest.read().which(path))
    }

    pub fn status(&self) -> Vec<MountStatus> {
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

    pub fn resolve(&self, root: PathBuf) -> Result<Option<u64>, ManagerError> {
        let Ok(root) = root.canonicalize() else {
            return Ok(None);
        };
        Ok(self.mounts_by_root.get(&root).copied())
    }

    pub fn update(
        &mut self,
        mount_id: u64,
        entries: Vec<ManifestEntry>,
    ) -> Result<(), ManagerError> {
        let session = self
            .mounts
            .get_mut(&mount_id)
            .ok_or(ManagerError::UnknownMountId(mount_id))?;

        let access_root = PathBuf::from(format!(
            "/proc/self/fd/{}",
            session.real_root_fd.as_raw_fd()
        ));
        let new_manifest = Manifest::from_entries(session.root.clone(), access_root, entries);

        let old_root_children: Vec<String> = session
            .manifest
            .read()
            .readdir("")
            .into_iter()
            .map(|(name, _is_dir)| name)
            .collect();
        let new_root_children: Vec<String> = new_manifest
            .readdir("")
            .into_iter()
            .map(|(name, _is_dir)| name)
            .collect();

        *session.manifest.write() = new_manifest;

        if let Some(ref notifier) = *session.notifier.read() {
            let _ = notifier.inval_inode(1, 0, 0);

            // Best-effort refresh of root directory entries.
            let mut names = old_root_children;
            names.extend(new_root_children);
            names.sort();
            names.dedup();
            for name in names {
                let _ = notifier.inval_entry(1, std::ffi::OsStr::new(&name));
            }
        }

        Ok(())
    }
}

/// Source of a file/directory.
#[derive(Clone, Debug)]
enum Source {
    Real,
    Layer { backend_path: PathBuf },
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
    entries: HashMap<String, Entry>,
}

impl Manifest {
    pub(crate) fn from_entries(
        display_root: PathBuf,
        access_root: PathBuf,
        entries: Vec<ManifestEntry>,
    ) -> Self {
        let mut map = HashMap::new();
        for e in entries {
            map.insert(
                e.virtual_path,
                Entry {
                    source: Source::Layer {
                        backend_path: e.backend_path,
                    },
                    is_dir: e.is_dir,
                },
            );
        }
        Self {
            display_root,
            access_root,
            entries: map,
        }
    }

    pub(crate) fn which(&self, path: &str) -> Option<OwnerInfoWire> {
        let path = path.trim_start_matches('/');
        self.entries.get(path).map(|entry| match &entry.source {
            Source::Real => OwnerInfoWire {
                owner: "real".to_string(),
                backend_path: self.display_root.join(path),
            },
            Source::Layer { backend_path } => OwnerInfoWire {
                owner: "layer".to_string(),
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

    pub(crate) fn add_entry(&mut self, path: &str, is_dir: bool) {
        let path = path.trim_start_matches('/');
        self.entries.insert(
            path.to_string(),
            Entry {
                source: Source::Real,
                is_dir,
            },
        );
    }

    pub(crate) fn remove_entry(&mut self, path: &str) {
        let path = path.trim_start_matches('/');
        self.entries.remove(path);
    }

    pub(crate) fn rename_entry(&mut self, old_path: &str, new_path: &str) {
        let old_path = old_path.trim_start_matches('/');
        let new_path = new_path.trim_start_matches('/');

        if old_path.is_empty() {
            return;
        }

        let old_prefix = if old_path.ends_with('/') {
            old_path.to_string()
        } else {
            format!("{old_path}/")
        };

        let mut to_move = Vec::new();
        for key in self.entries.keys() {
            if key == old_path || key.starts_with(&old_prefix) {
                to_move.push(key.clone());
            }
        }

        for key in to_move {
            let Some(entry) = self.entries.remove(&key) else {
                continue;
            };

            let new_key = if key == old_path {
                new_path.to_string()
            } else {
                let suffix = key.strip_prefix(old_path).unwrap_or("");
                if new_path.is_empty() {
                    suffix.trim_start_matches('/').to_string()
                } else {
                    format!("{new_path}{suffix}")
                }
            };

            self.entries.insert(new_key, entry);
        }
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
