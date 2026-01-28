use std::collections::HashMap;
use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;

use easy_fuser::prelude::BackgroundSession;
use easy_fuser::prelude::MountOption;
use parking_lot::RwLock;
use thiserror::Error;
use tracing::{debug, info, warn};

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
    session: Option<BackgroundSession>,
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
        let entry_count = entries.len();
        let root = root
            .canonicalize()
            .map_err(|e| ManagerError::InvalidRoot(e.to_string()))?;

        if self.mounts_by_root.contains_key(&root) {
            warn!(root = %root.display(), "mount rejected: already mounted");
            return Err(ManagerError::AlreadyMounted(root));
        }

        let real_root_fd = File::open(&root)?;
        let access_root = PathBuf::from(format!("/proc/self/fd/{}", real_root_fd.as_raw_fd()));

        // Clone fd for NueFs (it needs its own fd for openat operations)
        let fuse_root_fd = real_root_fd.try_clone()?;

        let manifest = Arc::new(RwLock::new(Manifest::from_entries(
            root.clone(),
            access_root.clone(),
            entries,
        )));
        let fs = NueFs::new(access_root, fuse_root_fd, manifest.clone());

        let options = vec![MountOption::FSName("nuefs".to_string())];
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let session = easy_fuser::spawn_mount::<PathBuf, _, _>(fs, &root, &options, threads)?;

        let notifier = Arc::new(RwLock::new(Some(session.notifier())));

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
        self.mounts_by_root.insert(root.clone(), mount_id);

        info!(
            mount_id,
            root = %root.display(),
            entries = entry_count,
            "FUSE session mounted"
        );
        Ok(mount_id)
    }

    pub fn unmount(&mut self, mount_id: u64) -> Result<(), ManagerError> {
        let session = self
            .mounts
            .remove(&mount_id)
            .ok_or(ManagerError::UnknownMountId(mount_id))?;

        let root = session.root.clone();
        self.mounts_by_root.remove(&root);

        debug!(mount_id, root = %root.display(), "unmounting FUSE session");

        // Use lazy unmount with timeout to avoid blocking indefinitely.
        // fusermount3 -u -z does MNT_DETACH which detaches immediately.
        let mut child = std::process::Command::new("fusermount3")
            .args(["-u", "-z"])
            .arg(&root)
            .spawn()
            .ok();

        if let Some(ref mut child) = child {
            // Wait up to 5 seconds for fusermount3
            for _ in 0..50 {
                if let Ok(Some(_)) = child.try_wait() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            // If still running, kill it
            let _ = child.kill();
        }

        // Drop session after unmount command.
        drop(session);

        info!(mount_id, root = %root.display(), "FUSE session unmounted");
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
        let entry_count = entries.len();
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

        debug!(
            mount_id,
            entries = entry_count,
            root = %session.root.display(),
            "manifest updated"
        );
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
            let source = if e.backend_path.starts_with(&display_root) {
                Source::Real
            } else {
                Source::Layer {
                    backend_path: e.backend_path,
                }
            };
            map.insert(
                e.virtual_path,
                Entry {
                    source,
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

    /// Check if a path is a Real source (under the mount root) or not in entries.
    /// Returns true for root path or Real sources, false for Layer sources.
    pub(crate) fn is_real_path(&self, path: &str) -> bool {
        let path = path.trim_start_matches('/');
        if path.is_empty() {
            return true; // Root is always real
        }

        // Check exact match
        if let Some(entry) = self.entries.get(path) {
            return matches!(entry.source, Source::Real);
        }

        // Check prefix match - if parent is a Layer, this is not a real path
        if self.find_prefix_match(path).is_some() {
            return false;
        }

        // Not in entries and no prefix match - assume it's a real path
        true
    }

    pub(crate) fn which(&self, path: &str) -> Option<OwnerInfoWire> {
        let path = path.trim_start_matches('/');

        // Try exact match first
        if let Some(entry) = self.entries.get(path) {
            return Some(match &entry.source {
                Source::Real => OwnerInfoWire {
                    owner: "real".to_string(),
                    backend_path: self.display_root.join(path),
                },
                Source::Layer { backend_path } => OwnerInfoWire {
                    owner: "layer".to_string(),
                    backend_path: backend_path.clone(),
                },
            });
        }

        // Try prefix match for Layer directories
        if let Some((backend_path, suffix)) = self.find_prefix_match(path) {
            let full_path = if suffix.is_empty() {
                backend_path.clone()
            } else {
                backend_path.join(&suffix)
            };
            return Some(OwnerInfoWire {
                owner: "layer".to_string(),
                backend_path: full_path,
            });
        }

        // Fallback to real path
        let real_path = self.access_root.join(path);
        if real_path.exists() {
            return Some(OwnerInfoWire {
                owner: "real".to_string(),
                backend_path: self.display_root.join(path),
            });
        }

        None
    }

    pub(crate) fn resolve(&self, path: &str) -> Option<PathBuf> {
        let path = path.trim_start_matches('/');

        // Try exact match first
        if let Some(entry) = self.entries.get(path) {
            return Some(match &entry.source {
                Source::Real => self.access_root.join(path),
                Source::Layer { backend_path, .. } => backend_path.clone(),
            });
        }

        // Try prefix match for Layer directories
        if let Some((backend_path, suffix)) = self.find_prefix_match(path) {
            let resolved = if suffix.is_empty() {
                backend_path
            } else {
                backend_path.join(&suffix)
            };
            if resolved.exists() {
                return Some(resolved);
            }
        }

        // Fallback to real path
        let real_path = self.access_root.join(path);
        if real_path.exists() {
            return Some(real_path);
        }

        None
    }

    /// Find the longest matching prefix entry for a path.
    /// Returns (backend_path, suffix) where the path maps to backend_path/suffix
    fn find_prefix_match(&self, path: &str) -> Option<(PathBuf, String)> {
        let mut best_match: Option<(PathBuf, String)> = None;
        let mut best_len = 0;

        for (entry_path, entry) in &self.entries {
            if !entry.is_dir {
                continue;
            }

            // Check if entry_path is a prefix of path
            if path.starts_with(entry_path.as_str()) {
                let rest = &path[entry_path.len()..];
                // Must be followed by "/" or be exact match
                if rest.is_empty() || rest.starts_with('/') {
                    if entry_path.len() > best_len {
                        if let Source::Layer { backend_path } = &entry.source {
                            best_len = entry_path.len();
                            let suffix = rest.trim_start_matches('/').to_string();
                            best_match = Some((backend_path.clone(), suffix));
                        }
                    }
                }
            }
        }

        best_match
    }

    pub(crate) fn readdir(&self, path: &str) -> Vec<(String, bool)> {
        let prefix = path.trim_start_matches('/');

        // First, try to find if this path maps to a Layer directory
        if let Some(entry) = self.entries.get(prefix) {
            if let Source::Layer { backend_path } = &entry.source {
                if entry.is_dir {
                    // Read directly from backend directory
                    return self.readdir_from_backend(backend_path);
                }
            }
        }

        // Try prefix match
        if let Some((backend_path, suffix)) = self.find_prefix_match(prefix) {
            let target = if suffix.is_empty() {
                backend_path
            } else {
                backend_path.join(&suffix)
            };
            return self.readdir_from_backend(&target);
        }

        // Fallback: collect from entries + real filesystem
        let prefix_with_slash = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };

        let mut children = HashMap::new();

        // Collect from entries
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

        // Also read from real filesystem
        let real_path = if prefix.is_empty() {
            self.access_root.clone()
        } else {
            self.access_root.join(prefix)
        };

        if real_path.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&real_path) {
                for entry in entries.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if !children.contains_key(&name) {
                        let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
                        children.insert(name, is_dir);
                    }
                }
            }
        }

        children.into_iter().collect()
    }

    /// Read directory contents directly from a backend path.
    fn readdir_from_backend(&self, backend_path: &PathBuf) -> Vec<(String, bool)> {
        let mut children = Vec::new();

        if let Ok(entries) = std::fs::read_dir(backend_path) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
                children.push((name, is_dir));
            }
        }

        children
    }

    pub(crate) fn add_entry_with_backend(
        &mut self,
        path: &str,
        backend_path: PathBuf,
        is_dir: bool,
    ) {
        let path = path.trim_start_matches('/');

        let source = if backend_path.starts_with(&self.access_root) {
            Source::Real
        } else {
            Source::Layer { backend_path }
        };

        self.entries
            .insert(path.to_string(), Entry { source, is_dir });
    }

    pub(crate) fn remove_entry(&mut self, path: &str) {
        let path = path.trim_start_matches('/');
        self.entries.remove(path);
    }

    pub(crate) fn rename_entry_with_backend(
        &mut self,
        old_path: &str,
        new_path: &str,
        old_backend: &PathBuf,
        new_backend: &PathBuf,
    ) {
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

            let entry = match entry.source {
                Source::Layer { backend_path } => {
                    // If this entry came from the renamed backend, update its backend path too.
                    let updated = backend_path
                        .strip_prefix(old_backend)
                        .ok()
                        .map(|suffix| {
                            if suffix.as_os_str().is_empty() {
                                new_backend.clone()
                            } else {
                                new_backend.join(suffix)
                            }
                        })
                        .unwrap_or(backend_path);

                    Entry {
                        source: Source::Layer {
                            backend_path: updated,
                        },
                        is_dir: entry.is_dir,
                    }
                }
                Source::Real => entry,
            };

            self.entries.insert(new_key, entry);
        }
    }

    pub(crate) fn create_target(&self, parent_path: &str) -> PathBuf {
        let parent_path = parent_path.trim_start_matches('/');

        // Try exact match first
        if let Some(entry) = self.entries.get(parent_path) {
            if let Source::Layer { backend_path, .. } = &entry.source {
                if entry.is_dir {
                    return backend_path.clone();
                }
            }
        }

        // Try prefix match
        if let Some((backend_path, suffix)) = self.find_prefix_match(parent_path) {
            return if suffix.is_empty() {
                backend_path
            } else {
                backend_path.join(&suffix)
            };
        }

        if parent_path.is_empty() {
            self.access_root.clone()
        } else {
            self.access_root.join(parent_path)
        }
    }
}
