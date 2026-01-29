use std::collections::HashMap;
use std::fs::File;
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
    /// Keeps the root fd alive. NueFs has a cloned fd for openat operations.
    #[allow(dead_code)]
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

        // Clone fd for NueFs (it needs its own fd for openat operations)
        let fuse_root_fd = real_root_fd.try_clone()?;

        let manifest = Arc::new(RwLock::new(Manifest::from_entries(root.clone(), entries)));
        let fs = NueFs::new(root.clone(), fuse_root_fd, manifest.clone());

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

        let new_manifest = Manifest::from_entries(session.root.clone(), entries);

        let old_root_children = session.manifest.read().entry_names_at("");
        let new_root_children = new_manifest.entry_names_at("");

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

/// How to access the backend path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AccessMethod {
    /// Use openat(root_fd, rel_path) - for paths under the mount root
    Openat,
    /// Use absolute path - for external layer paths
    Absolute,
}

#[derive(Clone, Debug)]
struct Entry {
    backend_path: PathBuf,
    access: AccessMethod,
    is_dir: bool,
}

/// Resolved path for FUSE operations.
#[derive(Clone, Debug)]
pub(crate) enum ResolvedPath {
    /// Path is relative to the mount root (use openat with root_fd)
    Openat(String),
    /// Path is an absolute path (external layer)
    Absolute(PathBuf),
}

impl ResolvedPath {
    /// Convert to absolute path, using root for Openat paths.
    pub(crate) fn to_path(&self, root: &PathBuf) -> PathBuf {
        match self {
            Self::Openat(rel) => root.join(rel),
            Self::Absolute(p) => p.clone(),
        }
    }

    /// Check if this is an Openat path.
    pub(crate) fn is_openat(&self) -> bool {
        matches!(self, Self::Openat(_))
    }
}

/// Result of readdir operation.
pub(crate) enum ReaddirResult {
    /// Directory is external (absolute path), children already read
    Absolute(Vec<(String, bool)>),
    /// Directory is under mount root, caller should use openat to read
    /// and merge with returned manifest children
    Openat {
        rel_path: String,
        manifest_children: Vec<(String, bool)>,
    },
}

/// Manifest: maps virtual paths to their backend sources.
pub(crate) struct Manifest {
    display_root: PathBuf,
    entries: HashMap<String, Entry>,
}

impl Manifest {
    /// Get direct child names from entries only (not from filesystem).
    /// Used for cache invalidation notifications.
    pub(crate) fn entry_names_at(&self, prefix: &str) -> Vec<String> {
        let prefix = prefix.trim_start_matches('/');
        let prefix_with_slash = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };

        let mut names = Vec::new();
        for rel_path in self.entries.keys() {
            if prefix.is_empty() {
                if !rel_path.contains('/') {
                    names.push(rel_path.clone());
                }
            } else if let Some(rest) = rel_path.strip_prefix(&prefix_with_slash) {
                if !rest.contains('/') {
                    names.push(rest.to_string());
                }
            }
        }
        names
    }
}

impl Manifest {
    pub(crate) fn from_entries(
        display_root: PathBuf,
        entries: Vec<ManifestEntry>,
    ) -> Self {
        let mut map = HashMap::new();
        for e in entries {
            let access = if e.backend_path.starts_with(&display_root) {
                AccessMethod::Openat
            } else {
                AccessMethod::Absolute
            };
            map.insert(
                e.virtual_path,
                Entry {
                    backend_path: e.backend_path,
                    access,
                    is_dir: e.is_dir,
                },
            );
        }
        Self {
            display_root,
            entries: map,
        }
    }

    pub(crate) fn which(&self, path: &str) -> Option<OwnerInfoWire> {
        let path = path.trim_start_matches('/');

        // Try exact match first
        if let Some(entry) = self.entries.get(path) {
            let owner = match entry.access {
                AccessMethod::Openat => "real",
                AccessMethod::Absolute => "layer",
            };
            return Some(OwnerInfoWire {
                owner: owner.to_string(),
                backend_path: entry.backend_path.clone(),
            });
        }

        // Try prefix match for any directory entry
        if let Some((entry, suffix)) = self.find_prefix_match(path) {
            let full_path = if suffix.is_empty() {
                entry.backend_path.clone()
            } else {
                entry.backend_path.join(&suffix)
            };
            let owner = match entry.access {
                AccessMethod::Openat => "real",
                AccessMethod::Absolute => "layer",
            };
            return Some(OwnerInfoWire {
                owner: owner.to_string(),
                backend_path: full_path,
            });
        }

        // Fallback: assume real path (caller will verify existence)
        Some(OwnerInfoWire {
            owner: "real".to_string(),
            backend_path: self.display_root.join(path),
        })
    }

    /// Resolve a virtual path to a ResolvedPath.
    /// Returns None only if the path doesn't exist in entries and has no prefix match.
    /// For Openat paths, the caller should verify existence using openat.
    pub(crate) fn resolve(&self, path: &str) -> Option<ResolvedPath> {
        let path = path.trim_start_matches('/');

        // Try exact match first
        if let Some(entry) = self.entries.get(path) {
            return Some(match entry.access {
                AccessMethod::Openat => ResolvedPath::Openat(path.to_string()),
                AccessMethod::Absolute => ResolvedPath::Absolute(entry.backend_path.clone()),
            });
        }

        // Try prefix match for any directory entry
        if let Some((entry, suffix)) = self.find_prefix_match(path) {
            let resolved = if suffix.is_empty() {
                entry.backend_path.clone()
            } else {
                entry.backend_path.join(&suffix)
            };
            return Some(match entry.access {
                AccessMethod::Openat => ResolvedPath::Openat(path.to_string()),
                AccessMethod::Absolute => ResolvedPath::Absolute(resolved),
            });
        }

        // Fallback: assume real path (caller will verify existence using openat)
        Some(ResolvedPath::Openat(path.to_string()))
    }

    /// Find the longest matching prefix entry for a path.
    /// Returns (&Entry, suffix) where the path maps to entry.backend_path/suffix.
    /// Now matches ALL directory entries (not just external layers).
    fn find_prefix_match(&self, path: &str) -> Option<(&Entry, String)> {
        let mut best_match: Option<(&Entry, String)> = None;
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
                        best_len = entry_path.len();
                        let suffix = rest.trim_start_matches('/').to_string();
                        best_match = Some((entry, suffix));
                    }
                }
            }
        }

        best_match
    }

    /// Read directory contents.
    ///
    /// Returns `ReaddirResult::Absolute` if the path resolves to an external layer (children already read),
    /// or `ReaddirResult::Openat` with relative path and manifest children for the caller to merge.
    pub(crate) fn readdir(&self, path: &str) -> ReaddirResult {
        let prefix = path.trim_start_matches('/');

        // Collect manifest children at this prefix (for merging in FUSE layer)
        let manifest_children: Vec<(String, bool)> = self
            .entry_names_at(prefix)
            .into_iter()
            .filter_map(|name| {
                let child_path = if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{prefix}/{name}")
                };
                self.entries.get(&child_path).map(|e| (name, e.is_dir))
            })
            .collect();

        // Try exact match first
        if let Some(entry) = self.entries.get(prefix) {
            if entry.is_dir {
                return match entry.access {
                    AccessMethod::Absolute => {
                        // External layer: read children directly and merge with manifest
                        let mut children = self.readdir_from_backend(&entry.backend_path);
                        // Add manifest children that aren't already present
                        let existing: std::collections::HashSet<String> =
                            children.iter().map(|(n, _)| n.clone()).collect();
                        for (name, is_dir) in manifest_children {
                            if !existing.contains(&name) {
                                children.push((name, is_dir));
                            }
                        }
                        ReaddirResult::Absolute(children)
                    }
                    AccessMethod::Openat => {
                        // Under mount root: return for openat with manifest children
                        ReaddirResult::Openat {
                            rel_path: prefix.to_string(),
                            manifest_children,
                        }
                    }
                };
            }
        }

        // Try prefix match for any directory entry
        if let Some((entry, suffix)) = self.find_prefix_match(prefix) {
            let target = if suffix.is_empty() {
                entry.backend_path.clone()
            } else {
                entry.backend_path.join(&suffix)
            };
            return match entry.access {
                AccessMethod::Absolute => {
                    ReaddirResult::Absolute(self.readdir_from_backend(&target))
                }
                AccessMethod::Openat => {
                    // This is a subdirectory under a registered Openat directory
                    ReaddirResult::Openat {
                        rel_path: prefix.to_string(),
                        manifest_children,
                    }
                }
            };
        }

        // Fallback: assume under mount root
        ReaddirResult::Openat {
            rel_path: prefix.to_string(),
            manifest_children,
        }
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

        let access = if backend_path.starts_with(&self.display_root) {
            AccessMethod::Openat
        } else {
            AccessMethod::Absolute
        };

        self.entries.insert(
            path.to_string(),
            Entry {
                backend_path,
                access,
                is_dir,
            },
        );
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

            // Update backend_path if it was under the old backend
            let updated_backend = entry
                .backend_path
                .strip_prefix(old_backend)
                .ok()
                .map(|suffix| {
                    if suffix.as_os_str().is_empty() {
                        new_backend.clone()
                    } else {
                        new_backend.join(suffix)
                    }
                })
                .unwrap_or(entry.backend_path);

            self.entries.insert(
                new_key,
                Entry {
                    backend_path: updated_backend,
                    access: entry.access,
                    is_dir: entry.is_dir,
                },
            );
        }
    }

    /// Returns the target directory for creating new files/directories.
    /// For Absolute (layer) paths, returns the absolute backend path.
    /// For Openat paths, returns relative path for caller to use with openat.
    pub(crate) fn create_target(&self, parent_path: &str) -> ResolvedPath {
        let parent_path = parent_path.trim_start_matches('/');

        // Try exact match first
        if let Some(entry) = self.entries.get(parent_path) {
            if entry.is_dir {
                return match entry.access {
                    AccessMethod::Openat => ResolvedPath::Openat(parent_path.to_string()),
                    AccessMethod::Absolute => ResolvedPath::Absolute(entry.backend_path.clone()),
                };
            }
        }

        // Try prefix match for any directory entry
        if let Some((entry, suffix)) = self.find_prefix_match(parent_path) {
            let target = if suffix.is_empty() {
                entry.backend_path.clone()
            } else {
                entry.backend_path.join(&suffix)
            };
            return match entry.access {
                AccessMethod::Openat => ResolvedPath::Openat(parent_path.to_string()),
                AccessMethod::Absolute => ResolvedPath::Absolute(target),
            };
        }

        // Fallback: Openat path
        ResolvedPath::Openat(parent_path.to_string())
    }
}
