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
    /// Keeps the real root fd alive for procfd-based IO paths.
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

        let manifest = Arc::new(RwLock::new(Manifest::from_entries(
            root.clone(),
            real_root_fd.as_raw_fd(),
            entries,
        )));
        let fs = NueFs::new(manifest.clone());

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

        let new_manifest = Manifest::from_entries(
            session.root.clone(),
            session.real_root_fd.as_raw_fd(),
            entries,
        );

        let old_root_children = session.manifest.read().entry_names_at("");
        let new_root_children = new_manifest.entry_names_at("");

        *session.manifest.write() = new_manifest;

        if let Some(ref notifier) = *session.notifier.read() {
            let _ = notifier.inval_inode(1, 0, 0);

            // Best-effort refresh of root directory entries.
            let names: std::collections::BTreeSet<String> = old_root_children
                .into_iter()
                .chain(new_root_children)
                .collect();
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Owner {
    Real,
    Layer,
}

impl Owner {
    fn as_str(self) -> &'static str {
        match self {
            Self::Real => "real",
            Self::Layer => "layer",
        }
    }
}

#[derive(Clone, Debug)]
struct Entry {
    display_backend: PathBuf,
    io_backend: PathBuf,
    owner: Owner,
    is_dir: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedPaths {
    pub(crate) display_path: PathBuf,
    pub(crate) io_path: PathBuf,
}

#[derive(Clone, Debug)]
pub(crate) struct DirTarget {
    pub(crate) display_dir: PathBuf,
    pub(crate) io_dir: PathBuf,
}

pub(crate) struct ReaddirPlan {
    pub(crate) io_dir: PathBuf,
    pub(crate) manifest_children: Vec<(String, bool)>,
}

/// Manifest: maps virtual paths to their backend sources.
pub(crate) struct Manifest {
    display_root: PathBuf,
    real_procfd_root: PathBuf,
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
        real_root_fd: i32,
        entries: Vec<ManifestEntry>,
    ) -> Self {
        let real_procfd_root = procfd_root(real_root_fd);
        let mut map = HashMap::new();
        for e in entries {
            let (owner, io_backend) = if e.backend_path.starts_with(&display_root) {
                let rel = e
                    .backend_path
                    .strip_prefix(&display_root)
                    .unwrap_or_else(|_| e.backend_path.as_path());
                (Owner::Real, real_procfd_root.join(rel))
            } else {
                (Owner::Layer, e.backend_path.clone())
            };

            map.insert(
                e.virtual_path,
                Entry {
                    display_backend: e.backend_path,
                    io_backend,
                    owner,
                    is_dir: e.is_dir,
                },
            );
        }

        Self {
            display_root,
            real_procfd_root,
            entries: map,
        }
    }

    pub(crate) fn which(&self, path: &str) -> Option<OwnerInfoWire> {
        let path = path.trim_start_matches('/');

        if let Some(entry) = self.entries.get(path) {
            return Some(OwnerInfoWire {
                owner: entry.owner.as_str().to_string(),
                backend_path: entry.display_backend.clone(),
            });
        }

        if let Some((entry, suffix)) = self.find_dir_prefix(path) {
            let backend_path = join_path(&entry.display_backend, suffix);
            return Some(OwnerInfoWire {
                owner: entry.owner.as_str().to_string(),
                backend_path,
            });
        }

        // Fallback: assume real path (caller will verify existence)
        Some(OwnerInfoWire {
            owner: "real".to_string(),
            backend_path: self.display_root.join(path),
        })
    }

    pub(crate) fn resolve_paths(&self, path: &str) -> ResolvedPaths {
        let path = path.trim_start_matches('/');

        if let Some(entry) = self.entries.get(path) {
            return ResolvedPaths {
                display_path: entry.display_backend.clone(),
                io_path: entry.io_backend.clone(),
            };
        }

        if let Some((entry, suffix)) = self.find_dir_prefix(path) {
            return ResolvedPaths {
                display_path: join_path(&entry.display_backend, suffix),
                io_path: join_path(&entry.io_backend, suffix),
            };
        }

        ResolvedPaths {
            display_path: self.display_root.join(path),
            io_path: self.real_procfd_root.join(path),
        }
    }

    pub(crate) fn create_target(&self, parent_path: &str) -> DirTarget {
        let parent_path = parent_path.trim_start_matches('/');

        if let Some(entry) = self.entries.get(parent_path) {
            if entry.is_dir {
                return DirTarget {
                    display_dir: entry.display_backend.clone(),
                    io_dir: entry.io_backend.clone(),
                };
            }
        }

        if let Some((entry, suffix)) = self.find_dir_prefix(parent_path) {
            return DirTarget {
                display_dir: join_path(&entry.display_backend, suffix),
                io_dir: join_path(&entry.io_backend, suffix),
            };
        }

        DirTarget {
            display_dir: self.display_root.join(parent_path),
            io_dir: self.real_procfd_root.join(parent_path),
        }
    }

    pub(crate) fn readdir_plan(&self, path: &str) -> ReaddirPlan {
        let prefix = path.trim_start_matches('/');

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

        let target = self.create_target(prefix);
        ReaddirPlan {
            io_dir: target.io_dir,
            manifest_children,
        }
    }

    fn find_dir_prefix<'a>(&'a self, path: &'a str) -> Option<(&'a Entry, &'a str)> {
        let mut best: Option<(&Entry, &str)> = None;
        let mut best_len = 0usize;

        for (entry_path, entry) in &self.entries {
            if !entry.is_dir {
                continue;
            }

            let Some(rest) = path.strip_prefix(entry_path.as_str()) else {
                continue;
            };

            if !(rest.is_empty() || rest.starts_with('/')) {
                continue;
            }

            if entry_path.len() <= best_len {
                continue;
            }

            best_len = entry_path.len();
            best = Some((entry, rest.trim_start_matches('/')));
        }

        best
    }

    pub(crate) fn add_entry_with_backend(
        &mut self,
        path: &str,
        backend_path: PathBuf,
        is_dir: bool,
    ) {
        let path = path.trim_start_matches('/');

        let owner = if backend_path.starts_with(&self.display_root) {
            Owner::Real
        } else {
            Owner::Layer
        };

        let io_backend = match owner {
            Owner::Real => {
                let rel = backend_path
                    .strip_prefix(&self.display_root)
                    .unwrap_or_else(|_| backend_path.as_path());
                self.real_procfd_root.join(rel)
            }
            Owner::Layer => backend_path.clone(),
        };

        self.entries.insert(
            path.to_string(),
            Entry {
                display_backend: backend_path,
                io_backend,
                owner,
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
                .display_backend
                .strip_prefix(old_backend)
                .ok()
                .map(|suffix| {
                    if suffix.as_os_str().is_empty() {
                        new_backend.clone()
                    } else {
                        new_backend.join(suffix)
                    }
                })
                .unwrap_or(entry.display_backend);

            self.add_entry_with_backend(&new_key, updated_backend, entry.is_dir);
        }
    }
}

fn procfd_root(raw_fd: i32) -> PathBuf {
    PathBuf::from("/proc/self/fd")
        .join(raw_fd.to_string())
        .join(".")
}

fn join_path(base: &PathBuf, suffix: &str) -> PathBuf {
    if suffix.is_empty() {
        base.clone()
    } else {
        base.join(suffix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tmp_dir() -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("nuefs-test-{nanos}"))
    }

    #[test]
    fn resolve_paths_maps_real_to_procfd() {
        let root = make_tmp_dir();
        std::fs::create_dir_all(&root).unwrap();
        let root = root.canonicalize().unwrap();

        let root_fd = File::open(&root).unwrap();
        let raw_fd = root_fd.as_raw_fd();

        let entries = vec![
            ManifestEntry {
                virtual_path: "real.txt".to_string(),
                backend_path: root.join("real.txt"),
                is_dir: false,
            },
            ManifestEntry {
                virtual_path: "vendor".to_string(),
                backend_path: PathBuf::from("/opt/vendor"),
                is_dir: true,
            },
        ];

        let manifest = Manifest::from_entries(root.clone(), raw_fd, entries);

        let p = manifest.resolve_paths("real.txt");
        assert_eq!(p.display_path, root.join("real.txt"));
        assert_eq!(p.io_path, procfd_root(raw_fd).join("real.txt"));

        let p = manifest.resolve_paths("vendor/a.txt");
        assert_eq!(p.display_path, PathBuf::from("/opt/vendor").join("a.txt"));
        assert_eq!(p.io_path, PathBuf::from("/opt/vendor").join("a.txt"));

        let p = manifest.resolve_paths("missing.txt");
        assert_eq!(p.display_path, root.join("missing.txt"));
        assert_eq!(p.io_path, procfd_root(raw_fd).join("missing.txt"));

        drop(root_fd);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn create_target_uses_dir_prefix_match() {
        let root = make_tmp_dir();
        std::fs::create_dir_all(&root).unwrap();
        let root = root.canonicalize().unwrap();

        let root_fd = File::open(&root).unwrap();
        let raw_fd = root_fd.as_raw_fd();

        let entries = vec![ManifestEntry {
            virtual_path: "vendor".to_string(),
            backend_path: PathBuf::from("/opt/vendor"),
            is_dir: true,
        }];

        let manifest = Manifest::from_entries(root.clone(), raw_fd, entries);

        let target = manifest.create_target("vendor/subdir");
        assert_eq!(
            target.display_dir,
            PathBuf::from("/opt/vendor").join("subdir")
        );
        assert_eq!(target.io_dir, PathBuf::from("/opt/vendor").join("subdir"));

        let target = manifest.create_target("local");
        assert_eq!(target.display_dir, root.join("local"));
        assert_eq!(target.io_dir, procfd_root(raw_fd).join("local"));

        drop(root_fd);
        std::fs::remove_dir_all(&root).unwrap();
    }
}
