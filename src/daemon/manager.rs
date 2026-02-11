use std::collections::HashMap;
use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use easy_fuser::prelude::BackgroundSession;
use easy_fuser::prelude::MountOption;
use parking_lot::RwLock;
use thiserror::Error;
use tracing::{debug, info, trace, warn};

use crate::types::{ManifestEntry, MountRoot, MountStatus, OwnerInfoWire};

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
        mount_roots: Vec<MountRoot>,
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
            mount_roots,
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
        mount_roots: Vec<MountRoot>,
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
            mount_roots,
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

#[derive(Clone, Debug)]
struct Entry {
    display_backend: PathBuf,
    io_backend: PathBuf,
    is_dir: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct DualPath {
    #[allow(dead_code)]
    pub(crate) display: PathBuf,
    pub(crate) io: PathBuf,
}

#[derive(Clone, Debug)]
struct MountRootEntry {
    virtual_prefix: String,
    display_backend: PathBuf,
    io_backend: PathBuf,
}

pub(crate) struct ReaddirPlan {
    pub(crate) io_dirs: Vec<PathBuf>,
    pub(crate) origin_children: Vec<(String, bool)>,
}

/// Manifest: maps virtual paths to their backend sources.
///
/// Stateless design: `origins` (immutable snapshot) + `mount_roots` (ordered
/// mount root list) replace the old mutable `entries` HashMap.
pub(crate) struct Manifest {
    display_root: PathBuf,
    real_procfd_root: PathBuf,
    origins: HashMap<String, Entry>,
    mount_roots: Vec<MountRootEntry>,
}

impl Manifest {
    /// Get direct child names from origins only (not from filesystem).
    /// Used for cache invalidation notifications.
    pub(crate) fn entry_names_at(&self, prefix: &str) -> Vec<String> {
        let prefix = prefix.trim_start_matches('/');
        self.origins
            .keys()
            .filter_map(|key| {
                let rest = if prefix.is_empty() {
                    key.as_str()
                } else {
                    key.strip_prefix(prefix)?.strip_prefix('/')?
                };
                (!rest.contains('/')).then(|| rest.to_string())
            })
            .collect()
    }
}

impl Manifest {
    pub(crate) fn from_entries(
        display_root: PathBuf,
        real_root_fd: i32,
        entries: Vec<ManifestEntry>,
        mount_roots: Vec<MountRoot>,
    ) -> Self {
        let real_procfd_root = procfd_root(real_root_fd);
        let mut map = HashMap::new();
        for e in entries {
            let io_backend = to_io_backend(&display_root, &real_procfd_root, &e.backend_path);
            map.insert(
                e.virtual_path,
                Entry {
                    display_backend: e.backend_path,
                    io_backend,
                    is_dir: e.is_dir,
                },
            );
        }

        let mr_entries: Vec<MountRootEntry> = mount_roots
            .into_iter()
            .map(|mr| {
                let io = to_io_backend(&display_root, &real_procfd_root, &mr.backend_path);
                MountRootEntry {
                    virtual_prefix: mr.virtual_prefix,
                    display_backend: mr.backend_path,
                    io_backend: io,
                }
            })
            .collect();

        Self {
            display_root,
            real_procfd_root,
            origins: map,
            mount_roots: mr_entries,
        }
    }

    fn owner_label(&self, backend_path: &PathBuf) -> String {
        if backend_path.starts_with(&self.display_root) {
            "self".to_string()
        } else {
            backend_path
                .to_string_lossy()
                .to_string()
        }
    }

    fn resolve_from_origins<'a>(&'a self, path: &'a str) -> Option<(&'a Entry, &'a str)> {
        if let Some(entry) = self.origins.get(path) {
            return Some((entry, ""));
        }
        self.find_dir_prefix(path)
    }

    pub(crate) fn which(&self, path: &str) -> Option<OwnerInfoWire> {
        let path = path.trim_start_matches('/');

        if let Some((entry, suffix)) = self.resolve_from_origins(path) {
            let backend_path = join_path(&entry.display_backend, suffix);
            return Some(OwnerInfoWire {
                owner: self.owner_label(&entry.display_backend),
                backend_path,
            });
        }

        // mount_roots scan
        for mr in &self.mount_roots {
            if let Some(suffix) = strip_mount_prefix(path, &mr.virtual_prefix) {
                let candidate = mr.display_backend.join(suffix);
                if candidate.symlink_metadata().is_ok() {
                    return Some(OwnerInfoWire {
                        owner: self.owner_label(&mr.display_backend),
                        backend_path: candidate,
                    });
                }
            }
        }

        None
    }

    pub(crate) fn resolve_paths(&self, path: &str) -> DualPath {
        let path = path.trim_start_matches('/');

        // 1. origins (exact match + dir prefix)
        if let Some((entry, suffix)) = self.resolve_from_origins(path) {
            return DualPath {
                display: join_path(&entry.display_backend, suffix),
                io: join_path(&entry.io_backend, suffix),
            };
        }

        // 2. mount_roots scan: first existing path wins
        for mr in &self.mount_roots {
            if let Some(suffix) = strip_mount_prefix(path, &mr.virtual_prefix) {
                let io_candidate = mr.io_backend.join(suffix);
                if io_candidate.symlink_metadata().is_ok() {
                    return DualPath {
                        display: mr.display_backend.join(suffix),
                        io: io_candidate,
                    };
                }
            }
        }

        // 3. fallback
        trace!(path, "no matching entry found, falling back to display_root");
        DualPath {
            display: self.display_root.join(path),
            io: self.real_procfd_root.join(path),
        }
    }

    pub(crate) fn resolve_io(&self, path: &str) -> PathBuf {
        self.resolve_paths(path).io
    }

    pub(crate) fn create_target_io(&self, parent_path: &str, child_name: &str) -> PathBuf {
        let parent_path = parent_path.trim_start_matches('/');
        let child_key = if parent_path.is_empty() {
            child_name.to_string()
        } else {
            format!("{parent_path}/{child_name}")
        };

        if let Some(entry) = self.origins.get(&child_key) {
            if entry.is_dir {
                return entry.io_backend.clone();
            }
            return entry
                .io_backend
                .parent()
                .unwrap_or(&entry.io_backend)
                .to_path_buf();
        }

        self.resolve_io(parent_path)
    }

    pub(crate) fn readdir_plan(&self, path: &str) -> ReaddirPlan {
        let prefix = path.trim_start_matches('/');

        let origin_children: Vec<(String, bool)> = self
            .entry_names_at(prefix)
            .into_iter()
            .filter_map(|name| {
                let child_path = if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{prefix}/{name}")
                };
                self.origins.get(&child_path).map(|e| (name, e.is_dir))
            })
            .collect();

        let mut io_dirs: Vec<PathBuf> = Vec::new();

        // origins (exact match + dir prefix) → add its io_backend
        if let Some((entry, suffix)) = self.resolve_from_origins(prefix) {
            if suffix.is_empty() && entry.is_dir || !suffix.is_empty() {
                io_dirs.push(join_path(&entry.io_backend, suffix));
            }
        }

        // mount_roots matching this prefix
        for mr in &self.mount_roots {
            if let Some(suffix) = strip_mount_prefix(prefix, &mr.virtual_prefix) {
                let candidate = mr.io_backend.join(suffix);
                if candidate.is_dir() {
                    io_dirs.push(candidate);
                }
            }
        }

        // Fallback: if no io_dirs found, use procfd root
        if io_dirs.is_empty() {
            if prefix.is_empty() {
                io_dirs.push(self.real_procfd_root.clone());
            } else {
                io_dirs.push(self.real_procfd_root.join(prefix));
            }
        }

        ReaddirPlan {
            io_dirs,
            origin_children,
        }
    }

    fn find_dir_prefix<'a>(&'a self, path: &'a str) -> Option<(&'a Entry, &'a str)> {
        let mut best: Option<(&Entry, &str)> = None;
        let mut best_len: Option<usize> = None;

        for (entry_path, entry) in &self.origins {
            if !entry.is_dir {
                continue;
            }

            if entry_path.is_empty() {
                if best_len.is_none() {
                    best = Some((entry, path));
                }
                continue;
            }

            let Some(rest) = path.strip_prefix(entry_path.as_str()) else {
                continue;
            };

            if !(rest.is_empty() || rest.starts_with('/')) {
                continue;
            }

            if best_len.is_some_and(|bl| entry_path.len() <= bl) {
                continue;
            }

            best_len = Some(entry_path.len());
            best = Some((entry, rest.trim_start_matches('/')));
        }

        best
    }
}

fn strip_mount_prefix<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    if prefix.is_empty() {
        return Some(path);
    }
    if path == prefix {
        return Some("");
    }
    let rest = path.strip_prefix(prefix)?;
    rest.strip_prefix('/')
}

fn to_io_backend(display_root: &Path, procfd_root: &Path, backend: &Path) -> PathBuf {
    backend
        .strip_prefix(display_root)
        .map(|rel| procfd_root.join(rel))
        .unwrap_or_else(|_| backend.to_path_buf())
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
                virtual_path: "".to_string(),
                backend_path: root.clone(),
                is_dir: true,
            },
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

        let manifest = Manifest::from_entries(root.clone(), raw_fd, entries, vec![]);

        let p = manifest.resolve_paths("real.txt");
        assert_eq!(p.display, root.join("real.txt"));
        assert_eq!(p.io, procfd_root(raw_fd).join("real.txt"));

        let p = manifest.resolve_paths("vendor/a.txt");
        assert_eq!(p.display, PathBuf::from("/opt/vendor").join("a.txt"));
        assert_eq!(p.io, PathBuf::from("/opt/vendor").join("a.txt"));

        let p = manifest.resolve_paths("missing.txt");
        assert_eq!(p.display, root.join("missing.txt"));
        assert_eq!(p.io, procfd_root(raw_fd).join("missing.txt"));

        drop(root_fd);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn create_target_uses_origin_after_rename() {
        let root = make_tmp_dir();
        std::fs::create_dir_all(&root).unwrap();
        let root = root.canonicalize().unwrap();

        let root_fd = File::open(&root).unwrap();
        let raw_fd = root_fd.as_raw_fd();

        let entries = vec![ManifestEntry {
            virtual_path: "config.toml".to_string(),
            backend_path: PathBuf::from("/ext/project/config.toml"),
            is_dir: false,
        }];

        let manifest = Manifest::from_entries(root.clone(), raw_fd, entries, vec![]);

        // origins still has config.toml → create should reuse its backend
        assert_eq!(
            manifest.create_target_io("", "config.toml"),
            PathBuf::from("/ext/project")
        );

        // Unrelated file → fallback to mount root
        assert_eq!(
            manifest.create_target_io("", "new_file.txt"),
            procfd_root(raw_fd)
        );

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

        let entries = vec![
            ManifestEntry {
                virtual_path: "".to_string(),
                backend_path: root.clone(),
                is_dir: true,
            },
            ManifestEntry {
                virtual_path: "vendor".to_string(),
                backend_path: PathBuf::from("/opt/vendor"),
                is_dir: true,
            },
        ];

        let manifest = Manifest::from_entries(root.clone(), raw_fd, entries, vec![]);

        assert_eq!(
            manifest.create_target_io("vendor", "lib.py"),
            PathBuf::from("/opt/vendor")
        );

        assert_eq!(
            manifest.create_target_io("", "local.txt"),
            procfd_root(raw_fd)
        );

        drop(root_fd);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn resolve_paths_scans_mount_roots() {
        let root = make_tmp_dir();
        std::fs::create_dir_all(&root).unwrap();
        let root = root.canonicalize().unwrap();

        // Create backend dirs with files
        let backend_a = root.join("_backend_a");
        let backend_b = root.join("_backend_b");
        std::fs::create_dir_all(&backend_a).unwrap();
        std::fs::create_dir_all(&backend_b).unwrap();
        std::fs::write(backend_a.join("a.txt"), "a").unwrap();
        std::fs::write(backend_b.join("b.txt"), "b").unwrap();

        let root_fd = File::open(&root).unwrap();
        let raw_fd = root_fd.as_raw_fd();

        let mount_roots = vec![
            MountRoot {
                virtual_prefix: "".to_string(),
                backend_path: backend_a.clone(),
            },
            MountRoot {
                virtual_prefix: "".to_string(),
                backend_path: backend_b.clone(),
            },
        ];

        let manifest = Manifest::from_entries(root.clone(), raw_fd, vec![], mount_roots);

        // a.txt exists in backend_a → first mount root wins
        let p = manifest.resolve_paths("a.txt");
        assert_eq!(p.io, procfd_root(raw_fd).join("_backend_a/a.txt"));

        // b.txt only exists in backend_b → second mount root
        let p = manifest.resolve_paths("b.txt");
        assert_eq!(p.io, procfd_root(raw_fd).join("_backend_b/b.txt"));

        // nonexistent → fallback to display_root
        let p = manifest.resolve_paths("missing.txt");
        assert_eq!(p.display, root.join("missing.txt"));

        drop(root_fd);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn readdir_plan_merges_multiple_backends() {
        let root = make_tmp_dir();
        std::fs::create_dir_all(&root).unwrap();
        let root = root.canonicalize().unwrap();

        let backend_a = root.join("_backend_a");
        let backend_b = root.join("_backend_b");
        std::fs::create_dir_all(&backend_a).unwrap();
        std::fs::create_dir_all(&backend_b).unwrap();
        std::fs::write(backend_a.join("shared.txt"), "a").unwrap();
        std::fs::write(backend_a.join("only_a.txt"), "a").unwrap();
        std::fs::write(backend_b.join("shared.txt"), "b").unwrap();
        std::fs::write(backend_b.join("only_b.txt"), "b").unwrap();

        let root_fd = File::open(&root).unwrap();
        let raw_fd = root_fd.as_raw_fd();

        let mount_roots = vec![
            MountRoot {
                virtual_prefix: "".to_string(),
                backend_path: backend_a.clone(),
            },
            MountRoot {
                virtual_prefix: "".to_string(),
                backend_path: backend_b.clone(),
            },
        ];

        let manifest = Manifest::from_entries(root.clone(), raw_fd, vec![], mount_roots);
        let plan = manifest.readdir_plan("");

        assert_eq!(plan.io_dirs.len(), 2);
        assert_eq!(plan.origin_children.len(), 0);

        drop(root_fd);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn resolve_paths_with_prefixed_mount_root() {
        let root = make_tmp_dir();
        std::fs::create_dir_all(&root).unwrap();
        let root = root.canonicalize().unwrap();

        let vendor_dir = root.join("_vendor");
        std::fs::create_dir_all(&vendor_dir).unwrap();
        std::fs::write(vendor_dir.join("lib.py"), "pass").unwrap();

        let root_fd = File::open(&root).unwrap();
        let raw_fd = root_fd.as_raw_fd();

        let mount_roots = vec![MountRoot {
            virtual_prefix: "vendor".to_string(),
            backend_path: vendor_dir.clone(),
        }];

        let manifest = Manifest::from_entries(root.clone(), raw_fd, vec![], mount_roots);

        let p = manifest.resolve_paths("vendor/lib.py");
        assert_eq!(p.io, procfd_root(raw_fd).join("_vendor/lib.py"));

        // Path outside prefix → fallback
        let p = manifest.resolve_paths("other.txt");
        assert_eq!(p.display, root.join("other.txt"));

        drop(root_fd);
        std::fs::remove_dir_all(&root).unwrap();
    }
}
