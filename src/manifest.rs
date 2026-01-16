use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::{Mount, OwnerInfo};

/// Source of a file/directory
#[derive(Clone, Debug)]
pub enum Source {
    /// From the real filesystem
    Real(PathBuf),
    /// From an layer
    Layer { source_root: PathBuf, backend_path: PathBuf },
}

/// Entry in the manifest
#[derive(Clone, Debug)]
pub struct Entry {
    pub source: Source,
    pub is_dir: bool,
}

/// Manifest: maps virtual paths to their backend sources
pub struct Manifest {
    root: PathBuf,
    #[allow(dead_code)]
    mounts: Vec<Mount>,
    /// Cache of resolved paths (relative path -> entry)
    entries: HashMap<String, Entry>,
}

impl Manifest {
    /// Build manifest by scanning real root and layer sources
    pub fn build(root: &Path, mounts: &[Mount]) -> Result<Self, ManifestError> {
        let mut manifest = Self {
            root: root.to_path_buf(),
            mounts: mounts.to_vec(),
            entries: HashMap::new(),
        };

        // Scan real filesystem first
        manifest.scan_real(root, "")?;

        // Then scan layers (later layers override earlier ones, first declared = highest priority)
        // So we iterate in reverse to let first declared win
        for mount in mounts.iter().rev() {
            manifest.scan_layer(mount)?;
        }

        Ok(manifest)
    }

    /// Scan real filesystem
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
                format!("{}/{}", prefix, name)
            };

            let metadata = entry.metadata().map_err(|e| ManifestError::Io(entry.path(), e))?;
            let is_dir = metadata.is_dir();

            self.entries.insert(
                rel_path.clone(),
                Entry {
                    source: Source::Real(entry.path()),
                    is_dir,
                },
            );

            if is_dir {
                self.scan_real(base, &rel_path)?;
            }
        }

        Ok(())
    }

    /// Scan an layer source and add/override entries
    fn scan_layer(&mut self, mount: &Mount) -> Result<(), ManifestError> {
        let target = mount.target.to_string_lossy();
        // Normalize target: "." or "./" -> ""
        let target = target.trim_start_matches("./");
        let target = if target == "." { "" } else { target };
        self.scan_layer_dir(&mount.source, target, mount)
    }

    fn scan_layer_dir(&mut self, dir: &Path, prefix: &str, mount: &Mount) -> Result<(), ManifestError> {
        if !dir.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(dir).map_err(|e| ManifestError::Io(dir.to_path_buf(), e))? {
            let entry = entry.map_err(|e| ManifestError::Io(dir.to_path_buf(), e))?;
            let name = entry.file_name().to_string_lossy().to_string();
            let rel_path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", prefix, name)
            };

            let metadata = entry.metadata().map_err(|e| ManifestError::Io(entry.path(), e))?;
            let is_dir = metadata.is_dir();

            // Check if this path already exists
            let existing = self.entries.get(&rel_path);

            // If both are directories, we merge (don't override)
            // If layer is file and real is file, layer wins
            // If types conflict, layer wins (for simplicity)
            let should_insert = match existing {
                None => true,
                Some(e) if e.is_dir && is_dir => false, // Both dirs, keep real's entry but scan layer
                Some(_) => true,                        // Override
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

    /// Query which backend owns a path
    pub fn which(&self, path: &str) -> Option<OwnerInfo> {
        let path = path.trim_start_matches('/');
        self.entries.get(path).map(|entry| match &entry.source {
            Source::Real(p) => OwnerInfo {
                owner: "real".to_string(),
                backend_path: p.clone(),
            },
            Source::Layer { source_root, backend_path } => OwnerInfo {
                owner: source_root.to_string_lossy().to_string(),
                backend_path: backend_path.clone(),
            },
        })
    }

    /// Get entry for a path
    pub fn get(&self, path: &str) -> Option<&Entry> {
        let path = path.trim_start_matches('/');
        self.entries.get(path)
    }

    /// Get backend path for a virtual path
    pub fn resolve(&self, path: &str) -> Option<PathBuf> {
        self.get(path).map(|e| match &e.source {
            Source::Real(p) => p.clone(),
            Source::Layer { backend_path, .. } => backend_path.clone(),
        })
    }

    /// List directory contents
    pub fn readdir(&self, path: &str) -> Vec<(String, bool)> {
        let prefix = path.trim_start_matches('/');
        let prefix_with_slash = if prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", prefix)
        };

        let mut children = HashMap::new();

        for (rel_path, entry) in &self.entries {
            if prefix.is_empty() {
                // Root directory: entries without '/'
                if !rel_path.contains('/') {
                    children.insert(rel_path.clone(), entry.is_dir);
                }
            } else if let Some(rest) = rel_path.strip_prefix(&prefix_with_slash) {
                // Subdirectory: direct children only
                if !rest.contains('/') {
                    children.insert(rest.to_string(), entry.is_dir);
                }
            }
        }

        children.into_iter().collect()
    }

    /// Get the real root path
    #[allow(dead_code)]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Determine where to create a new file
    pub fn create_target(&self, parent_path: &str) -> PathBuf {
        let parent_path = parent_path.trim_start_matches('/');

        // If parent is a pure layer directory, create in layer
        if let Some(entry) = self.get(parent_path) {
            if let Source::Layer { backend_path, .. } = &entry.source {
                if entry.is_dir {
                    return backend_path.clone();
                }
            }
        }

        // Otherwise, create in real
        if parent_path.is_empty() {
            self.root.clone()
        } else {
            self.root.join(parent_path)
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
