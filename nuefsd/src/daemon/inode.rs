use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};

use fuser::FileType;

pub(crate) const ROOT_INO: u64 = 1;

struct InodeEntry {
    path: Option<PathBuf>,
    lookup_count: u64,
    generation: u64,
}

pub(crate) struct InodeTable {
    entries: Vec<InodeEntry>,
    free_list: VecDeque<usize>,
    by_path: HashMap<PathBuf, usize>,
    next_generation: u64,
}

impl InodeTable {
    pub(crate) fn new() -> Self {
        let root_path = PathBuf::from("");
        let mut by_path = HashMap::new();
        by_path.insert(root_path.clone(), 0);
        Self {
            entries: vec![InodeEntry {
                path: Some(root_path),
                lookup_count: u64::MAX / 2, // root is never forgotten
                generation: 0,
            }],
            free_list: VecDeque::new(),
            by_path,
            next_generation: 1,
        }
    }

    fn alloc_slot(&mut self, path: PathBuf) -> (usize, u64) {
        if let Some(idx) = self.free_list.pop_front() {
            let generation = self.next_generation;
            self.next_generation += 1;
            self.entries[idx] = InodeEntry {
                path: Some(path),
                lookup_count: 0,
                generation: generation,
            };
            (idx, generation)
        } else {
            let generation = self.next_generation;
            self.next_generation += 1;
            let idx = self.entries.len();
            self.entries.push(InodeEntry {
                path: Some(path),
                lookup_count: 0,
                generation: generation,
            });
            (idx, generation)
        }
    }

    fn idx_to_ino(idx: usize) -> u64 {
        (idx as u64) + 1
    }

    fn ino_to_idx(ino: u64) -> usize {
        (ino - 1) as usize
    }

    /// Add or get an existing inode for a path. Does NOT increment lookup_count.
    pub(crate) fn add_or_get(&mut self, path: &Path) -> (u64, u64) {
        if let Some(&idx) = self.by_path.get(path) {
            let e = &self.entries[idx];
            return (Self::idx_to_ino(idx), e.generation);
        }
        let (idx, generation) = self.alloc_slot(path.to_path_buf());
        self.by_path.insert(path.to_path_buf(), idx);
        (Self::idx_to_ino(idx), generation)
    }

    /// Increment lookup_count for an existing inode.
    pub(crate) fn lookup(&mut self, ino: u64) {
        let idx = Self::ino_to_idx(ino);
        if let Some(e) = self.entries.get_mut(idx) {
            e.lookup_count = e.lookup_count.saturating_add(1);
        }
    }

    /// Decrement lookup_count. Returns remaining count.
    pub(crate) fn forget(&mut self, ino: u64, nlookup: u64) -> u64 {
        let idx = Self::ino_to_idx(ino);
        let Some(e) = self.entries.get_mut(idx) else {
            return 0;
        };
        e.lookup_count = e.lookup_count.saturating_sub(nlookup);
        if e.lookup_count == 0 {
            if let Some(path) = e.path.take() {
                self.by_path.remove(&path);
            }
            self.free_list.push_back(idx);
        }
        e.lookup_count
    }

    pub(crate) fn get_path(&self, ino: u64) -> Option<&Path> {
        let idx = Self::ino_to_idx(ino);
        self.entries
            .get(idx)
            .and_then(|e| e.path.as_deref())
    }

    pub(crate) fn get_inode(&self, path: &Path) -> Option<u64> {
        self.by_path.get(path).map(|&idx| Self::idx_to_ino(idx))
    }

    /// Rename: update self + all children with old prefix.
    pub(crate) fn rename(&mut self, old: &Path, new: &Path) {
        let old_str = old.as_os_str();
        let new_path = new.to_path_buf();

        // Collect all paths that need updating (self + children).
        let mut renames: Vec<(PathBuf, PathBuf)> = Vec::new();
        for (path, _) in &self.by_path {
            if path == old {
                renames.push((path.clone(), new_path.clone()));
            } else if path.starts_with(old) {
                // child: strip old prefix, join with new
                if let Ok(suffix) = path.strip_prefix(old_str) {
                    renames.push((path.clone(), new_path.join(suffix)));
                }
            }
        }

        for (old_p, new_p) in renames {
            if let Some(idx) = self.by_path.remove(&old_p) {
                self.entries[idx].path = Some(new_p.clone());
                self.by_path.insert(new_p, idx);
            }
        }
    }

    /// Unlink: remove from by_path so new lookups won't find it.
    /// The entry itself keeps its path so operations on open fds still work.
    /// The slot is reclaimed when forget drops lookup_count to 0.
    pub(crate) fn unlink(&mut self, path: &Path) {
        if let Some(&idx) = self.by_path.get(path) {
            self.by_path.remove(path);
            if self.entries[idx].lookup_count == 0 {
                self.entries[idx].path = None;
                self.free_list.push_back(idx);
            }
            // else: keep path in entry so getattr/read by ino still works
        }
    }
}

pub(crate) struct DirCacheEntry {
    pub(crate) ino: u64,
    pub(crate) kind: FileType,
    pub(crate) name: String,
}

pub(crate) struct DirCache {
    entries: HashMap<u64, Option<Vec<DirCacheEntry>>>,
    next_fh: u64,
}

impl DirCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            next_fh: 1,
        }
    }

    pub(crate) fn alloc(&mut self) -> u64 {
        let fh = self.next_fh;
        self.next_fh += 1;
        self.entries.insert(fh, None);
        fh
    }

    pub(crate) fn populate(&mut self, fh: u64, entries: Vec<DirCacheEntry>) {
        if let Some(slot) = self.entries.get_mut(&fh) {
            *slot = Some(entries);
        }
    }

    pub(crate) fn get(&self, fh: u64) -> Option<&[DirCacheEntry]> {
        self.entries.get(&fh)?.as_deref()
    }

    pub(crate) fn remove(&mut self, fh: u64) {
        self.entries.remove(&fh);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_inode_1() {
        let table = InodeTable::new();
        assert_eq!(table.get_path(ROOT_INO), Some(Path::new("")));
        assert_eq!(table.get_inode(Path::new("")), Some(ROOT_INO));
    }

    #[test]
    fn add_and_lookup() {
        let mut table = InodeTable::new();
        let (ino, generation) = table.add_or_get(Path::new("foo"));
        assert!(ino > ROOT_INO);
        assert!(generation > 0);

        // Same path returns same inode
        let (ino2, generation2) = table.add_or_get(Path::new("foo"));
        assert_eq!(ino, ino2);
        assert_eq!(generation, generation2);

        // Different path gets different inode
        let (ino3, _) = table.add_or_get(Path::new("bar"));
        assert_ne!(ino, ino3);
    }

    #[test]
    fn forget_and_reuse() {
        let mut table = InodeTable::new();
        let (ino, generation) = table.add_or_get(Path::new("temp"));
        table.lookup(ino);
        table.lookup(ino);

        assert_eq!(table.forget(ino, 1), 1);
        assert!(table.get_path(ino).is_some());

        assert_eq!(table.forget(ino, 1), 0);
        assert!(table.get_path(ino).is_none());
        assert!(table.get_inode(Path::new("temp")).is_none());

        let (ino2, generation2) = table.add_or_get(Path::new("reuse"));
        assert_eq!(ino, ino2);
        assert!(generation2 > generation);
    }

    #[test]
    fn rename_updates_children() {
        let mut table = InodeTable::new();
        let (dir_ino, _) = table.add_or_get(Path::new("old_dir"));
        let (child_ino, _) = table.add_or_get(Path::new("old_dir/file.txt"));
        let (deep_ino, _) = table.add_or_get(Path::new("old_dir/sub/deep.txt"));

        table.rename(Path::new("old_dir"), Path::new("new_dir"));

        assert_eq!(table.get_path(dir_ino), Some(Path::new("new_dir")));
        assert_eq!(table.get_path(child_ino), Some(Path::new("new_dir/file.txt")));
        assert_eq!(table.get_path(deep_ino), Some(Path::new("new_dir/sub/deep.txt")));

        assert!(table.get_inode(Path::new("old_dir")).is_none());
        assert_eq!(table.get_inode(Path::new("new_dir")), Some(dir_ino));
        assert_eq!(table.get_inode(Path::new("new_dir/file.txt")), Some(child_ino));
    }

    #[test]
    fn unlink_lifecycle() {
        let mut table = InodeTable::new();
        let (ino, _) = table.add_or_get(Path::new("file"));
        table.lookup(ino);

        table.unlink(Path::new("file"));
        // by_path removed, but path stays in entry because lookup_count > 0
        assert!(table.get_inode(Path::new("file")).is_none());
        assert_eq!(table.get_path(ino), Some(Path::new("file")));

        // forget releases the entry
        assert_eq!(table.forget(ino, 1), 0);
        assert!(table.get_path(ino).is_none());
    }

    #[test]
    fn dir_cache_lifecycle() {
        let mut cache = DirCache::new();
        let fh = cache.alloc();
        assert!(cache.get(fh).is_none()); // not yet populated

        cache.populate(fh, vec![
            DirCacheEntry { ino: 1, kind: FileType::Directory, name: ".".into() },
            DirCacheEntry { ino: 1, kind: FileType::Directory, name: "..".into() },
        ]);
        assert_eq!(cache.get(fh).unwrap().len(), 2);

        cache.remove(fh);
        assert!(cache.get(fh).is_none());
    }
}
