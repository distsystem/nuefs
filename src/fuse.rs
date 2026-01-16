use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use parking_lot::RwLock;

use crate::manifest::Manifest;

const TTL: Duration = Duration::from_secs(1);

/// FUSE layered filesystem
pub struct NueFs {
    root: PathBuf,
    manifest: Arc<Manifest>,
    /// inode -> path mapping
    inodes: RwLock<HashMap<u64, String>>,
    /// path -> inode mapping
    paths: RwLock<HashMap<String, u64>>,
    /// Next inode number
    next_ino: AtomicU64,
    /// Open file handles: fh -> File
    handles: RwLock<HashMap<u64, File>>,
    /// Next file handle
    next_fh: AtomicU64,
}

impl NueFs {
    pub fn new(root: PathBuf, manifest: Arc<Manifest>) -> Self {
        let mut inodes = HashMap::new();
        let mut paths = HashMap::new();

        // Root inode is always 1
        inodes.insert(1, String::new());
        paths.insert(String::new(), 1);

        Self {
            root,
            manifest,
            inodes: RwLock::new(inodes),
            paths: RwLock::new(paths),
            next_ino: AtomicU64::new(2),
            handles: RwLock::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
        }
    }

    /// Get or create inode for a path
    fn get_or_create_ino(&self, path: &str) -> u64 {
        let paths = self.paths.read();
        if let Some(&ino) = paths.get(path) {
            return ino;
        }
        drop(paths);

        let mut paths = self.paths.write();
        let mut inodes = self.inodes.write();

        // Double-check after acquiring write lock
        if let Some(&ino) = paths.get(path) {
            return ino;
        }

        let ino = self.next_ino.fetch_add(1, Ordering::SeqCst);
        paths.insert(path.to_string(), ino);
        inodes.insert(ino, path.to_string());
        ino
    }

    /// Get path for an inode
    fn get_path(&self, ino: u64) -> Option<String> {
        self.inodes.read().get(&ino).cloned()
    }

    /// Get file attributes for a backend path
    fn get_attr(&self, ino: u64, backend_path: &PathBuf) -> Option<FileAttr> {
        let metadata = fs::metadata(backend_path).ok()?;

        let kind = if metadata.is_dir() {
            FileType::Directory
        } else if metadata.is_symlink() {
            FileType::Symlink
        } else {
            FileType::RegularFile
        };

        let atime = metadata.accessed().unwrap_or(UNIX_EPOCH);
        let mtime = metadata.modified().unwrap_or(UNIX_EPOCH);
        let ctime = SystemTime::UNIX_EPOCH + Duration::from_secs(metadata.ctime() as u64);

        Some(FileAttr {
            ino,
            size: metadata.len(),
            blocks: metadata.blocks(),
            atime,
            mtime,
            ctime,
            crtime: ctime,
            kind,
            perm: (metadata.mode() & 0o7777) as u16,
            nlink: metadata.nlink() as u32,
            uid: metadata.uid(),
            gid: metadata.gid(),
            rdev: metadata.rdev() as u32,
            blksize: metadata.blksize() as u32,
            flags: 0,
        })
    }

    /// Allocate a new file handle
    fn alloc_fh(&self, file: File) -> u64 {
        let fh = self.next_fh.fetch_add(1, Ordering::SeqCst);
        self.handles.write().insert(fh, file);
        fh
    }

    /// Get file for a handle
    fn get_file(&self, fh: u64) -> Option<File> {
        self.handles.read().get(&fh).map(|f| f.try_clone().ok()).flatten()
    }

    /// Release a file handle
    fn release_fh(&self, fh: u64) {
        self.handles.write().remove(&fh);
    }
}

impl Filesystem for NueFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_path = match self.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let name = name.to_string_lossy();
        let child_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };

        let backend_path = match self.manifest.resolve(&child_path) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let ino = self.get_or_create_ino(&child_path);

        match self.get_attr(ino, &backend_path) {
            Some(attr) => reply.entry(&TTL, &attr, 0),
            None => reply.error(libc::ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let path = match self.get_path(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Root directory: return synthetic attrs to avoid recursive FUSE call
        if path.is_empty() {
            let now = SystemTime::now();
            let attr = FileAttr {
                ino: 1,
                size: 0,
                blocks: 0,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
                rdev: 0,
                blksize: 512,
                flags: 0,
            };
            reply.attr(&TTL, &attr);
            return;
        }

        let backend_path = match self.manifest.resolve(&path) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        match self.get_attr(ino, &backend_path) {
            Some(attr) => reply.attr(&TTL, &attr),
            None => reply.error(libc::ENOENT),
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        let path = match self.get_path(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (if path.is_empty() { 1 } else { ino }, FileType::Directory, "..".to_string()),
        ];

        for (name, is_dir) in self.manifest.readdir(&path) {
            let child_path = if path.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", path, name)
            };
            let child_ino = self.get_or_create_ino(&child_path);
            let kind = if is_dir { FileType::Directory } else { FileType::RegularFile };
            entries.push((child_ino, kind, name));
        }

        for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(ino, (i + 1) as i64, kind, name) {
                break;
            }
        }

        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        let path = match self.get_path(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let backend_path = match self.manifest.resolve(&path) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let file = match OpenOptions::new()
            .read(true)
            .write((flags & libc::O_WRONLY != 0) || (flags & libc::O_RDWR != 0))
            .open(&backend_path)
        {
            Ok(f) => f,
            Err(e) => {
                reply.error(e.raw_os_error().unwrap_or(libc::EIO));
                return;
            }
        };

        let fh = self.alloc_fh(file);
        reply.opened(fh, 0);
    }

    fn read(&mut self, _req: &Request, _ino: u64, fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        let mut file = match self.get_file(fh) {
            Some(f) => f,
            None => {
                reply.error(libc::EBADF);
                return;
            }
        };

        if let Err(e) = file.seek(SeekFrom::Start(offset as u64)) {
            reply.error(e.raw_os_error().unwrap_or(libc::EIO));
            return;
        }

        let mut buf = vec![0u8; size as usize];
        match file.read(&mut buf) {
            Ok(n) => reply.data(&buf[..n]),
            Err(e) => reply.error(e.raw_os_error().unwrap_or(libc::EIO)),
        }
    }

    fn write(&mut self, _req: &Request, _ino: u64, fh: u64, offset: i64, data: &[u8], _write_flags: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyWrite) {
        let mut handles = self.handles.write();
        let file = match handles.get_mut(&fh) {
            Some(f) => f,
            None => {
                reply.error(libc::EBADF);
                return;
            }
        };

        if let Err(e) = file.seek(SeekFrom::Start(offset as u64)) {
            reply.error(e.raw_os_error().unwrap_or(libc::EIO));
            return;
        }

        match file.write(data) {
            Ok(n) => reply.written(n as u32),
            Err(e) => reply.error(e.raw_os_error().unwrap_or(libc::EIO)),
        }
    }

    fn release(&mut self, _req: &Request, _ino: u64, fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) {
        self.release_fh(fh);
        reply.ok();
    }

    fn create(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, _umask: u32, flags: i32, reply: ReplyCreate) {
        let parent_path = match self.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let name = name.to_string_lossy();
        let child_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };

        // Determine where to create the file
        let target_dir = self.manifest.create_target(&parent_path);
        let backend_path = target_dir.join(&*name);

        // Create parent directories if needed
        if let Some(parent) = backend_path.parent() {
            if let Err(e) = fs::create_dir_all(parent) {
                reply.error(e.raw_os_error().unwrap_or(libc::EIO));
                return;
            }
        }

        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(flags & libc::O_TRUNC != 0)
            .mode(mode)
            .open(&backend_path)
        {
            Ok(f) => f,
            Err(e) => {
                reply.error(e.raw_os_error().unwrap_or(libc::EIO));
                return;
            }
        };

        let ino = self.get_or_create_ino(&child_path);
        let fh = self.alloc_fh(file);

        match self.get_attr(ino, &backend_path) {
            Some(attr) => reply.created(&TTL, &attr, 0, fh, 0),
            None => reply.error(libc::EIO),
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = match self.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let name = name.to_string_lossy();
        let child_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };

        let backend_path = match self.manifest.resolve(&child_path) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        match fs::remove_file(&backend_path) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.raw_os_error().unwrap_or(libc::EIO)),
        }
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, _umask: u32, reply: ReplyEntry) {
        let parent_path = match self.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let name = name.to_string_lossy();
        let child_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };

        let target_dir = self.manifest.create_target(&parent_path);
        let backend_path = target_dir.join(&*name);

        match fs::create_dir(&backend_path) {
            Ok(()) => {
                if let Err(e) = fs::set_permissions(&backend_path, fs::Permissions::from_mode(mode)) {
                    // Non-fatal, continue
                    eprintln!("Warning: failed to set permissions: {}", e);
                }
            }
            Err(e) => {
                reply.error(e.raw_os_error().unwrap_or(libc::EIO));
                return;
            }
        }

        let ino = self.get_or_create_ino(&child_path);

        match self.get_attr(ino, &backend_path) {
            Some(attr) => reply.entry(&TTL, &attr, 0),
            None => reply.error(libc::EIO),
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = match self.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let name = name.to_string_lossy();
        let child_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };

        let backend_path = match self.manifest.resolve(&child_path) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        match fs::remove_dir(&backend_path) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.raw_os_error().unwrap_or(libc::EIO)),
        }
    }

    fn rename(&mut self, _req: &Request, parent: u64, name: &OsStr, newparent: u64, newname: &OsStr, _flags: u32, reply: ReplyEmpty) {
        let parent_path = match self.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let newparent_path = match self.get_path(newparent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let name = name.to_string_lossy();
        let newname = newname.to_string_lossy();

        let old_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };

        let new_path = if newparent_path.is_empty() {
            newname.to_string()
        } else {
            format!("{}/{}", newparent_path, newname)
        };

        let old_backend = match self.manifest.resolve(&old_path) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // For new path, determine where it should go
        let new_backend = {
            // If new path already exists, use its backend
            if let Some(p) = self.manifest.resolve(&new_path) {
                p
            } else {
                // Otherwise, determine based on parent
                let target_dir = self.manifest.create_target(&newparent_path);
                target_dir.join(&*newname)
            }
        };

        // Check if rename crosses backends (different mount points)
        // For simplicity, we allow rename within the same filesystem
        match fs::rename(&old_backend, &new_backend) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.raw_os_error().unwrap_or(libc::EIO)),
        }
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let path = match self.get_path(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let backend_path = if path.is_empty() {
            self.root.clone()
        } else {
            match self.manifest.resolve(&path) {
                Some(p) => p,
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            }
        };

        // Handle truncate
        if let Some(size) = size {
            if let Some(fh) = fh {
                if let Some(file) = self.get_file(fh) {
                    if let Err(e) = file.set_len(size) {
                        reply.error(e.raw_os_error().unwrap_or(libc::EIO));
                        return;
                    }
                }
            } else if let Err(e) = fs::File::open(&backend_path).and_then(|f| f.set_len(size)) {
                reply.error(e.raw_os_error().unwrap_or(libc::EIO));
                return;
            }
        }

        // Handle chmod
        if let Some(mode) = mode {
            if let Err(e) = fs::set_permissions(&backend_path, fs::Permissions::from_mode(mode)) {
                reply.error(e.raw_os_error().unwrap_or(libc::EIO));
                return;
            }
        }

        // Handle chown (requires root)
        if uid.is_some() || gid.is_some() {
            // Skip chown for now - requires unsafe and root
        }

        // Handle utimes
        // TODO: implement atime/mtime setting

        match self.get_attr(ino, &backend_path) {
            Some(attr) => reply.attr(&TTL, &attr),
            None => reply.error(libc::EIO),
        }
    }
}
