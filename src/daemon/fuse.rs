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
    FileAttr, FileType, Filesystem, Notifier, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use parking_lot::RwLock;

use super::manager::Manifest;

const TTL: Duration = Duration::from_secs(1);

fn io_errno(err: &std::io::Error) -> i32 {
    err.raw_os_error().unwrap_or(libc::EIO)
}

fn join_child(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

macro_rules! option_or_reply {
    ($opt:expr, $reply:expr, $errno:expr) => {
        match $opt {
            Some(v) => v,
            None => {
                $reply.error($errno);
                return;
            }
        }
    };
}

macro_rules! io_or_reply {
    ($res:expr, $reply:expr) => {
        match $res {
            Ok(v) => v,
            Err(e) => {
                $reply.error(io_errno(&e));
                return;
            }
        }
    };
}

/// FUSE layered filesystem.
pub(crate) struct NueFs {
    real_root: PathBuf,
    manifest: Arc<RwLock<Manifest>>,
    inodes: RwLock<HashMap<u64, String>>,
    paths: RwLock<HashMap<String, u64>>,
    next_ino: AtomicU64,
    handles: RwLock<HashMap<u64, File>>,
    next_fh: AtomicU64,
    notifier: Arc<RwLock<Option<Notifier>>>,
}

impl NueFs {
    pub(crate) fn new(
        real_root: PathBuf,
        manifest: Arc<RwLock<Manifest>>,
        notifier: Arc<RwLock<Option<Notifier>>>,
    ) -> Self {
        let mut inodes = HashMap::new();
        let mut paths = HashMap::new();

        inodes.insert(1, String::new());
        paths.insert(String::new(), 1);

        Self {
            real_root,
            manifest,
            inodes: RwLock::new(inodes),
            paths: RwLock::new(paths),
            next_ino: AtomicU64::new(2),
            handles: RwLock::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            notifier,
        }
    }

    /// Notify kernel of entry invalidation (triggers inotify)
    fn notify_inval_entry(&self, parent_ino: u64, name: &str) {
        if let Some(ref notifier) = *self.notifier.read() {
            let _ = notifier.inval_entry(parent_ino, std::ffi::OsStr::new(name));
        }
    }

    fn get_or_create_ino(&self, path: &str) -> u64 {
        let paths = self.paths.read();
        if let Some(&ino) = paths.get(path) {
            return ino;
        }
        drop(paths);

        let mut paths = self.paths.write();
        let mut inodes = self.inodes.write();

        if let Some(&ino) = paths.get(path) {
            return ino;
        }

        let ino = self.next_ino.fetch_add(1, Ordering::SeqCst);
        paths.insert(path.to_string(), ino);
        inodes.insert(ino, path.to_string());
        ino
    }

    fn get_path(&self, ino: u64) -> Option<String> {
        self.inodes.read().get(&ino).cloned()
    }

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

    fn alloc_fh(&self, file: File) -> u64 {
        let fh = self.next_fh.fetch_add(1, Ordering::SeqCst);
        self.handles.write().insert(fh, file);
        fh
    }

    fn get_file(&self, fh: u64) -> Option<File> {
        self.handles
            .read()
            .get(&fh)
            .and_then(|f| f.try_clone().ok())
    }

    fn release_fh(&self, fh: u64) {
        self.handles.write().remove(&fh);
    }
}

impl Filesystem for NueFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_path = option_or_reply!(self.get_path(parent), reply, libc::ENOENT);
        let name = name.to_string_lossy();
        let child_path = join_child(&parent_path, name.as_ref());
        let backend_path =
            option_or_reply!(self.manifest.read().resolve(&child_path), reply, libc::ENOENT);
        let ino = self.get_or_create_ino(&child_path);
        let attr = option_or_reply!(self.get_attr(ino, &backend_path), reply, libc::ENOENT);
        reply.entry(&TTL, &attr, 0);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let path = option_or_reply!(self.get_path(ino), reply, libc::ENOENT);

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

        let backend_path = option_or_reply!(self.manifest.read().resolve(&path), reply, libc::ENOENT);
        let attr = option_or_reply!(self.get_attr(ino, &backend_path), reply, libc::ENOENT);
        reply.attr(&TTL, &attr);
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let path = option_or_reply!(self.get_path(ino), reply, libc::ENOENT);

        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (if path.is_empty() { 1 } else { ino }, FileType::Directory, "..".to_string()),
        ];

        for (name, is_dir) in self.manifest.read().readdir(&path) {
            let child_path = join_child(&path, &name);
            let child_ino = self.get_or_create_ino(&child_path);
            let kind = if is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            entries.push((child_ino, kind, name));
        }

        for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(ino, (i + 1) as i64, kind, name) {
                break;
            }
        }

        reply.ok();
    }

    fn readdirplus(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectoryPlus,
    ) {
        let path = option_or_reply!(self.get_path(ino), reply, libc::ENOENT);
        let manifest = self.manifest.read();

        let mut entries: Vec<(u64, FileAttr, String)> = Vec::new();

        // Add . and ..
        if let Some(dot_attr) = self.get_attr(ino, &self.real_root) {
            entries.push((ino, dot_attr, ".".to_string()));
        }
        let parent_ino = if path.is_empty() { 1 } else { ino };
        if let Some(dotdot_attr) = self.get_attr(parent_ino, &self.real_root) {
            entries.push((parent_ino, dotdot_attr, "..".to_string()));
        }

        for (name, _is_dir) in manifest.readdir(&path) {
            let child_path = join_child(&path, &name);
            if let Some(backend_path) = manifest.resolve(&child_path) {
                let child_ino = self.get_or_create_ino(&child_path);
                if let Some(attr) = self.get_attr(child_ino, &backend_path) {
                    entries.push((child_ino, attr, name));
                }
            }
        }

        drop(manifest);

        for (i, (child_ino, attr, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(child_ino, (i + 1) as i64, &name, &TTL, &attr, 0) {
                break;
            }
        }

        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        let path = option_or_reply!(self.get_path(ino), reply, libc::ENOENT);
        let backend_path = option_or_reply!(self.manifest.read().resolve(&path), reply, libc::ENOENT);
        let file = io_or_reply!(
            OpenOptions::new()
            .read(true)
            .write((flags & libc::O_WRONLY != 0) || (flags & libc::O_RDWR != 0))
            .open(&backend_path),
            reply
        );

        let fh = self.alloc_fh(file);
        reply.opened(fh, 0);
    }

    fn read(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let mut file = option_or_reply!(self.get_file(fh), reply, libc::EBADF);
        io_or_reply!(file.seek(SeekFrom::Start(offset as u64)), reply);

        let mut buf = vec![0u8; size as usize];
        let n = io_or_reply!(file.read(&mut buf), reply);
        reply.data(&buf[..n]);
    }

    fn write(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let mut handles = self.handles.write();
        let file = option_or_reply!(handles.get_mut(&fh), reply, libc::EBADF);
        io_or_reply!(file.seek(SeekFrom::Start(offset as u64)), reply);
        let n = io_or_reply!(file.write(data), reply);
        reply.written(n as u32);
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.release_fh(fh);
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let parent_path = option_or_reply!(self.get_path(parent), reply, libc::ENOENT);
        let name = name.to_string_lossy();
        let child_path = join_child(&parent_path, name.as_ref());
        let target_dir = self.manifest.read().create_target(&parent_path);
        let backend_path = target_dir.join(&*name);

        if let Some(parent) = backend_path.parent() {
            io_or_reply!(fs::create_dir_all(parent), reply);
        }

        let file = io_or_reply!(
            OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(flags & libc::O_TRUNC != 0)
            .mode(mode)
            .open(&backend_path),
            reply
        );

        let ino = self.get_or_create_ino(&child_path);
        let fh = self.alloc_fh(file);
        self.manifest.write().add_entry(&child_path, false);
        self.notify_inval_entry(parent, &name);

        let attr = option_or_reply!(self.get_attr(ino, &backend_path), reply, libc::EIO);
        reply.created(&TTL, &attr, 0, fh, 0);
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = option_or_reply!(self.get_path(parent), reply, libc::ENOENT);
        let name = name.to_string_lossy();
        let child_path = join_child(&parent_path, name.as_ref());
        let backend_path =
            option_or_reply!(self.manifest.read().resolve(&child_path), reply, libc::ENOENT);
        io_or_reply!(fs::remove_file(&backend_path), reply);
        self.manifest.write().remove_entry(&child_path);
        self.notify_inval_entry(parent, &name);
        reply.ok();
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let parent_path = option_or_reply!(self.get_path(parent), reply, libc::ENOENT);
        let name = name.to_string_lossy();
        let child_path = join_child(&parent_path, name.as_ref());
        let target_dir = self.manifest.read().create_target(&parent_path);
        let backend_path = target_dir.join(&*name);

        io_or_reply!(fs::create_dir(&backend_path), reply);
        if let Err(e) = fs::set_permissions(&backend_path, fs::Permissions::from_mode(mode)) {
            eprintln!("Warning: failed to set permissions: {e}");
        }

        let ino = self.get_or_create_ino(&child_path);
        self.manifest.write().add_entry(&child_path, true);
        self.notify_inval_entry(parent, &name);

        let attr = option_or_reply!(self.get_attr(ino, &backend_path), reply, libc::EIO);
        reply.entry(&TTL, &attr, 0);
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = option_or_reply!(self.get_path(parent), reply, libc::ENOENT);
        let name = name.to_string_lossy();
        let child_path = join_child(&parent_path, name.as_ref());
        let backend_path =
            option_or_reply!(self.manifest.read().resolve(&child_path), reply, libc::ENOENT);
        io_or_reply!(fs::remove_dir(&backend_path), reply);
        self.manifest.write().remove_entry(&child_path);
        self.notify_inval_entry(parent, &name);
        reply.ok();
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let parent_path = option_or_reply!(self.get_path(parent), reply, libc::ENOENT);
        let newparent_path = option_or_reply!(self.get_path(newparent), reply, libc::ENOENT);

        let name = name.to_string_lossy();
        let newname = newname.to_string_lossy();

        let old_path = join_child(&parent_path, name.as_ref());
        let new_path = join_child(&newparent_path, newname.as_ref());

        let (old_backend, new_backend) = {
            let manifest = self.manifest.read();
            let old_backend = option_or_reply!(manifest.resolve(&old_path), reply, libc::ENOENT);
            let new_backend = match manifest.resolve(&new_path) {
                Some(p) => p,
                None => {
                    let target_dir = manifest.create_target(&newparent_path);
                    target_dir.join(newname.as_ref())
                }
            };
            (old_backend, new_backend)
        };

        io_or_reply!(fs::rename(&old_backend, &new_backend), reply);
        self.manifest.write().rename_entry(&old_path, &new_path);
        reply.ok();
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
        let path = option_or_reply!(self.get_path(ino), reply, libc::ENOENT);

        let backend_path = if path.is_empty() {
            self.real_root.clone()
        } else {
            option_or_reply!(self.manifest.read().resolve(&path), reply, libc::ENOENT)
        };

        if let Some(size) = size {
            if let Some(fh) = fh {
                if let Some(file) = self.get_file(fh) {
                    io_or_reply!(file.set_len(size), reply);
                }
            } else {
                io_or_reply!(fs::File::open(&backend_path).and_then(|f| f.set_len(size)), reply);
            }
        }

        if let Some(mode) = mode {
            io_or_reply!(
                fs::set_permissions(&backend_path, fs::Permissions::from_mode(mode)),
                reply
            );
        }

        if uid.is_some() || gid.is_some() {
            // Skip chown for now - requires unsafe and root.
        }

        let attr = option_or_reply!(self.get_attr(ino, &backend_path), reply, libc::EIO);
        reply.attr(&TTL, &attr);
    }
}
