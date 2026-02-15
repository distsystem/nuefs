use std::ffi::OsStr;
use std::os::fd::{AsFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use parking_lot::RwLock;
use tracing::{debug, error, warn};

use super::inode::{DirCache, DirCacheEntry, InodeTable, ROOT_INO};
use super::manager::Manifest;

fn map_io_err(e: std::io::Error) -> libc::c_int {
    e.raw_os_error().unwrap_or(libc::EIO)
}

fn map_errno(e: rustix::io::Errno) -> libc::c_int {
    e.raw_os_error()
}

fn mode_to_filetype(mode: u32) -> FileType {
    match mode & libc::S_IFMT as u32 {
        x if x == libc::S_IFREG as u32 => FileType::RegularFile,
        x if x == libc::S_IFDIR as u32 => FileType::Directory,
        x if x == libc::S_IFLNK as u32 => FileType::Symlink,
        x if x == libc::S_IFCHR as u32 => FileType::CharDevice,
        x if x == libc::S_IFBLK as u32 => FileType::BlockDevice,
        x if x == libc::S_IFIFO as u32 => FileType::NamedPipe,
        x if x == libc::S_IFSOCK as u32 => FileType::Socket,
        _ => FileType::RegularFile,
    }
}

fn stat_to_fuse_attr(stat: &rustix::fs::Stat, ino: u64) -> FileAttr {
    FileAttr {
        ino,
        size: stat.st_size as u64,
        blocks: stat.st_blocks as u64,
        atime: UNIX_EPOCH + Duration::new(stat.st_atime as u64, stat.st_atime_nsec as u32),
        mtime: UNIX_EPOCH + Duration::new(stat.st_mtime as u64, stat.st_mtime_nsec as u32),
        ctime: UNIX_EPOCH + Duration::new(stat.st_ctime as u64, stat.st_ctime_nsec as u32),
        crtime: UNIX_EPOCH,
        kind: mode_to_filetype(stat.st_mode as u32),
        perm: (stat.st_mode & 0o7777) as u16,
        nlink: stat.st_nlink as u32,
        uid: stat.st_uid as u32,
        gid: stat.st_gid as u32,
        rdev: stat.st_rdev as u32,
        blksize: stat.st_blksize as u32,
        flags: 0,
    }
}

fn stat_path(path: &Path) -> Result<rustix::fs::Stat, libc::c_int> {
    rustix::fs::statat(rustix::fs::CWD, path, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)
        .map_err(map_errno)
}

fn fstat_fd(fd: BorrowedFd<'_>) -> Result<rustix::fs::Stat, libc::c_int> {
    rustix::fs::fstat(fd).map_err(map_errno)
}

pub(crate) struct NueFs {
    manifest: Arc<RwLock<Manifest>>,
    inodes: InodeTable,
    dir_cache: DirCache,
    default_ttl: Duration,
}

impl NueFs {
    pub(crate) fn new(manifest: Arc<RwLock<Manifest>>) -> Self {
        Self {
            manifest,
            inodes: InodeTable::new(),
            dir_cache: DirCache::new(),
            default_ttl: Duration::from_secs(1),
        }
    }

    fn to_rel_string(path: &Path) -> String {
        path.to_string_lossy().trim_start_matches('/').to_string()
    }

    fn join_child(parent: &Path, name: &OsStr) -> PathBuf {
        if parent.as_os_str().is_empty() {
            PathBuf::from(name)
        } else {
            parent.join(name)
        }
    }

    fn resolve_io(&self, rel_path: &str) -> PathBuf {
        self.manifest.read().resolve_io(rel_path)
    }

    fn resolve_child_io_cascade(&self, parent: &Path, name: &OsStr) -> PathBuf {
        let child_path = Self::join_child(parent, name);
        let child_rel = Self::to_rel_string(&child_path);
        self.manifest.read().resolve_io_cascade(&child_rel)
    }

    fn create_io(&self, parent: &Path, name: &OsStr) -> PathBuf {
        let rel_parent = Self::to_rel_string(parent);
        let name_str = name.to_string_lossy();
        self.manifest
            .read()
            .create_target_io(&rel_parent, &name_str)
            .join(name)
    }

    fn read_dir_children(path: &Path) -> Vec<(String, bool)> {
        let Ok(entries) = std::fs::read_dir(path) else {
            return Vec::new();
        };

        entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let name = entry.file_name().to_string_lossy().to_string();
                let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
                Some((name, is_dir))
            })
            .collect()
    }

    fn merge_multi_dir_children(
        io_dirs: &[PathBuf],
        origin_children: Vec<(String, bool)>,
    ) -> Vec<(String, bool)> {
        let mut seen = std::collections::HashSet::new();
        let mut result: Vec<(String, bool)> = Vec::new();

        for dir in io_dirs {
            for (name, is_dir) in Self::read_dir_children(dir) {
                if seen.insert(name.clone()) {
                    result.push((name, is_dir));
                }
            }
        }

        for (name, is_dir) in origin_children {
            if seen.insert(name.clone()) {
                result.push((name, is_dir));
            }
        }

        result
    }

    /// Get the virtual path for an inode, or reply with ESTALE.
    fn vpath_or_stale(&self, ino: u64) -> Result<PathBuf, libc::c_int> {
        self.inodes
            .get_path(ino)
            .map(|p| p.to_path_buf())
            .ok_or(libc::ESTALE)
    }

    fn time_or_now_to_system_time(t: TimeOrNow) -> SystemTime {
        match t {
            TimeOrNow::SpecificTime(t) => t,
            TimeOrNow::Now => SystemTime::now(),
        }
    }
}

impl Filesystem for NueFs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_path = match self.vpath_or_stale(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        let child_vpath = Self::join_child(&parent_path, name);
        let child_rel = Self::to_rel_string(&child_vpath);
        debug!(parent = %parent_path.display(), name = %name.to_string_lossy(), "FUSE lookup");

        let io = self.resolve_io(&child_rel);
        let stat = match stat_path(&io) {
            Ok(s) => s,
            Err(e) => {
                debug!(path = %child_vpath.display(), errno = e, "lookup failed");
                return reply.error(e);
            }
        };

        let (ino, generation) = self.inodes.add_or_get(&child_vpath);
        self.inodes.lookup(ino);
        let attr = stat_to_fuse_attr(&stat, ino);
        reply.entry(&self.default_ttl, &attr, generation);
    }

    fn forget(&mut self, _req: &Request<'_>, ino: u64, nlookup: u64) {
        self.inodes.forget(ino, nlookup);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, fh: Option<u64>, reply: ReplyAttr) {
        // Try fh first — works even for unlinked-but-still-open files.
        if let Some(fh) = fh {
            let fd = unsafe { BorrowedFd::borrow_raw(fh as RawFd) };
            if let Ok(stat) = fstat_fd(fd) {
                return reply.attr(&self.default_ttl, &stat_to_fuse_attr(&stat, ino));
            }
        }

        let vpath = match self.vpath_or_stale(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let rel = Self::to_rel_string(&vpath);
        debug!(ino, path = %vpath.display(), "FUSE getattr");

        let io = self.resolve_io(&rel);
        match stat_path(&io) {
            Ok(stat) => reply.attr(&self.default_ttl, &stat_to_fuse_attr(&stat, ino)),
            Err(e) => {
                debug!(ino, path = %vpath.display(), errno = e, "getattr failed");
                reply.error(e);
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let vpath = match self.vpath_or_stale(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let rel = Self::to_rel_string(&vpath);
        debug!(ino, path = %vpath.display(), "FUSE setattr");

        let io = self.resolve_io(&rel);

        // chmod
        if let Some(mode) = mode {
            if let Err(e) = rustix::fs::chmodat(
                rustix::fs::CWD,
                &io,
                rustix::fs::Mode::from_raw_mode(mode),
                rustix::fs::AtFlags::empty(),
            ) {
                warn!(path = %vpath.display(), io = %io.display(), error = %e, "setattr chmod failed");
                return reply.error(map_errno(e));
            }
        }

        // chown
        if uid.is_some() || gid.is_some() {
            if let Err(e) = rustix::fs::chownat(
                rustix::fs::CWD,
                &io,
                uid.map(rustix::fs::Uid::from_raw),
                gid.map(rustix::fs::Gid::from_raw),
                rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
            ) {
                warn!(path = %vpath.display(), io = %io.display(), error = %e, "setattr chown failed");
                return reply.error(map_errno(e));
            }
        }

        // truncate
        if let Some(size) = size {
            let truncate_result = if let Some(fh) = fh {
                let fd = unsafe { BorrowedFd::borrow_raw(fh as RawFd) };
                rustix::fs::ftruncate(fd, size)
            } else {
                let fd = match rustix::fs::open(
                    &io,
                    rustix::fs::OFlags::WRONLY | rustix::fs::OFlags::CLOEXEC,
                    rustix::fs::Mode::empty(),
                ) {
                    Ok(fd) => fd,
                    Err(e) => return reply.error(map_errno(e)),
                };
                rustix::fs::ftruncate(&fd, size)
            };
            if let Err(e) = truncate_result {
                warn!(path = %vpath.display(), error = %e, "setattr truncate failed");
                return reply.error(map_errno(e));
            }
        }

        // utimens
        if atime.is_some() || mtime.is_some() {
            let to_timespec = |t: Option<TimeOrNow>| -> rustix::fs::Timespec {
                match t {
                    Some(ton) => {
                        let st = Self::time_or_now_to_system_time(ton);
                        let d = st.duration_since(UNIX_EPOCH).unwrap_or_default();
                        rustix::fs::Timespec {
                            tv_sec: d.as_secs() as rustix::fs::Secs,
                            tv_nsec: d.subsec_nanos() as rustix::fs::Nsecs,
                        }
                    }
                    None => rustix::fs::Timespec {
                        tv_sec: 0,
                        tv_nsec: rustix::fs::UTIME_OMIT,
                    },
                }
            };
            let times = rustix::fs::Timestamps {
                last_access: to_timespec(atime),
                last_modification: to_timespec(mtime),
            };
            if let Err(e) = rustix::fs::utimensat(
                rustix::fs::CWD,
                &io,
                &times,
                rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
            ) {
                warn!(path = %vpath.display(), io = %io.display(), error = %e, "setattr utimens failed");
                return reply.error(map_errno(e));
            }
        }

        // Final getattr
        match stat_path(&io) {
            Ok(stat) => reply.attr(&self.default_ttl, &stat_to_fuse_attr(&stat, ino)),
            Err(e) => reply.error(e),
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let vpath = match self.vpath_or_stale(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let rel = Self::to_rel_string(&vpath);
        debug!(ino, path = %vpath.display(), "FUSE readlink");

        let io = self.resolve_io(&rel);
        match std::fs::read_link(&io) {
            Ok(target) => reply.data(target.as_os_str().as_bytes()),
            Err(e) => reply.error(map_io_err(e)),
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let parent_path = match self.vpath_or_stale(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let io_path = self.create_io(&parent_path, name);
        debug!(parent = %parent_path.display(), name = %name.to_string_lossy(), io = %io_path.display(), mode, "FUSE mkdir");

        if let Err(e) = rustix::fs::mkdirat(
            rustix::fs::CWD,
            &io_path,
            rustix::fs::Mode::from_raw_mode(mode),
        ) {
            error!(parent = %parent_path.display(), name = %name.to_string_lossy(), io = %io_path.display(), error = %e, "FUSE mkdir failed");
            return reply.error(map_errno(e));
        }

        let child_vpath = Self::join_child(&parent_path, name);
        let stat = match stat_path(&io_path) {
            Ok(s) => s,
            Err(e) => return reply.error(e),
        };

        let (ino, generation) = self.inodes.add_or_get(&child_vpath);
        self.inodes.lookup(ino);
        let attr = stat_to_fuse_attr(&stat, ino);
        debug!(parent = %parent_path.display(), name = %name.to_string_lossy(), ino, "FUSE mkdir OK");
        reply.entry(&self.default_ttl, &attr, generation);
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = match self.vpath_or_stale(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        debug!(parent = %parent_path.display(), name = %name.to_string_lossy(), "FUSE unlink");

        let io = self.resolve_child_io_cascade(&parent_path, name);
        if let Err(e) = std::fs::remove_file(&io) {
            return reply.error(map_io_err(e));
        }

        let child_vpath = Self::join_child(&parent_path, name);
        self.inodes.unlink(&child_vpath);
        reply.ok();
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = match self.vpath_or_stale(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        debug!(parent = %parent_path.display(), name = %name.to_string_lossy(), "FUSE rmdir");

        let io = self.resolve_child_io_cascade(&parent_path, name);
        if let Err(e) = std::fs::remove_dir(&io) {
            return reply.error(map_io_err(e));
        }

        let child_vpath = Self::join_child(&parent_path, name);
        self.inodes.unlink(&child_vpath);
        reply.ok();
    }

    fn symlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let parent_path = match self.vpath_or_stale(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        debug!(parent = %parent_path.display(), name = %link_name.to_string_lossy(), target = %target.display(), "FUSE symlink");

        let io_path = self.create_io(&parent_path, link_name);
        if let Err(e) = std::os::unix::fs::symlink(target, &io_path) {
            return reply.error(map_io_err(e));
        }

        let child_vpath = Self::join_child(&parent_path, link_name);
        let stat = match stat_path(&io_path) {
            Ok(s) => s,
            Err(e) => return reply.error(e),
        };

        let (ino, generation) = self.inodes.add_or_get(&child_vpath);
        self.inodes.lookup(ino);
        reply.entry(&self.default_ttl, &stat_to_fuse_attr(&stat, ino), generation);
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let parent_path = match self.vpath_or_stale(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let newparent_path = match self.vpath_or_stale(newparent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        let old_vpath = Self::join_child(&parent_path, name);
        let new_vpath = Self::join_child(&newparent_path, newname);
        let old_rel = Self::to_rel_string(&old_vpath);
        debug!(old = %old_vpath.display(), newparent = %newparent_path.display(), newname = %newname.to_string_lossy(), "FUSE rename");

        let manifest = self.manifest.read();
        let old_io = manifest.resolve_io(&old_rel);
        let new_parent_rel = Self::to_rel_string(&newparent_path);
        let new_io = manifest.resolve_io(&new_parent_rel).join(newname);
        drop(manifest);

        if let Err(e) = std::fs::rename(&old_io, &new_io) {
            return reply.error(map_io_err(e));
        }

        // If the destination already had an inode, unlink it
        self.inodes.unlink(&new_vpath);
        self.inodes.rename(&old_vpath, &new_vpath);
        reply.ok();
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let old_vpath = match self.vpath_or_stale(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let newparent_path = match self.vpath_or_stale(newparent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let old_rel = Self::to_rel_string(&old_vpath);
        debug!(old = %old_vpath.display(), newparent = %newparent_path.display(), newname = %newname.to_string_lossy(), "FUSE link");

        let old_io = self.resolve_io(&old_rel);
        let new_io = self.create_io(&newparent_path, newname);

        if let Err(e) = std::fs::hard_link(&old_io, &new_io) {
            warn!(old = %old_io.display(), new = %new_io.display(), error = %e, "FUSE link failed");
            return reply.error(map_io_err(e));
        }

        let new_vpath = Self::join_child(&newparent_path, newname);
        let stat = match stat_path(&new_io) {
            Ok(s) => s,
            Err(e) => return reply.error(e),
        };

        let (new_ino, generation) = self.inodes.add_or_get(&new_vpath);
        self.inodes.lookup(new_ino);
        reply.entry(&self.default_ttl, &stat_to_fuse_attr(&stat, new_ino), generation);
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let vpath = match self.vpath_or_stale(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let rel = Self::to_rel_string(&vpath);
        debug!(ino, path = %vpath.display(), flags, "FUSE open");

        let io = self.resolve_io(&rel);
        let oflags =
            rustix::fs::OFlags::from_bits_retain(flags as u32) | rustix::fs::OFlags::CLOEXEC;
        match rustix::fs::open(&io, oflags, rustix::fs::Mode::empty()) {
            Ok(fd) => reply.opened(fd.into_raw_fd() as u64, 0),
            Err(e) => {
                warn!(path = %vpath.display(), io = %io.display(), flags, error = %e, "FUSE open failed");
                reply.error(map_errno(e));
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let fd = unsafe { BorrowedFd::borrow_raw(fh as RawFd) };
        let mut buf = vec![0u8; size as usize];
        match rustix::io::pread(fd, &mut buf, offset as u64) {
            Ok(n) => reply.data(&buf[..n]),
            Err(e) => reply.error(map_errno(e)),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let fd = unsafe { BorrowedFd::borrow_raw(fh as RawFd) };
        match rustix::io::pwrite(fd, data, offset as u64) {
            Ok(n) => reply.written(n as u32),
            Err(e) => reply.error(map_errno(e)),
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        drop(unsafe { OwnedFd::from_raw_fd(fh as RawFd) });
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let fd = unsafe { BorrowedFd::borrow_raw(fh as RawFd) };
        let result = if datasync {
            rustix::fs::fdatasync(fd)
        } else {
            rustix::fs::fsync(fd)
        };
        match result {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(map_errno(e)),
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let parent_path = match self.vpath_or_stale(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let io_path = self.create_io(&parent_path, name);
        debug!(parent = %parent_path.display(), name = %name.to_string_lossy(), io = %io_path.display(), mode, "FUSE create");

        let oflags = rustix::fs::OFlags::from_bits_retain(flags as u32)
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::CLOEXEC;
        let fmode = rustix::fs::Mode::from_raw_mode(mode);
        let fd = match rustix::fs::open(&io_path, oflags, fmode) {
            Ok(fd) => fd,
            Err(e) => {
                error!(parent = %parent_path.display(), name = %name.to_string_lossy(), io = %io_path.display(), error = %e, "FUSE create failed");
                return reply.error(map_errno(e));
            }
        };

        let stat = match fstat_fd(fd.as_fd()) {
            Ok(s) => s,
            Err(e) => return reply.error(e),
        };

        let child_vpath = Self::join_child(&parent_path, name);
        let (ino, generation) = self.inodes.add_or_get(&child_vpath);
        self.inodes.lookup(ino);
        let attr = stat_to_fuse_attr(&stat, ino);
        reply.created(&self.default_ttl, &attr, generation, fd.into_raw_fd() as u64, 0);
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        if self.vpath_or_stale(ino).is_err() {
            return reply.error(libc::ESTALE);
        }
        let fh = self.dir_cache.alloc();
        reply.opened(fh, 0);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let vpath = match self.vpath_or_stale(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        // Populate cache on first call (offset == 0 and not yet populated).
        if self.dir_cache.get(fh).is_none() {
            let rel_path = Self::to_rel_string(&vpath);
            debug!(ino, path = %vpath.display(), "FUSE readdir (populate)");

            let plan = self.manifest.read().readdir_plan(&rel_path);
            let children = Self::merge_multi_dir_children(&plan.io_dirs, plan.origin_children);

            // ".." ino: look up parent
            let dotdot_ino = if ino == ROOT_INO {
                ROOT_INO
            } else {
                vpath
                    .parent()
                    .and_then(|p| self.inodes.get_inode(p))
                    .unwrap_or(ROOT_INO)
            };

            let mut entries = Vec::with_capacity(children.len() + 2);
            entries.push(DirCacheEntry {
                ino,
                kind: FileType::Directory,
                name: ".".into(),
            });
            entries.push(DirCacheEntry {
                ino: dotdot_ino,
                kind: FileType::Directory,
                name: "..".into(),
            });

            // Use a placeholder ino for children; kernel will LOOKUP each one.
            for (name, is_dir) in children {
                let kind = if is_dir {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                };
                entries.push(DirCacheEntry {
                    ino: !1u64,
                    kind,
                    name,
                });
            }

            self.dir_cache.populate(fh, entries);
        }

        let Some(entries) = self.dir_cache.get(fh) else {
            return reply.error(libc::EBADF);
        };

        let start = offset as usize;
        for (i, entry) in entries.iter().enumerate().skip(start) {
            // reply.add returns true when buffer is full
            if reply.add(entry.ino, (i + 1) as i64, entry.kind, &entry.name) {
                return reply.ok();
            }
        }
        reply.ok();
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        self.dir_cache.remove(fh);
        reply.ok();
    }

    fn statfs(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyStatfs) {
        let vpath = match self.vpath_or_stale(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let rel = Self::to_rel_string(&vpath);
        let io = self.resolve_io(&rel);
        let c_path =
            match std::ffi::CString::new(io.as_os_str().as_bytes()) {
                Ok(p) => p,
                Err(_) => return reply.error(libc::EINVAL),
            };

        let mut stat: libc::statvfs64 = unsafe { std::mem::zeroed() };
        let ret = unsafe { libc::statvfs64(c_path.as_ptr(), &mut stat) };
        if ret != 0 {
            let err = std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO);
            return reply.error(err);
        }

        reply.statfs(
            stat.f_blocks,
            stat.f_bfree,
            stat.f_bavail,
            stat.f_files,
            stat.f_ffree,
            stat.f_bsize as u32,
            stat.f_namemax as u32,
            stat.f_frsize as u32,
        );
    }
}
