use std::ffi::{OsStr, OsString};
use std::os::fd::{AsFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuse_mt::{
    CallbackResult, CreatedEntry, DirectoryEntry, FileAttr, FileType, FilesystemMT, RequestInfo,
    ResultCreate, ResultData, ResultEmpty, ResultEntry, ResultOpen, ResultReaddir, ResultSlice,
    ResultStatfs, ResultWrite, Statfs,
};
use parking_lot::RwLock;
use tracing::{debug, error, warn};

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

fn stat_to_attr(stat: &rustix::fs::Stat) -> FileAttr {
    FileAttr {
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
        flags: 0,
    }
}

fn stat_path(path: &Path) -> Result<FileAttr, libc::c_int> {
    let stat =
        rustix::fs::statat(rustix::fs::CWD, path, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)
            .map_err(map_errno)?;
    Ok(stat_to_attr(&stat))
}

fn fstat_fd(fd: BorrowedFd<'_>) -> Result<FileAttr, libc::c_int> {
    let stat = rustix::fs::fstat(fd).map_err(map_errno)?;
    Ok(stat_to_attr(&stat))
}

pub(crate) struct NueFs {
    manifest: Arc<RwLock<Manifest>>,
    default_ttl: Duration,
}

impl NueFs {
    pub(crate) fn new(manifest: Arc<RwLock<Manifest>>) -> Self {
        Self {
            manifest,
            default_ttl: Duration::from_secs(1),
        }
    }

    fn display_path(path: &Path) -> String {
        let s = path.to_string_lossy();
        if s == "/" || s.is_empty() {
            "/".to_string()
        } else {
            s.to_string()
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
}

impl FilesystemMT for NueFs {
    fn getattr(&self, _req: RequestInfo, path: &Path, fh: Option<u64>) -> ResultEntry {
        let rel = Self::to_rel_string(path);
        debug!(path = %Self::display_path(path), "FUSE getattr");

        if let Some(fh) = fh {
            let fd = unsafe { BorrowedFd::borrow_raw(fh as RawFd) };
            if let Ok(attr) = fstat_fd(fd) {
                return Ok((self.default_ttl, attr));
            }
        }

        let io = self.resolve_io(&rel);
        let attr = stat_path(&io).map_err(|e| {
            debug!(path = %Self::display_path(path), errno = e, "getattr failed");
            e
        })?;
        Ok((self.default_ttl, attr))
    }

    fn readdir(&self, _req: RequestInfo, path: &Path, _fh: u64) -> ResultReaddir {
        let rel_path = Self::to_rel_string(path);
        debug!(path = %Self::display_path(path), "FUSE readdir");

        let plan = self.manifest.read().readdir_plan(&rel_path);
        let children = Self::merge_multi_dir_children(&plan.io_dirs, plan.origin_children);

        debug!(path = %Self::display_path(path), io_dirs = ?plan.io_dirs, children = children.len(), "FUSE readdir plan");
        let mut entries = Vec::with_capacity(children.len() + 2);
        entries.push(DirectoryEntry {
            name: ".".into(),
            kind: FileType::Directory,
        });
        entries.push(DirectoryEntry {
            name: "..".into(),
            kind: FileType::Directory,
        });

        for (name, is_dir) in children {
            let kind = if is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            entries.push(DirectoryEntry {
                name: OsString::from(name),
                kind,
            });
        }

        Ok(entries)
    }

    fn open(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
        let rel_path = Self::to_rel_string(path);
        debug!(path = %Self::display_path(path), flags, "FUSE open");

        let io = self.resolve_io(&rel_path);
        let oflags =
            rustix::fs::OFlags::from_bits_retain(flags) | rustix::fs::OFlags::CLOEXEC;
        let fd = rustix::fs::open(&io, oflags, rustix::fs::Mode::empty()).map_err(|e| {
            warn!(path = %Self::display_path(path), io = %io.display(), flags, error = %e, "FUSE open failed");
            map_errno(e)
        })?;
        Ok((fd.into_raw_fd() as u64, 0))
    }

    fn read(
        &self,
        _req: RequestInfo,
        _path: &Path,
        fh: u64,
        offset: u64,
        size: u32,
        callback: impl FnOnce(ResultSlice<'_>) -> CallbackResult,
    ) -> CallbackResult {
        let fd = unsafe { BorrowedFd::borrow_raw(fh as RawFd) };
        let mut buf = vec![0u8; size as usize];
        match rustix::io::pread(fd, &mut buf, offset) {
            Ok(n) => callback(Ok(&buf[..n])),
            Err(e) => callback(Err(map_errno(e))),
        }
    }

    fn write(
        &self,
        _req: RequestInfo,
        _path: &Path,
        fh: u64,
        offset: u64,
        data: Vec<u8>,
        _flags: u32,
    ) -> ResultWrite {
        let fd = unsafe { BorrowedFd::borrow_raw(fh as RawFd) };
        let n = rustix::io::pwrite(fd, &data, offset).map_err(map_errno)?;
        Ok(n as u32)
    }

    fn flush(&self, _req: RequestInfo, _path: &Path, _fh: u64, _lock_owner: u64) -> ResultEmpty {
        Ok(())
    }

    fn release(
        &self,
        _req: RequestInfo,
        _path: &Path,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> ResultEmpty {
        drop(unsafe { OwnedFd::from_raw_fd(fh as RawFd) });
        Ok(())
    }

    fn fsync(&self, _req: RequestInfo, _path: &Path, fh: u64, datasync: bool) -> ResultEmpty {
        let fd = unsafe { BorrowedFd::borrow_raw(fh as RawFd) };
        if datasync {
            rustix::fs::fdatasync(fd).map_err(map_errno)?;
        } else {
            rustix::fs::fsync(fd).map_err(map_errno)?;
        }
        Ok(())
    }

    fn create(
        &self,
        _req: RequestInfo,
        parent: &Path,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> ResultCreate {
        let io_path = self.create_io(parent, name);
        debug!(parent = %Self::display_path(parent), name = %name.to_string_lossy(), io = %io_path.display(), mode, "FUSE create");

        let oflags = rustix::fs::OFlags::from_bits_retain(flags)
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::CLOEXEC;
        let fmode = rustix::fs::Mode::from_raw_mode(mode);
        let fd = rustix::fs::open(&io_path, oflags, fmode).map_err(|e| {
            error!(parent = %Self::display_path(parent), name = %name.to_string_lossy(), io = %io_path.display(), error = %e, "FUSE create failed");
            map_errno(e)
        })?;
        let attr = fstat_fd(fd.as_fd())?;
        Ok(CreatedEntry {
            ttl: self.default_ttl,
            attr,
            fh: fd.into_raw_fd() as u64,
            flags: 0,
        })
    }

    fn mkdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr, mode: u32) -> ResultEntry {
        let io_path = self.create_io(parent, name);
        debug!(parent = %Self::display_path(parent), name = %name.to_string_lossy(), io = %io_path.display(), mode, "FUSE mkdir");

        rustix::fs::mkdirat(
            rustix::fs::CWD,
            &io_path,
            rustix::fs::Mode::from_raw_mode(mode),
        )
        .map_err(|e| {
            error!(parent = %Self::display_path(parent), name = %name.to_string_lossy(), io = %io_path.display(), error = %e, "FUSE mkdir failed");
            map_errno(e)
        })?;
        let attr = stat_path(&io_path)?;
        debug!(parent = %Self::display_path(parent), name = %name.to_string_lossy(), "FUSE mkdir OK");
        Ok((self.default_ttl, attr))
    }

    fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
        let rel_path = Self::to_rel_string(path);
        debug!(path = %Self::display_path(path), "FUSE readlink");

        let io = self.resolve_io(&rel_path);
        let target = std::fs::read_link(&io).map_err(map_io_err)?;
        Ok(target.as_os_str().as_bytes().to_vec())
    }

    fn symlink(
        &self,
        _req: RequestInfo,
        parent: &Path,
        name: &OsStr,
        target: &Path,
    ) -> ResultEntry {
        debug!(parent = %Self::display_path(parent), name = %name.to_string_lossy(), target = %target.display(), "FUSE symlink");

        let io_path = self.create_io(parent, name);
        std::os::unix::fs::symlink(target, &io_path).map_err(map_io_err)?;
        let attr = stat_path(&io_path)?;
        Ok((self.default_ttl, attr))
    }

    fn link(
        &self,
        _req: RequestInfo,
        path: &Path,
        newparent: &Path,
        newname: &OsStr,
    ) -> ResultEntry {
        let old_rel = Self::to_rel_string(path);
        debug!(old = %Self::display_path(path), newparent = %Self::display_path(newparent), newname = %newname.to_string_lossy(), "FUSE link");

        let old_io = self.resolve_io(&old_rel);
        let new_io = self.create_io(newparent, newname);

        std::fs::hard_link(&old_io, &new_io).map_err(|e| {
            warn!(old = %old_io.display(), new = %new_io.display(), error = %e, "FUSE link failed");
            map_io_err(e)
        })?;
        let attr = stat_path(&new_io)?;
        Ok((self.default_ttl, attr))
    }

    fn unlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
        debug!(parent = %Self::display_path(parent), name = %name.to_string_lossy(), "FUSE unlink");

        let io = self.resolve_child_io_cascade(parent, name);
        std::fs::remove_file(&io).map_err(map_io_err)?;
        Ok(())
    }

    fn rmdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
        debug!(parent = %Self::display_path(parent), name = %name.to_string_lossy(), "FUSE rmdir");

        let io = self.resolve_child_io_cascade(parent, name);
        std::fs::remove_dir(&io).map_err(map_io_err)?;
        Ok(())
    }

    fn rename(
        &self,
        _req: RequestInfo,
        parent: &Path,
        name: &OsStr,
        newparent: &Path,
        newname: &OsStr,
    ) -> ResultEmpty {
        let old_path = Self::join_child(parent, name);
        let old_rel = Self::to_rel_string(&old_path);
        debug!(old = %Self::display_path(&old_path), newparent = %Self::display_path(newparent), newname = %newname.to_string_lossy(), "FUSE rename");

        let manifest = self.manifest.read();
        let old_io = manifest.resolve_io(&old_rel);
        let new_io = if parent == newparent {
            let rel_parent = Self::to_rel_string(parent);
            manifest.resolve_io(&rel_parent).join(newname)
        } else {
            let newparent_rel = Self::to_rel_string(newparent);
            manifest.resolve_io(&newparent_rel).join(newname)
        };
        drop(manifest);

        std::fs::rename(&old_io, &new_io).map_err(map_io_err)?;
        Ok(())
    }

    fn chmod(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, mode: u32) -> ResultEmpty {
        let rel_path = Self::to_rel_string(path);
        debug!(path = %Self::display_path(path), mode, "FUSE chmod");

        let io = self.resolve_io(&rel_path);
        rustix::fs::chmodat(
            rustix::fs::CWD,
            &io,
            rustix::fs::Mode::from_raw_mode(mode),
            rustix::fs::AtFlags::empty(),
        )
        .map_err(|e| {
            warn!(path = %Self::display_path(path), io = %io.display(), error = %e, "FUSE chmod failed");
            map_errno(e)
        })?;
        Ok(())
    }

    fn chown(
        &self,
        _req: RequestInfo,
        path: &Path,
        _fh: Option<u64>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> ResultEmpty {
        let rel_path = Self::to_rel_string(path);
        debug!(path = %Self::display_path(path), ?uid, ?gid, "FUSE chown");

        let io = self.resolve_io(&rel_path);
        rustix::fs::chownat(
            rustix::fs::CWD,
            &io,
            uid.map(rustix::fs::Uid::from_raw),
            gid.map(rustix::fs::Gid::from_raw),
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|e| {
            warn!(path = %Self::display_path(path), io = %io.display(), error = %e, "FUSE chown failed");
            map_errno(e)
        })?;
        Ok(())
    }

    fn truncate(
        &self,
        _req: RequestInfo,
        path: &Path,
        fh: Option<u64>,
        size: u64,
    ) -> ResultEmpty {
        debug!(path = %Self::display_path(path), size, "FUSE truncate");

        if let Some(fh) = fh {
            let fd = unsafe { BorrowedFd::borrow_raw(fh as RawFd) };
            rustix::fs::ftruncate(fd, size).map_err(map_errno)?;
        } else {
            let rel_path = Self::to_rel_string(path);
            let io = self.resolve_io(&rel_path);
            let fd = rustix::fs::open(
                &io,
                rustix::fs::OFlags::WRONLY | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map_err(map_errno)?;
            rustix::fs::ftruncate(&fd, size).map_err(map_errno)?;
        }
        Ok(())
    }

    fn utimens(
        &self,
        _req: RequestInfo,
        path: &Path,
        _fh: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> ResultEmpty {
        let rel_path = Self::to_rel_string(path);
        debug!(path = %Self::display_path(path), "FUSE utimens");

        let io = self.resolve_io(&rel_path);
        let to_timespec = |t: Option<SystemTime>| -> rustix::fs::Timespec {
            match t {
                Some(st) => {
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
        rustix::fs::utimensat(
            rustix::fs::CWD,
            &io,
            &times,
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|e| {
            warn!(path = %Self::display_path(path), io = %io.display(), error = %e, "FUSE utimens failed");
            map_errno(e)
        })?;
        Ok(())
    }

    fn statfs(&self, _req: RequestInfo, path: &Path) -> ResultStatfs {
        let rel = Self::to_rel_string(path);
        let io = self.resolve_io(&rel);
        let c_path = std::ffi::CString::new(io.as_os_str().as_bytes()).map_err(|_| libc::EINVAL)?;
        let mut stat: libc::statvfs64 = unsafe { std::mem::zeroed() };
        let ret = unsafe { libc::statvfs64(c_path.as_ptr(), &mut stat) };
        if ret != 0 {
            return Err(std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO));
        }
        Ok(Statfs {
            blocks: stat.f_blocks,
            bfree: stat.f_bfree,
            bavail: stat.f_bavail,
            files: stat.f_files,
            ffree: stat.f_ffree,
            bsize: stat.f_bsize as u32,
            namelen: stat.f_namemax as u32,
            frsize: stat.f_frsize as u32,
        })
    }

    fn opendir(&self, _req: RequestInfo, _path: &Path, _flags: u32) -> ResultOpen {
        Ok((0, 0))
    }

    fn releasedir(&self, _req: RequestInfo, _path: &Path, _fh: u64, _flags: u32) -> ResultEmpty {
        Ok(())
    }
}
