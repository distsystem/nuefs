use std::ffi::{CString, OsStr, OsString};
use std::fs::File;
use std::os::fd::{AsFd, AsRawFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use easy_fuser::prelude::*;
use easy_fuser::templates::fd_handler_helper::FdHandlerHelper;
use easy_fuser::templates::DefaultFuseHandler;
use easy_fuser::types::errors::{ErrorKind, PosixError};
use easy_fuser::unix_fs;
use parking_lot::RwLock;
use tracing::{debug, warn};

use super::manager::{Manifest, ReaddirResult, ResolvedPath};

pub(crate) struct NueFs {
    real_root: PathBuf,
    real_root_fd: File,
    manifest: Arc<RwLock<Manifest>>,
    inner: FdHandlerHelper<PathBuf>,
}

impl NueFs {
    pub(crate) fn new(real_root: PathBuf, real_root_fd: File, manifest: Arc<RwLock<Manifest>>) -> Self {
        Self {
            real_root,
            real_root_fd,
            manifest,
            inner: FdHandlerHelper::new(DefaultFuseHandler::new()),
        }
    }

    /// Open a file relative to the real root fd using openat.
    /// This avoids path resolution through the FUSE mount.
    fn open_relative(&self, rel_path: &str) -> std::io::Result<File> {
        self.open_relative_with_flags(rel_path, libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
    }

    /// Open a file relative to the real root fd using openat with custom flags.
    fn open_relative_with_flags(&self, rel_path: &str, flags: i32) -> std::io::Result<File> {
        use std::os::unix::io::FromRawFd;

        if rel_path.is_empty() || rel_path == "." {
            // For root, duplicate the fd
            let new_fd = unsafe { libc::dup(self.real_root_fd.as_raw_fd()) };
            if new_fd < 0 {
                return Err(std::io::Error::last_os_error());
            }
            return Ok(unsafe { File::from_raw_fd(new_fd) });
        }

        let c_path = Self::to_cstring(rel_path)?;

        let fd = unsafe {
            libc::openat(
                self.real_root_fd.as_raw_fd(),
                c_path.as_ptr(),
                flags | libc::O_CLOEXEC,
            )
        };

        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(unsafe { File::from_raw_fd(fd) })
    }

    /// Create a file relative to the real root fd using openat.
    /// Returns (fd, backend_path) where backend_path is the absolute path for manifest.
    fn create_relative(&self, rel_path: &str, mode: u32, flags: i32) -> std::io::Result<(File, PathBuf)> {
        use std::os::unix::io::FromRawFd;

        debug!(
            rel_path,
            mode = format!("{:#o}", mode),
            flags = format!("{:#x}", flags),
            root_fd = self.real_root_fd.as_raw_fd(),
            "create_relative starting"
        );

        let c_path = Self::to_cstring(rel_path)?;

        // Extract permission bits only (mode may contain S_IFREG)
        let perm_mode = mode & 0o7777;

        debug!(perm_mode = format!("{:#o}", perm_mode), "calling openat");

        let fd = unsafe {
            libc::openat(
                self.real_root_fd.as_raw_fd(),
                c_path.as_ptr(),
                flags | libc::O_CREAT | libc::O_CLOEXEC,
                perm_mode,
            )
        };

        debug!(fd, "openat returned");

        if fd < 0 {
            let err = std::io::Error::last_os_error();
            warn!(error = %err, "create_relative openat failed");
            return Err(err);
        }

        let backend_path = self.real_root.join(rel_path);
        debug!(backend_path = %backend_path.display(), "create_relative success");
        Ok((unsafe { File::from_raw_fd(fd) }, backend_path))
    }

    /// Remove a file or directory, using *at syscalls for Real paths.
    fn remove_at(&self, rel_path: &str, is_dir: bool) -> FuseResult<()> {
        match self.manifest.read().resolve(rel_path) {
            Some(ResolvedPath::Openat(rel)) => {
                let c_path = Self::to_cstring(&rel).map_err(|_| {
                    PosixError::new(ErrorKind::InvalidArgument, "invalid path")
                })?;
                let flags = if is_dir { libc::AT_REMOVEDIR } else { 0 };
                let result = unsafe {
                    libc::unlinkat(self.real_root_fd.as_raw_fd(), c_path.as_ptr(), flags)
                };
                if result < 0 {
                    let e = std::io::Error::last_os_error();
                    return Err(PosixError::new(ErrorKind::InputOutputError, e.to_string()));
                }
                Ok(())
            }
            Some(ResolvedPath::Absolute(p)) => {
                if is_dir {
                    unix_fs::rmdir(&p)
                } else {
                    unix_fs::unlink(&p)
                }
            }
            None => Err(ErrorKind::FileNotFound.to_error("not found")),
        }
    }

    /// Set file attributes using *at syscalls (avoids FUSE deadlock).
    fn setattr_relative(&self, rel_path: &str, request: &SetAttrRequest) -> std::io::Result<libc::stat> {
        let c_path = Self::to_cstring(rel_path)?;

        // chmod
        if let Some(mode) = request.mode {
            let result = unsafe {
                libc::fchmodat(
                    self.real_root_fd.as_raw_fd(),
                    c_path.as_ptr(),
                    mode,
                    0,
                )
            };
            if result < 0 {
                return Err(std::io::Error::last_os_error());
            }
        }

        // chown
        if request.uid.is_some() || request.gid.is_some() {
            let uid = request.uid.map(|u| u as libc::uid_t).unwrap_or(u32::MAX);
            let gid = request.gid.map(|g| g as libc::gid_t).unwrap_or(u32::MAX);
            let result = unsafe {
                libc::fchownat(
                    self.real_root_fd.as_raw_fd(),
                    c_path.as_ptr(),
                    uid,
                    gid,
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if result < 0 {
                return Err(std::io::Error::last_os_error());
            }
        }

        // truncate
        if let Some(size) = request.size {
            let file = self.open_relative_with_flags(rel_path, libc::O_WRONLY)?;
            let result = unsafe { libc::ftruncate(file.as_raw_fd(), size as libc::off_t) };
            if result < 0 {
                return Err(std::io::Error::last_os_error());
            }
        }

        // utimens
        if request.atime.is_some() || request.mtime.is_some() {
            let to_timespec = |t: Option<fuser::TimeOrNow>| -> libc::timespec {
                match t {
                    Some(fuser::TimeOrNow::Now) => libc::timespec {
                        tv_sec: 0,
                        tv_nsec: libc::UTIME_NOW,
                    },
                    Some(fuser::TimeOrNow::SpecificTime(st)) => {
                        let d = st.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                        libc::timespec {
                            tv_sec: d.as_secs() as libc::time_t,
                            tv_nsec: d.subsec_nanos() as libc::c_long,
                        }
                    }
                    None => libc::timespec {
                        tv_sec: 0,
                        tv_nsec: libc::UTIME_OMIT,
                    },
                }
            };
            let times = [to_timespec(request.atime), to_timespec(request.mtime)];
            let result = unsafe {
                libc::utimensat(
                    self.real_root_fd.as_raw_fd(),
                    c_path.as_ptr(),
                    times.as_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if result < 0 {
                return Err(std::io::Error::last_os_error());
            }
        }

        self.lstat_relative(rel_path)
    }

    /// Create hard link using linkat (avoids FUSE deadlock).
    fn link_relative(&self, old_rel: &str, new_rel: &str) -> std::io::Result<()> {
        let c_old = Self::to_cstring(old_rel)?;
        let c_new = Self::to_cstring(new_rel)?;

        let result = unsafe {
            libc::linkat(
                self.real_root_fd.as_raw_fd(),
                c_old.as_ptr(),
                self.real_root_fd.as_raw_fd(),
                c_new.as_ptr(),
                0,
            )
        };

        if result < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Create symlink using symlinkat (avoids FUSE deadlock).
    fn symlink_relative(&self, rel_path: &str, target: &Path) -> std::io::Result<()> {
        let c_link = Self::to_cstring(rel_path)?;
        let c_target = CString::new(target.as_os_str().as_encoded_bytes())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid target"))?;

        let result = unsafe {
            libc::symlinkat(
                c_target.as_ptr(),
                self.real_root_fd.as_raw_fd(),
                c_link.as_ptr(),
            )
        };

        if result < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Read symlink target using readlinkat (avoids FUSE deadlock).
    fn readlink_relative(&self, rel_path: &str) -> std::io::Result<Vec<u8>> {
        let c_path = Self::to_cstring(rel_path)?;
        let mut buf = vec![0u8; libc::PATH_MAX as usize];

        let len = unsafe {
            libc::readlinkat(
                self.real_root_fd.as_raw_fd(),
                c_path.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
            )
        };

        if len < 0 {
            return Err(std::io::Error::last_os_error());
        }

        buf.truncate(len as usize);
        Ok(buf)
    }

    /// Get file attributes using fstatat (lstat equivalent for relative paths).
    /// This avoids path resolution through the FUSE mount.
    fn lstat_relative(&self, rel_path: &str) -> std::io::Result<libc::stat> {
        let c_path = Self::to_cstring(rel_path)?;
        let mut stat_buf: libc::stat = unsafe { std::mem::zeroed() };

        let result = unsafe {
            libc::fstatat(
                self.real_root_fd.as_raw_fd(),
                c_path.as_ptr(),
                &mut stat_buf,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };

        if result < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(stat_buf)
    }

    /// Convert libc::stat to FileAttribute
    fn stat_to_attr(&self, stat: &libc::stat) -> FileAttribute {
        use std::time::{Duration, UNIX_EPOCH};

        let kind = match stat.st_mode & libc::S_IFMT {
            libc::S_IFDIR => FileKind::Directory,
            libc::S_IFLNK => FileKind::Symlink,
            libc::S_IFREG => FileKind::RegularFile,
            _ => FileKind::RegularFile,
        };

        let atime = UNIX_EPOCH + Duration::new(stat.st_atime as u64, stat.st_atime_nsec as u32);
        let mtime = UNIX_EPOCH + Duration::new(stat.st_mtime as u64, stat.st_mtime_nsec as u32);
        let ctime = UNIX_EPOCH + Duration::new(stat.st_ctime as u64, stat.st_ctime_nsec as u32);

        FileAttribute {
            size: stat.st_size as u64,
            blocks: stat.st_blocks as u64,
            atime,
            mtime,
            ctime,
            crtime: ctime, // Use ctime as fallback for crtime
            kind,
            perm: (stat.st_mode & 0o7777) as u16,
            nlink: stat.st_nlink as u32,
            uid: stat.st_uid,
            gid: stat.st_gid,
            rdev: stat.st_rdev as u32,
            blksize: stat.st_blksize as u32,
            flags: 0,
            generation: Some(0),
            ttl: Some(self.get_default_ttl()),
        }
    }

    /// Rename within Real paths using renameat2.
    fn rename_at(&self, old_rel: &str, new_rel: &str, flags: u32) -> std::io::Result<()> {
        let c_old = Self::to_cstring(old_rel)?;
        let c_new = Self::to_cstring(new_rel)?;

        let result = unsafe {
            libc::renameat2(
                self.real_root_fd.as_raw_fd(),
                c_old.as_ptr(),
                self.real_root_fd.as_raw_fd(),
                c_new.as_ptr(),
                flags,
            )
        };

        if result < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Create a directory relative to the real root fd using mkdirat.
    /// Returns the backend_path for manifest.
    fn mkdir_relative(&self, rel_path: &str, mode: u32) -> std::io::Result<PathBuf> {
        let c_path = Self::to_cstring(rel_path)?;

        let result = unsafe {
            libc::mkdirat(
                self.real_root_fd.as_raw_fd(),
                c_path.as_ptr(),
                mode,
            )
        };

        if result < 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(self.real_root.join(rel_path))
    }

    fn display_path(path: &Path) -> String {
        if path.as_os_str().is_empty() {
            "/".to_string()
        } else {
            path.to_string_lossy().to_string()
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

    /// Read directory using openat for Real paths.
    fn readdir_real(&self, rel_path: &str) -> std::io::Result<Vec<(String, bool)>> {
        let dir_fd = if rel_path.is_empty() {
            unsafe { libc::dup(self.real_root_fd.as_raw_fd()) }
        } else {
            let c_path = Self::to_cstring(rel_path)?;
            unsafe {
                libc::openat(
                    self.real_root_fd.as_raw_fd(),
                    c_path.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
                )
            }
        };

        if dir_fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let dir = unsafe { libc::fdopendir(dir_fd) };
        if dir.is_null() {
            unsafe { libc::close(dir_fd) };
            return Err(std::io::Error::last_os_error());
        }

        let mut entries = Vec::new();
        loop {
            // Clear errno before readdir
            unsafe { *libc::__errno_location() = 0 };
            let entry = unsafe { libc::readdir(dir) };
            if entry.is_null() {
                let errno = unsafe { *libc::__errno_location() };
                if errno != 0 {
                    unsafe { libc::closedir(dir) };
                    return Err(std::io::Error::from_raw_os_error(errno));
                }
                break;
            }

            let d_name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
            let name = d_name.to_string_lossy().to_string();
            if name == "." || name == ".." {
                continue;
            }

            let d_type = unsafe { (*entry).d_type };
            let is_dir = d_type == libc::DT_DIR;
            entries.push((name, is_dir));
        }

        unsafe { libc::closedir(dir) };
        Ok(entries)
    }

    fn parent_path(path: &Path) -> PathBuf {
        path.parent().map_or_else(PathBuf::new, Path::to_path_buf)
    }

    fn with_ttl(&self, mut attr: FileAttribute) -> FileAttribute {
        if attr.ttl.is_none() {
            attr.ttl = Some(self.get_default_ttl());
        }
        attr
    }

    fn to_cstring(s: &str) -> std::io::Result<CString> {
        CString::new(s)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid path"))
    }

    fn map_io_error(e: std::io::Error) -> PosixError {
        let kind = match e.kind() {
            std::io::ErrorKind::NotFound => ErrorKind::FileNotFound,
            std::io::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            _ => ErrorKind::InputOutputError,
        };
        PosixError::new(kind, e.to_string())
    }

    fn get_file_attr(&self, rel_path: &str) -> FuseResult<FileAttribute> {
        debug!(path = %rel_path, "get_file_attr: acquiring read lock");
        let resolved = self.manifest.read().resolve(rel_path);
        debug!(path = %rel_path, resolved = ?resolved, "get_file_attr: lock released");

        match resolved {
            Some(ResolvedPath::Openat(rel)) => {
                debug!(path = %rel_path, rel = %rel, "get_file_attr: Openat mode");
                // Try open_relative first; if it fails (e.g., for symlinks with O_NOFOLLOW),
                // fall back to lstat_relative which uses fstatat
                match self.open_relative(&rel) {
                    Ok(f) => unix_fs::getattr(f.as_fd()).map(|a| self.with_ttl(a)),
                    Err(e) => {
                        debug!(path = %rel_path, error = %e, "get_file_attr: openat failed, trying fstatat");
                        // Fallback: use fstatat (lstat) for symlinks - avoids FUSE deadlock
                        self.lstat_relative(&rel)
                            .map(|stat| self.stat_to_attr(&stat))
                            .map_err(|e| {
                                debug!(path = %rel_path, error = %e, "get_file_attr: fstatat also failed");
                                ErrorKind::FileNotFound.to_error(e.to_string())
                            })
                    }
                }
            }
            Some(ResolvedPath::Absolute(path)) => {
                debug!(path = %rel_path, backend = %path.display(), "get_file_attr: Absolute mode");
                unix_fs::lookup(&path).map(|a| self.with_ttl(a))
            }
            None => {
                debug!(path = %rel_path, "get_file_attr: not found");
                Err(ErrorKind::FileNotFound.to_error("not found"))
            }
        }
    }

    fn resolve_backend_path(&self, resolved: &ResolvedPath, name: &OsStr) -> PathBuf {
        match resolved {
            ResolvedPath::Openat(rel) => {
                if rel.is_empty() {
                    self.real_root.join(name)
                } else {
                    self.real_root.join(rel).join(name)
                }
            }
            ResolvedPath::Absolute(dir) => dir.join(name),
        }
    }

    fn file_not_found(path: &Path) -> PosixError {
        ErrorKind::FileNotFound.to_error(&format!("{}: not found", Self::display_path(path)))
    }

    fn bad_file_handle() -> PosixError {
        ErrorKind::BadFileDescriptor.to_error("bad file handle")
    }
}

impl FuseHandler<PathBuf> for NueFs {
    fn get_inner(&self) -> &dyn FuseHandler<PathBuf> {
        &self.inner
    }

    fn lookup(&self, _req: &RequestInfo, parent_id: PathBuf, name: &OsStr) -> FuseResult<FileAttribute> {
        let child_path = Self::join_child(&parent_id, name);
        let child_rel = Self::to_rel_string(&child_path);
        debug!(
            parent = %Self::display_path(&parent_id),
            name = %name.to_string_lossy(),
            path = %child_rel,
            "FUSE lookup START"
        );

        let result = self.get_file_attr(&child_rel)
            .map_err(|_| Self::file_not_found(&child_path));
        debug!(
            path = %child_rel,
            ok = result.is_ok(),
            "FUSE lookup END"
        );
        result
    }

    fn getattr(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        _file_handle: Option<BorrowedFileHandle<'_>>,
    ) -> FuseResult<FileAttribute> {
        debug!(path = %Self::display_path(&file_id), "FUSE getattr");
        self.get_file_attr(&Self::to_rel_string(&file_id))
            .map_err(|_| Self::file_not_found(&file_id))
    }

    fn readdir(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        _file_handle: BorrowedFileHandle<'_>,
    ) -> FuseResult<Vec<(OsString, FileKind)>> {
        debug!(path = %Self::display_path(&file_id), "FUSE readdir");
        let rel_path = Self::to_rel_string(&file_id);
        let mut entries: Vec<(OsString, FileKind)> = Vec::new();
        entries.push((".".into(), FileKind::Directory));
        entries.push(("..".into(), FileKind::Directory));

        let children = match self.manifest.read().readdir(&rel_path) {
            ReaddirResult::Absolute(children) => children,
            ReaddirResult::Openat {
                rel_path,
                manifest_children,
            } => {
                // Read real directory using openat and merge with manifest children
                let mut merged = self.readdir_real(&rel_path).unwrap_or_default();
                let existing: std::collections::HashSet<String> =
                    merged.iter().map(|(n, _)| n.clone()).collect();
                for (name, is_dir) in manifest_children {
                    if !existing.contains(&name) {
                        merged.push((name, is_dir));
                    }
                }
                merged
            }
        };

        for (name, is_dir) in children {
            let kind = if is_dir {
                FileKind::Directory
            } else {
                FileKind::RegularFile
            };
            entries.push((OsString::from(name), kind));
        }

        Ok(entries)
    }

    fn readdirplus(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        _file_handle: BorrowedFileHandle<'_>,
    ) -> FuseResult<Vec<(OsString, FileAttribute)>> {
        let rel_path = Self::to_rel_string(&file_id);
        let mut entries: Vec<(OsString, FileAttribute)> = Vec::new();

        // "." entry
        if let Ok(attr) = self.get_file_attr(&rel_path) {
            entries.push((".".into(), attr));
        }

        // ".." entry
        let parent_rel = Self::to_rel_string(&Self::parent_path(&file_id));
        if let Ok(attr) = self.get_file_attr(&parent_rel) {
            entries.push(("..".into(), attr));
        }

        // Get children
        let children = match self.manifest.read().readdir(&rel_path) {
            ReaddirResult::Absolute(children) => children,
            ReaddirResult::Openat {
                rel_path,
                manifest_children,
            } => {
                // Read real directory using openat and merge with manifest children
                let mut merged = self.readdir_real(&rel_path).unwrap_or_default();
                let existing: std::collections::HashSet<String> =
                    merged.iter().map(|(n, _)| n.clone()).collect();
                for (name, is_dir) in manifest_children {
                    if !existing.contains(&name) {
                        merged.push((name, is_dir));
                    }
                }
                merged
            }
        };

        // Child entries
        for (name, _is_dir) in children {
            let child_rel = Self::to_rel_string(&Self::join_child(&file_id, OsStr::new(&name)));
            if let Ok(attr) = self.get_file_attr(&child_rel) {
                entries.push((OsString::from(name), attr));
            }
        }

        Ok(entries)
    }

    fn open(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        flags: OpenFlags,
    ) -> FuseResult<(OwnedFileHandle, FUSEOpenResponseFlags)> {
        debug!(path = %Self::display_path(&file_id), ?flags, "FUSE open");
        let rel_path = Self::to_rel_string(&file_id);
        let resolved = self.manifest.read().resolve(&rel_path);

        match resolved {
            Some(ResolvedPath::Openat(rel)) => {
                let libc_flags = flags.bits() as i32;
                let file = self
                    .open_relative_with_flags(&rel, libc_flags)
                    .map_err(Self::map_io_error)?;
                let handle = OwnedFileHandle::from_owned_fd(file.into())
                    .ok_or_else(Self::bad_file_handle)?;
                Ok((handle, FUSEOpenResponseFlags::empty()))
            }
            Some(ResolvedPath::Absolute(path)) => {
                let fd = unix_fs::open(&path, flags)?;
                let handle = OwnedFileHandle::from_owned_fd(fd).ok_or_else(Self::bad_file_handle)?;
                Ok((handle, FUSEOpenResponseFlags::empty()))
            }
            None => Err(Self::file_not_found(&file_id)),
        }
    }

    fn create(
        &self,
        _req: &RequestInfo,
        parent_id: PathBuf,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: OpenFlags,
    ) -> FuseResult<(OwnedFileHandle, FileAttribute, FUSEOpenResponseFlags)> {
        let rel_parent = Self::to_rel_string(&parent_id);
        let child_path = Self::join_child(&parent_id, name);
        let child_rel = Self::to_rel_string(&child_path);
        debug!(
            parent = %Self::display_path(&parent_id),
            name = %name.to_string_lossy(),
            mode,
            "FUSE create"
        );

        // Apply umask
        let effective_mode = mode & !umask;

        // Get create target and release read lock before calling create_relative
        let create_target = self.manifest.read().create_target(&rel_parent);

        match create_target {
            ResolvedPath::Openat(_) => {
                // Use openat to avoid FUSE deadlock
                let libc_flags = flags.bits() as i32;
                let (file, backend_path) = self
                    .create_relative(&child_rel, effective_mode, libc_flags)
                    .map_err(|e| PosixError::new(ErrorKind::InputOutputError, e.to_string()))?;

                let attr = unix_fs::getattr(file.as_fd())?;
                let handle = OwnedFileHandle::from_owned_fd(file.into())
                    .ok_or_else(Self::bad_file_handle)?;

                self.manifest
                    .write()
                    .add_entry_with_backend(&child_rel, backend_path, false);

                Ok((handle, self.with_ttl(attr), FUSEOpenResponseFlags::empty()))
            }
            ResolvedPath::Absolute(dir) => {
                // Layer paths are not under FUSE mount, safe to use unix_fs::create
                let backend_path = dir.join(name);
                let (fd, attr) = unix_fs::create(&backend_path, mode, umask, flags)?;
                let handle = OwnedFileHandle::from_owned_fd(fd).ok_or_else(Self::bad_file_handle)?;

                self.manifest
                    .write()
                    .add_entry_with_backend(&child_rel, backend_path, false);

                Ok((handle, self.with_ttl(attr), FUSEOpenResponseFlags::empty()))
            }
        }
    }

    fn mkdir(
        &self,
        _req: &RequestInfo,
        parent_id: PathBuf,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> FuseResult<FileAttribute> {
        let rel_parent = Self::to_rel_string(&parent_id);
        let child_path = Self::join_child(&parent_id, name);
        let child_rel = Self::to_rel_string(&child_path);
        debug!(
            parent = %Self::display_path(&parent_id),
            name = %name.to_string_lossy(),
            mode,
            "FUSE mkdir"
        );

        // Apply umask
        let effective_mode = mode & !umask;

        // Get create target and release read lock before calling mkdir_relative
        let create_target = self.manifest.read().create_target(&rel_parent);

        match create_target {
            ResolvedPath::Openat(_) => {
                // Use mkdirat to avoid FUSE deadlock
                let backend_path = self
                    .mkdir_relative(&child_rel, effective_mode)
                    .map_err(|e| PosixError::new(ErrorKind::InputOutputError, e.to_string()))?;

                // Get attributes using openat
                let file = self
                    .open_relative(&child_rel)
                    .map_err(|e| PosixError::new(ErrorKind::InputOutputError, e.to_string()))?;
                let attr = unix_fs::getattr(file.as_fd())?;

                self.manifest
                    .write()
                    .add_entry_with_backend(&child_rel, backend_path, true);

                Ok(self.with_ttl(attr))
            }
            ResolvedPath::Absolute(dir) => {
                // Layer paths are not under FUSE mount, safe to use unix_fs::mkdir
                let backend_path = dir.join(name);
                let attr = unix_fs::mkdir(&backend_path, mode, umask)?;

                self.manifest
                    .write()
                    .add_entry_with_backend(&child_rel, backend_path, true);

                Ok(self.with_ttl(attr))
            }
        }
    }

    fn readlink(&self, _req: &RequestInfo, file_id: PathBuf) -> FuseResult<Vec<u8>> {
        let rel_path = Self::to_rel_string(&file_id);
        debug!(path = %Self::display_path(&file_id), "FUSE readlink");

        let resolved = self.manifest.read().resolve(&rel_path)
            .ok_or_else(|| Self::file_not_found(&file_id))?;

        match resolved {
            ResolvedPath::Openat(rel) => {
                self.readlink_relative(&rel).map_err(Self::map_io_error)
            }
            ResolvedPath::Absolute(path) => unix_fs::readlink(&path),
        }
    }

    fn symlink(
        &self,
        _req: &RequestInfo,
        parent_id: PathBuf,
        link_name: &OsStr,
        target: &Path,
    ) -> FuseResult<FileAttribute> {
        let parent_rel = Self::to_rel_string(&parent_id);
        let child_path = Self::join_child(&parent_id, link_name);
        let child_rel = Self::to_rel_string(&child_path);
        debug!(
            parent = %Self::display_path(&parent_id),
            link_name = %link_name.to_string_lossy(),
            target = %target.display(),
            "FUSE symlink START"
        );

        debug!(path = %child_rel, "symlink: acquiring read lock for create_target");
        let create_target = self.manifest.read().create_target(&parent_rel);
        debug!(path = %child_rel, target = ?create_target, "symlink: read lock released");

        let backend_path = self.resolve_backend_path(&create_target, link_name);

        match create_target {
            ResolvedPath::Openat(_) => {
                debug!(path = %child_rel, "symlink: using symlinkat");
                self.symlink_relative(&child_rel, target)
                    .map_err(Self::map_io_error)?;
            }
            ResolvedPath::Absolute(_) => {
                debug!(path = %child_rel, backend = %backend_path.display(), "symlink: using unix_fs::symlink");
                unix_fs::symlink(&backend_path, target)?;
            }
        }

        let attr = self.lstat_relative(&child_rel)
            .map(|stat| self.stat_to_attr(&stat))
            .map_err(Self::map_io_error)?;

        debug!(path = %child_rel, "symlink: acquiring write lock");
        self.manifest
            .write()
            .add_entry_with_backend(&child_rel, backend_path, false);
        debug!(path = %child_rel, "symlink: write lock released, done");
        Ok(self.with_ttl(attr))
    }

    fn link(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        newparent: PathBuf,
        newname: &OsStr,
    ) -> FuseResult<FileAttribute> {
        let old_rel = Self::to_rel_string(&file_id);
        let newparent_rel = Self::to_rel_string(&newparent);
        let new_path = Self::join_child(&newparent, newname);
        let new_rel = Self::to_rel_string(&new_path);
        debug!(
            old = %Self::display_path(&file_id),
            newparent = %Self::display_path(&newparent),
            newname = %newname.to_string_lossy(),
            "FUSE link"
        );

        let old_resolved = self.manifest.read().resolve(&old_rel)
            .ok_or_else(|| Self::file_not_found(&file_id))?;
        let new_target = self.manifest.read().create_target(&newparent_rel);

        let new_backend = self.resolve_backend_path(&new_target, newname);

        // Use linkat if both paths are under mount root
        let both_openat = old_resolved.is_openat() && new_target.is_openat();
        if both_openat {
            self.link_relative(&old_rel, &new_rel)
                .map_err(Self::map_io_error)?;
        } else {
            let old_backend = old_resolved.to_path(&self.real_root);
            std::fs::hard_link(&old_backend, &new_backend).map_err(|e| {
                PosixError::new(ErrorKind::InputOutputError, e.to_string())
            })?;
        }

        let attr = self.lstat_relative(&new_rel)
            .map(|stat| self.stat_to_attr(&stat))
            .map_err(Self::map_io_error)?;

        self.manifest
            .write()
            .add_entry_with_backend(&new_rel, new_backend, false);
        Ok(self.with_ttl(attr))
    }

    fn unlink(&self, _req: &RequestInfo, parent_id: PathBuf, name: &OsStr) -> FuseResult<()> {
        let child_path = Self::join_child(&parent_id, name);
        let child_rel = Self::to_rel_string(&child_path);
        debug!(
            parent = %Self::display_path(&parent_id),
            name = %name.to_string_lossy(),
            path = %child_rel,
            "FUSE unlink START"
        );
        self.remove_at(&child_rel, false)?;
        debug!(path = %child_rel, "unlink: acquiring write lock");
        self.manifest.write().remove_entry(&child_rel);
        debug!(path = %child_rel, "FUSE unlink END");
        Ok(())
    }

    fn rmdir(&self, _req: &RequestInfo, parent_id: PathBuf, name: &OsStr) -> FuseResult<()> {
        let child_path = Self::join_child(&parent_id, name);
        let child_rel = Self::to_rel_string(&child_path);
        debug!(
            parent = %Self::display_path(&parent_id),
            name = %name.to_string_lossy(),
            "FUSE rmdir"
        );
        self.remove_at(&child_rel, true)?;
        self.manifest.write().remove_entry(&child_rel);
        Ok(())
    }

    fn rename(
        &self,
        _req: &RequestInfo,
        parent_id: PathBuf,
        name: &OsStr,
        newparent: PathBuf,
        newname: &OsStr,
        flags: RenameFlags,
    ) -> FuseResult<()> {
        let old_path = Self::join_child(&parent_id, name);
        let new_path = Self::join_child(&newparent, newname);
        let old_rel = Self::to_rel_string(&old_path);
        let new_rel = Self::to_rel_string(&new_path);
        let newparent_rel = Self::to_rel_string(&newparent);
        debug!(
            old = %Self::display_path(&old_path),
            new = %Self::display_path(&new_path),
            ?flags,
            "FUSE rename"
        );

        let old_resolved = self.manifest.read().resolve(&old_rel)
            .ok_or_else(|| Self::file_not_found(&old_path))?;
        let new_target = self.manifest.read().create_target(&newparent_rel);

        // Use efficient renameat if both are under mount root
        let both_openat = old_resolved.is_openat() && new_target.is_openat();

        let old_backend = old_resolved.to_path(&self.real_root);
        let new_backend = self.resolve_backend_path(&new_target, newname);

        if both_openat {
            self.rename_at(&old_rel, &new_rel, flags.bits())
                .map_err(|e| PosixError::new(ErrorKind::InputOutputError, e.to_string()))?;
        } else {
            unix_fs::rename(&old_backend, &new_backend, flags)?;
        }

        self.manifest.write().rename_entry_with_backend(
            &old_rel,
            &new_rel,
            &old_backend,
            &new_backend,
        );
        Ok(())
    }

    fn setattr(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        request: SetAttrRequest,
    ) -> FuseResult<FileAttribute> {
        debug!(path = %Self::display_path(&file_id), "FUSE setattr");
        let rel_path = Self::to_rel_string(&file_id);
        let resolved = self.manifest.read().resolve(&rel_path)
            .ok_or_else(|| Self::file_not_found(&file_id))?;

        let attr = match resolved {
            ResolvedPath::Openat(_) => {
                self.setattr_relative(&rel_path, &request)
                    .map(|stat| self.stat_to_attr(&stat))
                    .map_err(Self::map_io_error)?
            }
            ResolvedPath::Absolute(path) => unix_fs::setattr(&path, request)?,
        };
        Ok(self.with_ttl(attr))
    }
}
