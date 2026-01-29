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
            Some(ResolvedPath::Real(rel)) => {
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
            Some(ResolvedPath::Layer(p)) => {
                if is_dir {
                    unix_fs::rmdir(&p)
                } else {
                    unix_fs::unlink(&p)
                }
            }
            None => Err(ErrorKind::FileNotFound.to_error("not found")),
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
        match self.manifest.read().resolve(rel_path) {
            Some(ResolvedPath::Real(rel)) => self
                .open_relative(&rel)
                .map_err(Self::map_io_error)
                .and_then(|f| unix_fs::getattr(f.as_fd()).map(|a| self.with_ttl(a))),
            Some(ResolvedPath::Layer(path)) => {
                unix_fs::lookup(&path).map(|a| self.with_ttl(a))
            }
            None => Err(ErrorKind::FileNotFound.to_error("not found")),
        }
    }

    fn resolve_backend_path(&self, resolved: &ResolvedPath, name: &OsStr) -> PathBuf {
        match resolved {
            ResolvedPath::Real(rel) => {
                if rel.is_empty() {
                    self.real_root.join(name)
                } else {
                    self.real_root.join(rel).join(name)
                }
            }
            ResolvedPath::Layer(dir) => dir.join(name),
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
        debug!(
            parent = %Self::display_path(&parent_id),
            name = %name.to_string_lossy(),
            path = %Self::display_path(&child_path),
            "FUSE lookup"
        );

        self.get_file_attr(&Self::to_rel_string(&child_path))
            .map_err(|_| Self::file_not_found(&child_path))
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
            ReaddirResult::Layer(children) => children,
            ReaddirResult::Real(rel) => {
                // Use openat to read real directory
                self.readdir_real(&rel).unwrap_or_default()
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
        let manifest = self.manifest.read();
        let children = match manifest.readdir(&rel_path) {
            ReaddirResult::Layer(children) => children,
            ReaddirResult::Real(rel) => {
                drop(manifest);
                self.readdir_real(&rel).unwrap_or_default()
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
            Some(ResolvedPath::Real(rel)) => {
                let libc_flags = flags.bits() as i32;
                let file = self
                    .open_relative_with_flags(&rel, libc_flags)
                    .map_err(Self::map_io_error)?;
                let handle = OwnedFileHandle::from_owned_fd(file.into())
                    .ok_or_else(Self::bad_file_handle)?;
                Ok((handle, FUSEOpenResponseFlags::empty()))
            }
            Some(ResolvedPath::Layer(path)) => {
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
            ResolvedPath::Real(_) => {
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
            ResolvedPath::Layer(dir) => {
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
            ResolvedPath::Real(_) => {
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
            ResolvedPath::Layer(dir) => {
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

        let resolved = self.manifest.read().resolve(&rel_path);
        match resolved {
            Some(ResolvedPath::Real(rel)) => {
                let full_path = self.real_root.join(&rel);
                unix_fs::readlink(&full_path)
            }
            Some(ResolvedPath::Layer(path)) => unix_fs::readlink(&path),
            None => Err(Self::file_not_found(&file_id)),
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
            "FUSE symlink"
        );

        let create_target = self.manifest.read().create_target(&parent_rel);
        match create_target {
            ResolvedPath::Real(_) => {
                let full_path = self.real_root.join(&child_rel);
                let attr = unix_fs::symlink(&full_path, target)?;
                self.manifest
                    .write()
                    .add_entry_with_backend(&child_rel, full_path, false);
                Ok(self.with_ttl(attr))
            }
            ResolvedPath::Layer(dir) => {
                let backend_path = dir.join(link_name);
                let attr = unix_fs::symlink(&backend_path, target)?;
                self.manifest
                    .write()
                    .add_entry_with_backend(&child_rel, backend_path, false);
                Ok(self.with_ttl(attr))
            }
        }
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

        let old_resolved = self.manifest.read().resolve(&old_rel);
        let new_target = self.manifest.read().create_target(&newparent_rel);

        let old_backend = match &old_resolved {
            Some(ResolvedPath::Real(rel)) => self.real_root.join(rel),
            Some(ResolvedPath::Layer(p)) => p.clone(),
            None => return Err(Self::file_not_found(&file_id)),
        };

        let new_backend = self.resolve_backend_path(&new_target, newname);

        // Create hard link
        std::fs::hard_link(&old_backend, &new_backend).map_err(|e| {
            PosixError::new(ErrorKind::InputOutputError, e.to_string())
        })?;

        let attr = unix_fs::lookup(&new_backend)?;
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
            "FUSE unlink"
        );
        self.remove_at(&child_rel, false)?;
        self.manifest.write().remove_entry(&child_rel);
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

        let old_resolved = self.manifest.read().resolve(&old_rel);
        let new_target = self.manifest.read().create_target(&newparent_rel);

        // Check if both are Real paths - can use efficient renameat
        let both_real = matches!(&old_resolved, Some(ResolvedPath::Real(_)))
            && matches!(&new_target, ResolvedPath::Real(_));

        // Compute backend paths for manifest update
        let old_backend = match &old_resolved {
            Some(ResolvedPath::Real(rel)) => self.real_root.join(rel),
            Some(ResolvedPath::Layer(p)) => p.clone(),
            None => return Err(Self::file_not_found(&old_path)),
        };
        let new_backend = self.resolve_backend_path(&new_target, newname);

        // Perform rename
        if both_real {
            self.rename_at(&old_rel, &new_rel, flags.bits())
                .map_err(|e| PosixError::new(ErrorKind::InputOutputError, e.to_string()))?;
        } else {
            // For mixed Real/Layer renames, use full paths bypassing FUSE
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
        let resolved = self.manifest.read().resolve(&rel_path);

        match resolved {
            Some(ResolvedPath::Real(rel)) => {
                // Build full path bypassing FUSE
                let full_path = self.real_root.join(&rel);
                let attr = unix_fs::setattr(&full_path, request)?;
                Ok(self.with_ttl(attr))
            }
            Some(ResolvedPath::Layer(path)) => {
                let attr = unix_fs::setattr(&path, request)?;
                Ok(self.with_ttl(attr))
            }
            None => Err(Self::file_not_found(&file_id)),
        }
    }
}
