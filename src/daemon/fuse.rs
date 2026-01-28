use std::ffi::{OsStr, OsString};
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

use super::manager::Manifest;

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
        use std::ffi::CString;
        use std::os::unix::io::FromRawFd;

        if rel_path.is_empty() || rel_path == "." {
            // For root, duplicate the fd
            let new_fd = unsafe { libc::dup(self.real_root_fd.as_raw_fd()) };
            if new_fd < 0 {
                return Err(std::io::Error::last_os_error());
            }
            return Ok(unsafe { File::from_raw_fd(new_fd) });
        }

        let c_path = CString::new(rel_path).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid path")
        })?;

        let fd = unsafe {
            libc::openat(
                self.real_root_fd.as_raw_fd(),
                c_path.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };

        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(unsafe { File::from_raw_fd(fd) })
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

    fn resolve_backend(&self, path: &Path) -> Option<PathBuf> {
        if path.as_os_str().is_empty() {
            Some(self.real_root.clone())
        } else {
            self.manifest.read().resolve(&Self::to_rel_string(path))
        }
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
        let backend_path = self
            .manifest
            .read()
            .resolve(&Self::to_rel_string(&child_path))
            .ok_or_else(|| Self::file_not_found(&child_path))?;
        let attr = unix_fs::lookup(&backend_path)?;
        Ok(self.with_ttl(attr))
    }

    fn getattr(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        _file_handle: Option<BorrowedFileHandle<'_>>,
    ) -> FuseResult<FileAttribute> {
        debug!(path = %Self::display_path(&file_id), "FUSE getattr");

        // For root or paths under real_root, use openat to avoid FUSE deadlock
        let rel_path = Self::to_rel_string(&file_id);
        if file_id.as_os_str().is_empty() || self.manifest.read().is_real_path(&rel_path) {
            debug!(path = %Self::display_path(&file_id), "getattr using openat for real path");
            match self.open_relative(&rel_path) {
                Ok(file) => {
                    match unix_fs::getattr(file.as_fd()) {
                        Ok(attr) => return Ok(self.with_ttl(attr)),
                        Err(e) => {
                            warn!(path = %Self::display_path(&file_id), error = %e, "getattr fstat failed");
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    warn!(path = %Self::display_path(&file_id), error = %e, "getattr openat failed");
                    return Err(PosixError::new(ErrorKind::InputOutputError, e.to_string()));
                }
            }
        }

        // For Layer paths, use regular lookup (they're not under FUSE mount)
        let backend_path = match self.resolve_backend(&file_id) {
            Some(p) => p,
            None => {
                warn!(path = %Self::display_path(&file_id), "getattr: no backend path");
                return Err(Self::file_not_found(&file_id));
            }
        };
        debug!(path = %Self::display_path(&file_id), backend = %backend_path.display(), "getattr using lookup for layer path");
        match unix_fs::lookup(&backend_path) {
            Ok(attr) => Ok(self.with_ttl(attr)),
            Err(e) => {
                warn!(path = %Self::display_path(&file_id), backend = %backend_path.display(), error = %e, "getattr lookup failed");
                Err(e)
            }
        }
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

        for (name, is_dir) in self.manifest.read().readdir(&rel_path) {
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

        if let Some(backend_path) = self.resolve_backend(&file_id) {
            if let Ok(attr) = unix_fs::lookup(&backend_path) {
                entries.push((".".into(), self.with_ttl(attr)));
            }
        }

        let parent = Self::parent_path(&file_id);
        if let Some(backend_path) = self.resolve_backend(&parent) {
            if let Ok(attr) = unix_fs::lookup(&backend_path) {
                entries.push(("..".into(), self.with_ttl(attr)));
            }
        }

        let manifest = self.manifest.read();
        for (name, _is_dir) in manifest.readdir(&rel_path) {
            let child_path = Self::join_child(&file_id, OsStr::new(&name));
            if let Some(backend_path) = manifest.resolve(&Self::to_rel_string(&child_path)) {
                if let Ok(attr) = unix_fs::lookup(&backend_path) {
                    entries.push((OsString::from(name), self.with_ttl(attr)));
                }
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
        let backend_path = self
            .manifest
            .read()
            .resolve(&Self::to_rel_string(&file_id))
            .ok_or_else(|| Self::file_not_found(&file_id))?;
        let fd = unix_fs::open(&backend_path, flags)?;
        let handle = OwnedFileHandle::from_owned_fd(fd).ok_or_else(Self::bad_file_handle)?;
        Ok((handle, FUSEOpenResponseFlags::empty()))
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
        debug!(
            parent = %Self::display_path(&parent_id),
            name = %name.to_string_lossy(),
            mode,
            "FUSE create"
        );

        let target_dir = self.manifest.read().create_target(&rel_parent);
        let backend_path = target_dir.join(name);

        let (fd, attr) = unix_fs::create(&backend_path, mode, umask, flags)?;
        let handle = OwnedFileHandle::from_owned_fd(fd).ok_or_else(Self::bad_file_handle)?;

        self.manifest
            .write()
            .add_entry_with_backend(&Self::to_rel_string(&child_path), backend_path, false);

        Ok((handle, self.with_ttl(attr), FUSEOpenResponseFlags::empty()))
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
        debug!(
            parent = %Self::display_path(&parent_id),
            name = %name.to_string_lossy(),
            mode,
            "FUSE mkdir"
        );

        let target_dir = self.manifest.read().create_target(&rel_parent);
        let backend_path = target_dir.join(name);
        let attr = unix_fs::mkdir(&backend_path, mode, umask)?;

        self.manifest
            .write()
            .add_entry_with_backend(&Self::to_rel_string(&child_path), backend_path, true);

        Ok(self.with_ttl(attr))
    }

    fn unlink(&self, _req: &RequestInfo, parent_id: PathBuf, name: &OsStr) -> FuseResult<()> {
        let child_path = Self::join_child(&parent_id, name);
        debug!(
            parent = %Self::display_path(&parent_id),
            name = %name.to_string_lossy(),
            "FUSE unlink"
        );
        let backend_path = self
            .manifest
            .read()
            .resolve(&Self::to_rel_string(&child_path))
            .ok_or_else(|| Self::file_not_found(&child_path))?;
        unix_fs::unlink(&backend_path)?;
        self.manifest
            .write()
            .remove_entry(&Self::to_rel_string(&child_path));
        Ok(())
    }

    fn rmdir(&self, _req: &RequestInfo, parent_id: PathBuf, name: &OsStr) -> FuseResult<()> {
        let child_path = Self::join_child(&parent_id, name);
        debug!(
            parent = %Self::display_path(&parent_id),
            name = %name.to_string_lossy(),
            "FUSE rmdir"
        );
        let backend_path = self
            .manifest
            .read()
            .resolve(&Self::to_rel_string(&child_path))
            .ok_or_else(|| Self::file_not_found(&child_path))?;
        unix_fs::rmdir(&backend_path)?;
        self.manifest
            .write()
            .remove_entry(&Self::to_rel_string(&child_path));
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

        let (old_backend, new_backend) = {
            let manifest = self.manifest.read();
            let old_backend = manifest
                .resolve(&old_rel)
                .ok_or_else(|| Self::file_not_found(&old_path))?;
            let new_backend = match manifest.resolve(&new_rel) {
                Some(path) => path,
                None => {
                    let target_dir = manifest.create_target(&newparent_rel);
                    target_dir.join(newname)
                }
            };
            (old_backend, new_backend)
        };

        unix_fs::rename(&old_backend, &new_backend, flags)?;
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
        let backend_path = self
            .resolve_backend(&file_id)
            .ok_or_else(|| Self::file_not_found(&file_id))?;
        let attr = unix_fs::setattr(&backend_path, request)?;
        Ok(self.with_ttl(attr))
    }
}
