use std::ffi::{CString, OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use easy_fuser::prelude::*;
use easy_fuser::templates::fd_handler_helper::FdHandlerHelper;
use easy_fuser::templates::DefaultFuseHandler;
use easy_fuser::types::errors::{ErrorKind, PosixError};
use easy_fuser::unix_fs;
use parking_lot::RwLock;
use tracing::debug;

use super::manager::{DirTarget, Manifest, ResolvedPaths};

pub(crate) struct NueFs {
    manifest: Arc<RwLock<Manifest>>,
    inner: FdHandlerHelper<PathBuf>,
}

impl NueFs {
    pub(crate) fn new(manifest: Arc<RwLock<Manifest>>) -> Self {
        Self {
            manifest,
            inner: FdHandlerHelper::new(DefaultFuseHandler::new()),
        }
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

    fn map_std_io_error(e: std::io::Error) -> PosixError {
        let kind = match e.kind() {
            std::io::ErrorKind::NotFound => ErrorKind::FileNotFound,
            std::io::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            _ => ErrorKind::InputOutputError,
        };
        PosixError::new(kind, e.to_string())
    }

    fn resolve_paths(&self, rel_path: &str) -> ResolvedPaths {
        self.manifest.read().resolve_paths(rel_path)
    }

    fn create_target(&self, parent_rel: &str) -> DirTarget {
        self.manifest.read().create_target(parent_rel)
    }

    fn get_file_attr(&self, rel_path: &str) -> FuseResult<FileAttribute> {
        let resolved = self.resolve_paths(rel_path);
        unix_fs::lookup(&resolved.io_path).map(|a| self.with_ttl(a))
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

    fn merge_children(
        mut base: Vec<(String, bool)>,
        manifest_children: Vec<(String, bool)>,
    ) -> Vec<(String, bool)> {
        let existing: std::collections::HashSet<_> = base.iter().map(|(n, _)| n.clone()).collect();
        for (name, is_dir) in manifest_children {
            if !existing.contains(&name) {
                base.push((name, is_dir));
            }
        }
        base
    }

    fn cstring_from_path(path: &Path) -> Result<CString, PosixError> {
        CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            PosixError::new(
                ErrorKind::InvalidArgument,
                format!("{}: invalid path", path.display()),
            )
        })
    }

    fn apply_setattr(path: &Path, request: &SetAttrRequest) -> Result<(), PosixError> {
        let c_path = Self::cstring_from_path(path)?;

        if let Some(mode) = request.mode {
            let result = unsafe { libc::chmod(c_path.as_ptr(), mode) };
            if result < 0 {
                return Err(PosixError::last_error(format!(
                    "{}: chmod failed",
                    path.display()
                )));
            }
        }

        if request.uid.is_some() || request.gid.is_some() {
            let uid = request.uid.map(|u| u as libc::uid_t).unwrap_or(u32::MAX);
            let gid = request.gid.map(|g| g as libc::gid_t).unwrap_or(u32::MAX);
            let result = unsafe {
                libc::fchownat(
                    libc::AT_FDCWD,
                    c_path.as_ptr(),
                    uid,
                    gid,
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if result < 0 {
                return Err(PosixError::last_error(format!(
                    "{}: chown failed",
                    path.display()
                )));
            }
        }

        if let Some(size) = request.size {
            let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC) };
            if fd < 0 {
                return Err(PosixError::last_error(format!(
                    "{}: open failed for truncate",
                    path.display()
                )));
            }

            let result = unsafe { libc::ftruncate(fd, size as libc::off_t) };
            unsafe { libc::close(fd) };
            if result < 0 {
                return Err(PosixError::last_error(format!(
                    "{}: truncate failed",
                    path.display()
                )));
            }
        }

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
                    libc::AT_FDCWD,
                    c_path.as_ptr(),
                    times.as_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if result < 0 {
                return Err(PosixError::last_error(format!(
                    "{}: utimensat failed",
                    path.display()
                )));
            }
        }

        Ok(())
    }
}

impl FuseHandler<PathBuf> for NueFs {
    fn get_inner(&self) -> &dyn FuseHandler<PathBuf> {
        &self.inner
    }

    fn lookup(
        &self,
        _req: &RequestInfo,
        parent_id: PathBuf,
        name: &OsStr,
    ) -> FuseResult<FileAttribute> {
        let child_path = Self::join_child(&parent_id, name);
        let child_rel = Self::to_rel_string(&child_path);
        debug!(parent = %Self::display_path(&parent_id), name = %name.to_string_lossy(), path = %child_rel, "FUSE lookup");
        self.get_file_attr(&child_rel)
            .map_err(|_| Self::file_not_found(&child_path))
    }

    fn getattr(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        _file_handle: Option<BorrowedFileHandle<'_>>,
    ) -> FuseResult<FileAttribute> {
        let rel = Self::to_rel_string(&file_id);
        debug!(path = %Self::display_path(&file_id), "FUSE getattr");
        self.get_file_attr(&rel)
            .map_err(|_| Self::file_not_found(&file_id))
    }

    fn readdir(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        _file_handle: BorrowedFileHandle<'_>,
    ) -> FuseResult<Vec<(OsString, FileKind)>> {
        let rel_path = Self::to_rel_string(&file_id);
        debug!(path = %Self::display_path(&file_id), "FUSE readdir");

        let plan = self.manifest.read().readdir_plan(&rel_path);
        let base = Self::read_dir_children(&plan.io_dir);
        let children = Self::merge_children(base, plan.manifest_children);

        let mut entries: Vec<(OsString, FileKind)> = Vec::with_capacity(children.len() + 2);
        entries.push((".".into(), FileKind::Directory));
        entries.push(("..".into(), FileKind::Directory));

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
        debug!(path = %Self::display_path(&file_id), "FUSE readdirplus");

        let plan = self.manifest.read().readdir_plan(&rel_path);
        let base = Self::read_dir_children(&plan.io_dir);
        let children = Self::merge_children(base, plan.manifest_children);

        let mut entries: Vec<(OsString, FileAttribute)> = Vec::new();

        if let Ok(attr) = self.get_file_attr(&rel_path) {
            entries.push((".".into(), attr));
        }

        let parent_rel = Self::to_rel_string(&Self::parent_path(&file_id));
        if let Ok(attr) = self.get_file_attr(&parent_rel) {
            entries.push(("..".into(), attr));
        }

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
        let rel_path = Self::to_rel_string(&file_id);
        debug!(path = %Self::display_path(&file_id), ?flags, "FUSE open");

        let resolved = self.resolve_paths(&rel_path);
        let fd = unix_fs::open(&resolved.io_path, flags | OpenFlags::CLOSE_ON_EXEC)?;
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
        let child_rel = Self::to_rel_string(&child_path);
        debug!(parent = %Self::display_path(&parent_id), name = %name.to_string_lossy(), mode, "FUSE create");

        let target = self.create_target(&rel_parent);
        let io_path = target.io_dir.join(name);
        let display_path = target.display_dir.join(name);

        let (fd, attr) = unix_fs::create(&io_path, mode, umask, flags | OpenFlags::CLOSE_ON_EXEC)?;
        let handle = OwnedFileHandle::from_owned_fd(fd).ok_or_else(Self::bad_file_handle)?;

        self.manifest
            .write()
            .add_entry_with_backend(&child_rel, display_path, false);

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
        let child_rel = Self::to_rel_string(&child_path);
        debug!(parent = %Self::display_path(&parent_id), name = %name.to_string_lossy(), mode, "FUSE mkdir");

        let target = self.create_target(&rel_parent);
        let io_path = target.io_dir.join(name);
        let display_path = target.display_dir.join(name);

        let attr = unix_fs::mkdir(&io_path, mode, umask)?;
        self.manifest
            .write()
            .add_entry_with_backend(&child_rel, display_path, true);
        Ok(self.with_ttl(attr))
    }

    fn readlink(&self, _req: &RequestInfo, file_id: PathBuf) -> FuseResult<Vec<u8>> {
        let rel_path = Self::to_rel_string(&file_id);
        debug!(path = %Self::display_path(&file_id), "FUSE readlink");

        let resolved = self.resolve_paths(&rel_path);
        unix_fs::readlink(&resolved.io_path)
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
        debug!(parent = %Self::display_path(&parent_id), name = %link_name.to_string_lossy(), target = %target.display(), "FUSE symlink");

        let target_dir = self.create_target(&parent_rel);
        let io_path = target_dir.io_dir.join(link_name);
        let display_path = target_dir.display_dir.join(link_name);

        let attr = unix_fs::symlink(&io_path, target)?;
        self.manifest
            .write()
            .add_entry_with_backend(&child_rel, display_path, false);
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
        debug!(old = %Self::display_path(&file_id), newparent = %Self::display_path(&newparent), newname = %newname.to_string_lossy(), "FUSE link");

        let old_paths = self.resolve_paths(&old_rel);
        let target_dir = self.create_target(&newparent_rel);
        let new_io = target_dir.io_dir.join(newname);
        let new_display = target_dir.display_dir.join(newname);

        std::fs::hard_link(&old_paths.io_path, &new_io).map_err(Self::map_std_io_error)?;
        let attr = unix_fs::lookup(&new_io)?;
        self.manifest
            .write()
            .add_entry_with_backend(&new_rel, new_display, false);
        Ok(self.with_ttl(attr))
    }

    fn unlink(&self, _req: &RequestInfo, parent_id: PathBuf, name: &OsStr) -> FuseResult<()> {
        let child_path = Self::join_child(&parent_id, name);
        let child_rel = Self::to_rel_string(&child_path);
        debug!(parent = %Self::display_path(&parent_id), name = %name.to_string_lossy(), path = %child_rel, "FUSE unlink");

        let resolved = self.resolve_paths(&child_rel);
        unix_fs::unlink(&resolved.io_path)?;
        self.manifest.write().remove_entry(&child_rel);
        Ok(())
    }

    fn rmdir(&self, _req: &RequestInfo, parent_id: PathBuf, name: &OsStr) -> FuseResult<()> {
        let child_path = Self::join_child(&parent_id, name);
        let child_rel = Self::to_rel_string(&child_path);
        debug!(parent = %Self::display_path(&parent_id), name = %name.to_string_lossy(), "FUSE rmdir");

        let resolved = self.resolve_paths(&child_rel);
        unix_fs::rmdir(&resolved.io_path)?;
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
        debug!(old = %Self::display_path(&old_path), new = %Self::display_path(&new_path), ?flags, "FUSE rename");

        let old_paths = self.resolve_paths(&old_rel);
        let target_dir = self.create_target(&newparent_rel);
        let new_io = target_dir.io_dir.join(newname);
        let new_display = target_dir.display_dir.join(newname);

        unix_fs::rename(&old_paths.io_path, &new_io, flags)?;
        self.manifest.write().rename_entry_with_backend(
            &old_rel,
            &new_rel,
            &old_paths.display_path,
            &new_display,
        );
        Ok(())
    }

    fn setattr(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        request: SetAttrRequest,
    ) -> FuseResult<FileAttribute> {
        let rel_path = Self::to_rel_string(&file_id);
        debug!(path = %Self::display_path(&file_id), "FUSE setattr");

        let resolved = self.resolve_paths(&rel_path);
        Self::apply_setattr(&resolved.io_path, &request)?;

        let attr = unix_fs::lookup(&resolved.io_path)?;
        Ok(self.with_ttl(attr))
    }
}
