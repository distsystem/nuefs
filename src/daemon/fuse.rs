use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use easy_fuser::prelude::*;
use easy_fuser::templates::fd_handler_helper::FdHandlerHelper;
use easy_fuser::templates::DefaultFuseHandler;
use easy_fuser::types::errors::{ErrorKind, PosixError};
use easy_fuser::unix_fs;
use parking_lot::RwLock;
use rustix::fd::OwnedFd;
use tracing::debug;

use super::manager::{DualPath, Manifest};

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

    fn resolve_dual(&self, rel_path: &str) -> DualPath {
        self.manifest.read().resolve_paths(rel_path)
    }

    fn get_file_attr(&self, rel_path: &str) -> FuseResult<FileAttribute> {
        let resolved = self.resolve_dual(rel_path);
        unix_fs::lookup(&resolved.io).map(|a| self.with_ttl(a))
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

    fn map_rustix_error(e: rustix::io::Errno, context: &str) -> PosixError {
        PosixError::new(
            match e {
                rustix::io::Errno::NOENT => ErrorKind::FileNotFound,
                rustix::io::Errno::ACCESS | rustix::io::Errno::PERM => ErrorKind::PermissionDenied,
                _ => ErrorKind::InputOutputError,
            },
            format!("{context}: {e}"),
        )
    }

    fn apply_setattr(path: &Path, request: &SetAttrRequest) -> Result<(), PosixError> {
        if let Some(mode) = request.mode {
            rustix::fs::chmodat(rustix::fs::CWD, path, rustix::fs::Mode::from_raw_mode(mode), rustix::fs::AtFlags::empty())
                .map_err(|e| Self::map_rustix_error(e, &format!("{}: chmod", path.display())))?;
        }

        if request.uid.is_some() || request.gid.is_some() {
            let uid = request.uid.map(|u| rustix::fs::Uid::from_raw(u));
            let gid = request.gid.map(|g| rustix::fs::Gid::from_raw(g));
            rustix::fs::chownat(rustix::fs::CWD, path, uid, gid, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)
                .map_err(|e| Self::map_rustix_error(e, &format!("{}: chown", path.display())))?;
        }

        if let Some(size) = request.size {
            let fd: OwnedFd = rustix::fs::open(path, rustix::fs::OFlags::WRONLY | rustix::fs::OFlags::CLOEXEC, rustix::fs::Mode::empty())
                .map_err(|e| Self::map_rustix_error(e, &format!("{}: open for truncate", path.display())))?;
            rustix::fs::ftruncate(&fd, size)
                .map_err(|e| Self::map_rustix_error(e, &format!("{}: truncate", path.display())))?;
        }

        if request.atime.is_some() || request.mtime.is_some() {
            let to_timespec = |t: Option<fuser::TimeOrNow>| -> rustix::fs::Timespec {
                match t {
                    Some(fuser::TimeOrNow::Now) => rustix::fs::Timespec { tv_sec: 0, tv_nsec: rustix::fs::UTIME_NOW },
                    Some(fuser::TimeOrNow::SpecificTime(st)) => {
                        let d = st.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                        rustix::fs::Timespec { tv_sec: d.as_secs() as rustix::fs::Secs, tv_nsec: d.subsec_nanos() as rustix::fs::Nsecs }
                    }
                    None => rustix::fs::Timespec { tv_sec: 0, tv_nsec: rustix::fs::UTIME_OMIT },
                }
            };

            let times = rustix::fs::Timestamps {
                last_access: to_timespec(request.atime),
                last_modification: to_timespec(request.mtime),
            };
            rustix::fs::utimensat(rustix::fs::CWD, path, &times, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)
                .map_err(|e| Self::map_rustix_error(e, &format!("{}: utimensat", path.display())))?;
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
        let parent_rel = Self::to_rel_string(&Self::parent_path(&file_id));
        debug!(path = %Self::display_path(&file_id), "FUSE readdirplus");

        // Single lock: resolve all paths and merge children.
        let manifest = self.manifest.read();
        let plan = manifest.readdir_plan(&rel_path);
        let dot_io = manifest.resolve_paths(&rel_path).io;
        let dotdot_io = manifest.resolve_paths(&parent_rel).io;

        let base = Self::read_dir_children(&plan.io_dir);
        let children = Self::merge_children(base, plan.manifest_children);

        let child_ios: Vec<(String, PathBuf)> = children
            .into_iter()
            .map(|(name, _)| {
                let child_rel =
                    Self::to_rel_string(&Self::join_child(&file_id, OsStr::new(&name)));
                let io = manifest.resolve_paths(&child_rel).io;
                (name, io)
            })
            .collect();
        drop(manifest);

        let mut entries: Vec<(OsString, FileAttribute)> = Vec::new();

        if let Ok(attr) = unix_fs::lookup(&dot_io).map(|a| self.with_ttl(a)) {
            entries.push((".".into(), attr));
        }
        if let Ok(attr) = unix_fs::lookup(&dotdot_io).map(|a| self.with_ttl(a)) {
            entries.push(("..".into(), attr));
        }
        for (name, io) in child_ios {
            if let Ok(attr) = unix_fs::lookup(&io).map(|a| self.with_ttl(a)) {
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

        let resolved = self.resolve_dual(&rel_path);
        let fd = unix_fs::open(&resolved.io, flags | OpenFlags::CLOSE_ON_EXEC)?;
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

        let target = self.resolve_dual(&rel_parent);
        let io_path = target.io.join(name);
        let display_path = target.display.join(name);

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

        let target = self.resolve_dual(&rel_parent);
        let io_path = target.io.join(name);
        let display_path = target.display.join(name);

        let attr = unix_fs::mkdir(&io_path, mode, umask)?;
        self.manifest
            .write()
            .add_entry_with_backend(&child_rel, display_path, true);
        Ok(self.with_ttl(attr))
    }

    fn readlink(&self, _req: &RequestInfo, file_id: PathBuf) -> FuseResult<Vec<u8>> {
        let rel_path = Self::to_rel_string(&file_id);
        debug!(path = %Self::display_path(&file_id), "FUSE readlink");

        let resolved = self.resolve_dual(&rel_path);
        unix_fs::readlink(&resolved.io)
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

        let target_dir = self.resolve_dual(&parent_rel);
        let io_path = target_dir.io.join(link_name);
        let display_path = target_dir.display.join(link_name);

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

        let old_paths = self.resolve_dual(&old_rel);
        let target_dir = self.resolve_dual(&newparent_rel);
        let new_io = target_dir.io.join(newname);
        let new_display = target_dir.display.join(newname);

        std::fs::hard_link(&old_paths.io, &new_io).map_err(Self::map_std_io_error)?;
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

        let resolved = self.resolve_dual(&child_rel);
        unix_fs::unlink(&resolved.io)?;
        self.manifest.write().remove_entry(&child_rel);
        Ok(())
    }

    fn rmdir(&self, _req: &RequestInfo, parent_id: PathBuf, name: &OsStr) -> FuseResult<()> {
        let child_path = Self::join_child(&parent_id, name);
        let child_rel = Self::to_rel_string(&child_path);
        debug!(parent = %Self::display_path(&parent_id), name = %name.to_string_lossy(), "FUSE rmdir");

        let resolved = self.resolve_dual(&child_rel);
        unix_fs::rmdir(&resolved.io)?;
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

        let old_paths = self.resolve_dual(&old_rel);
        let target_dir = self.resolve_dual(&newparent_rel);
        let new_io = target_dir.io.join(newname);
        let new_display = target_dir.display.join(newname);

        unix_fs::rename(&old_paths.io, &new_io, flags)?;
        self.manifest.write().rename_entry_with_backend(
            &old_rel,
            &new_rel,
            &old_paths.display,
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

        let resolved = self.resolve_dual(&rel_path);
        Self::apply_setattr(&resolved.io, &request)?;

        let attr = unix_fs::lookup(&resolved.io)?;
        Ok(self.with_ttl(attr))
    }
}
