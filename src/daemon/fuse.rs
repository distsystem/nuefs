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

use super::manager::Manifest;

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

    fn get_file_attr(&self, rel_path: &str) -> FuseResult<FileAttribute> {
        let io = self.resolve_io(rel_path);
        unix_fs::lookup(&io).map(|a| self.with_ttl(a))
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

        // First: filesystem children from each io_dir (first occurrence wins)
        for dir in io_dirs {
            for (name, is_dir) in Self::read_dir_children(dir) {
                if seen.insert(name.clone()) {
                    result.push((name, is_dir));
                }
            }
        }

        // Then: origin children (collapsed chains, non-physical dirs, etc.)
        for (name, is_dir) in origin_children {
            if seen.insert(name.clone()) {
                result.push((name, is_dir));
            }
        }

        result
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
        file_handle: Option<BorrowedFileHandle<'_>>,
    ) -> FuseResult<FileAttribute> {
        let rel = Self::to_rel_string(&file_id);
        debug!(path = %Self::display_path(&file_id), "FUSE getattr");

        if let Some(fh) = file_handle {
            if let Ok(attr) = unix_fs::getattr(fh.as_borrowed_fd()) {
                return Ok(self.with_ttl(attr));
            }
        }

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
        let children = Self::merge_multi_dir_children(&plan.io_dirs, plan.origin_children);

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

    fn open(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        flags: OpenFlags,
    ) -> FuseResult<(OwnedFileHandle, FUSEOpenResponseFlags)> {
        let rel_path = Self::to_rel_string(&file_id);
        debug!(path = %Self::display_path(&file_id), ?flags, "FUSE open");

        let io = self.resolve_io(&rel_path);
        let fd = unix_fs::open(&io, flags | OpenFlags::CLOSE_ON_EXEC)?;
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
        debug!(parent = %Self::display_path(&parent_id), name = %name.to_string_lossy(), mode, "FUSE create");

        let io_path = self.create_io(&parent_id, name);
        let (fd, attr) = unix_fs::create(&io_path, mode, umask, flags | OpenFlags::CLOSE_ON_EXEC)?;
        let handle = OwnedFileHandle::from_owned_fd(fd).ok_or_else(Self::bad_file_handle)?;

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
        debug!(parent = %Self::display_path(&parent_id), name = %name.to_string_lossy(), mode, "FUSE mkdir");

        let io_path = self.create_io(&parent_id, name);
        let attr = unix_fs::mkdir(&io_path, mode, umask)?;
        Ok(self.with_ttl(attr))
    }

    fn readlink(&self, _req: &RequestInfo, file_id: PathBuf) -> FuseResult<Vec<u8>> {
        let rel_path = Self::to_rel_string(&file_id);
        debug!(path = %Self::display_path(&file_id), "FUSE readlink");

        unix_fs::readlink(&self.resolve_io(&rel_path))
    }

    fn symlink(
        &self,
        _req: &RequestInfo,
        parent_id: PathBuf,
        link_name: &OsStr,
        target: &Path,
    ) -> FuseResult<FileAttribute> {
        debug!(parent = %Self::display_path(&parent_id), name = %link_name.to_string_lossy(), target = %target.display(), "FUSE symlink");

        let io_path = self.create_io(&parent_id, link_name);
        let attr = unix_fs::symlink(&io_path, target)?;
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
        debug!(old = %Self::display_path(&file_id), newparent = %Self::display_path(&newparent), newname = %newname.to_string_lossy(), "FUSE link");

        let old_io = self.resolve_io(&old_rel);
        let new_io = self.create_io(&newparent, newname);

        std::fs::hard_link(&old_io, &new_io).map_err(Self::map_std_io_error)?;
        let attr = unix_fs::lookup(&new_io)?;
        Ok(self.with_ttl(attr))
    }

    fn unlink(&self, _req: &RequestInfo, parent_id: PathBuf, name: &OsStr) -> FuseResult<()> {
        debug!(parent = %Self::display_path(&parent_id), name = %name.to_string_lossy(), "FUSE unlink");

        unix_fs::unlink(&self.resolve_child_io_cascade(&parent_id, name))?;
        Ok(())
    }

    fn rmdir(&self, _req: &RequestInfo, parent_id: PathBuf, name: &OsStr) -> FuseResult<()> {
        debug!(parent = %Self::display_path(&parent_id), name = %name.to_string_lossy(), "FUSE rmdir");

        unix_fs::rmdir(&self.resolve_child_io_cascade(&parent_id, name))?;
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
        debug!(old = %Self::display_path(&old_path), new = %Self::display_path(&new_path), ?flags, "FUSE rename");

        let manifest = self.manifest.read();
        let old_io = manifest.resolve_io(&old_rel);
        let new_io = if parent_id == newparent {
            old_io.parent().unwrap_or(&old_io).join(newname)
        } else {
            let newparent_rel = Self::to_rel_string(&newparent);
            manifest.resolve_io(&newparent_rel).join(newname)
        };
        drop(manifest);

        unix_fs::rename(&old_io, &new_io, flags)?;
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

        let io = self.resolve_io(&rel_path);
        Self::apply_setattr(&io, &request)?;

        let attr = unix_fs::lookup(&io)?;
        Ok(self.with_ttl(attr))
    }
}
