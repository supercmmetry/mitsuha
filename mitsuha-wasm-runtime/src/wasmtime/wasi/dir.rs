use std::any::Any;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use async_trait::async_trait;
use mitsuha_filesystem::{AsyncFileSystem, NativeFileType};
use mitsuha_filesystem::async_fs::AsyncNativeFileSystem;
use path_absolutize::Absolutize;
use wasi_common::dir::{OpenResult, ReaddirCursor, ReaddirEntity};
use wasi_common::file::{FdFlags, Filestat, OFlags};
use wasi_common::{Error, ErrorExt, SystemTimeSpec, WasiDir};
use crate::wasmtime::wasi::file::{File, OpenOptions};

#[derive(Clone)]
pub struct Dir {
    fs: Arc<AsyncNativeFileSystem>,
    path: PathBuf,
}

impl Dir {
    pub fn new(fs: Arc<AsyncNativeFileSystem>, path: &str) -> Self {
        Self {
            fs,
            path: Path::new(path).to_path_buf(),
        }
    }
}

#[async_trait]
impl WasiDir for Dir {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn open_file(&self, _symlink_follow: bool, path: &str, oflags: OFlags, read: bool, write: bool, fdflags: FdFlags) -> Result<OpenResult, Error> {
        let mut opts = OpenOptions::new();

        if oflags.contains(OFlags::CREATE | OFlags::EXCLUSIVE) {
            opts.create_new = true;
            opts.write = true;
        } else if oflags.contains(OFlags::CREATE) {
            opts.create = true;
            opts.write = true;
        }

        if oflags.contains(OFlags::TRUNCATE) {
            opts.truncate = true;
        }
        if read {
            opts.read = true;
        }

        if write {
            opts.write = true;
        } else {
            opts.read = true;
        }

        if fdflags.contains(FdFlags::APPEND) {
            opts.append = true;
        }

        if fdflags.intersects(
            FdFlags::DSYNC | FdFlags::SYNC | FdFlags::RSYNC,
        ) {
            return Err(Error::not_supported().context("SYNC family of FdFlags"));
        }

        if oflags.contains(OFlags::DIRECTORY) {
            if oflags.contains(OFlags::CREATE)
                || oflags.contains(OFlags::EXCLUSIVE)
                || oflags.contains(OFlags::TRUNCATE)
            {
                return Err(Error::invalid_argument().context("directory oflags"));
            }
        }

        let path = Path::new(path);
        let exists = self.fs.exists(path).await.map_err(|e| Error::trap(e))?;

        let mut metadata = None;

        if exists {
            metadata = Some(self.fs.get_metadata(path).await.map_err(|e| Error::trap(e))?);
        }

        if metadata.as_ref().is_some_and(|m| m.file_type == NativeFileType::Dir) {
            Ok(OpenResult::Dir(Box::new(self.clone())))
        } else if metadata.is_some() && oflags.contains(OFlags::DIRECTORY) {
            Err(Error::not_dir().context("expected directory but got file"))
        } else if oflags.contains(OFlags::DIRECTORY) {
            self.fs.create_dir(path).await.map_err(|e| Error::trap(e))?;

            Ok(OpenResult::Dir(Box::new(self.clone())))
        } else {
            Ok(OpenResult::File(Box::new(File::new(self.fs.as_ref().clone(), path.to_path_buf(), opts).await?)))
        }
    }

    async fn create_dir(&self, path: &str) -> Result<(), Error> {
        let absolute_pathbuf = self.path.join(path);
        let absolute_cow_path = absolute_pathbuf.as_path()
            .absolutize_virtually("/")
            .map_err(|e| Error::trap(e.into()))?;

        let absolute_path = absolute_cow_path.deref();

        match self.fs.dir_exists(absolute_path).await {
            Ok(true) => return Err(Error::exist()),
            Err(e) => return Err(Error::trap(e)),
            _ => {}
        }


        if let Err(e) = self.fs.create_dir(absolute_path).await {
            return Err(Error::trap(e));
        }

        Ok(())
    }

    async fn readdir(&self, _cursor: ReaddirCursor) -> Result<Box<dyn Iterator<Item=Result<ReaddirEntity, Error>> + Send>, Error> {
        todo!()
    }

    async fn symlink(&self, _old_path: &str, _new_path: &str) -> Result<(), Error> {
        Err(Error::not_supported())
    }

    async fn remove_dir(&self, path: &str) -> Result<(), Error> {
        let absolute_pathbuf = self.path.join(path);
        let absolute_path = absolute_pathbuf.as_path();

        // TODO: distinguish b/w notfound and notdir
        match self.fs.dir_exists(absolute_path).await {
            Ok(false) => return Err(Error::not_found()),
            Err(e) => return Err(Error::trap(e)),
            _ => {}
        }

        if let Err(e) = self.fs.delete(absolute_path).await {
            return Err(Error::trap(e));
        }

        Ok(())
    }

    async fn unlink_file(&self, _path: &str) -> Result<(), Error> {
        Err(Error::not_supported())
    }

    async fn read_link(&self, _path: &str) -> Result<PathBuf, Error> {
        Err(Error::not_supported())
    }

    async fn get_filestat(&self) -> Result<Filestat, Error> {
        todo!()
    }

    async fn get_path_filestat(&self, _path: &str, _follow_symlinks: bool) -> Result<Filestat, Error> {
        todo!()
    }

    async fn rename(&self, _path: &str, _dest_dir: &dyn WasiDir, _dest_path: &str) -> Result<(), Error> {
        todo!()
    }

    async fn hard_link(&self, _path: &str, _target_dir: &dyn WasiDir, _target_path: &str) -> Result<(), Error> {
        Err(Error::not_supported())
    }

    async fn set_times(&self, _path: &str, _atime: Option<SystemTimeSpec>, _mtime: Option<SystemTimeSpec>, _follow_symlinks: bool) -> Result<(), Error> {
        todo!()
    }
}