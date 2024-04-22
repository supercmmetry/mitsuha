use mitsuha_filesystem::async_fs::AsyncNativeFileSystem;
use mitsuha_filesystem::async_io::AsyncFile;
use mitsuha_filesystem::util::PathExt;
use mitsuha_filesystem::{AsyncFileSystem, NativeFileType};
use path_absolutize::Absolutize;
use std::any::Any;
use std::io::{IoSlice, IoSliceMut, SeekFrom};
use std::ops::Deref;
use std::os::fd::BorrowedFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use wasi_common::file::{Advice, FdFlags, FileType, Filestat};
use wasi_common::{Error, ErrorExt, SystemTimeSpec, WasiFile};

#[derive(Debug, Clone)]
pub struct OpenOptions {
    pub(crate) read: bool,
    pub(crate) write: bool,
    pub(crate) append: bool,
    pub(crate) truncate: bool,
    pub(crate) create: bool,
    pub(crate) create_new: bool,
    pub(crate) dir_required: bool,
    pub(crate) maybe_dir: bool,
    pub(crate) readdir_required: bool,
    pub(crate) fdflags: FdFlags,
}

impl OpenOptions {
    pub const fn new() -> Self {
        Self {
            read: false,
            write: false,
            append: false,
            truncate: false,
            create: false,
            create_new: false,
            dir_required: false,
            maybe_dir: false,
            readdir_required: false,
            fdflags: FdFlags::empty(),
        }
    }
}

pub struct File {
    fs: Arc<AsyncNativeFileSystem>,
    file: Arc<RwLock<AsyncFile<AsyncNativeFileSystem>>>,
    path: PathBuf,
    fdflags: Arc<RwLock<FdFlags>>,
}

impl File {
    pub async fn new(
        fs: AsyncNativeFileSystem,
        path: PathBuf,
        open_options: OpenOptions,
    ) -> Result<Self, Error> {
        let absolute_cow_path = path
            .absolutize_virtually("/")
            .map_err(|e| Error::trap(e.into()))?;
        let absolute_path = absolute_cow_path.deref();

        let obj = Self {
            fs: Arc::new(fs.clone()),
            file: Arc::new(RwLock::new(AsyncFile::new(
                fs,
                absolute_path.to_string().map_err(|e| Error::trap(e))?,
            ))),
            path: absolute_path.to_path_buf(),
            fdflags: Arc::new(RwLock::new(FdFlags::empty())),
        };

        obj.open(open_options).await?;

        Ok(obj)
    }

    async fn open(&self, open_options: OpenOptions) -> Result<(), Error> {
        if open_options.create {
            if !self
                .fs
                .exists(self.path.as_path())
                .await
                .map_err(|e| Error::trap(e))?
            {
                self.fs
                    .create_empty_file(self.path.as_path())
                    .await
                    .map_err(|e| Error::trap(e))?;
            }
        } else if open_options.create_new {
            self.fs
                .create_empty_file(self.path.as_path())
                .await
                .map_err(|e| Error::trap(e))?;
        }

        if open_options.truncate {
            self.fs
                .truncate(self.path.as_path(), 0u64)
                .await
                .map_err(|e| Error::trap(e))?;
        }

        if open_options.append {
            self.file.write().await.seek(SeekFrom::End(0)).await?;
        }

        *self.fdflags.write().await = open_options.fdflags;

        Ok(())
    }

    fn conv_filetype(f: NativeFileType) -> FileType {
        match f {
            NativeFileType::File => FileType::RegularFile,
            NativeFileType::Dir => FileType::Directory,
            NativeFileType::Other => FileType::Unknown,
        }
    }

    async fn get_filetype_(&self) -> Result<FileType, Error> {
        let path = self.path.as_path();

        match self.fs.get_metadata(path).await {
            Ok(metadata) => {
                let file_type = Self::conv_filetype(metadata.file_type);

                Ok(file_type)
            }
            Err(e) => Err(Error::trap(e)),
        }
    }

    async fn get_filestat_(&self) -> Result<Filestat, Error> {
        let path = self.path.as_path();

        let size = self
            .fs
            .get_file_size(path)
            .await
            .map_err(|e| Error::trap(e))?;

        match self.fs.get_metadata(path).await {
            Ok(metadata) => {
                let file_stat = Filestat {
                    device_id: 0,
                    inode: 0,
                    filetype: Self::conv_filetype(metadata.file_type),
                    nlink: 0,
                    size,
                    atim: None,
                    mtim: None,
                    ctim: None,
                };

                Ok(file_stat)
            }
            Err(e) => Err(Error::trap(e)),
        }
    }

    async fn set_len(&self, len: u64) -> Result<(), Error> {
        self.fs
            .truncate(self.path.as_path(), len)
            .await
            .map_err(|e| Error::trap(e))
    }

    async fn set_fdflags_(&self, flags: FdFlags) -> Result<(), Error> {
        *self.fdflags.write().await = flags;

        if flags.intersects(FdFlags::APPEND) {
            self.file
                .write()
                .await
                .seek(SeekFrom::End(0))
                .await
                .map_err(|e| Error::trap(e.into()))?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl WasiFile for File {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn get_filetype(&self) -> Result<FileType, Error> {
        self.get_filetype_().await
    }

    fn pollable(&self) -> Option<BorrowedFd> {
        None
    }

    fn isatty(&self) -> bool {
        // TODO: Decide whether this is required
        false
    }

    async fn datasync(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn sync(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn get_fdflags(&self) -> Result<FdFlags, Error> {
        let flags = *self.fdflags.read().await;

        Ok(flags)
    }

    async fn set_fdflags(&mut self, flags: FdFlags) -> Result<(), Error> {
        if flags.intersects(FdFlags::DSYNC | FdFlags::SYNC | FdFlags::RSYNC) {
            return Err(Error::invalid_argument().context("cannot set DSYNC, SYNC, or RSYNC flag"));
        }

        self.set_fdflags_(flags).await
    }

    async fn get_filestat(&self) -> Result<Filestat, Error> {
        self.get_filestat_().await
    }

    async fn set_filestat_size(&self, size: u64) -> Result<(), Error> {
        self.set_len(size).await
    }

    async fn advise(&self, _offset: u64, _len: u64, _advice: Advice) -> Result<(), Error> {
        Ok(())
    }

    async fn set_times(
        &self,
        _atime: Option<SystemTimeSpec>,
        _mtime: Option<SystemTimeSpec>,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn read_vectored<'a>(&self, bufs: &mut [IoSliceMut<'a>]) -> Result<u64, Error> {
        let mut file = self.file.write().await;

        let mut bytes_read = 0u64;
        for buf in bufs {
            if !buf.is_empty() {
                bytes_read += file.read(buf).await? as u64;
            }
        }

        Ok(bytes_read)
    }

    async fn read_vectored_at<'a>(
        &self,
        bufs: &mut [IoSliceMut<'a>],
        offset: u64,
    ) -> Result<u64, Error> {
        let mut file = self.file.write().await;

        file.seek(SeekFrom::Start(offset)).await?;

        self.read_vectored(bufs).await
    }

    async fn write_vectored<'a>(&self, bufs: &[IoSlice<'a>]) -> Result<u64, Error> {
        let mut file = self.file.write().await;

        let mut bytes_written = 0u64;
        for buf in bufs {
            bytes_written += buf.len() as u64;

            if !buf.is_empty() {
                file.write(buf).await?;
            }
        }

        file.flush().await?;

        Ok(bytes_written)
    }

    async fn write_vectored_at<'a>(&self, bufs: &[IoSlice<'a>], offset: u64) -> Result<u64, Error> {
        let mut file = self.file.write().await;

        file.seek(SeekFrom::Start(offset)).await?;

        self.write_vectored(bufs).await
    }

    async fn seek(&self, pos: SeekFrom) -> Result<u64, Error> {
        let offset = self.file.write().await.seek(pos).await?;

        Ok(offset)
    }

    async fn peek(&self, buf: &mut [u8]) -> Result<u64, Error> {
        let mut file = self.file.write().await;

        let bytes_read = file.read(buf).await?;

        file.seek(SeekFrom::Current(-(bytes_read as i64))).await?;

        Ok(bytes_read as u64)
    }

    fn num_ready_bytes(&self) -> Result<u64, Error> {
        // TODO: Check whether we really need to implement this.
        Ok(0)
    }
}
