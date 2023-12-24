use std::{
    async_iter::AsyncIterator, collections::HashMap, future::poll_fn, path::Path, sync::Arc,
};

use async_trait::async_trait;
use mitsuha_core::{config, storage::Storage};
use mitsuha_core_types::kernel::AsyncKernel;
use mitsuha_filesystem::{
    async_fs::AsyncNativeFileSystemBuilder, constant::NativeFileSystemConstants, AsyncFileSystem,
};
use mitsuha_storage::UnifiedStorage;
use tokio::io::ReadBuf;

struct UnifiedStorageKernel {
    storage: Arc<Box<dyn Storage>>,
}

impl UnifiedStorageKernel {
    pub fn new(storage: Arc<Box<dyn Storage>>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl AsyncKernel for UnifiedStorageKernel {
    async fn run_job(&self, _spec: mitsuha_core_types::kernel::JobSpec) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_job_status(
        &self,
        _handle: String,
        _extensions: std::collections::HashMap<String, String>,
    ) -> anyhow::Result<mitsuha_core_types::kernel::JobStatus> {
        todo!()
    }

    async fn extend_job(
        &self,
        _handle: String,
        _ttl: u64,
        _extensions: std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn abort_job(
        &self,
        _handle: String,
        _extensions: std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn store_data(
        &self,
        spec: mitsuha_core_types::kernel::StorageSpec,
    ) -> anyhow::Result<()> {
        Ok(self.storage.store(spec).await?)
    }

    async fn load_data(
        &self,
        handle: String,
        extensions: std::collections::HashMap<String, String>,
    ) -> anyhow::Result<Vec<u8>> {
        Ok(self.storage.load(handle, extensions).await?)
    }

    async fn persist_data(
        &self,
        handle: String,
        ttl: u64,
        extensions: std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        Ok(self.storage.persist(handle, ttl, extensions).await?)
    }

    async fn clear_data(
        &self,
        handle: String,
        extensions: std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        Ok(self.storage.clear(handle, extensions).await?)
    }
}

async fn make_unified_storage(config: &config::storage::Storage) -> Arc<Box<dyn Storage>> {
    UnifiedStorage::new(&config).await.unwrap()
}

#[macro_export]
macro_rules! mnfs_test {
    ($name: ident, $config: expr, $ext: expr) => {
        #[tokio::test]
        #[serial]
        async fn $name() -> anyhow::Result<()> {
            __mnfs_macro_reset_fs().await;
            fs::$name($config, $ext).await?;
            Ok(())
        }
    };
}

#[macro_export]
macro_rules! mnfs_test_suite {
    ($config: expr, $extensions: expr) => {
        async fn __mnfs_macro_reset_fs_default() {
            _ = fs::reset_fs($config, $extensions).await;
        }

        mnfs_test_suite!($config, $extensions, __mnfs_macro_reset_fs_default);
    };
    ($config: expr, $extensions: expr, $reset_fn: ident) => {
        async fn __mnfs_macro_reset_fs() {
            $reset_fn().await;
        }

        mnfs_test!(test_rw_basic, $config, $extensions);

        mnfs_test!(test_rw_paged_uniform, $config, $extensions);

        mnfs_test!(test_rw_paged_non_uniform, $config, $extensions);

        mnfs_test!(test_rw_paged_one_byte, $config, $extensions);

        mnfs_test!(test_rw_paged_exact, $config, $extensions);

        mnfs_test!(test_rw_paged_large, $config, $extensions);

        mnfs_test!(test_list_paged_uniform, $config, $extensions);

        mnfs_test!(test_list_paged_non_uniform, $config, $extensions);

        mnfs_test!(test_list_paged_one_byte, $config, $extensions);

        mnfs_test!(test_list_paged_exact, $config, $extensions);

        mnfs_test!(test_list_paged_large, $config, $extensions);

        mnfs_test!(test_deletion_files_only_small, $config, $extensions);

        mnfs_test!(test_deletion_files_only_large, $config, $extensions);

        mnfs_test!(test_deletion_files_and_dir, $config, $extensions);
    };
}

pub async fn reset_fs(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    let inner_kernel = UnifiedStorageKernel::new(make_unified_storage(config).await);

    let kernel: Arc<Box<dyn AsyncKernel>> = Arc::new(Box::new(inner_kernel));

    let mut extensions = extensions.clone();

    extensions.insert(
        NativeFileSystemConstants::DirListPageSize.to_string(),
        128.to_string(),
    );

    let fs = AsyncNativeFileSystemBuilder::new(kernel)
        .with_extensions(&extensions)?
        .build();

    fs.delete(Path::new("/")).await?;

    Ok(())
}

pub async fn test_rw_basic(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    let inner_kernel = UnifiedStorageKernel::new(make_unified_storage(config).await);

    let kernel: Arc<Box<dyn AsyncKernel>> = Arc::new(Box::new(inner_kernel));

    let fs = AsyncNativeFileSystemBuilder::new(kernel)
        .with_extensions(extensions)?
        .build();

    let file_path = Path::new("/sample.txt");

    fs.create_dir(Path::new("/")).await?;
    fs.create_empty_file(file_path).await?;
    fs.write_to_offset(file_path, 0u64, vec![1u8; 32].as_slice())
        .await?;

    let mut buf = [0u8; 32];
    fs.read_from_offset(file_path, 0u64, &mut ReadBuf::new(&mut buf))
        .await?;

    for byte in buf {
        assert_eq!(byte, 1u8);
    }

    Ok(())
}

async fn rw_paged_test(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
    data_size: u64,
    page_size: u64,
) -> anyhow::Result<()> {
    let mut extensions = extensions.clone();
    let other_extensions = extensions.clone();

    extensions.insert(
        NativeFileSystemConstants::FilePartMaxSize.to_string(),
        page_size.to_string(),
    );

    let inner_kernel = UnifiedStorageKernel::new(make_unified_storage(config).await);

    let kernel: Arc<Box<dyn AsyncKernel>> = Arc::new(Box::new(inner_kernel));
    let fs = AsyncNativeFileSystemBuilder::new(kernel.clone())
        .with_extensions(&extensions)?
        .build();

    let file_path = Path::new("/sample.txt");

    fs.create_dir(Path::new("/")).await?;
    fs.create_empty_file(file_path).await?;
    fs.write_to_offset(file_path, 0u64, vec![1u8; data_size as usize].as_slice())
        .await?;

    let fs = AsyncNativeFileSystemBuilder::new(kernel.clone())
        .with_extensions(&other_extensions)?
        .build();

    let mut buf = vec![0u8; data_size as usize];

    fs.read_from_offset(file_path, 0u64, &mut ReadBuf::new(&mut buf))
        .await?;

    for byte in buf {
        assert_eq!(byte, 1u8);
    }

    Ok(())
}

pub async fn test_rw_paged_uniform(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    rw_paged_test(config, extensions, 128, 8).await
}

pub async fn test_rw_paged_non_uniform(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    rw_paged_test(config, extensions, 128, 7).await
}

pub async fn test_rw_paged_one_byte(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    rw_paged_test(config, extensions, 3, 1).await
}

pub async fn test_rw_paged_exact(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    rw_paged_test(config, extensions, 128, 128).await
}

pub async fn test_rw_paged_large(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    rw_paged_test(config, extensions, 128, 192).await
}

async fn list_paged_test(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
    list_size: u64,
    page_size: u64,
) -> anyhow::Result<()> {
    let mut extensions = extensions.clone();

    extensions.insert(
        NativeFileSystemConstants::DirListPageSize.to_string(),
        page_size.to_string(),
    );

    let inner_kernel = UnifiedStorageKernel::new(make_unified_storage(config).await);

    let kernel: Arc<Box<dyn AsyncKernel>> = Arc::new(Box::new(inner_kernel));
    let fs = AsyncNativeFileSystemBuilder::new(kernel.clone())
        .with_extensions(&extensions)?
        .build();

    fs.create_dir(Path::new("/")).await?;

    let mut expected_items = vec![];
    for file_index in 0..list_size {
        let file_path = format!("/sample{}.txt", file_index);
        expected_items.push(file_path.clone());

        fs.create_empty_file(Path::new(&file_path)).await?;
    }

    expected_items.sort();

    let mut index = 0usize;

    let mut iter = Box::pin(fs.list_dir(Path::new("/")).await?);

    while let Some(item) = poll_fn(|cx| iter.as_mut().poll_next(cx)).await {
        assert_eq!("/".to_string() + item?.as_str(), expected_items[index]);
        index += 1;
    }

    assert_ne!(index, 0usize);

    Ok(())
}

pub async fn test_list_paged_uniform(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    list_paged_test(config, extensions, 128, 8).await
}

pub async fn test_list_paged_non_uniform(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    list_paged_test(config, extensions, 128, 7).await
}

pub async fn test_list_paged_one_byte(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    list_paged_test(config, extensions, 3, 1).await
}

pub async fn test_list_paged_exact(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    list_paged_test(config, extensions, 128, 128).await
}

pub async fn test_list_paged_large(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    list_paged_test(config, extensions, 128, 192).await
}

async fn deletion_test(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
    file_size: usize,
    file_count: u64,
    dir_deletion: bool,
) -> anyhow::Result<()> {
    let mut extensions = extensions.clone();

    extensions.insert(
        NativeFileSystemConstants::DirListPageSize.to_string(),
        128.to_string(),
    );

    let inner_kernel = UnifiedStorageKernel::new(make_unified_storage(config).await);

    let kernel: Arc<Box<dyn AsyncKernel>> = Arc::new(Box::new(inner_kernel));
    let fs = AsyncNativeFileSystemBuilder::new(kernel.clone())
        .with_extensions(&extensions)?
        .build();

    let root_path = Path::new("/");

    fs.create_dir(root_path).await?;

    for file_index in 0..file_count {
        let file_path = format!("/sample{}.txt", file_index);

        let path = Path::new(&file_path);
        fs.create_empty_file(path).await?;

        fs.write_to_offset(path, 0, &vec![1u8; file_size]).await?;
    }

    if dir_deletion {
        let sub_dir = Path::new("/dir");

        fs.create_dir(sub_dir).await?;

        for file_index in 0..file_count {
            let file_path = format!("/dir/sample{}.txt", file_index);

            let path = Path::new(&file_path);
            fs.create_empty_file(path).await?;

            fs.write_to_offset(path, 0, &vec![1u8; file_size]).await?;
        }

        fs.delete(sub_dir).await?;

        assert!(fs.list_dir(sub_dir).await.is_err());

        let mut index = 0usize;

        let mut iter = Box::pin(fs.list_dir(root_path).await?);

        while let Some(_) = poll_fn(|cx| iter.as_mut().poll_next(cx)).await {
            index += 1;
        }

        assert_eq!(index, 100usize);
    }

    fs.delete(root_path).await?;

    assert!(fs.list_dir(root_path).await.is_err());

    Ok(())
}

pub async fn test_deletion_files_only_small(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    deletion_test(config, extensions, 32, 100, false).await
}

pub async fn test_deletion_files_only_large(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    deletion_test(config, extensions, 1048576, 10, false).await
}

pub async fn test_deletion_files_and_dir(
    config: &config::storage::Storage,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<()> {
    deletion_test(config, extensions, 32, 100, true).await
}
