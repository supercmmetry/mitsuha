use std::collections::HashMap;

use async_trait::async_trait;
use mitsuha_core_types::{kernel::StorageSpec, storage::StorageCapability};
use mitsuha_filesystem::{NativeFileLease, NativeFileMetadata};
use serde::{Deserialize, Serialize};

use crate::{errors::Error, selector::Label, types, unsupported_op};

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StorageLocality {
    Solid { cache_name: Option<String> },
    Cache { ttl: u64 },
}

impl StorageLocality {
    pub fn is_cache(&self) -> bool {
        match self {
            Self::Cache { .. } => true,
            _ => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StorageKind {
    Memory,
    Local,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageClass {
    pub kind: StorageKind,
    pub locality: StorageLocality,
    pub name: String,
    pub labels: Vec<Label>,
    pub extensions: HashMap<String, String>,
}

impl StorageClass {
    pub fn get_extension_property(&self, key: &str) -> types::Result<String> {
        self.extensions
            .get(key)
            .ok_or(Error::UnknownWithMsgOnly {
                message: format!(
                    "extension '{}' was not found in storage class '{}'",
                    key, self.name
                ),
            })
            .map(|x| x.clone())
    }
}

#[async_trait]
pub trait Storage: FileSystem + GarbageCollectable + Send + Sync {
    async fn store(&self, spec: StorageSpec) -> types::Result<()>;

    async fn load(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>>;

    async fn exists(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<bool>;

    async fn persist(
        &self,
        handle: String,
        time: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()>;

    async fn clear(&self, handle: String, extensions: HashMap<String, String>)
        -> types::Result<()>;

    async fn size(&self) -> types::Result<usize>;

    async fn capabilities(
        &self,
        _handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<Vec<StorageCapability>> {
        Ok(vec![])
    }
}

#[async_trait]
pub trait GarbageCollectable: Send + Sync {
    async fn garbage_collect(&self) -> types::Result<Vec<String>>;
}

#[async_trait]
pub trait FileSystem {
    #[allow(unused_variables)]
    async fn store_file_part(
        &self,
        handle: String,
        part_index: u64,
        part_size: u64,
        ttl: u64,
        data: Vec<u8>,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        Err(unsupported_op!("store_file_part"))
    }

    #[allow(unused_variables)]
    async fn load_file_part(
        &self,
        handle: String,
        part_index: u64,
        part_size: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>> {
        Err(unsupported_op!("load_file_part"))
    }

    #[allow(unused_variables)]
    async fn get_file_part_count(
        &self,
        handle: String,
        part_size: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<u64> {
        Err(unsupported_op!("get_file_part_count"))
    }

    #[allow(unused_variables)]
    async fn get_metadata(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<NativeFileMetadata> {
        Err(unsupported_op!("get_metadata"))
    }

    #[allow(unused_variables)]
    async fn set_metadata(
        &self,
        handle: String,
        metadata: NativeFileMetadata,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        Err(unsupported_op!("set_metadata"))
    }

    #[allow(unused_variables)]
    async fn path_exists(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<bool> {
        Err(unsupported_op!("path_exists"))
    }

    #[allow(unused_variables)]
    async fn list(
        &self,
        handle: String,
        page_index: u64,
        page_size: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<String>> {
        Err(unsupported_op!("list"))
    }

    #[allow(unused_variables)]
    async fn add_list_item(
        &self,
        handle: String,
        item: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        Err(unsupported_op!("add_list_item"))
    }

    #[allow(unused_variables)]
    async fn remove_list_item(
        &self,
        handle: String,
        item: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        Err(unsupported_op!("remove_list_item"))
    }

    #[allow(unused_variables)]
    async fn truncate(
        &self,
        handle: String,
        len: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        Err(unsupported_op!("truncate"))
    }

    #[allow(unused_variables)]
    async fn acquire_lease(
        &self,
        handle: String,
        lease: NativeFileLease,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        Err(unsupported_op!("acquire_lease"))
    }

    #[allow(unused_variables)]
    async fn renew_lease(
        &self,
        handle: String,
        lease_id: String,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        Err(unsupported_op!("renew_lease"))
    }

    #[allow(unused_variables)]
    async fn release_lease(
        &self,
        handle: String,
        lease_id: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        Err(unsupported_op!("release_lease"))
    }

    #[allow(unused_variables)]
    async fn copy_path(
        &self,
        source_handle: String,
        destination_handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        Err(unsupported_op!("copy_path"))
    }

    #[allow(unused_variables)]
    async fn move_path(
        &self,
        source_handle: String,
        destination_handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        Err(unsupported_op!("move_path"))
    }

    #[allow(unused_variables)]
    async fn delete_path(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        Err(unsupported_op!("delete_path"))
    }

    #[allow(unused_variables)]
    async fn get_capabilities(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<StorageCapability>> {
        Err(unsupported_op!("get_capabilities"))
    }

    #[allow(unused_variables)]
    async fn get_storage_class(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<String> {
        Err(unsupported_op!("get_storage_class"))
    }
}
