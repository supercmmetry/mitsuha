use std::{collections::HashMap, num::ParseIntError, sync::Arc, time::Duration};

use async_trait::async_trait;
use mitsuha_core::{
    config,
    constants::StorageControlConstants,
    errors::Error,
    selector::Label,
    storage::{
        FileSystem, GarbageCollectable, Storage, StorageClass, StorageKind, StorageLocality,
    },
    types, unknown_err, unsupported_op,
};
use mitsuha_core_types::{kernel::StorageSpec, storage::StorageCapability};
use mitsuha_filesystem::{
    constant::NativeFileSystemConstants,
    event::{NativeFileSystemEvent, NativeFileSystemEventContext},
    NativeFileLease, NativeFileMetadata,
};

use crate::{local::LocalStorage, memory::MemoryStorage};

#[derive(Clone)]
pub struct UnifiedStorage {
    stores: Arc<HashMap<String, Arc<Box<dyn Storage>>>>,
    classes: Arc<HashMap<String, StorageClass>>,
}

#[async_trait]
impl Storage for UnifiedStorage {
    async fn store(&self, mut spec: StorageSpec) -> types::Result<()> {
        self.sanitize_handle(&mut spec.handle)?;

        if self.validate_mnfs_call(&spec.extensions)? {
            return self.process_mnfs_store_context(spec).await;
        }

        self.store_internal(spec).await
    }

    async fn load(
        &self,
        mut handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>> {
        self.sanitize_handle(&mut handle)?;

        if self.validate_mnfs_call(&extensions)? {
            return self.process_mnfs_load_context(handle, extensions).await
            .map_err(|e| unknown_err!(e));
        }

        self.load_internal(handle, extensions)
        .await.map_err(|e| unknown_err!(e))
    }

    async fn exists(
        &self,
        mut handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<bool> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage.exists(handle, extensions).await
    }

    async fn persist(
        &self,
        mut handle: String,
        time: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.sanitize_handle(&mut handle)?;

        if self.validate_mnfs_call(&extensions)? {
            return self
                .process_mnfs_persist_context(handle, time, extensions)
                .await;
        }

        self.persist_internal(handle, time, extensions).await
    }

    async fn clear(
        &self,
        mut handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.sanitize_handle(&mut handle)?;

        if self.validate_mnfs_call(&extensions)? {
            return self.process_mnfs_clear_context(handle, extensions).await;
        }

        self.clear_internal(handle, extensions).await
    }

    async fn size(&self) -> types::Result<usize> {
        let mut total_size = 0usize;

        for storage in self.stores.values() {
            total_size += storage.size().await?;
        }

        Ok(total_size)
    }

    async fn capabilities(
        &self,
        mut handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<StorageCapability>> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage.capabilities(handle, extensions).await
    }
}

#[async_trait]
impl FileSystem for UnifiedStorage {
    async fn store_file_part(
        &self,
        mut handle: String,
        part_index: u64,
        part_size: u64,
        ttl: u64,
        data: Vec<u8>,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage
            .store_file_part(handle, part_index, part_size, ttl, data, extensions)
            .await
    }

    async fn load_file_part(
        &self,
        mut handle: String,
        part_index: u64,
        part_size: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage
            .load_file_part(handle, part_index, part_size, extensions)
            .await
    }

    async fn get_file_part_count(
        &self,
        mut handle: String,
        part_size: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<u64> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage
            .get_file_part_count(handle, part_size, extensions)
            .await
    }

    async fn get_metadata(
        &self,
        mut handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<NativeFileMetadata> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage.get_metadata(handle, extensions).await
    }

    async fn set_metadata(
        &self,
        mut handle: String,
        metadata: NativeFileMetadata,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage
            .set_metadata(handle, metadata, ttl, extensions)
            .await
    }

    async fn path_exists(
        &self,
        mut handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<bool> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage.path_exists(handle, extensions).await
    }

    async fn list(
        &self,
        mut handle: String,
        page_index: u64,
        page_size: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<String>> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage
            .list(handle, page_index, page_size, extensions)
            .await
    }

    async fn add_list_item(
        &self,
        mut handle: String,
        item: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage.add_list_item(handle, item, extensions).await
    }

    async fn remove_list_item(
        &self,
        mut handle: String,
        item: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage.remove_list_item(handle, item, extensions).await
    }

    async fn truncate(
        &self,
        mut handle: String,
        len: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage.truncate(handle, len, extensions).await
    }

    async fn acquire_lease(
        &self,
        mut handle: String,
        lease: NativeFileLease,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage.acquire_lease(handle, lease, ttl, extensions).await
    }

    async fn renew_lease(
        &self,
        mut handle: String,
        lease_id: String,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage.renew_lease(handle, lease_id, ttl, extensions).await
    }

    async fn release_lease(
        &self,
        mut handle: String,
        lease_id: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage.release_lease(handle, lease_id, extensions).await
    }

    async fn copy_path(
        &self,
        mut source_handle: String,
        mut destination_handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.sanitize_handle(&mut source_handle)?;
        self.sanitize_handle(&mut destination_handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage
            .copy_path(source_handle, destination_handle, extensions)
            .await
    }

    async fn move_path(
        &self,
        mut source_handle: String,
        mut destination_handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.sanitize_handle(&mut source_handle)?;
        self.sanitize_handle(&mut destination_handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage
            .move_path(source_handle, destination_handle, extensions)
            .await
    }

    async fn delete_path(
        &self,
        mut handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        storage.delete_path(handle, extensions).await
    }

    async fn get_capabilities(
        &self,
        mut handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<StorageCapability>> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        let result = storage
            .get_capabilities(handle.clone(), extensions.clone())
            .await;
        if result.is_ok() {
            return result;
        }

        self.capabilities(handle, extensions).await
    }

    async fn get_storage_class(
        &self,
        mut handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<String> {
        self.sanitize_handle(&mut handle)?;

        let storage = self.get_storage_by_selector(&extensions)?;

        let result = storage.get_storage_class(handle, extensions.clone()).await;
        if result.is_ok() {
            return result;
        }

        self.get_solid_storage_name_by_selector(&extensions)
    }
}

#[async_trait]
impl GarbageCollectable for UnifiedStorage {
    async fn garbage_collect(&self) -> types::Result<Vec<String>> {
        let mut deleted_handles = vec![];

        for (name, collectable) in self.stores.iter() {
            let handles = collectable.garbage_collect().await?;

            if !self.classes.get(name).unwrap().locality.is_cache() {
                deleted_handles.extend(handles);
            }
        }

        Ok(deleted_handles)
    }
}

impl UnifiedStorage {
    pub fn new(config: &config::storage::Storage) -> types::Result<Arc<Box<dyn Storage>>> {
        let mut unified_storage = Self {
            stores: Default::default(),
            classes: Default::default(),
        };

        let mut stores = HashMap::new();
        let mut classes = HashMap::new();

        for storage_class in config.classes.iter() {
            let name = &storage_class.name;

            if stores.contains_key(name) || classes.contains_key(name) {
                return Err(Error::StorageInitFailed {
                    message: format!("duplicate storage class name '{}' was found during unified storage initialization.", name),
                    source: anyhow::anyhow!("")
                });
            }

            let storage_impl: Arc<Box<dyn Storage>> = match storage_class.kind {
                StorageKind::Memory => MemoryStorage::new()?,
                StorageKind::Local => LocalStorage::new(storage_class.clone())?,
            };

            let mut processed_storage_class = storage_class.clone();

            processed_storage_class.labels.push(Label {
                name: StorageControlConstants::StorageLabel.to_string(),
                value: storage_class.name.clone(),
            });

            classes.insert(name.clone(), processed_storage_class);

            stores.insert(name.clone(), storage_impl);
        }

        unified_storage.classes = Arc::new(classes);
        unified_storage.stores = Arc::new(stores);

        let output: Arc<Box<dyn Storage>> = Arc::new(Box::new(unified_storage));

        Self::start_gc(output.clone());

        Ok(output)
    }

    async fn store_internal(&self, spec: StorageSpec) -> types::Result<()> {
        let storage_name = self.get_solid_storage_name_by_selector(&spec.extensions)?;

        let class = self.classes.get(&storage_name).unwrap();
        let storage = self.stores.get(&storage_name).unwrap();

        let handle = spec.handle.clone();
        let extensions = spec.extensions.clone();

        storage.store(spec).await?;

        if let StorageLocality::Solid {
            cache_name: Some(cache_name),
        } = class.locality.clone()
        {
            let cache = self.stores.get(&cache_name).unwrap();
            cache.clear(handle, extensions).await?;
        }

        Ok(())
    }

    async fn load_internal(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>> {
        let storage_name = self.get_solid_storage_name_by_selector(&extensions)?;
        let class = self.classes.get(&storage_name).unwrap();

        if let StorageLocality::Solid {
            cache_name: Some(cache_name),
        } = class.locality.clone()
        {
            let cache = self.stores.get(&cache_name).unwrap();
            let result = cache.load(handle.clone(), extensions.clone()).await;

            match result {
                Ok(data) => return Ok(data),
                Err(_e) => {
                    // Log error
                }
            }
        }

        let storage = self.stores.get(&storage_name).unwrap();
        let data = storage.load(handle.clone(), extensions.clone()).await?;

        if let StorageLocality::Solid {
            cache_name: Some(cache_name),
        } = class.locality.clone()
        {
            let cache = self.stores.get(&cache_name).unwrap();
            let cache_class = self.classes.get(&cache_name).unwrap();

            match cache_class.locality {
                StorageLocality::Cache { ttl } => {
                    let spec = StorageSpec {
                        handle: handle.clone(),
                        ttl,
                        data: data.clone(),
                        extensions: HashMap::new(),
                    };

                    // TODO: spawn a different task for cache store operation
                    let result = cache.store(spec).await;

                    match result {
                        Err(_e) => {
                            // Log error
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        Ok(data)
    }

    async fn persist_internal(
        &self,
        handle: String,
        time: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let storage = self.get_storage_by_selector(&extensions)?;

        storage.persist(handle, time, extensions).await
    }

    async fn clear_internal(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let storage = self.get_storage_by_selector(&extensions)?;

        storage.clear(handle, extensions).await?;

        Ok(())
    }

    fn get_solid_storage_name_by_selector(
        &self,
        extensions: &HashMap<String, String>,
    ) -> types::Result<String> {
        if let Some(query) =
            extensions.get(&StorageControlConstants::StorageSelectorQuery.to_string())
        {
            let label: Label = serde_json::from_str(query.as_str()).map_err(|e| {
                Error::StorageOperationFailed {
                    message: format!("failed to parse storage selector."),
                    source: e.into(),
                }
            })?;

            for (name, class) in self.classes.iter() {
                match class.locality {
                    mitsuha_core::storage::StorageLocality::Solid { .. } => {
                        if class.labels.contains(&label) {
                            return Ok(name.clone());
                        }
                    }
                    mitsuha_core::storage::StorageLocality::Cache { .. } => continue,
                }
            }
        }

        return Err(Error::StorageOperationFailed {
            message: format!("failed to get a storage that satisfies the selector."),
            source: anyhow::anyhow!(""),
        });
    }

    fn get_storage_by_selector(
        &self,
        extensions: &HashMap<String, String>,
    ) -> types::Result<Arc<Box<dyn Storage>>> {
        let storage_name = self.get_solid_storage_name_by_selector(&extensions)?;
        let storage = self.stores.get(&storage_name).unwrap();

        Ok(storage.clone())
    }

    fn sanitize_handle(&self, handle: &mut String) -> types::Result<()> {
        if handle.contains("..") {
            return Err(unsupported_op!("handles containing '..' are not allowed"));
        }

        if handle.ends_with(".") || handle.contains("./") {
            return Err(unsupported_op!(
                "handles containing '.' as filename are not allowed"
            ));
        }

        if handle.ends_with("/") {
            handle.pop();
        }

        if !handle.starts_with("/") {
            *handle = format!("/{}", handle);
        }

        Ok(())
    }

    fn validate_mnfs_call(&self, extensions: &HashMap<String, String>) -> types::Result<bool> {
        if let Some("true") = extensions
            .get(&NativeFileSystemConstants::EnableFileSystemMode.to_string())
            .map(|x| x.as_str())
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn contains_mnfs_suffix(&self, handle: &String) -> bool {
        handle.contains(&NativeFileSystemConstants::MnfsSuffix.to_string())
    }

    async fn process_mnfs_store_context(&self, spec: StorageSpec) -> types::Result<()> {
        let ctx = NativeFileSystemEventContext::Store { spec };
        let event = ctx.get_event().map_err(|e| unknown_err!(e))?;

        let inner_spec = ctx.to_spec().map_err(|e| unknown_err!(e))?;

        if event.is_none() && self.contains_mnfs_suffix(&inner_spec.handle) {
            return Err(unknown_err!("could not get event from context"));
        } else if event.is_none() {
            return self.store_internal(inner_spec).await;
        }

        match event.unwrap() {
            NativeFileSystemEvent::AddDirListItem { handle, item } => {
                self.add_list_item(handle, item, inner_spec.extensions)
                    .await
            }
            NativeFileSystemEvent::StorePart {
                handle,
                part_index,
                part_size,
            } => {
                self.store_file_part(
                    handle,
                    part_index,
                    part_size,
                    inner_spec.ttl,
                    inner_spec.data,
                    inner_spec.extensions,
                )
                .await
            }
            NativeFileSystemEvent::SetMetadata { handle } => {
                let data = inner_spec.data;
                let value: musubi_api::types::Value = data
                    .try_into()
                    .map_err(|e: anyhow::Error| unknown_err!(e))?;
                let metadata: NativeFileMetadata =
                    musubi_api::types::from_value(&value).map_err(|e| unknown_err!(e))?;

                self.set_metadata(handle, metadata, inner_spec.ttl, inner_spec.extensions)
                    .await
            }
            NativeFileSystemEvent::AcquireLease { handle, lease, ttl } => {
                self.acquire_lease(handle, lease, ttl, inner_spec.extensions)
                    .await
            }
            x => Err(unsupported_op!(format!(
                "event {:?} is not supported within 'store' context",
                x
            ))),
        }
    }

    async fn process_mnfs_load_context(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>> {
        let ctx = NativeFileSystemEventContext::Load {
            handle,
            extensions: extensions.clone(),
        };

        let event = ctx.get_event().map_err(|e| unknown_err!(e))?;

        if event.is_none() {
            return Err(unknown_err!("could not get event from context"));
        }

        match event.unwrap() {
            NativeFileSystemEvent::GetPartCount { handle } => {
                let part_size = extensions
                    .get(&NativeFileSystemConstants::FilePartMaxSize.to_string())
                    .ok_or(unknown_err!("cannot get max file part size"))?
                    .parse()
                    .map_err(|e: ParseIntError| unknown_err!(e))?;

                let part_count = self
                    .get_file_part_count(handle, part_size, extensions)
                    .await?;

                let data = musubi_api::types::Value::U64(part_count)
                    .try_into()
                    .map_err(|e: anyhow::Error| unknown_err!(e))?;

                Ok(data)
            }
            NativeFileSystemEvent::LoadPart {
                handle,
                part_index,
                part_size,
            } => {
                self.load_file_part(handle, part_index, part_size, extensions)
                    .await
            }
            NativeFileSystemEvent::GetMetadata { handle } => {
                let metadata = self.get_metadata(handle, extensions).await?;

                let value = musubi_api::types::to_value(&metadata).map_err(|e| unknown_err!(e))?;
                let data = value
                    .try_into()
                    .map_err(|e: anyhow::Error| unknown_err!(e))?;

                Ok(data)
            }
            NativeFileSystemEvent::Exists { handle } => {
                let path_exists = self.path_exists(handle, extensions).await?;
                let value = musubi_api::types::Value::Bool(path_exists);
                let data = value
                    .try_into()
                    .map_err(|e: anyhow::Error| unknown_err!(e))?;

                Ok(data)
            }
            NativeFileSystemEvent::ListDir {
                handle,
                page_index,
                page_size,
            } => {
                let list = self.list(handle, page_index, page_size, extensions).await?;
                let value = musubi_api::types::Value::Array(
                    list.into_iter()
                        .map(|x| musubi_api::types::Value::String(x))
                        .collect(),
                );
                let data = value
                    .try_into()
                    .map_err(|e: anyhow::Error| unknown_err!(e))?;

                Ok(data)
            }
            NativeFileSystemEvent::GetCapabilities { handle } => {
                let capabilities = self.get_capabilities(handle, extensions).await?;

                let value = musubi_api::types::Value::Array(
                    capabilities
                        .into_iter()
                        .map(|x| musubi_api::types::Value::String(x.to_string()))
                        .collect(),
                );
                let data = value
                    .try_into()
                    .map_err(|e: anyhow::Error| unknown_err!(e))?;

                Ok(data)
            }
            NativeFileSystemEvent::GetStorageClass { handle } => {
                let storage_class = self.get_storage_class(handle, extensions).await?;

                let value = musubi_api::types::Value::String(storage_class);
                let data = value
                    .try_into()
                    .map_err(|e: anyhow::Error| unknown_err!(e))?;

                Ok(data)
            }
            NativeFileSystemEvent::Copy {
                source_handle,
                destination_handle,
            } => {
                self.copy_path(source_handle, destination_handle, extensions)
                    .await?;

                Ok(vec![])
            }
            NativeFileSystemEvent::Move {
                source_handle,
                destination_handle,
            } => {
                self.move_path(source_handle, destination_handle, extensions)
                    .await?;

                Ok(vec![])
            }
            x => Err(unsupported_op!(format!(
                "event {:?} is not supported within 'load' context",
                x
            ))),
        }
    }

    async fn process_mnfs_persist_context(
        &self,
        handle: String,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let ctx = NativeFileSystemEventContext::Persist {
            handle,
            ttl,
            extensions: extensions.clone(),
        };
        let event = ctx.get_event().map_err(|e| unknown_err!(e))?;

        if event.is_none() {
            return Err(unknown_err!("could not get event from context"));
        }

        match event.unwrap() {
            NativeFileSystemEvent::RenewLease {
                handle,
                lease_id,
                ttl,
            } => self.renew_lease(handle, lease_id, ttl, extensions).await,
            x => Err(unsupported_op!(format!(
                "event {:?} is not supported within 'persist' context",
                x
            ))),
        }
    }

    async fn process_mnfs_clear_context(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let ctx = NativeFileSystemEventContext::Clear {
            handle,
            extensions: extensions.clone(),
        };

        let event = ctx.get_event().map_err(|e| unknown_err!(e))?;

        if event.is_none() {
            return self.delete_path(ctx.get_handle().clone(), extensions).await;
        }

        match event.unwrap() {
            NativeFileSystemEvent::ReleaseLease { handle, lease_id } => {
                self.release_lease(handle, lease_id, extensions).await
            }
            NativeFileSystemEvent::RemoveDirListItem { handle, item } => {
                self.remove_list_item(handle, item, extensions).await
            }
            NativeFileSystemEvent::Truncate { handle, len } => {
                self.truncate(handle, len, extensions).await
            }
            x => Err(unsupported_op!(format!(
                "event {:?} is not supported within 'clear' context",
                x
            ))),
        }
    }

    fn start_gc(collectable: Arc<Box<dyn Storage>>) {
        tokio::task::spawn(async move {
            loop {
                tracing::debug!("running gc cycle");

                if let Err(e) = collectable.garbage_collect().await {
                    tracing::debug!("failed to run gc cycle. error: {}", e);
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}
