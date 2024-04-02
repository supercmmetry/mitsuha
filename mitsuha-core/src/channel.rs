use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core_types::{
    channel::{ComputeInput, ComputeOutput},
    kernel::{AsyncKernel, JobSpec, JobStatus, StorageSpec},
};

use crate::types::Extensions;
use crate::{constants::Constants, errors::Error, kernel::Kernel, types};

pub trait ComputeInputExt {
    fn get_opkind(&self) -> String;

    fn get_handle(&self) -> String;

    fn get_optional_ttl(&self) -> Option<u64>;

    fn get_original_handle(&self) -> String;

    fn get_extensions(&self) -> &Extensions;

    fn get_extensions_mut(&mut self) -> &mut Extensions;

    fn is_storage_input(&self) -> bool;
}

impl ComputeInputExt for ComputeInput {
    fn get_opkind(&self) -> String {
        match self {
            ComputeInput::Store { .. } => "store".to_string(),
            ComputeInput::Load { .. } => "load".to_string(),
            ComputeInput::Persist { .. } => "persist".to_string(),
            ComputeInput::Clear { .. } => "clear".to_string(),
            ComputeInput::Run { .. } => "run".to_string(),
            ComputeInput::Extend { .. } => "extend".to_string(),
            ComputeInput::Status { .. } => "status".to_string(),
            ComputeInput::Abort { .. } => "abort".to_string(),
        }
    }

    fn get_handle(&self) -> String {
        match self {
            ComputeInput::Store { spec } => spec.handle.clone(),
            ComputeInput::Load { handle, .. } => handle.clone(),
            ComputeInput::Persist { handle, .. } => handle.clone(),
            ComputeInput::Clear { handle, .. } => handle.clone(),
            ComputeInput::Run { spec } => spec.handle.clone(),
            ComputeInput::Extend { handle, .. } => handle.clone(),
            ComputeInput::Status { handle, .. } => handle.clone(),
            ComputeInput::Abort { handle, .. } => handle.clone(),
        }
    }

    fn get_optional_ttl(&self) -> Option<u64> {
        match self {
            ComputeInput::Store { spec } => Some(spec.ttl),
            ComputeInput::Load { .. } => None,
            ComputeInput::Persist { ttl, .. } => Some(*ttl),
            ComputeInput::Clear { .. } => None,
            ComputeInput::Run { spec } => Some(spec.ttl),
            ComputeInput::Extend { ttl, .. } => Some(*ttl),
            ComputeInput::Status { .. } => None,
            ComputeInput::Abort { .. } => None,
        }
    }

    fn get_original_handle(&self) -> String {
        self.get_extensions()
            .get(&Constants::OriginalHandle.to_string())
            .cloned()
            .unwrap_or(self.get_handle())
    }

    fn get_extensions(&self) -> &Extensions {
        match self {
            ComputeInput::Store { spec } => &spec.extensions,
            ComputeInput::Load { extensions, .. } => extensions,
            ComputeInput::Persist { extensions, .. } => extensions,
            ComputeInput::Clear { extensions, .. } => extensions,
            ComputeInput::Run { spec } => &spec.extensions,
            ComputeInput::Extend { extensions, .. } => extensions,
            ComputeInput::Status { extensions, .. } => extensions,
            ComputeInput::Abort { extensions, .. } => extensions,
        }
    }

    fn get_extensions_mut(&mut self) -> &mut Extensions {
        match self {
            ComputeInput::Store { spec } => &mut spec.extensions,
            ComputeInput::Load { extensions, .. } => extensions,
            ComputeInput::Persist { extensions, .. } => extensions,
            ComputeInput::Clear { extensions, .. } => extensions,
            ComputeInput::Run { spec } => &mut spec.extensions,
            ComputeInput::Extend { extensions, .. } => extensions,
            ComputeInput::Status { extensions, .. } => extensions,
            ComputeInput::Abort { extensions, .. } => extensions,
        }
    }

    fn is_storage_input(&self) -> bool {
        match self {
            ComputeInput::Store { .. } => true,
            ComputeInput::Load { .. } => true,
            ComputeInput::Persist { .. } => true,
            ComputeInput::Clear { .. } => true,
            _ => false,
        }
    }
}

#[async_trait]
pub trait ComputeChannel: Send + Sync {
    type Context;

    fn id(&self) -> String;

    async fn compute(&self, ctx: Self::Context, elem: ComputeInput)
        -> types::Result<ComputeOutput>;

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = Self::Context>>>);
}

pub struct ComputeKernel<Context> {
    channel: Arc<Box<dyn ComputeChannel<Context = Context>>>,
}

#[async_trait]
impl<Context> Kernel for ComputeKernel<Context>
where
    Context: Send + Default,
{
    async fn run_job(&self, spec: JobSpec) -> types::Result<()> {
        self.channel
            .compute(Context::default(), ComputeInput::Run { spec })
            .await?;
        Ok(())
    }

    async fn extend_job(
        &self,
        handle: String,
        ttl: u64,
        extensions: Extensions,
    ) -> types::Result<()> {
        self.channel
            .compute(
                Context::default(),
                ComputeInput::Extend {
                    handle,
                    ttl,
                    extensions,
                },
            )
            .await?;
        Ok(())
    }

    async fn abort_job(&self, handle: String, extensions: Extensions) -> types::Result<()> {
        self.channel
            .compute(
                Context::default(),
                ComputeInput::Abort { handle, extensions },
            )
            .await?;
        Ok(())
    }

    async fn get_job_status(
        &self,
        handle: String,
        extensions: Extensions,
    ) -> types::Result<JobStatus> {
        let output = self
            .channel
            .compute(
                Context::default(),
                ComputeInput::Status { handle, extensions },
            )
            .await?;
        match output {
            ComputeOutput::Status { status } => Ok(status),
            _ => Err(Error::UnknownWithMsgOnly {
                message: "expected ComputeOutput with status type".to_string(),
            }),
        }
    }

    async fn store_data(&self, spec: StorageSpec) -> types::Result<()> {
        self.channel
            .compute(Context::default(), ComputeInput::Store { spec })
            .await?;
        Ok(())
    }

    async fn load_data(&self, handle: String, extensions: Extensions) -> types::Result<Vec<u8>> {
        let output = self
            .channel
            .compute(
                Context::default(),
                ComputeInput::Load { handle, extensions },
            )
            .await?;
        match output {
            ComputeOutput::Loaded { data } => Ok(data),
            _ => Err(Error::UnknownWithMsgOnly {
                message: "expected ComputeOutput with loaded type".to_string(),
            }),
        }
    }

    async fn persist_data(
        &self,
        handle: String,
        ttl: u64,
        extensions: Extensions,
    ) -> types::Result<()> {
        self.channel
            .compute(
                Context::default(),
                ComputeInput::Persist {
                    handle,
                    ttl,
                    extensions,
                },
            )
            .await?;
        Ok(())
    }

    async fn clear_data(&self, handle: String, extensions: Extensions) -> types::Result<()> {
        self.channel
            .compute(
                Context::default(),
                ComputeInput::Clear { handle, extensions },
            )
            .await?;
        Ok(())
    }
}

impl<Context> ComputeKernel<Context>
where
    Context: Send,
{
    pub fn new(channel: Arc<Box<dyn ComputeChannel<Context = Context>>>) -> Self {
        Self { channel }
    }
}

pub struct MusubiKernelWrapper(Box<dyn Kernel>);

impl MusubiKernelWrapper {
    pub fn new(inner: Box<dyn Kernel>) -> Self {
        Self(inner)
    }
}

#[async_trait]
impl AsyncKernel for MusubiKernelWrapper {
    async fn run_job(&self, spec: JobSpec) -> anyhow::Result<()> {
        self.0.run_job(spec).await?;
        Ok(())
    }

    async fn get_job_status(
        &self,
        handle: String,
        extensions: Extensions,
    ) -> anyhow::Result<JobStatus> {
        let output = self.0.get_job_status(handle, extensions).await?;
        Ok(output)
    }

    async fn extend_job(
        &self,
        handle: String,
        ttl: u64,
        extensions: Extensions,
    ) -> anyhow::Result<()> {
        self.0.extend_job(handle, ttl, extensions).await?;
        Ok(())
    }

    async fn abort_job(&self, handle: String, extensions: Extensions) -> anyhow::Result<()> {
        self.0.abort_job(handle, extensions).await?;
        Ok(())
    }

    async fn store_data(&self, spec: StorageSpec) -> anyhow::Result<()> {
        self.0.store_data(spec).await?;
        Ok(())
    }

    async fn load_data(&self, handle: String, extensions: Extensions) -> anyhow::Result<Vec<u8>> {
        let output = self.0.load_data(handle, extensions).await?;
        Ok(output)
    }

    async fn persist_data(
        &self,
        handle: String,
        ttl: u64,
        extensions: Extensions,
    ) -> anyhow::Result<()> {
        self.0.persist_data(handle, ttl, extensions).await?;
        Ok(())
    }

    async fn clear_data(&self, handle: String, extensions: Extensions) -> anyhow::Result<()> {
        self.0.clear_data(handle, extensions).await?;
        Ok(())
    }
}

use crate::job::mgr::{JobManager, JobManagerProvider};
use dashmap::DashMap;
use lazy_static::lazy_static;
use tokio::sync::RwLock;
use uuid::Uuid;

pub trait StateProvider: Send + Sync {
    fn get_value(&self, key: &String) -> Option<String>;

    fn set_value(&self, key: String, value: String);

    fn reset_state(&self);
}

#[derive(Clone)]
pub struct ChannelContext {
    channel_start: Option<Arc<Box<dyn ComputeChannel<Context = Self>>>>,
    state: Arc<DashMap<String, String>>,
    mgr: Arc<RwLock<ChannelManager>>,
}

impl ChannelContext {
    pub fn get_mgr(&self) -> Arc<RwLock<ChannelManager>> {
        self.mgr.clone()
    }

    pub fn get_channel_start(&self) -> Arc<Box<dyn ComputeChannel<Context = Self>>> {
        self.channel_start.as_ref().unwrap().clone()
    }

    pub fn set_channel_start(
        &mut self,
        channel_start: Arc<Box<dyn ComputeChannel<Context = Self>>>,
    ) {
        self.channel_start = Some(channel_start);
    }
}

impl Default for ChannelContext {
    fn default() -> Self {
        Self {
            mgr: ChannelManager::global_rw(),
            state: Default::default(),
            channel_start: Default::default(),
        }
    }
}

impl StateProvider for ChannelContext {
    fn get_value(&self, key: &String) -> Option<String> {
        Some(self.state.get(key)?.clone())
    }

    fn set_value(&self, key: String, value: String) {
        self.state.insert(key, value);
    }

    fn reset_state(&self) {
        self.state.clear();
    }
}

#[async_trait]
impl JobManagerProvider for ChannelContext {
    async fn get_job_mgr(&self) -> JobManager<Self> {
        self.get_mgr().read().await.get_job_mgr().clone()
    }
}

#[async_trait]
pub trait ChannelUtilityProvider {
    async fn sign_compute_input(&self, compute_input: &mut ComputeInput);

    async fn unsign_compute_input(&self, compute_input: &mut ComputeInput);

    async fn is_compute_input_signed(&self, compute_input: &ComputeInput) -> bool;

    async fn truncate_privileged_fields(&self, compute_input: &mut ComputeInput);

    async fn append_skip_channel_list(
        &self,
        compute_input: &mut ComputeInput,
        channel_ids: Vec<String>,
    );

    async fn should_skip_channel(&self, channel_id: &String, compute_input: &ComputeInput) -> bool;
}

#[async_trait]
impl ChannelUtilityProvider for ChannelContext {
    async fn sign_compute_input(&self, compute_input: &mut ComputeInput) {
        self.mgr.read().await.sign_compute_input(compute_input)
    }

    async fn unsign_compute_input(&self, compute_input: &mut ComputeInput) {
        self.mgr.read().await.unsign_compute_input(compute_input)
    }

    async fn is_compute_input_signed(&self, compute_input: &ComputeInput) -> bool {
        self.mgr.read().await.is_compute_input_signed(compute_input)
    }

    async fn truncate_privileged_fields(&self, compute_input: &mut ComputeInput) {
        self.mgr
            .read()
            .await
            .truncate_privileged_fields(compute_input)
    }

    async fn append_skip_channel_list(
        &self,
        compute_input: &mut ComputeInput,
        channel_ids: Vec<String>,
    ) {
        self.mgr
            .read()
            .await
            .append_skip_channel_list(compute_input, channel_ids)
    }

    async fn should_skip_channel(&self, channel_id: &String, compute_input: &ComputeInput) -> bool {
        self.mgr
            .read()
            .await
            .should_skip_channel(channel_id, compute_input)
    }
}

lazy_static! {
    static ref GLOBAL_CHANNEL_MANAGER: Arc<RwLock<ChannelManager>> =
        Arc::new(RwLock::new(ChannelManager::default()));
}

#[derive(Clone)]
pub struct ChannelManager {
    pub job_manager: Option<JobManager<ChannelContext>>,
    pub channel_start: Option<Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>>,
    pub channel_map: Arc<DashMap<String, Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>>>,
    pub signature: String,
}

impl Default for ChannelManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ChannelManager {
    pub fn new() -> Self {
        Self {
            job_manager: None,
            channel_map: Default::default(),
            channel_start: None,
            signature: Uuid::new_v4().to_string(),
        }
    }

    pub fn global_rw() -> Arc<RwLock<Self>> {
        GLOBAL_CHANNEL_MANAGER.clone()
    }

    pub async fn global() -> Arc<Self> {
        Arc::new(GLOBAL_CHANNEL_MANAGER.read().await.clone())
    }

    pub fn get_job_mgr(&self) -> &JobManager<ChannelContext> {
        self.job_manager.as_ref().unwrap()
    }

    pub fn sign_compute_input(&self, compute_input: &mut ComputeInput) {
        compute_input.get_extensions_mut().insert(
            Constants::ComputeInputSignature.to_string(),
            self.signature.clone(),
        );
    }

    pub fn unsign_compute_input(&self, compute_input: &mut ComputeInput) {
        compute_input
            .get_extensions_mut()
            .remove(&Constants::ComputeInputSignature.to_string());
    }

    pub fn is_compute_input_signed(&self, compute_input: &ComputeInput) -> bool {
        match compute_input
            .get_extensions()
            .get(&Constants::ComputeInputSignature.to_string())
        {
            Some(v) => v == &self.signature.to_string(),
            None => false,
        }
    }

    pub fn truncate_privileged_fields(&self, compute_input: &mut ComputeInput) {
        if self.is_compute_input_signed(compute_input) {
            return;
        }

        compute_input
            .get_extensions_mut()
            .remove(&Constants::ChannelSkipList.to_string());
    }

    pub fn append_skip_channel_list(
        &self,
        compute_input: &mut ComputeInput,
        channel_ids: Vec<String>,
    ) {
        let mut existing_list: HashSet<String> = compute_input
            .get_extensions()
            .get(&Constants::ChannelSkipList.to_string())
            .map(|v| v.split(",").map(|x| x.to_string()).collect())
            .unwrap_or_default();

        for channel_id in channel_ids {
            existing_list.insert(channel_id);
        }

        compute_input.get_extensions_mut().insert(
            Constants::ChannelSkipList.to_string(),
            existing_list
                .iter()
                .cloned()
                .collect::<Vec<String>>()
                .join(","),
        );
    }

    pub fn should_skip_channel(&self, channel_id: &String, compute_input: &ComputeInput) -> bool {
        if !self.is_compute_input_signed(compute_input) {
            return false;
        }

        let existing_list: HashSet<String> = compute_input
            .get_extensions()
            .get(&Constants::ChannelSkipList.to_string())
            .map(|v| v.split(",").map(|x| x.to_string()).collect())
            .unwrap_or_default();

        existing_list.contains(channel_id)
    }
}
