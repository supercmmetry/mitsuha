use crate::config::ConfKey;
use crate::constant::SchedulerConstants;
use crate::{job_command_queue, job_queue, metric, partition, util};
use async_trait::async_trait;
use chrono::Utc;
use mitsuha_core::channel::{
    ChannelUtilityProvider, ComputeChannel, ComputeInputExt, StateProvider,
};
use mitsuha_core::config::Config;
use mitsuha_core::constants::Constants;
use mitsuha_core::errors::Error;
use mitsuha_core::errors::ToUnknownErrorResult;
use mitsuha_core::job::cost::JobCost;
use mitsuha_core::job::ctrl::PostJobHook;
use mitsuha_core::job::mgr::JobManagerProvider;
use mitsuha_core::types::Extensions;
use mitsuha_core::{err_unsupported_op, types};
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};
use mitsuha_core_types::kernel::{JobSpec, StorageSpec};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

type PartitionId = Arc<RwLock<String>>;

#[derive(Clone)]
struct SchedulerState<Context> {
    pub partition_repository: partition::Repository,
    pub job_queue_repository: job_queue::Repository,
    pub job_command_queue_repository: job_command_queue::Repository,
    pub partition_id: PartitionId,
    pub bypass_channel_ids: Vec<String>,
    pub partition_poll_interval: Duration,
    pub channel: Arc<Box<dyn ComputeChannel<Context = Context>>>,
    pub removed_job_handles: Arc<RwLock<Vec<String>>>,
}

impl<Context> SchedulerState<Context>
where
    Context: 'static + Default + JobManagerProvider + StateProvider + Clone,
{
    pub async fn rotate_partition(&self) -> types::Result<()> {
        let ctx = Context::default();
        let aborted_job_count = ctx.get_job_mgr().await.abort_all_jobs().await?;
        tracing::info!("aborted {} jobs for partition rotation", aborted_job_count);

        let old_partition_id = self.partition_id.read().await.clone();

        if let Err(e) = self
            .partition_repository
            .remove(old_partition_id.clone())
            .await
        {
            tracing::warn!(
                "failed to remove old partition '{}', error = {}",
                old_partition_id,
                e
            );
        }

        let partition = self.partition_repository.create().await?;
        let new_partition_id = partition.id;

        tracing::warn!(
            "rotating partition id from '{}' to '{}'",
            self.partition_id.read().await,
            new_partition_id
        );

        self.removed_job_handles.write().await.clear();

        *self.partition_id.write().await = new_partition_id;

        Ok(())
    }
}

#[async_trait]
trait SchedulerChannelExt<Context> {
    async fn store_compute_input(
        &self,
        state: &SchedulerState<Context>,
        ctx: Context,
        compute_input: ComputeInput,
    ) -> types::Result<String>;

    async fn load_compute_input(
        &self,
        state: &SchedulerState<Context>,
        ctx: Context,
        storage_handle: String,
    ) -> types::Result<ComputeInput>;

    async fn remove_compute_input(
        &self,
        state: &SchedulerState<Context>,
        ctx: Context,
        storage_handle: String,
    ) -> types::Result<()>;
}

#[async_trait]
impl<Context> SchedulerChannelExt<Context> for Arc<Box<dyn ComputeChannel<Context = Context>>>
where
    Context: Send + Sync + ChannelUtilityProvider,
{
    async fn store_compute_input(
        &self,
        state: &SchedulerState<Context>,
        ctx: Context,
        input: ComputeInput,
    ) -> types::Result<String> {
        let storage_handle = util::generate_scheduler_store_handle();

        let value = musubi_api::types::to_value(&input).to_unknown_err_result()?;

        // TODO: Set TTL to INF
        let spec = StorageSpec {
            handle: storage_handle.clone(),
            data: value.try_into().to_unknown_err_result()?,
            ttl: 1200,
            extensions: input.get_extensions().clone(),
        };

        let mut input = ComputeInput::Store { spec };

        ctx.sign_compute_input(&mut input).await;
        ctx.append_skip_channel_list(&mut input, state.bypass_channel_ids.clone())
            .await;

        self.compute(ctx, input).await?;

        Ok(storage_handle)
    }

    async fn load_compute_input(
        &self,
        state: &SchedulerState<Context>,
        ctx: Context,
        storage_handle: String,
    ) -> types::Result<ComputeInput> {
        let mut input = ComputeInput::Load {
            handle: storage_handle,
            extensions: Default::default(),
        };

        ctx.sign_compute_input(&mut input).await;
        ctx.append_skip_channel_list(&mut input, state.bypass_channel_ids.clone())
            .await;

        let output = self.compute(ctx, input).await?;

        match output {
            ComputeOutput::Loaded { data } => {
                let value = musubi_api::types::Value::try_from(data).to_unknown_err_result()?;

                musubi_api::types::from_value(&value).to_unknown_err_result()
            }
            _ => Err(err_unsupported_op!("expected loaded compute output")),
        }
    }

    async fn remove_compute_input(
        &self,
        state: &SchedulerState<Context>,
        ctx: Context,
        storage_handle: String,
    ) -> types::Result<()> {
        let mut input = ComputeInput::Clear {
            handle: storage_handle,
            extensions: Default::default(),
        };

        ctx.sign_compute_input(&mut input).await;
        ctx.append_skip_channel_list(&mut input, state.bypass_channel_ids.clone())
            .await;

        self.compute(ctx, input).await?;

        Ok(())
    }
}

macro_rules! wait_for_eventslice {
    ($v: expr) => {
        match $v.as_mut() {
            Some(join_handle) => {
                if !join_handle.is_finished() {
                    return Ok(());
                } else {
                    if let Err(e) = join_handle.await.to_unknown_err_result() {
                        return Err(e);
                    }
                }
            }
            _ => {}
        }
    };
}

pub fn mark_compute_input_as_queued(compute_input: &mut ComputeInput) {
    compute_input.get_extensions_mut().insert(
        Constants::SchedulerComputeInputQueued.to_string(),
        "true".to_string(),
    );
}

pub fn is_compute_input_queued(compute_input: &ComputeInput) -> bool {
    compute_input
        .get_extensions()
        .get(&Constants::SchedulerComputeInputQueued.to_string())
        .map(|x| x.parse().unwrap_or_default())
        .unwrap_or_default()
}

#[derive(Default)]
struct EventLoopSlice<Context> {
    rotate_expired_partition: Option<JoinHandle<types::Result<()>>>,
    remove_stale_partitions: Option<JoinHandle<types::Result<()>>>,
    renew_partition: Option<JoinHandle<types::Result<()>>>,
    consume_from_job_queue: Option<JoinHandle<types::Result<()>>>,
    consume_from_job_command_queue: Option<JoinHandle<types::Result<()>>>,
    process_batch_event: Option<JoinHandle<types::Result<()>>>,
    __context: std::marker::PhantomData<Context>,
}

impl<Context> EventLoopSlice<Context>
where
    Context: 'static
        + Default
        + Clone
        + Send
        + Sync
        + StateProvider
        + JobManagerProvider
        + ChannelUtilityProvider,
{
    pub(crate) async fn run(&mut self, state: &SchedulerState<Context>) -> types::Result<()> {
        let rotate_expired_partition_result = self.rotate_expired_partition(state).await;
        let remove_stale_partitions_result = self.remove_stale_partitions(state).await;
        let renew_partition_result = self.renew_partition(state).await;
        let process_batch_event_result = self.process_batch_event(state).await;
        let consume_from_job_queue_result = self.consume_from_job_queue(state).await;
        let consume_from_job_command_queue_result = self.consume_from_job_command_queue(state).await;
        
        rotate_expired_partition_result?;
        remove_stale_partitions_result?;
        renew_partition_result?;
        process_batch_event_result?;
        consume_from_job_queue_result?;
        consume_from_job_command_queue_result?;
        
        Ok(())
    }

    async fn rotate_expired_partition(
        &mut self,
        state: &SchedulerState<Context>,
    ) -> types::Result<()> {
        wait_for_eventslice!(self.rotate_expired_partition);

        let partition_id = state.partition_id.read().await.clone();
        let state = state.clone();

        let task = async move {
            let partition = state
                .partition_repository
                .read_by_id(partition_id.clone())
                .await?;

            if partition.is_none() || partition.unwrap().is_lease_expired() {
                state.rotate_partition().await?;
            }

            Ok(())
        };

        self.rotate_expired_partition = Some(tokio::task::spawn(task));

        Ok(())
    }

    async fn remove_stale_partitions(
        &mut self,
        state: &SchedulerState<Context>,
    ) -> types::Result<()> {
        wait_for_eventslice!(self.remove_stale_partitions);

        let partition_repo = state.partition_repository.clone();
        let task = async move { 
            partition_repo.remove_stale_partitions().await
        };

        self.remove_stale_partitions = Some(tokio::task::spawn(task));

        Ok(())
    }

    async fn renew_partition(&mut self, state: &SchedulerState<Context>) -> types::Result<()> {
        wait_for_eventslice!(self.renew_partition);

        let partition_repo = state.partition_repository.clone();
        let partition_id = state.partition_id.read().await.clone();
        let task = async move { partition_repo.renew_lease(partition_id.clone()).await };

        self.renew_partition = Some(tokio::task::spawn(task));

        Ok(())
    }

    async fn consume_from_job_queue(
        &mut self,
        state: &SchedulerState<Context>,
    ) -> types::Result<()> {
        wait_for_eventslice!(self.consume_from_job_queue);

        let partition_id = state.partition_id.read().await.clone();

        let state = state.clone();

        let task = async move {
            let ctx = Context::default();
            let before = Utc::now().timestamp_millis();

            // TODO: Put this in a different thread so that batch_event does not bottleneck this.
            if let Some(job) = state
                .job_queue_repository
                .consume_from_partition(partition_id.clone())
                .await?
            {
                let mut input = state
                    .channel
                    .load_compute_input(&state, ctx.clone(), job.storage_handle.clone())
                    .await?;

                ctx.sign_compute_input(&mut input).await;

                mark_compute_input_as_queued(&mut input);

                let ctx = ctx.clone();

                ctx.set_value(
                    SchedulerConstants::JobHandleParameter.to_string(),
                    job.job_handle,
                );

                ctx.set_value(
                    SchedulerConstants::StorageHandleParameter.to_string(),
                    job.storage_handle,
                );

                state.channel.compute(ctx, input).await?;
            }

            let time_taken_job_consume = Utc::now().timestamp_millis() - before;

            if time_taken_job_consume > 100 {
                tracing::warn!("job_queue consumption took {} ms", time_taken_job_consume);
            }

            Ok(())
        };

        self.consume_from_job_queue = Some(tokio::task::spawn(task));

        Ok(())
    }

    async fn consume_from_job_command_queue(
        &mut self,
        state: &SchedulerState<Context>,
    ) -> types::Result<()> {
        wait_for_eventslice!(self.consume_from_job_command_queue);

        let partition_id = state.partition_id.read().await.clone();
        let state = state.clone();

        let task = async move {
            let ctx = Context::default();
            if let Some(job_command) = state
                .job_command_queue_repository
                .consume_from_partition(partition_id.clone())
                .await?
            {
                let mut input = state
                    .channel
                    .load_compute_input(&state, ctx.clone(), job_command.storage_handle.clone())
                    .await?;

                ctx.sign_compute_input(&mut input).await;
                mark_compute_input_as_queued(&mut input);

                let ctx = ctx.clone();

                ctx.set_value(
                    SchedulerConstants::JobCommandIdParameter.to_string(),
                    job_command.id.to_string(),
                );
                ctx.set_value(
                    SchedulerConstants::StorageHandleParameter.to_string(),
                    job_command.storage_handle,
                );

                state.channel.compute(ctx, input).await?;
            }

            Ok(())
        };

        self.consume_from_job_command_queue = Some(tokio::task::spawn(task));

        Ok(())
    }

    async fn process_batch_event(&mut self, state: &SchedulerState<Context>) -> types::Result<()> {
        wait_for_eventslice!(self.process_batch_event);

        let partition_id = state.partition_id.read().await.clone();
        let state = state.clone();

        let task = async move {
            let mut guard = state.removed_job_handles.write().await;

            let removed_job_handles = guard.clone();

            let before = Utc::now().timestamp_millis();

            let result = state
                .job_queue_repository
                .batch_event(partition_id, 16, removed_job_handles)
                .await;

            if let Err(e) = result {
                match e {
                    Error::EntityNotFoundError { .. } | Error::EntityConflictError { .. } => {
                        return Err(e);
                    }
                    _ => {
                        tracing::warn!("batch_event failed with error: {}", e);
                    }
                }
            }

            let time_taken = Utc::now().timestamp_millis() - before;

            if time_taken > 50 {
                tracing::warn!("batch_event took {} ms", time_taken);
            }

            guard.clear();

            Ok(())
        };

        self.process_batch_event = Some(tokio::task::spawn(task));

        Ok(())
    }
}

#[derive(Clone)]
pub struct Scheduler<Context> {
    state: SchedulerState<Context>,
}

impl<Context> Scheduler<Context>
where
    Context: 'static
        + Default
        + Clone
        + Send
        + Sync
        + StateProvider
        + JobManagerProvider
        + ChannelUtilityProvider,
{
    pub async fn new(
        channel: Arc<Box<dyn ComputeChannel<Context = Context>>>,
        properties: Extensions,
    ) -> types::Result<Self> {
        let partition_lease_duration_seconds: u64 = properties
            .get(&ConfKey::PartitionLeaseDurationSeconds.to_string())
            .unwrap_or(&"15".to_string())
            .parse()
            .to_unknown_err_result()?;

        let partition_lease_skew_duration_seconds: u64 = properties
            .get(&ConfKey::PartitionLeaseSkewDurationSeconds.to_string())
            .unwrap_or(&"5".to_string())
            .parse()
            .to_unknown_err_result()?;

        let partition_poll_interval_milliseconds: u64 = properties
            .get(&ConfKey::PartitionPollIntervalMilliSeconds.to_string())
            .unwrap_or(&"1".to_string())
            .parse()
            .to_unknown_err_result()?;

        let max_shards: u64 = properties
            .get(&ConfKey::MaxShards.to_string())
            .unwrap_or(&u64::MAX.to_string())
            .parse()
            .to_unknown_err_result()?;

        let bypass_channel_ids: Vec<String> = properties
            .get(&ConfKey::BypassChannelIds.to_string())
            .map(|x| x.split(",").map(|x| x.to_string()).collect())
            .unwrap_or_default();

        let config = Config::global().await.to_unknown_err_result()?;

        let partition_repository = partition::service::Service::new(
            chrono::Duration::seconds(partition_lease_duration_seconds as i64),
            chrono::Duration::seconds(partition_lease_skew_duration_seconds as i64),
            config.job.scheduler.core_scheduling_capacity.compute as i64,
            max_shards as i64,
        )
        .await;

        partition_repository.register_module().await?;

        let job_queue_repository = job_queue::service::Service::new(max_shards as i64).await;
        let job_command_queue_repository = job_command_queue::service::Service::new().await;

        let partition = partition_repository.create().await?;
        let partition_poll_interval = Duration::from_millis(partition_poll_interval_milliseconds);

        let partition_id = Arc::new(RwLock::new(partition.id));

        let state = SchedulerState {
            partition_id,
            partition_repository,
            job_queue_repository,
            job_command_queue_repository,
            partition_poll_interval,
            channel,
            bypass_channel_ids,
            removed_job_handles: Default::default(),
        };

        Self::partition_poller(state.clone()).await?;

        Ok(Self { state })
    }

    async fn partition_poller(state: SchedulerState<Context>) -> types::Result<()> {
        let event_loop = async move {
            let mut event_loop_slice = EventLoopSlice::<Context>::default();

            loop {
                if let Err(e) = event_loop_slice.run(&state).await {
                    tracing::error!("failed to poll scheduler partition. error={}", e);

                    if let Err(e) = state.rotate_partition().await {
                        tracing::error!(
                            "partition rotation failed with error: {}, exiting process...",
                            e
                        );

                        // TODO: Instead of exiting directly, call some shutdown hook

                        std::process::exit(1);
                    }
                }

                tokio::time::sleep(state.partition_poll_interval.clone()).await;
            }
        };

        tokio::task::spawn_blocking(|| Handle::current().block_on(event_loop));

        Ok(())
    }

    pub(crate) async fn rotate_partition(&self) -> types::Result<()> {
        self.state.rotate_partition().await
    }

    /// Schedule a compute input into the mitsuha-scheduler. If the compute input was already scheduled, this
    /// method returns true. If true is returned, the expectation is for the next channel to process the compute
    /// input.
    pub async fn schedule(
        &self,
        ctx: Context,
        compute_input: &ComputeInput,
    ) -> types::Result<bool> {
        if is_compute_input_queued(&compute_input) {
            return Ok(true);
        }

        match &compute_input {
            ComputeInput::Run { spec } => {
                let storage_handle = self
                    .state
                    .channel
                    .store_compute_input(&self.state, ctx, compute_input.clone())
                    .await?;

                self.state
                    .job_queue_repository
                    .add_job_to_queue(&spec, storage_handle)
                    .await?;

                Ok(false)
            }
            ComputeInput::Extend { .. } | ComputeInput::Abort { .. } => {
                let storage_handle = self
                    .state
                    .channel
                    .store_compute_input(&self.state, ctx, compute_input.clone())
                    .await?;

                self.state
                    .job_command_queue_repository
                    .create(&compute_input, storage_handle)
                    .await?;

                Ok(false)
            }
            _ => Ok(true),
        }
    }

    pub async fn remove_job(
        &self,
        ctx: Context,
        job_handle: String,
        storage_handle: String,
    ) -> types::Result<()> {
        self.state
            .removed_job_handles
            .write()
            .await
            .push(job_handle);

        Ok(())
    }

    pub async fn remove_job_command(
        &self,
        ctx: Context,
        job_command_id: i64,
        storage_handle: String,
    ) -> types::Result<()> {
        self.state
            .job_command_queue_repository
            .mark_as_completed(job_command_id)
            .await?;

        self.state
            .channel
            .remove_compute_input(&self.state, ctx, storage_handle)
            .await?;

        Ok(())
    }
}

pub struct SchedulerPostJobHook<Context> {
    scheduler: Scheduler<Context>,
}

impl<Context> SchedulerPostJobHook<Context> {
    pub fn new(scheduler: Scheduler<Context>) -> Self {
        Self { scheduler }
    }
}

#[async_trait]
impl<Context> PostJobHook<Context> for SchedulerPostJobHook<Context>
where
    Context:
        'static + Default + ChannelUtilityProvider + Clone + JobManagerProvider + StateProvider,
{
    async fn run(&self, ctx: Context) -> types::Result<()> {
        match (
            ctx.get_value(&SchedulerConstants::JobHandleParameter.to_string()),
            ctx.get_value(&SchedulerConstants::StorageHandleParameter.to_string()),
        ) {
            (Some(job_handle), Some(storage_handle)) => {
                ctx.get_job_mgr().await.dequeue_job(&job_handle).await?;

                if let Err(e) = self
                    .scheduler
                    .remove_job(ctx, job_handle, storage_handle)
                    .await
                {
                    tracing::error!("failed to remove job from queue, error: {}", e);

                    if let Err(e) = self.scheduler.rotate_partition().await {
                        tracing::error!("partition rotation failed! exiting process. error: {}", e);
                        // TODO: Instead of exiting directly, call some shutdown hook
                        std::process::exit(0x1);
                    }

                    return Err(e);
                }
            }
            _ => {}
        }

        Ok(())
    }
}
