use crate::channel::{ComputeChannel, StateProvider};
use crate::constants::Constants;
use crate::errors::Error;
use crate::job::ctx::JobState;
use crate::job::mgr::JobManagerProvider;
use crate::types;
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::AbortHandle;
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};
use mitsuha_core_types::kernel::{JobSpec, JobStatus, JobStatusType, StorageSpec};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::Instrument;

#[async_trait]
pub trait PostJobHook<Context: StateProvider>: Send + Sync {
    async fn run(&self, ctx: Context) -> types::Result<()>;
}

pub struct JobController<Context: JobManagerProvider + StateProvider> {
    spec: JobSpec,
    task: JoinHandle<types::Result<()>>,
    abort_handle: AbortHandle,
    channel: Arc<Box<dyn ComputeChannel<Context = Context>>>,
    channel_context: Context,
    prev_status_update: Option<DateTime<Utc>>,
    post_job_hooks: Vec<Arc<dyn PostJobHook<Context>>>,
}

struct JobCompletionHook {
    notifier: Sender<JobState>,
    job_handle: String,
}

#[async_trait]
impl<Context> PostJobHook<Context> for JobCompletionHook
where
    Context: 'static + JobManagerProvider + StateProvider,
{
    async fn run(&self, _ctx: Context) -> types::Result<()> {
        match self.notifier.send(JobState::Completed).await {
            Ok(_) => {
                tracing::debug!("completion_notifier triggered for job!");
            }
            Err(e) => {
                tracing::debug!("failed to trigger completion_notifier. error: {}", e);
            }
        }

        Ok(())
    }
}

impl<Context> JobController<Context>
where
    Context: 'static + JobManagerProvider + StateProvider,
{
    pub fn new(
        spec: JobSpec,
        task: JoinHandle<types::Result<()>>,
        abort_handle: AbortHandle,
        channel: Arc<Box<dyn ComputeChannel<Context = Context>>>,
        channel_context: Context,
    ) -> Self {
        Self {
            spec,
            task,
            abort_handle,
            channel,
            channel_context,
            prev_status_update: None,
            post_job_hooks: Vec::new(),
        }
    }

    pub fn add_post_job_hook(&mut self, hook: Arc<dyn PostJobHook<Context>>) {
        self.post_job_hooks.push(hook);
    }

    async fn run_post_job_hooks(
        ctx: &Context,
        post_job_hooks: &Vec<Arc<dyn PostJobHook<Context>>>,
    ) {
        for hook in post_job_hooks.iter() {
            if let Err(e) = hook.run(ctx.clone()).await {
                tracing::error!("failed to run hook, error: {}", e);
            }
        }
    }

    async fn update_status(
        spec: &JobSpec,
        channel: Arc<Box<dyn ComputeChannel<Context = Context>>>,
        channel_context: &Context,
        status_type: JobStatusType,
        current_time: DateTime<Utc>,
    ) -> types::Result<()> {
        tracing::debug!(
            "updating status for job '{}' to '{:?}'",
            spec.handle,
            status_type
        );

        let status = JobStatus {
            status: status_type,
            extensions: [(
                Constants::JobStatusLastUpdated.to_string(),
                current_time.to_rfc3339(),
            )]
            .into_iter()
            .collect(),
        };

        let status_data = musubi_api::types::to_value(&status)
            .map_err(|e| Error::Unknown { source: e })?
            .try_into()
            .map_err(|e| Error::Unknown { source: e })?;

        let spec = StorageSpec {
            handle: spec.get_status_handle(),
            data: status_data,
            ttl: spec
                .extensions
                .get(&Constants::JobOutputTTL.to_string())
                .ok_or(Error::Unknown {
                    source: anyhow!("failed to get job output ttl"),
                })?
                .parse::<u64>()
                .map_err(|e| Error::Unknown { source: e.into() })?,
            extensions: spec.extensions.clone(),
        };

        channel
            .compute(channel_context.clone(), ComputeInput::Store { spec })
            .await?;

        Ok(())
    }

    pub async fn run(
        mut self,
        handle: String,
        ctx: &Context,
        updater: Sender<JobState>,
        mut updation_target: Receiver<JobState>,
        status_updater: Sender<JobState>,
    ) -> types::Result<ComputeOutput> {
        let completion_hook = JobCompletionHook {
            notifier: updater.clone(),
            job_handle: handle.clone(),
        };

        let other_ctx = ctx.clone();
        let task = self.task;
        let post_job_hooks = self.post_job_hooks;
        let observable_task_future = async move {
            let result = task.await;

            completion_hook.run(other_ctx.clone()).await?;

            result.map_err(|e| Error::Unknown { source: e.into() })?
        };

        let observable_task: JoinHandle<types::Result<()>> =
            tokio::task::spawn(observable_task_future.instrument(tracing::Span::current()));

        let mut max_expiry = Utc::now();

        loop {
            let current_time = Utc::now();

            match updation_target.recv().await {
                Some(x) => match x {
                    JobState::ExpireAt(x) if x <= current_time => {
                        self.abort_handle.abort();
                        _ = observable_task.await;

                        Self::run_post_job_hooks(ctx, &post_job_hooks).await;
                        ctx.get_job_mgr().await.dequeue_job(&handle).await?;

                        Self::update_status(
                            &self.spec,
                            self.channel.clone(),
                            &self.channel_context,
                            JobStatusType::ExpiredAt {
                                datetime: x.clone(),
                            },
                            current_time,
                        )
                        .await?;

                        tracing::info!(
                            "job with handle '{}' expired at '{}'",
                            &handle,
                            x.to_string()
                        );

                        return Err(Error::JobExpired {
                            handle,
                            expiry: x.to_string(),
                        });
                    }
                    JobState::Aborted => {
                        self.abort_handle.abort();
                        _ = observable_task.await;
                        _ = status_updater.send(JobState::Aborted).await;

                        Self::run_post_job_hooks(ctx, &post_job_hooks).await;
                        ctx.get_job_mgr().await.dequeue_job(&handle).await?;

                        Self::update_status(
                            &self.spec,
                            self.channel.clone(),
                            &self.channel_context,
                            JobStatusType::Aborted,
                            current_time,
                        )
                        .await?;

                        tracing::info!("job with handle '{}' was aborted", &handle);

                        return Err(Error::JobAborted { handle });
                    }
                    JobState::Completed => {
                        let result = observable_task.await;

                        Self::run_post_job_hooks(ctx, &post_job_hooks).await;
                        ctx.get_job_mgr().await.dequeue_job(&handle).await?;

                        match status_updater.send(JobState::Completed).await {
                            Ok(_) => {
                                tracing::debug!(
                                    "status_updater triggered for job '{}'!",
                                    &self.spec.handle
                                );
                            }
                            Err(e) => {
                                tracing::debug!(
                                    "failed to trigger status_updater for job '{}'. error: {}",
                                    &self.spec.handle,
                                    e
                                );
                            }
                        }

                        result.map_err(|e| Error::Unknown { source: e.into() })??;

                        Self::update_status(
                            &self.spec,
                            self.channel.clone(),
                            &self.channel_context,
                            JobStatusType::Completed,
                            current_time,
                        )
                        .await?;

                        tracing::info!("job with handle '{}' was completed", &handle);

                        return Ok(ComputeOutput::Completed);
                    }
                    JobState::ExpireAt(x) if x > current_time => {
                        if x >= max_expiry {
                            max_expiry = x;
                            _ = updater.send(JobState::ExpireAt(x)).await;
                        }

                        if self.prev_status_update.is_none() {
                            self.prev_status_update = Some(current_time);
                        }

                        match self.prev_status_update {
                            // TODO: Make status updation interval configurable
                            Some(t)
                                if current_time > t && (current_time - t).num_seconds() >= 1 =>
                            {
                                Self::update_status(
                                    &self.spec,
                                    self.channel.clone(),
                                    &self.channel_context,
                                    JobStatusType::Running,
                                    current_time,
                                )
                                .await?;

                                self.prev_status_update = Some(current_time);
                            }
                            Some(_) => {}
                            None => {
                                self.prev_status_update = Some(current_time);
                                Self::update_status(
                                    &self.spec,
                                    self.channel.clone(),
                                    &self.channel_context,
                                    JobStatusType::Running,
                                    current_time,
                                )
                                .await?;
                            }
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }
    }
}
