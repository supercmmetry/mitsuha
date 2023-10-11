use anyhow::anyhow;
use chrono::{DateTime, Utc};
use futures::stream::AbortHandle;
use mitsuha_core::{
    constants::Constants,
    errors::Error,
    types,
};
use mitsuha_core_types::{kernel::{JobSpec, JobStatusType, JobStatus, StorageSpec}, channel::{ComputeInput, ComputeOutput}};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::Instrument;

use crate::context::ChannelContext;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum JobState {
    Completed,
    Aborted,
    ExpireAt(DateTime<Utc>),
}

pub struct JobController {
    spec: JobSpec,
    task: JoinHandle<types::Result<()>>,
    abort_handle: AbortHandle,
    channel_context: ChannelContext,
    prev_status_update: Option<DateTime<Utc>>,
}

impl JobController {
    pub fn new(
        spec: JobSpec,
        task: JoinHandle<types::Result<()>>,
        abort_handle: AbortHandle,
        channel_context: ChannelContext,
    ) -> Self {
        Self {
            spec,
            task,
            abort_handle,
            channel_context,
            prev_status_update: None,
        }
    }

    async fn update_status(
        spec: &JobSpec,
        channel_context: &ChannelContext,
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

        let status_data = musubi_api::types::to_value(status)
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

        channel_context
            .get_channel_start()
            .unwrap()
            .compute(channel_context.clone(), ComputeInput::Store { spec })
            .await?;

        Ok(())
    }

    pub async fn run(
        mut self,
        handle: String,
        updater: Sender<JobState>,
        mut updation_target: Receiver<JobState>,
        status_updater: Sender<JobState>,
    ) -> types::Result<ComputeOutput> {
        let completion_notifier = updater.clone();
        let job_handle = self.spec.handle.clone();

        let observable_task_future = async move {
            let result = self.task.await;
            match completion_notifier.send(JobState::Completed).await {
                Ok(_) => {
                    tracing::debug!("completion_notifier triggered for job '{}'!", job_handle);
                }
                Err(e) => {
                    tracing::debug!(
                        "failed to trigger completion_notifier for job '{}'. error: {}",
                        job_handle,
                        e
                    );
                }
            }

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

                        Self::update_status(
                            &self.spec,
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

                        Self::update_status(
                            &self.spec,
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
