use std::sync::Arc;

use chrono::{Duration, Utc};
use dashmap::DashMap;
use mitsuha_core::{
    channel::ComputeChannel,
    kernel::{JobStatus, JobStatusType},
    types,
};

use crate::{job_controller::JobState, system::JobContext};
use mitsuha_core::errors::Error;

#[derive(Default, Clone)]
pub struct ChannelContext {
    job_context_map: Arc<DashMap<String, JobContext>>,
    pub channel_start: Option<Arc<Box<dyn ComputeChannel<Context = Self>>>>,
    pub channel_map: Arc<DashMap<String, Arc<Box<dyn ComputeChannel<Context = Self>>>>>,
}

impl ChannelContext {
    pub async fn get_job_status(&self, handle: &String) -> types::Result<JobStatus> {
        match self.job_context_map.get_mut(handle) {
            Some(mut ctx) => {
                let obj = ctx.get_state().unwrap();

                let job_status_type = match obj {
                    JobState::Aborted => JobStatusType::Aborted,
                    JobState::Completed => JobStatusType::Completed,
                    JobState::ExpireAt(x) if x <= Utc::now() => {
                        JobStatusType::ExpiredAt { datetime: x }
                    }
                    _ => JobStatusType::Running,
                };

                Ok(JobStatus {
                    status: job_status_type,
                    extensions: Default::default(),
                })
            }
            None => Err(Error::JobNotFound {
                handle: handle.clone(),
            }),
        }
    }

    pub async fn extend_job(&self, handle: &String, ttl: u64) -> types::Result<()> {
        match self.job_context_map.get_mut(handle) {
            Some(mut ctx) => {
                let mut obj = ctx.get_state().unwrap();

                match obj {
                    JobState::ExpireAt(x) if x > Utc::now() => {
                        obj = JobState::ExpireAt(x + Duration::seconds(ttl as i64));
                        ctx.set_state(obj).await.unwrap();

                        Ok(())
                    }
                    _ => Err(Error::JobNotFound {
                        handle: handle.clone(),
                    }),
                }
            }
            None => Err(Error::JobNotFound {
                handle: handle.clone(),
            }),
        }
    }

    pub async fn abort_job(&self, handle: &String) -> types::Result<()> {
        match self.job_context_map.get_mut(handle) {
            Some(mut ctx) => {
                let mut obj = ctx.get_state().unwrap();

                match obj {
                    JobState::ExpireAt(x) if x > Utc::now() => {
                        obj = JobState::Aborted;
                        ctx.set_state(obj).await.unwrap();

                        Ok(())
                    }
                    x => {
                        tracing::warn!("cannot abort job as JobState='{:?}'", x);

                        Err(Error::JobNotFound {
                            handle: handle.clone(),
                        })
                    }
                }
            }
            None => Err(Error::JobNotFound {
                handle: handle.clone(),
            }),
        }
    }

    pub fn register_job_context(&self, handle: String, ctx: JobContext) {
        tracing::info!("registering job context for handle '{}'", handle);

        self.job_context_map.insert(handle, ctx);
    }

    pub fn deregister_job_context(&self, handle: &String) {
        tracing::info!("deregistering job context for handle '{}'", handle);

        self.job_context_map.remove(handle);
    }

    pub fn get_channel_start(&self) -> Option<Arc<Box<dyn ComputeChannel<Context = Self>>>> {
        self.channel_start.clone()
    }

    pub fn set_channel_start(&mut self, channel: Arc<Box<dyn ComputeChannel<Context = Self>>>) {
        self.channel_start = Some(channel);
    }
}
