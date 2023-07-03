use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use dashmap::DashMap;
use mitsuha_core::{
    kernel::{JobStatus, JobStatusType},
    types,
};

use crate::{job_controller::JobState, system::{SystemContext, JobContext}};
use mitsuha_core::errors::Error;


#[derive(Default, Clone)]
pub struct ChannelContext {
    job_context_map: Arc<DashMap<String, JobContext>>,
}

#[async_trait]
impl SystemContext for ChannelContext {
    async fn get_job_status(&self, handle: &String) -> types::Result<JobStatus> {
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

    async fn extend_job(&self, handle: &String, ttl: u64) -> types::Result<()> {
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

    async fn abort_job(&self, handle: &String) -> types::Result<()> {
        match self.job_context_map.get_mut(handle) {
            Some(mut ctx) => {
                let mut obj = ctx.get_state().unwrap();

                match obj {
                    JobState::ExpireAt(x) if x > Utc::now() => {
                        obj = JobState::Aborted;
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

    fn register_job_context(&self, handle: String, ctx: JobContext) {
        self.job_context_map.insert(handle, ctx);
    }
}
