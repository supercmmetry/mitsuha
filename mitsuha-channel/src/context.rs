use std::sync::{Arc, RwLock};

use chrono::{Duration, Utc};
use dashmap::DashMap;
use mitsuha_core::{
    kernel::{JobStatus, JobStatusType},
    types,
};

use crate::{job_future::JobState, system::SystemContext};
use mitsuha_core::errors::Error;

#[derive(Default)]
pub struct ChannelContext {
    job_states: DashMap<String, Arc<RwLock<JobState>>>,
}

impl SystemContext for ChannelContext {
    fn get_job_status(&self, handle: &String) -> types::Result<JobStatus> {
        match self.job_states.get(handle) {
            Some(state) => {
                let obj = state.read().unwrap();

                let job_status_type = match *obj {
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

    fn extend_job(&self, handle: &String, ttl: u64) -> types::Result<()> {
        match self.job_states.get_mut(handle) {
            Some(state) => {
                let mut obj = state.write().unwrap();

                match *obj {
                    JobState::ExpireAt(x) if x > Utc::now() => {
                        *obj = JobState::ExpireAt(x + Duration::seconds(ttl as i64));
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

    fn abort_job(&self, handle: &String) -> types::Result<()> {
        match self.job_states.get_mut(handle) {
            Some(state) => {
                let mut obj = state.write().unwrap();

                match *obj {
                    JobState::ExpireAt(x) if x > Utc::now() => {
                        *obj = JobState::Aborted;
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

    fn make_job_state(&self, handle: String, state: JobState) -> Arc<RwLock<JobState>> {
        let state = Arc::new(RwLock::new(state));
        self.job_states.insert(handle, state.clone());

        state
    }
}
