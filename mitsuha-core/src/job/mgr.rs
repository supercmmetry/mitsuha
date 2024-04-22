use crate::channel::{ComputeChannel, StateProvider};
use crate::config::Config;
use crate::constants::Constants;
use crate::errors::{Error, ToUnknownErrorResult};
use crate::job::cost::{JobCost, JobCostEvaluator};
use crate::job::ctrl::{JobController, PostJobHook};
use crate::job::ctx::{JobContext, JobState};
use crate::{metric, types};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use dashmap::DashMap;
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};
use mitsuha_core_types::kernel::{JobSpec, JobStatus, JobStatusType};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::log::kv::Source;

#[async_trait]
pub trait JobManagerProvider: Send + Sync + Clone {
    async fn get_job_mgr(&self) -> JobManager<Self>;
}

#[derive(Clone)]
pub struct JobManager<Context: Clone> {
    instance_id: String,
    channel: Arc<Box<dyn ComputeChannel<Context = Context>>>,
    channel_context: Arc<Box<Context>>,
    job_context_map: Arc<DashMap<String, JobContext>>,
    queued_jobs: Arc<RwLock<HashMap<String, JobCost>>>,
    maximum_concurrent_cost: Arc<JobCost>,
    current_concurrent_cost: Arc<RwLock<JobCost>>,
    job_cost_evaluator: Arc<Box<dyn JobCostEvaluator>>,
    post_job_hooks: Arc<RwLock<Vec<Arc<dyn PostJobHook<Context>>>>>,
}

impl<Context> JobManager<Context>
where
    Context: 'static + JobManagerProvider + StateProvider + Clone,
{
    pub fn new(
        channel: Arc<Box<dyn ComputeChannel<Context = Context>>>,
        channel_context: Arc<Box<Context>>,
        max_concurrent_cost: JobCost,
        job_cost_evaluator: Arc<Box<dyn JobCostEvaluator>>,
        instance_id: String,
    ) -> types::Result<Self> {
        let obj = Self {
            channel,
            channel_context,
            job_context_map: Arc::new(DashMap::new()),
            queued_jobs: Arc::new(RwLock::new(HashMap::new())),
            maximum_concurrent_cost: Arc::new(max_concurrent_cost),
            current_concurrent_cost: Arc::new(RwLock::new(Default::default())),
            job_cost_evaluator,
            instance_id,
            post_job_hooks: Arc::new(RwLock::new(Vec::new())),
        };

        Ok(obj)
    }

    pub fn get_instance_id(&self) -> String {
        self.instance_id.clone()
    }

    pub fn get_job_cost_evaluator(&self) -> Arc<Box<dyn JobCostEvaluator>> {
        self.job_cost_evaluator.clone()
    }

    pub async fn get_queued_jobs_count(&self) -> usize {
        self.queued_jobs.read().await.len()
    }

    pub async fn queue_job(&self, spec: &JobSpec) -> types::Result<bool> {
        let mut queued_jobs = self.queued_jobs.write().await;

        let current_cost = self.current_concurrent_cost.read().await.clone();

        let job_cost = self.job_cost_evaluator.get_cost(spec)?;

        metric::job_request_count_metric()
            .with_label_values(&[self.instance_id.as_str()])
            .inc();

        if current_cost + job_cost.clone() > *self.maximum_concurrent_cost.deref() {
            return Ok(false);
        }

        *self.current_concurrent_cost.write().await += job_cost.clone();

        queued_jobs.insert(spec.handle.clone(), job_cost.clone());

        // Track job cost metrics after queueing successfully
        metric::job_queued_compute_cost_metric()
            .with_label_values(&[self.instance_id.as_str()])
            .observe(job_cost.compute as f64);

        Ok(true)
    }

    pub async fn get_job_cost(&self, handle: &String) -> Option<JobCost> {
        self.queued_jobs.read().await.get(handle).cloned()
    }

    pub async fn dequeue_job(&self, handle: &String) -> types::Result<()> {
        tracing::info!("dequeuing job");

        if let Some(cost) = self.queued_jobs.write().await.remove(handle) {
            *self.current_concurrent_cost.write().await -= cost;
        }

        Ok(())
    }

    pub async fn get_local_job_status(&self, handle: &String) -> types::Result<JobStatus> {
        match self.job_context_map.get_mut(handle) {
            Some(mut ctx) => {
                // Get the observed state of the job
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
                    extensions: [(
                        Constants::JobStatusLastUpdated.to_string(),
                        Utc::now().to_rfc3339(),
                    )]
                    .into_iter()
                    .collect(),
                })
            }
            None => Err(Error::JobNotFound {
                handle: handle.clone(),
            }),
        }
    }

    pub async fn get_job_status(
        &self,
        handle: &String,
        extensions: HashMap<String, String>,
    ) -> types::Result<JobStatus> {
        match self.get_local_job_status(handle).await {
            Ok(status) => Ok(status),
            Err(_) => {
                // Could not find job_status in local context. Fetch it from FS.

                let output = self
                    .channel
                    .compute(
                        *self.channel_context.clone().deref().clone(),
                        ComputeInput::Load {
                            handle: JobSpec::to_status_handle(&handle),
                            extensions,
                        },
                    )
                    .await;

                match output {
                    Ok(ComputeOutput::Loaded { data }) => {
                        let raw_value: musubi_api::types::Value =
                            data.try_into().to_unknown_err_result()?;

                        let job_status: JobStatus =
                            musubi_api::types::from_value(&raw_value).to_unknown_err_result()?;

                        return Ok(job_status);
                    }
                    Err(e) => {
                        tracing::error!("failed to get job status from storage. error: {}", e);
                    }
                    _ => {}
                }

                Err(Error::JobNotFound {
                    handle: handle.clone(),
                })
            }
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

    pub async fn abort_all_jobs(&self) -> types::Result<usize> {
        let mut aborted_job_count = 0usize;

        let mut job_handles = Vec::new();

        for entry in self.job_context_map.iter() {
            job_handles.push(entry.key().clone());
        }

        for job_handle in job_handles {
            let result = self.abort_job(&job_handle).await;

            match result {
                Ok(()) => {
                    aborted_job_count += 1;
                }
                Err(e) => {
                    tracing::error!(
                        "failed to abort job with handle='{}', error: {}",
                        job_handle,
                        e
                    );
                }
            }
        }

        Ok(aborted_job_count)
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
        tracing::info!("registering job context");

        self.job_context_map.insert(handle, ctx);
    }

    pub fn deregister_job_context(&self, handle: &String) {
        tracing::info!("removing job context");

        self.job_context_map.remove(handle);
    }

    pub async fn add_post_job_hook(&self, hook: Arc<dyn PostJobHook<Context>>) {
        self.post_job_hooks.write().await.push(hook);
    }

    pub async fn inject_post_job_hooks(&self, ctrl: &mut JobController<Context>) {
        self.post_job_hooks
            .read()
            .await
            .iter()
            .for_each(|x| ctrl.add_post_job_hook(x.clone()));
    }
}
