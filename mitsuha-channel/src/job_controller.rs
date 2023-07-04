use chrono::{DateTime, Utc};
use mitsuha_core::{channel::ComputeOutput, errors::Error, types};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum JobState {
    Completed,
    Aborted,
    ExpireAt(DateTime<Utc>),
}

pub struct JobController {
    task: JoinHandle<types::Result<()>>,
}

impl JobController {
    pub fn new(task: JoinHandle<types::Result<()>>) -> Self {
        Self { task }
    }

    pub async fn run(
        self,
        handle: String,
        updater: Sender<JobState>,
        mut updation_target: Receiver<JobState>,
        status_updater: Sender<JobState>,
    ) -> types::Result<ComputeOutput> {
        let completion_notifier = updater.clone();

        let observable_task: JoinHandle<types::Result<()>> = tokio::task::spawn(async move {
            let result = self.task.await;
            _ = completion_notifier.send(JobState::Completed).await;

            result.map_err(|e| Error::Unknown { source: e.into() })?
        });

        let mut max_expiry = Utc::now();

        loop {
            match updation_target.recv().await {
                Some(x) => match x {
                    JobState::ExpireAt(x) if x <= Utc::now() => {
                        observable_task.abort();
                        _ = observable_task.await;

                        log::info!(
                            "job with handle: '{}' expired at '{}'",
                            &handle,
                            x.to_string()
                        );

                        return Err(Error::JobExpired {
                            handle,
                            expiry: x.to_string(),
                        });
                    }
                    JobState::Aborted => {
                        observable_task.abort();
                        _ = observable_task.await;
                        _ = status_updater.send(JobState::Aborted).await;

                        log::info!("job with handle: '{}' was aborted", &handle);

                        return Err(Error::JobAborted { handle });
                    }
                    JobState::Completed => {
                        let result = observable_task.await;
                        _ = status_updater.send(JobState::Completed).await;

                        result.map_err(|e| Error::Unknown { source: e.into() })??;

                        log::info!("job with handle: '{}' was completed", &handle);

                        return Ok(ComputeOutput::Completed);
                    }
                    JobState::ExpireAt(x) if x > Utc::now() => {
                        if x >= max_expiry {
                            max_expiry = x;
                            _ = updater.send(JobState::ExpireAt(x)).await;
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }
    }
}
