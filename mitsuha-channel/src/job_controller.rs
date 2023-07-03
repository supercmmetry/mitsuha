use chrono::{DateTime, Utc};
use mitsuha_core::{channel::ComputeOutput, types, errors::Error};
use tokio::{task::JoinHandle, sync::mpsc::{Sender, Receiver}};


#[derive(Clone, Eq, PartialEq)]
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

    pub async fn run(self, handle: String, updater: Sender<JobState>, mut updation_target: Receiver<JobState>, status_updater: Sender<JobState>) -> types::Result<ComputeOutput> {
        let completion_notifier = updater.clone();
        
        let observable_task: JoinHandle<types::Result<()>> = tokio::task::spawn(async move {
            self.task.await.map_err(|e| Error::Unknown { source: e.into() })??;
            _ = completion_notifier.send(JobState::Completed).await;

            Ok(())
        });

        let mut max_expiry = Utc::now();
        
        loop {

            match updation_target.recv().await {
                Some(x) => {
                    match x {
                        JobState::ExpireAt(x) if x <= Utc::now() => {
                            observable_task.abort();
                            _ = observable_task.await;
                            return Err(Error::JobExpired {
                                handle, expiry: x.to_string()
                            });
                        },
                        JobState::Aborted => {
                            observable_task.abort();
                            _ = observable_task.await;
                            _ = status_updater.send(JobState::Aborted).await;

                            return Err(Error::JobAborted {
                                handle,
                            });
                        },
                        JobState::Completed => {
                            observable_task.await.map_err(|e| Error::Unknown { source: e.into() })??;
                            _ = status_updater.send(JobState::Completed).await;
                            return Ok(ComputeOutput::Completed);
                        },
                        JobState::ExpireAt(x) if x > Utc::now() => {
                            if x >= max_expiry {
                                max_expiry = x;
                                _ = updater.send(JobState::ExpireAt(x)).await;
                            }
                        },
                        _ => {}
                    }
                },
                _ => {}
            }
        }
    }
}