use crate::errors::{Error, ToUnknownErrorResult};
use crate::types;
use chrono::{DateTime, Utc};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum JobState {
    Completed,
    Aborted,
    ExpireAt(DateTime<Utc>),
}

pub struct JobContext {
    handle: String,
    updater: Sender<JobState>,
    reader: Receiver<JobState>,
    desired: JobState,
    actual: JobState,
}

impl JobContext {
    pub async fn new(
        handle: String,
        updater: Sender<JobState>,
        reader: Receiver<JobState>,
        desired: JobState,
    ) -> Self {
        updater.send(desired.clone()).await.unwrap();

        Self {
            handle,
            updater,
            reader,
            actual: desired.clone(),
            desired,
        }
    }

    pub async fn set_state(&mut self, desired: JobState) -> types::Result<()> {
        self.desired = desired;

        if self.desired != self.actual {
            tracing::info!(
                "updating job context from {:?} to {:?} for handle: '{}'",
                self.actual,
                self.desired,
                self.handle
            );

            self.updater
                .send(self.desired.clone())
                .await
                .to_unknown_err_result()?;
        }

        Ok(())
    }

    pub fn get_state(&mut self) -> types::Result<JobState> {
        match self.reader.try_recv() {
            Ok(x) => {
                tracing::info!(
                    "received new job state {:?} for handle: '{}'",
                    x,
                    self.handle
                );

                self.actual = x;
            }
            Err(_) => {}
        }

        Ok(self.actual.clone())
    }
}
