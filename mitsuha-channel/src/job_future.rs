use std::{future::Future, sync::{Arc, RwLock}, pin::Pin, task::Poll};

use chrono::{Utc, DateTime};
use mitsuha_core::{types, channel::ComputeOutput};
use tokio::task::{JoinHandle, JoinError};

type JobOutput = types::Result<ComputeOutput>;

#[derive(Clone)]
pub enum JobState {
    Completed,
    Aborted,
    ExpireAt(DateTime<Utc>)
}

pub struct JobFuture {
    handle: String,
    task: JoinHandle<JobOutput>,
    state: Arc<RwLock<JobState>>,
}

impl Future for JobFuture {
    type Output = JobOutput;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let state_ptr = self.state.clone();
        let state_obj = state_ptr.read().unwrap().clone();

        let fut = unsafe {
            Pin::new_unchecked(&mut self.task)
        };

        if fut.is_finished() {
            let poll_value = Self::flatten_join_poll_value(fut.poll(cx));
            if let Poll::Ready(_) = poll_value {
                *state_ptr.write().unwrap() = JobState::Completed;
            }

            return poll_value;
        }

        match state_obj {
            JobState::Aborted => {
                fut.abort();
                Poll::Ready(Err(mitsuha_core::errors::Error::JobAborted { handle: self.handle.clone() }))
            },
            JobState::ExpireAt(x) if x <= Utc::now() => {
                fut.abort();
                Poll::Ready(Err(mitsuha_core::errors::Error::JobExpired { handle: self.handle.clone(), expiry: x.to_string() }))
            },
            _ => Self::flatten_join_poll_value(fut.poll(cx))
        }
    }
}

impl JobFuture {
    pub fn new(handle: String, task: JoinHandle<JobOutput>, state: Arc<RwLock<JobState>>) -> Self {
        Self {
            handle,
            task,
            state,
        }
    }

    fn flatten_join_poll_value<T>(r: Poll<Result<Result<T, mitsuha_core::errors::Error>, JoinError>>) -> Poll<Result<T, mitsuha_core::errors::Error>> {
        match r {
            Poll::Pending => Poll::Pending,
            Poll::Ready(x) => {
                if let Err(e) = x {
                    Poll::Ready(Err(mitsuha_core::errors::Error::Unknown { source: e.into() }))
                } else {
                    Poll::Ready(x.unwrap())
                }
            }
        }
    }
}