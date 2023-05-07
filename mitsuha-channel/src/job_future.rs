use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Poll, Waker}, time::Duration,
};

use chrono::{DateTime, Utc};
use mitsuha_core::{channel::ComputeOutput, types};
use tokio::task::{JoinError, JoinHandle};

type JobOutput = types::Result<ComputeOutput>;

#[derive(Clone)]
pub enum JobState {
    Completed,
    Aborted,
    ExpireAt(DateTime<Utc>),
}

#[derive(Clone)]
pub enum FutureState {
    Pending(Waker),
    NotStarted,
    Completed,
}

pub struct JobFuture {
    handle: String,
    task: JoinHandle<JobOutput>,
    state: Arc<RwLock<JobState>>,
    fut_state: Arc<RwLock<FutureState>>,
}

impl Future for JobFuture {
    type Output = JobOutput;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let state_ptr = self.state.clone();
        let state_obj = state_ptr.read().unwrap().clone();
        let fut_state = self.fut_state.clone();

        let fut = unsafe { Pin::new_unchecked(&mut self.task) };

        if fut.is_finished() {
            let poll_value = Self::flatten_join_poll_value(fut.poll(cx));
            if let Poll::Ready(_) = poll_value {
                *fut_state.write().unwrap() = FutureState::Completed;
                *state_ptr.write().unwrap() = JobState::Completed;
            }

            return poll_value;
        }

        match state_obj {
            JobState::Aborted => {
                *fut_state.write().unwrap() = FutureState::Completed;

                fut.abort();
                Poll::Ready(Err(mitsuha_core::errors::Error::JobAborted {
                    handle: self.handle.clone(),
                }))
            }
            JobState::ExpireAt(x) if x <= Utc::now() => {
                *fut_state.write().unwrap() = FutureState::Completed;

                fut.abort();
                Poll::Ready(Err(mitsuha_core::errors::Error::JobExpired {
                    handle: self.handle.clone(),
                    expiry: x.to_string(),
                }))
            }
            _ => {
                *fut_state.write().unwrap() = FutureState::Pending(cx.waker().clone());
                Poll::Pending
            },
        }
    }
}

impl JobFuture {
    pub fn new(handle: String, task: JoinHandle<JobOutput>, state: Arc<RwLock<JobState>>) -> Self {
        let fut_state: Arc<RwLock<FutureState>> = Arc::new(RwLock::new(FutureState::NotStarted));
        let cloned_fut_state = fut_state.clone();

        tokio::task::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(1)).await;

                match cloned_fut_state.read().unwrap().clone() {
                    FutureState::Pending(waker) => waker.wake(),
                    FutureState::Completed => break,
                    _ => {}
                }
            }
        });

        Self {
            handle,
            task,
            state,
            fut_state,
        }
    }

    fn flatten_join_poll_value<T>(
        r: Poll<Result<Result<T, mitsuha_core::errors::Error>, JoinError>>,
    ) -> Poll<Result<T, mitsuha_core::errors::Error>> {
        match r {
            Poll::Pending => Poll::Pending,
            Poll::Ready(x) => {
                if let Err(e) = x {
                    Poll::Ready(Err(mitsuha_core::errors::Error::Unknown {
                        source: e.into(),
                    }))
                } else {
                    Poll::Ready(x.unwrap())
                }
            }
        }
    }
}
