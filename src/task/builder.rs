use std::future::Future;

use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::task::{Task, TaskState};

pub struct TaskBuilder {
    name: String,
    run: Option<BoxFuture<'static, eyre::Result>>,
}

impl TaskBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            run: None,
        }
    }

    pub fn with_task<F>(mut self, future: F) -> Self
    where
        F: Future<Output = eyre::Result<()>> + Send + 'static,
    {
        self.run = Some(future.boxed());
        self
    }

    pub fn spawn(self) -> Task {
        let Self { name, run } = self;
        let run = run.expect("TaskBuilder: no task defined; call .with_task() first");
        let span = tracing::error_span!("task", name = %name);
        let (shutdown, mut shutdown_receiver) = oneshot::channel();
        let handle = tokio::spawn(
            async move {
                log::debug!("Task started");
                let state = tokio::select! {
                    biased;
                    _ = &mut shutdown_receiver => TaskState::Cancelled,
                    result = run => match result {
                        Ok(()) => TaskState::Finished,
                        Err(error) => {
                            log::error!("Task failed: {error:#}");
                            TaskState::Error
                        },
                    },
                };
                log::debug!("Task ended: {state:?}");
                state
            }
            .instrument(span),
        );

        Task {
            name,
            handle,
            shutdown: Some(shutdown),
        }
    }
}
