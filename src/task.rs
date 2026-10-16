use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub mod builder;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TaskState {
    Finished,
    Cancelled,
    Error,
}

pub struct Task {
    pub name: String,
    handle: JoinHandle<TaskState>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Task {
    pub fn cancel(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }

    pub async fn join_blocking(&mut self) -> eyre::Result<TaskState> {
        Ok((&mut self.handle).await?)
    }
}
