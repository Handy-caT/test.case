use crossfire::mpsc::Array;
use crossfire::stream::AsyncStream;
use tokio::sync::watch;

use crate::entity::exchange::response::PartialOrderBookDepth;
use crate::service::exchange::status::{FeedStatus, FeedStatusSender};
use crate::task::Task;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DepthParseError {
    Malformed,
    Invalid,
}

pub struct DepthSubscription {
    pub task: Task,
    pub stream: AsyncStream<Array<PartialOrderBookDepth>>,
    pub status_sender: FeedStatusSender,
    pub status_receiver: watch::Receiver<FeedStatus>,
}
