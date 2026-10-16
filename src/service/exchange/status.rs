use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FeedStatus {
    pub session_id: u64,
    pub connected: bool,
}

#[derive(Clone)]
pub struct FeedStatusSender {
    sender: watch::Sender<FeedStatus>,
}

impl FeedStatusSender {
    pub fn new() -> (Self, watch::Receiver<FeedStatus>) {
        let (sender, receiver) = watch::channel(FeedStatus::default());
        (Self { sender }, receiver)
    }

    pub fn connected(&self, session_id: u64) {
        self.sender.send_replace(FeedStatus {
            session_id,
            connected: true,
        });
    }

    pub fn disconnected(&self) {
        let session_id = self.sender.borrow().session_id;
        self.sender.send_replace(FeedStatus {
            session_id,
            connected: false,
        });
    }

    pub fn publish_if_connected(&self, session_id: u64, publish: impl FnOnce()) -> bool {
        let mut publish = Some(publish);
        let mut published = false;
        self.sender.send_if_modified(|status| {
            if status.connected && status.session_id == session_id {
                publish
                    .take()
                    .expect("session publication executes at most once")();
                published = true;
            }
            false
        });
        published
    }
}
