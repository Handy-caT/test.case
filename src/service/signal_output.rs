use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossfire::Rx;
use crossfire::mpsc::Array;
use prometheus::IntCounter;
use tokio::sync::Notify;

use crate::service::strategy::signal_generator::SignalResult;

pub struct SignalOutput {
    pub receiver: Rx<Array<SignalResult>>,
    pub result_space: Arc<Notify>,
    pub received: IntCounter,
    pub max_book_age: Duration,
}

impl SignalOutput {
    pub fn spawn(self) -> eyre::Result<()> {
        std::thread::Builder::new()
            .name("signal-stdout".to_owned())
            .spawn(move || {
                let stdout = std::io::stdout();
                while let Ok(signal) = self.receiver.recv() {
                    self.received.inc();
                    self.result_space.notify_one();
                    if signal.is_expired(Instant::now(), self.max_book_age) {
                        continue;
                    }
                    let mut output = stdout.lock();
                    if signal.is_expired(Instant::now(), self.max_book_age) {
                        continue;
                    }
                    if let Err(error) = writeln!(output, "{signal}") {
                        log::warn!("Signal stdout output stopped: {error}");
                        break;
                    }
                }
            })?;
        Ok(())
    }
}
