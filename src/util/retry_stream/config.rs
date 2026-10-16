use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use crate::util::retry_stream::strategies::ExpBackoffStrategy;
use crate::util::retry_stream::stream::{
    ConnectionAttemptResult, ConnectionSeries, DisconnectReason,
};

pub type DurationIterator = Box<dyn Iterator<Item = Duration> + Send + Sync>;

#[derive(Clone)]
pub struct ReconnectOptions {
    pub retry_delays: Arc<dyn Fn() -> DurationIterator + Send + Sync>,
    pub initial_attempts: NonZeroUsize,
    pub reconnect_attempts: NonZeroUsize,
    pub connect_timeout: Duration,
    pub subscription_timeout: Duration,
    pub on_attempt_finished: Arc<dyn Fn(ConnectionAttemptResult, Duration) + Send + Sync>,
    pub on_connect: Arc<dyn Fn(u64) + Send + Sync>,
    pub on_disconnect: Arc<dyn Fn(DisconnectReason) + Send + Sync>,
    pub on_exhausted: Arc<dyn Fn(ConnectionSeries) + Send + Sync>,
}

impl ReconnectOptions {
    pub fn new() -> Self {
        Self {
            retry_delays: Arc::new(|| Box::new(ExpBackoffStrategy::default().into_iter())),
            initial_attempts: NonZeroUsize::new(10).expect("10 is nonzero"),
            reconnect_attempts: NonZeroUsize::new(10).expect("10 is nonzero"),
            connect_timeout: Duration::from_secs(10),
            subscription_timeout: Duration::from_secs(5),
            on_attempt_finished: Arc::new(|_, _| {}),
            on_connect: Arc::new(|_| {}),
            on_disconnect: Arc::new(|_| {}),
            on_exhausted: Arc::new(|_| {}),
        }
    }

    pub fn with_retries_generator<F, I, IN>(mut self, retries_generator: F) -> Self
    where
        F: 'static + Send + Sync + Fn() -> IN,
        I: 'static + Send + Sync + Iterator<Item = Duration>,
        IN: IntoIterator<IntoIter = I, Item = Duration>,
    {
        self.retry_delays = Arc::new(move || Box::new(retries_generator().into_iter()));
        self
    }

    pub fn with_attempt_limits(
        mut self,
        initial_attempts: NonZeroUsize,
        reconnect_attempts: NonZeroUsize,
    ) -> Self {
        self.initial_attempts = initial_attempts;
        self.reconnect_attempts = reconnect_attempts;
        self
    }

    pub fn with_timeouts(
        mut self,
        connect_timeout: Duration,
        subscription_timeout: Duration,
    ) -> Self {
        self.connect_timeout = connect_timeout;
        self.subscription_timeout = subscription_timeout;
        self
    }

    pub fn with_on_attempt_finished(
        mut self,
        callback: impl Fn(ConnectionAttemptResult, Duration) + 'static + Send + Sync,
    ) -> Self {
        self.on_attempt_finished = Arc::new(callback);
        self
    }

    pub fn with_on_connect(mut self, callback: impl Fn(u64) + 'static + Send + Sync) -> Self {
        self.on_connect = Arc::new(callback);
        self
    }

    pub fn with_on_disconnect(
        mut self,
        callback: impl Fn(DisconnectReason) + 'static + Send + Sync,
    ) -> Self {
        self.on_disconnect = Arc::new(callback);
        self
    }

    pub fn with_on_exhausted(
        mut self,
        callback: impl Fn(ConnectionSeries) + 'static + Send + Sync,
    ) -> Self {
        self.on_exhausted = Arc::new(callback);
        self
    }
}

impl Default for ReconnectOptions {
    fn default() -> Self {
        Self::new()
    }
}
