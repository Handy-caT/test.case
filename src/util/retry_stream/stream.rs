use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::task::AtomicWaker;
use futures::{Sink, Stream};
use tokio_tungstenite::tungstenite::Message;

use crate::util::retry_stream::ReconnectOptions;

pub trait UnderlyingStream<C, I, E>
where
    C: Clone + Send + Unpin,
    E: Error,
{
    type Stream: Sized + Unpin + 'static;

    fn establish(
        ctor_arg: C,
        subscribe: Vec<Message>,
        connect_timeout: Duration,
        subscription_timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Stream, E>> + Send>>;

    fn is_write_disconnect_error(err: &E) -> bool;

    fn read_disconnect_reason(_item: &I) -> Option<DisconnectReason> {
        None
    }

    fn is_timeout_error(_err: &E) -> bool {
        false
    }

    fn exhaust_err() -> E;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionAttemptResult {
    Success,
    Error,
    Timeout,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionSeries {
    Initial,
    Recovery,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum DisconnectReason {
    ReadError = 1,
    WriteError = 2,
    Close = 3,
    EndOfStream = 4,
    UsefulDataTimeout = 5,
    PingTimeout = 6,
    PingWriteTimeout = 7,
}

impl DisconnectReason {
    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::ReadError),
            2 => Some(Self::WriteError),
            3 => Some(Self::Close),
            4 => Some(Self::EndOfStream),
            5 => Some(Self::UsefulDataTimeout),
            6 => Some(Self::PingTimeout),
            7 => Some(Self::PingWriteTimeout),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct ReconnectControl {
    shared: Arc<ControlState>,
}

#[derive(Default)]
struct ControlState {
    session_id: AtomicU64,
    force_disconnect: AtomicU8,
    expected_write_session: AtomicU64,
    waker: AtomicWaker,
}

impl ReconnectControl {
    pub fn session_id(&self) -> u64 {
        self.shared.session_id.load(Ordering::Acquire)
    }

    pub fn force_disconnect(&self, reason: DisconnectReason) {
        self.shared
            .force_disconnect
            .store(reason as u8, Ordering::Release);
        self.shared.waker.wake();
    }

    pub fn require_write_session(&self, session_id: u64) {
        self.shared
            .expected_write_session
            .store(session_id, Ordering::Release);
    }

    pub fn clear_write_session_requirement(&self) {
        self.shared
            .expected_write_session
            .store(0, Ordering::Release);
    }
}

struct AttemptCompletion<S, E> {
    result: Result<S, E>,
    duration: Duration,
}

type ReconnectAttempt<S, E> = Pin<Box<dyn Future<Output = AttemptCompletion<S, E>> + Send>>;

struct ReconnectStatus<T, C, I, E>
where
    T: UnderlyingStream<C, I, E>,
    C: Clone + Send + Unpin,
    E: Error,
{
    retry_delays: crate::util::retry_stream::config::DurationIterator,
    attempts_started: usize,
    reconnect_attempt: Option<ReconnectAttempt<T::Stream, E>>,
    _marker: PhantomData<(C, I)>,
}

pub struct ReconnectStream<T, C, I, E>
where
    T: UnderlyingStream<C, I, E>,
    C: Clone + Send + Unpin,
    E: Error,
{
    status: Status<T, C, I, E>,
    stream: T::Stream,
    options: ReconnectOptions,
    ctor_arg: C,
    subscribe: Vec<Message>,
    control: Arc<ControlState>,
}

enum Status<T, C, I, E>
where
    T: UnderlyingStream<C, I, E>,
    C: Clone + Send + Unpin,
    E: Error,
{
    Connected,
    Disconnected(ReconnectStatus<T, C, I, E>),
    FailedAndExhausted,
}

impl<T, C, I, E> ReconnectStream<T, C, I, E>
where
    T: UnderlyingStream<C, I, E>,
    C: Clone + Send + Unpin + 'static,
    I: Unpin,
    E: Error + Unpin,
{
    pub async fn connect_with_options(
        ctor_arg: C,
        subscription: Vec<Message>,
        options: ReconnectOptions,
    ) -> Result<Self, E> {
        let mut retry_delays = (options.retry_delays)();
        let control = Arc::new(ControlState::default());
        let mut last_error = None;

        for attempt_index in 0..options.initial_attempts.get() {
            if attempt_index > 0 {
                tokio::time::sleep(retry_delays.next().unwrap_or_default()).await;
            }

            let completion = Self::establish_attempt(
                ctor_arg.clone(),
                subscription.clone(),
                options.connect_timeout,
                options.subscription_timeout,
            )
            .await;
            (options.on_attempt_finished)(
                Self::attempt_result(&completion.result),
                completion.duration,
            );

            match completion.result {
                Ok(stream) => {
                    let session_id = Self::advance_session(&control);
                    (options.on_connect)(session_id);
                    log::info!(
                        "WebSocket connected: series=initial attempt={}/{} session_id={session_id}",
                        attempt_index + 1,
                        options.initial_attempts.get()
                    );
                    return Ok(Self {
                        status: Status::Connected,
                        stream,
                        options,
                        ctor_arg,
                        subscribe: subscription,
                        control,
                    });
                }
                Err(error) => {
                    let result = if T::is_timeout_error(&error) {
                        ConnectionAttemptResult::Timeout
                    } else {
                        ConnectionAttemptResult::Error
                    };
                    log::debug!(
                        "WebSocket connection attempt failed: series=initial attempt={}/{} result={:?} error={error}",
                        attempt_index + 1,
                        options.initial_attempts.get(),
                        result
                    );
                    if attempt_index == 0 && attempt_index + 1 < options.initial_attempts.get() {
                        log::warn!(
                            "initial WebSocket connection failed; retrying: attempt={}/{} error={error}",
                            attempt_index + 1,
                            options.initial_attempts.get()
                        );
                    }
                    last_error = Some(error);
                }
            }
        }

        (options.on_exhausted)(ConnectionSeries::Initial);
        Err(last_error.expect("positive attempt limit performs at least one attempt"))
    }

    pub fn control(&self) -> ReconnectControl {
        ReconnectControl {
            shared: Arc::clone(&self.control),
        }
    }

    async fn establish_attempt(
        ctor_arg: C,
        subscription: Vec<Message>,
        connect_timeout: Duration,
        subscription_timeout: Duration,
    ) -> AttemptCompletion<T::Stream, E> {
        let started = Instant::now();
        let result = T::establish(
            ctor_arg,
            subscription,
            connect_timeout,
            subscription_timeout,
        )
        .await;
        AttemptCompletion {
            result,
            duration: started.elapsed(),
        }
    }

    fn attempt_result(result: &Result<T::Stream, E>) -> ConnectionAttemptResult {
        match result {
            Ok(_) => ConnectionAttemptResult::Success,
            Err(error) if T::is_timeout_error(error) => ConnectionAttemptResult::Timeout,
            Err(_) => ConnectionAttemptResult::Error,
        }
    }

    fn advance_session(control: &ControlState) -> u64 {
        let session_id = control
            .session_id
            .load(Ordering::Relaxed)
            .wrapping_add(1)
            .max(1);
        control.session_id.store(session_id, Ordering::Release);
        session_id
    }

    fn schedule_reconnect_attempt(&mut self) {
        let max_attempts = self.options.reconnect_attempts.get();
        let Status::Disconnected(status) = &mut self.status else {
            return;
        };

        if status.attempts_started >= max_attempts {
            self.status = Status::FailedAndExhausted;
            (self.options.on_exhausted)(ConnectionSeries::Recovery);
            return;
        }

        status.attempts_started += 1;
        let delay = status.retry_delays.next().unwrap_or_default();
        let ctor_arg = self.ctor_arg.clone();
        let subscribe = self.subscribe.clone();
        let connect_timeout = self.options.connect_timeout;
        let subscription_timeout = self.options.subscription_timeout;
        status.reconnect_attempt = Some(Box::pin(async move {
            tokio::time::sleep(delay).await;
            Self::establish_attempt(ctor_arg, subscribe, connect_timeout, subscription_timeout)
                .await
        }));
    }

    fn on_disconnect(mut self: Pin<&mut Self>, reason: DisconnectReason, cx: &mut Context<'_>) {
        let this = self.as_mut().get_mut();
        if !matches!(this.status, Status::Connected) {
            return;
        }

        (this.options.on_disconnect)(reason);
        this.status = Status::Disconnected(ReconnectStatus {
            retry_delays: (this.options.retry_delays)(),
            attempts_started: 0,
            reconnect_attempt: None,
            _marker: PhantomData,
        });
        this.schedule_reconnect_attempt();
        cx.waker().wake_by_ref();
    }

    fn apply_forced_disconnect(mut self: Pin<&mut Self>, cx: &mut Context<'_>) {
        self.control.waker.register(cx.waker());
        let code = self.control.force_disconnect.swap(0, Ordering::AcqRel);
        let reason = DisconnectReason::from_code(code);

        if let Some(reason) = reason {
            self.as_mut().on_disconnect(reason, cx);
        }
    }

    fn poll_disconnect(mut self: Pin<&mut Self>, cx: &mut Context<'_>) {
        let completion = {
            let this = self.as_mut().get_mut();
            let Status::Disconnected(status) = &mut this.status else {
                unreachable!("only a disconnected stream polls a reconnect")
            };
            let reconnect_attempt = status
                .reconnect_attempt
                .as_mut()
                .expect("a reconnect attempt is scheduled before polling");
            match reconnect_attempt.as_mut().poll(cx) {
                Poll::Ready(completion) => completion,
                Poll::Pending => return,
            }
        };

        (self.options.on_attempt_finished)(
            Self::attempt_result(&completion.result),
            completion.duration,
        );
        let (attempt, limit) = match &self.status {
            Status::Disconnected(status) => (
                status.attempts_started,
                self.options.reconnect_attempts.get(),
            ),
            _ => unreachable!("a reconnect completion belongs to a disconnected stream"),
        };

        match completion.result {
            Ok(stream) => {
                let this = self.as_mut().get_mut();
                this.stream = stream;
                this.status = Status::Connected;
                let session_id = Self::advance_session(&this.control);
                (this.options.on_connect)(session_id);
                log::info!(
                    "WebSocket reconnected: series=recovery attempt={attempt}/{limit} session_id={session_id}"
                );
                cx.waker().wake_by_ref();
            }
            Err(error) => {
                let this = self.as_mut().get_mut();
                let exhausted = match &this.status {
                    Status::Disconnected(status) => {
                        status.attempts_started >= this.options.reconnect_attempts.get()
                    }
                    _ => unreachable!("failed reconnect remains disconnected until exhausted"),
                };
                let result = if T::is_timeout_error(&error) {
                    ConnectionAttemptResult::Timeout
                } else {
                    ConnectionAttemptResult::Error
                };
                log::debug!(
                    "WebSocket recovery attempt failed: series=recovery attempt={attempt}/{limit} result={:?} error={error}",
                    result
                );
                if exhausted {
                    this.status = Status::FailedAndExhausted;
                    (this.options.on_exhausted)(ConnectionSeries::Recovery);
                    log::error!(
                        "WebSocket recovery attempts exhausted: attempt={attempt}/{limit} last_error={error}"
                    );
                } else {
                    this.schedule_reconnect_attempt();
                }
                cx.waker().wake_by_ref();
            }
        }
    }
}

impl<T, C, I, E> Stream for ReconnectStream<T, C, I, E>
where
    T: UnderlyingStream<C, I, E>,
    T::Stream: Stream<Item = I>,
    C: Clone + Send + Unpin + 'static,
    I: Unpin,
    E: Error + Unpin,
{
    type Item = I;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().apply_forced_disconnect(cx);

        match self.as_mut().get_mut().status {
            Status::Connected => {
                let poll = {
                    let this = self.as_mut().get_mut();
                    Pin::new(&mut this.stream).poll_next(cx)
                };
                match poll {
                    Poll::Ready(Some(item)) => {
                        if let Some(reason) = T::read_disconnect_reason(&item) {
                            self.as_mut().on_disconnect(reason, cx);
                        }
                        Poll::Ready(Some(item))
                    }
                    Poll::Ready(None) => {
                        self.as_mut()
                            .on_disconnect(DisconnectReason::EndOfStream, cx);
                        Poll::Pending
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            Status::Disconnected(_) => {
                self.poll_disconnect(cx);
                Poll::Pending
            }
            Status::FailedAndExhausted => Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<T, C, I, I2, E> Sink<I> for ReconnectStream<T, C, I2, E>
where
    T: UnderlyingStream<C, I2, E>,
    T::Stream: Sink<I, Error = E>,
    C: Clone + Send + Unpin + 'static,
    I2: Unpin,
    E: Error + Unpin,
{
    type Error = E;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        self.as_mut().apply_forced_disconnect(cx);
        let this = self.as_mut().get_mut();
        let expected_session = this.control.expected_write_session.load(Ordering::Acquire);
        if expected_session != 0
            && expected_session != this.control.session_id.load(Ordering::Acquire)
        {
            return Poll::Ready(Err(T::exhaust_err()));
        }

        match this.status {
            Status::Connected => {
                let poll = Pin::new(&mut this.stream).poll_ready(cx);
                if matches!(&poll, Poll::Ready(Err(error)) if T::is_write_disconnect_error(error)) {
                    let reason = DisconnectReason::WriteError;
                    self.as_mut().on_disconnect(reason, cx);
                    Poll::Pending
                } else {
                    poll
                }
            }
            Status::Disconnected(_) => {
                self.poll_disconnect(cx);
                Poll::Pending
            }
            Status::FailedAndExhausted => Poll::Ready(Err(T::exhaust_err())),
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: I) -> Result<(), E> {
        let this = self.as_mut().get_mut();
        let expected_session = this.control.expected_write_session.load(Ordering::Acquire);
        if !matches!(this.status, Status::Connected)
            || (expected_session != 0
                && expected_session != this.control.session_id.load(Ordering::Acquire))
        {
            return Err(T::exhaust_err());
        }

        let result = Pin::new(&mut this.stream).start_send(item);
        if matches!(&result, Err(error) if T::is_write_disconnect_error(error)) {
            (this.options.on_disconnect)(DisconnectReason::WriteError);
            this.status = Status::Disconnected(ReconnectStatus {
                retry_delays: (this.options.retry_delays)(),
                attempts_started: 0,
                reconnect_attempt: None,
                _marker: PhantomData,
            });
            this.schedule_reconnect_attempt();
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        self.as_mut().apply_forced_disconnect(cx);
        let this = self.as_mut().get_mut();
        let expected_session = this.control.expected_write_session.load(Ordering::Acquire);
        if expected_session != 0
            && expected_session != this.control.session_id.load(Ordering::Acquire)
        {
            return Poll::Ready(Err(T::exhaust_err()));
        }

        match this.status {
            Status::Connected => {
                let poll = Pin::new(&mut this.stream).poll_flush(cx);
                if matches!(&poll, Poll::Ready(Err(error)) if T::is_write_disconnect_error(error)) {
                    self.as_mut()
                        .on_disconnect(DisconnectReason::WriteError, cx);
                    Poll::Pending
                } else {
                    poll
                }
            }
            Status::Disconnected(_) => {
                self.poll_disconnect(cx);
                Poll::Pending
            }
            Status::FailedAndExhausted => Poll::Ready(Err(T::exhaust_err())),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        self.as_mut().apply_forced_disconnect(cx);
        let this = self.as_mut().get_mut();
        match this.status {
            Status::Connected => Pin::new(&mut this.stream).poll_close(cx),
            Status::Disconnected(_) => {
                self.poll_disconnect(cx);
                Poll::Pending
            }
            Status::FailedAndExhausted => Poll::Ready(Err(T::exhaust_err())),
        }
    }
}
