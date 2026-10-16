use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use futures::{Sink, Stream, ready};
use tokio::sync::watch;
use tokio::time::{self, Interval, MissedTickBehavior, Sleep};
use tokio_tungstenite::tungstenite::http::Uri;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use crate::config::FeedConfig;
use crate::service::exchange::status::{FeedStatus, FeedStatusSender};
use crate::service::exchange::transport::metrics::{FeedTransportMetrics, TransportErrorKind};
use crate::util::retry_stream::tungstenite::ReconnectWs;
use crate::util::retry_stream::{
    ConnectionSeries, DisconnectReason, ReconnectControl, ReconnectOptions,
};
use crate::util::time::utc_timestamp_nanoseconds;

pub mod metrics;

pub struct TransportFrame {
    pub message: Message,
    pub received_at: u64,
    pub received_instant: Instant,
    pub session_id: u64,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct PendingPing {
    session_id: u64,
    payload: [u8; 16],
    sent_at: Instant,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum PingWriteState {
    Ready(PendingPing),
    Flush,
}

pub struct FeedTransport {
    websocket: ReconnectWs,
    settings: FeedConfig,
    metrics: FeedTransportMetrics,
    status_sender: FeedStatusSender,
    status_receiver: watch::Receiver<FeedStatus>,
    control: ReconnectControl,
    status: FeedStatus,
    disconnect_pending: bool,
    useful_deadline: Pin<Box<Sleep>>,
    ping_interval: Interval,
    pending_ping: Option<PendingPing>,
    ping_write: Option<PingWriteState>,
    ping_write_deadline: Pin<Box<Sleep>>,
    ping_deadline: Pin<Box<Sleep>>,
    next_ping_sequence: u64,
}

impl FeedTransport {
    pub async fn connect(
        uri: Uri,
        settings: FeedConfig,
        metrics: FeedTransportMetrics,
        status_sender: FeedStatusSender,
        status_receiver: watch::Receiver<FeedStatus>,
    ) -> eyre::Result<Self> {
        let attempts = NonZeroUsize::new(settings.max_attempts)
            .expect("feed attempt count is validated as positive");
        let attempt_metrics = metrics.clone();
        let connect_metrics = metrics.clone();
        let disconnect_metrics = metrics.clone();
        let exhaustion_metrics = metrics.clone();
        let connect_status_sender = status_sender.clone();
        let disconnect_status_sender = status_sender.clone();
        let options = ReconnectOptions::new()
            .with_retries_generator(move || settings.backoff)
            .with_attempt_limits(attempts, attempts)
            .with_timeouts(settings.connect_timeout, settings.subscription_timeout)
            .with_on_attempt_finished(move |result, duration| {
                attempt_metrics.record_attempt(result, duration);
            })
            .with_on_connect(move |session_id| {
                connect_status_sender.connected(session_id);
                connect_metrics.set_connected(true);
            })
            .with_on_disconnect(move |reason| {
                disconnect_status_sender.disconnected();
                disconnect_metrics.record_disconnect(reason);
                disconnect_metrics.set_connected(false);
                log::warn!("WebSocket disconnected; recovering: reason={reason:?}");
            })
            .with_on_exhausted(move |series: ConnectionSeries| {
                exhaustion_metrics.record_exhaustion(series);
            });
        let websocket = ReconnectWs::connect_with_options(uri, vec![], options).await?;
        let control = websocket.control();
        let now = time::Instant::now();
        let initial_status = *status_receiver.borrow();
        let mut ping_interval =
            time::interval_at(now + settings.ping_interval, settings.ping_interval);
        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        Ok(Self {
            websocket,
            settings,
            metrics,
            status_sender,
            status_receiver,
            control,
            status: initial_status,
            disconnect_pending: !initial_status.connected,
            useful_deadline: Box::pin(time::sleep(settings.useful_data_timeout)),
            ping_interval,
            pending_ping: None,
            ping_write: None,
            ping_write_deadline: Box::pin(time::sleep(settings.ping_write_timeout)),
            ping_deadline: Box::pin(time::sleep(settings.ping_timeout)),
            next_ping_sequence: 0,
        })
    }

    pub fn accepted_update(&mut self, session_id: u64) -> bool {
        if !self.status.connected
            || self.disconnect_pending
            || self.status.session_id != session_id
            || self.control.session_id() != session_id
        {
            return false;
        }
        self.useful_deadline
            .as_mut()
            .reset(time::Instant::now() + self.settings.useful_data_timeout);
        true
    }

    fn observe_status(&mut self) {
        let status = *self.status_receiver.borrow_and_update();
        if status.session_id != self.status.session_id {
            self.status = status;
            self.disconnect_pending = !status.connected;
            self.pending_ping = None;
            self.ping_write = None;
            self.control.clear_write_session_requirement();
            self.useful_deadline
                .as_mut()
                .reset(time::Instant::now() + self.settings.useful_data_timeout);
            self.ping_interval
                .reset_at(time::Instant::now() + self.settings.ping_interval);
        } else {
            self.status = status;
            if !status.connected {
                self.disconnect_pending = true;
                self.pending_ping = None;
                self.ping_write = None;
                self.control.clear_write_session_requirement();
            }
        }
    }

    fn disconnect(&mut self, reason: DisconnectReason) {
        self.disconnect_pending = true;
        self.pending_ping = None;
        self.ping_write = None;
        self.control.clear_write_session_requirement();
        self.control.force_disconnect(reason);
    }

    fn start_ping(&mut self) {
        self.next_ping_sequence = self.next_ping_sequence.wrapping_add(1);
        let mut payload = [0; 16];
        payload[..8].copy_from_slice(&self.status.session_id.to_be_bytes());
        payload[8..].copy_from_slice(&self.next_ping_sequence.to_be_bytes());
        let ping = PendingPing {
            session_id: self.status.session_id,
            payload,
            sent_at: Instant::now(),
        };
        self.control.require_write_session(ping.session_id);
        self.pending_ping = Some(ping);
        self.ping_write = Some(PingWriteState::Ready(ping));
        self.ping_write_deadline
            .as_mut()
            .reset(time::Instant::now() + self.settings.ping_write_timeout);
        self.ping_deadline
            .as_mut()
            .reset(time::Instant::now() + self.settings.ping_timeout);
    }

    fn process_frame(&mut self, message: Message) -> TransportFrame {
        let received_instant = Instant::now();
        let received_at = utc_timestamp_nanoseconds();
        self.metrics.set_last_frame_timestamp(received_at);
        self.metrics.record_received_bytes(message.len());
        if let Message::Pong(payload) = &message
            && let Some(ping) = self.pending_ping
            && matches!(self.ping_write, Some(PingWriteState::Flush) | None)
            && ping.session_id == self.status.session_id
            && ping.payload.as_slice() == payload.as_ref()
        {
            self.metrics
                .record_ping_rtt(ping.sent_at.elapsed(), received_at);
            self.pending_ping = None;
        }
        TransportFrame {
            message,
            received_at,
            received_instant,
            session_id: self.status.session_id,
        }
    }

    fn poll_ping_write(&mut self, context: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        let Some(state) = self.ping_write else {
            return Poll::Ready(Ok(()));
        };
        if let PingWriteState::Ready(ping) = state {
            ready!(Pin::new(&mut self.websocket).poll_ready(context))?;
            Pin::new(&mut self.websocket)
                .start_send(Message::Ping(ping.payload.to_vec().into()))?;
            self.ping_write = Some(PingWriteState::Flush);
        }
        ready!(Pin::new(&mut self.websocket).poll_flush(context))?;
        self.ping_write = None;
        self.control.clear_write_session_requirement();
        Poll::Ready(Ok(()))
    }

    fn poll_next_transport(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<eyre::Result<TransportFrame>>> {
        self.observe_status();

        let stream_poll = Pin::new(&mut self.websocket).poll_next(context);
        self.observe_status();
        let output = match stream_poll {
            Poll::Ready(Some(Ok(message))) => Some(self.process_frame(message)),
            Poll::Ready(Some(Err(error))) => {
                log::debug!("WebSocket read failed: {error}");
                self.observe_status();
                None
            }
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => None,
        };

        if self.status.connected
            && !self.disconnect_pending
            && self.useful_deadline.as_mut().poll(context).is_ready()
        {
            self.metrics
                .record_error(TransportErrorKind::UsefulDataTimeout);
            self.disconnect(DisconnectReason::UsefulDataTimeout);
        }

        if self.status.connected
            && !self.disconnect_pending
            && self.pending_ping.is_some()
            && self.ping_deadline.as_mut().poll(context).is_ready()
        {
            self.metrics.record_error(TransportErrorKind::PingTimeout);
            self.disconnect(DisconnectReason::PingTimeout);
        }

        if self.status.connected
            && !self.disconnect_pending
            && self.ping_write.is_some()
            && self.ping_write_deadline.as_mut().poll(context).is_ready()
        {
            self.metrics
                .record_error(TransportErrorKind::PingWriteTimeout);
            self.disconnect(DisconnectReason::PingWriteTimeout);
        }

        if self.status.connected
            && !self.disconnect_pending
            && self.pending_ping.is_none()
            && self.ping_interval.poll_tick(context).is_ready()
        {
            self.start_ping();
        }

        if let Poll::Ready(Err(error)) = self.poll_ping_write(context) {
            log::debug!("WebSocket Ping write failed: {error}");
            self.disconnect(DisconnectReason::WriteError);
        }

        if let Some(frame) = output {
            Poll::Ready(Some(Ok(frame)))
        } else {
            Poll::Pending
        }
    }
}

impl Stream for FeedTransport {
    type Item = eyre::Result<TransportFrame>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_transport(context)
    }
}

impl Drop for FeedTransport {
    fn drop(&mut self) {
        self.control.clear_write_session_requirement();
        self.status_sender.disconnected();
        self.metrics.set_connected(false);
    }
}
