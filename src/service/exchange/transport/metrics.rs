use std::time::Duration;

use prometheus::{Gauge, Histogram, IntCounter};

use crate::metrics::{Metrics, buckets};
use crate::service::exchange::metrics::FEED_ERRORS_HELP;
use crate::util::retry_stream::{ConnectionAttemptResult, ConnectionSeries, DisconnectReason};

#[derive(Clone, Copy)]
pub enum TransportErrorKind {
    Connect,
    ConnectTimeout,
    Read,
    Write,
    UsefulDataTimeout,
    PingWriteTimeout,
    PingTimeout,
}

#[derive(Clone)]
pub struct FeedTransportMetrics {
    connected: Gauge,
    attempts: [IntCounter; 3],
    attempt_durations: [Histogram; 3],
    reconnects: [IntCounter; 7],
    retry_exhaustions: IntCounter,
    last_frame_timestamp: Gauge,
    errors: [IntCounter; 7],
    received_bytes: IntCounter,
    ping_rtt: Histogram,
    last_pong_timestamp: Gauge,
}

impl FeedTransportMetrics {
    pub fn new(metrics: &mut Metrics, exchange: &str) -> prometheus::Result<Self> {
        let feed_connected = metrics.gauge_vec(
            "feed_connected",
            "Whether the exchange feed currently has a subscribed WebSocket connection.",
            &["exchange"],
        )?;
        let feed_attempts = metrics.counter_vec(
            "feed_connection_attempts_total",
            "Completed WebSocket connection and subscription attempts.",
            &["exchange", "result"],
        )?;
        let feed_attempt_duration = metrics.histogram_vec(
            "feed_connection_duration_nanoseconds",
            "Duration of one completed connection and subscription attempt in nanoseconds.",
            &["exchange", "result"],
            buckets::CONNECTION_BUCKETS,
        )?;
        let feed_reconnects = metrics.counter_vec(
            "feed_reconnects_total",
            "Recovery series started after the exchange feed became disconnected.",
            &["exchange", "reason"],
        )?;
        let feed_retry_exhaustions = metrics.counter_vec(
            "feed_retry_exhaustions_total",
            "Connection or recovery series that exhausted its configured attempt budget.",
            &["exchange"],
        )?;
        let feed_last_frame = metrics.gauge_vec(
            "feed_last_frame_timestamp_nanoseconds",
            "Unix timestamp in nanoseconds when the transport yielded the last WebSocket frame.",
            &["exchange"],
        )?;
        let feed_errors =
            metrics.counter_vec("feed_errors_total", FEED_ERRORS_HELP, &["exchange", "kind"])?;
        let feed_received_bytes = metrics.counter_vec(
            "feed_received_bytes_total",
            "WebSocket application payload bytes yielded by the transport.",
            &["exchange"],
        )?;
        let feed_ping_rtt = metrics.histogram_vec(
            "feed_ping_rtt_nanoseconds",
            "Duration from starting one client Ping send operation to its matching Pong on the same connection, in nanoseconds.",
            &["exchange"],
            buckets::RTT_BUCKETS,
        )?;
        let feed_last_pong = metrics.gauge_vec(
            "feed_last_pong_timestamp_nanoseconds",
            "Unix timestamp in nanoseconds of the last matching client Ping response.",
            &["exchange"],
        )?;

        let instance = Self {
            connected: feed_connected.with_label_values(&[exchange]),
            attempts: [
                feed_attempts.with_label_values(&[exchange, "success"]),
                feed_attempts.with_label_values(&[exchange, "error"]),
                feed_attempts.with_label_values(&[exchange, "timeout"]),
            ],
            attempt_durations: [
                feed_attempt_duration.with_label_values(&[exchange, "success"]),
                feed_attempt_duration.with_label_values(&[exchange, "error"]),
                feed_attempt_duration.with_label_values(&[exchange, "timeout"]),
            ],
            reconnects: [
                feed_reconnects.with_label_values(&[exchange, "read_error"]),
                feed_reconnects.with_label_values(&[exchange, "write_error"]),
                feed_reconnects.with_label_values(&[exchange, "close"]),
                feed_reconnects.with_label_values(&[exchange, "eof"]),
                feed_reconnects.with_label_values(&[exchange, "useful_data_timeout"]),
                feed_reconnects.with_label_values(&[exchange, "ping_timeout"]),
                feed_reconnects.with_label_values(&[exchange, "ping_write_timeout"]),
            ],
            retry_exhaustions: feed_retry_exhaustions.with_label_values(&[exchange]),
            last_frame_timestamp: feed_last_frame.with_label_values(&[exchange]),
            errors: [
                feed_errors.with_label_values(&[exchange, "connect"]),
                feed_errors.with_label_values(&[exchange, "connect_timeout"]),
                feed_errors.with_label_values(&[exchange, "read"]),
                feed_errors.with_label_values(&[exchange, "write"]),
                feed_errors.with_label_values(&[exchange, "useful_data_timeout"]),
                feed_errors.with_label_values(&[exchange, "ping_write_timeout"]),
                feed_errors.with_label_values(&[exchange, "ping_timeout"]),
            ],
            received_bytes: feed_received_bytes.with_label_values(&[exchange]),
            ping_rtt: feed_ping_rtt.with_label_values(&[exchange]),
            last_pong_timestamp: feed_last_pong.with_label_values(&[exchange]),
        };
        instance.last_frame_timestamp.set(f64::NAN);
        instance.last_pong_timestamp.set(f64::NAN);
        Ok(instance)
    }

    pub fn set_connected(&self, connected: bool) {
        self.connected.set(f64::from(connected));
    }

    pub fn record_attempt(&self, result: ConnectionAttemptResult, duration: Duration) {
        let index = match result {
            ConnectionAttemptResult::Success => 0,
            ConnectionAttemptResult::Error => 1,
            ConnectionAttemptResult::Timeout => 2,
        };
        self.attempts[index].inc();
        self.attempt_durations[index].observe(duration.as_nanos() as f64);
        if result != ConnectionAttemptResult::Success {
            self.record_error(if result == ConnectionAttemptResult::Timeout {
                TransportErrorKind::ConnectTimeout
            } else {
                TransportErrorKind::Connect
            });
        }
    }

    pub fn record_disconnect(&self, reason: DisconnectReason) {
        self.reconnects[match reason {
            DisconnectReason::ReadError => 0,
            DisconnectReason::WriteError => 1,
            DisconnectReason::Close => 2,
            DisconnectReason::EndOfStream => 3,
            DisconnectReason::UsefulDataTimeout => 4,
            DisconnectReason::PingTimeout => 5,
            DisconnectReason::PingWriteTimeout => 6,
        }]
        .inc();
        match reason {
            DisconnectReason::ReadError => self.record_error(TransportErrorKind::Read),
            DisconnectReason::WriteError => self.record_error(TransportErrorKind::Write),
            _ => {}
        }
    }

    pub fn record_exhaustion(&self, _series: ConnectionSeries) {
        self.retry_exhaustions.inc();
    }

    pub fn set_last_frame_timestamp(&self, timestamp_nanoseconds: u64) {
        self.last_frame_timestamp.set(timestamp_nanoseconds as f64);
    }

    pub fn record_error(&self, kind: TransportErrorKind) {
        self.errors[match kind {
            TransportErrorKind::Connect => 0,
            TransportErrorKind::ConnectTimeout => 1,
            TransportErrorKind::Read => 2,
            TransportErrorKind::Write => 3,
            TransportErrorKind::UsefulDataTimeout => 4,
            TransportErrorKind::PingWriteTimeout => 5,
            TransportErrorKind::PingTimeout => 6,
        }]
        .inc();
    }

    pub fn record_received_bytes(&self, bytes: usize) {
        self.received_bytes.inc_by(bytes as u64);
    }

    pub fn record_ping_rtt(&self, duration: Duration, pong_timestamp_nanoseconds: u64) {
        self.ping_rtt.observe(duration.as_nanos() as f64);
        self.last_pong_timestamp
            .set(pong_timestamp_nanoseconds as f64);
    }
}
