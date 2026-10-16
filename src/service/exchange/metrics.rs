use prometheus::{Gauge, IntCounter};

use crate::metrics::Metrics;
use crate::service::exchange::depth::DepthParseError;

pub const FEED_MESSAGES_HELP: &str =
    "Depth feed messages grouped by their validation and sequence result.";
pub const FEED_ERRORS_HELP: &str =
    "Feed errors grouped by a fixed processing or transport category.";

#[derive(Clone, Copy)]
pub enum FeedMessageResult {
    Accepted,
    Duplicate,
    Outdated,
    WrongSymbol,
    Invalid,
    Control,
}

#[derive(Clone, Copy)]
pub enum DepthErrorKind {
    Parse,
    Validation,
}

#[derive(Clone)]
pub struct DepthFeedMetrics {
    last_valid_update_timestamp: Gauge,
    messages: [IntCounter; 6],
    errors: [IntCounter; 2],
}

impl DepthFeedMetrics {
    pub fn new(metrics: &mut Metrics, exchange: &str) -> prometheus::Result<Self> {
        let last_valid_update = metrics.gauge_vec(
            "feed_last_valid_update_timestamp_nanoseconds",
            "Unix timestamp in nanoseconds when the feed accepted its last new valid depth snapshot.",
            &["exchange"],
        )?;
        let feed_messages = metrics.counter_vec(
            "feed_messages_total",
            FEED_MESSAGES_HELP,
            &["exchange", "result"],
        )?;
        let feed_errors =
            metrics.counter_vec("feed_errors_total", FEED_ERRORS_HELP, &["exchange", "kind"])?;
        let instance = Self {
            last_valid_update_timestamp: last_valid_update.with_label_values(&[exchange]),
            messages: [
                feed_messages.with_label_values(&[exchange, "accepted"]),
                feed_messages.with_label_values(&[exchange, "duplicate"]),
                feed_messages.with_label_values(&[exchange, "outdated"]),
                feed_messages.with_label_values(&[exchange, "wrong_symbol"]),
                feed_messages.with_label_values(&[exchange, "invalid"]),
                feed_messages.with_label_values(&[exchange, "control"]),
            ],
            errors: [
                feed_errors.with_label_values(&[exchange, "parse"]),
                feed_errors.with_label_values(&[exchange, "validation"]),
            ],
        };
        instance.last_valid_update_timestamp.set(f64::NAN);
        Ok(instance)
    }

    pub fn set_last_valid_update_timestamp(&self, timestamp_nanoseconds: u64) {
        self.last_valid_update_timestamp
            .set(timestamp_nanoseconds as f64);
    }

    pub fn record_message(&self, result: FeedMessageResult) {
        self.messages[match result {
            FeedMessageResult::Accepted => 0,
            FeedMessageResult::Duplicate => 1,
            FeedMessageResult::Outdated => 2,
            FeedMessageResult::WrongSymbol => 3,
            FeedMessageResult::Invalid => 4,
            FeedMessageResult::Control => 5,
        }]
        .inc();
    }

    pub fn record_error(&self, kind: DepthErrorKind) {
        self.errors[match kind {
            DepthErrorKind::Parse => 0,
            DepthErrorKind::Validation => 1,
        }]
        .inc();
    }

    pub fn record_parse_error(&self, error: DepthParseError) {
        self.record_message(FeedMessageResult::Invalid);
        self.record_error(match error {
            DepthParseError::Malformed => DepthErrorKind::Parse,
            DepthParseError::Invalid => DepthErrorKind::Validation,
        });
    }
}
