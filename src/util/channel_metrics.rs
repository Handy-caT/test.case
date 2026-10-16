use prometheus::{Gauge, IntCounter};

use crate::metrics::Metrics;

pub struct ChannelMetrics {
    pub sender: ChannelSenderMetrics,
    pub received: IntCounter,
    pub queue_depth: Gauge,
}

#[derive(Clone)]
pub struct ChannelSenderMetrics {
    pub sent: IntCounter,
    pub failures_full: IntCounter,
    pub failures_disconnected: IntCounter,
}

impl ChannelMetrics {
    pub fn new(metrics: &mut Metrics, name: &str, capacity: usize) -> prometheus::Result<Self> {
        let sent = metrics.counter_vec(
            "channel_sent_total",
            "Messages successfully accepted by a nonblocking channel send.",
            &["channel"],
        )?;
        let received = metrics.counter_vec(
            "channel_received_total",
            "Messages actually removed from a channel by its receiver.",
            &["channel"],
        )?;
        let failures = metrics.counter_vec(
            "channel_send_failures_total",
            "Nonblocking channel sends that failed because the queue was full or disconnected.",
            &["channel", "reason"],
        )?;
        let queue_depth = metrics.gauge_vec(
            "channel_queue_depth",
            "Periodically sampled number of messages currently waiting in a channel.",
            &["channel"],
        )?;
        let channel_capacity = metrics.gauge_vec(
            "channel_capacity",
            "Configured bounded channel capacity in messages.",
            &["channel"],
        )?;

        let sender = ChannelSenderMetrics {
            sent: sent.with_label_values(&[name]),
            failures_full: failures.with_label_values(&[name, "full"]),
            failures_disconnected: failures.with_label_values(&[name, "disconnected"]),
        };
        let received = received.with_label_values(&[name]);
        let queue_depth = queue_depth.with_label_values(&[name]);
        channel_capacity
            .with_label_values(&[name])
            .set(capacity as f64);
        queue_depth.set(0.0);

        Ok(Self {
            sender,
            received,
            queue_depth,
        })
    }
}
