use std::time::Duration;

use prometheus::{Gauge, Histogram, IntCounter};

use crate::metrics::{Metrics, buckets};
use crate::util::channel_metrics::ChannelSenderMetrics;

#[derive(Clone)]
pub struct BookMetrics {
    last_update_timestamp: Gauge,
    updates: IntCounter,
    pub validation_errors: IntCounter,
    bid_levels: Gauge,
    ask_levels: Gauge,
    update_latency: Histogram,
}

#[derive(Clone)]
pub struct BookUpdateMetrics {
    pub duration: Histogram,
    pub binance: BookMetrics,
    pub aster: BookMetrics,
    pub binance_depth_received: IntCounter,
    pub aster_depth_received: IntCounter,
    pub updates: ChannelSenderMetrics,
    pub update_queue_depth: Gauge,
}

impl BookMetrics {
    pub fn new(metrics: &mut Metrics, exchange: &str, symbol: &str) -> prometheus::Result<Self> {
        let last_update = metrics.gauge_vec(
            "book_last_update_timestamp_nanoseconds",
            "Local Unix receive timestamp in nanoseconds of the last published depth snapshot.",
            &["exchange", "symbol"],
        )?;
        let updates = metrics.counter_vec(
            "book_updates_total",
            "Depth snapshots successfully published to the shared order-book storage.",
            &["exchange", "symbol"],
        )?;
        let validation_errors = metrics.counter_vec(
            "book_validation_errors_total",
            "Depth snapshots rejected by book symbol validation or row construction.",
            &["exchange", "symbol"],
        )?;
        let levels = metrics.gauge_vec(
            "book_levels",
            "Number of stored order-book levels by exchange, symbol, and side.",
            &["exchange", "symbol", "side"],
        )?;
        let update_latency = metrics
            .histogram_vec(
                "book_update_latency_nanoseconds",
                "Duration from transport frame receipt to order-book publication in nanoseconds.",
                &["exchange"],
                buckets::BOOK_LATENCY_BUCKETS,
            )?
            .with_label_values(&[exchange]);

        let instance = Self {
            last_update_timestamp: last_update.with_label_values(&[exchange, symbol]),
            updates: updates.with_label_values(&[exchange, symbol]),
            validation_errors: validation_errors.with_label_values(&[exchange, symbol]),
            bid_levels: levels.with_label_values(&[exchange, symbol, "bid"]),
            ask_levels: levels.with_label_values(&[exchange, symbol, "ask"]),
            update_latency,
        };
        instance.last_update_timestamp.set(0.0);
        instance.bid_levels.set(0.0);
        instance.ask_levels.set(0.0);
        Ok(instance)
    }

    pub fn record_publication(
        &self,
        timestamp_nanoseconds: u64,
        bid_levels: usize,
        ask_levels: usize,
        latency: Duration,
    ) {
        self.updates.inc();
        self.last_update_timestamp.set(timestamp_nanoseconds as f64);
        self.bid_levels.set(bid_levels as f64);
        self.ask_levels.set(ask_levels as f64);
        self.update_latency.observe(latency.as_nanos() as f64);
    }
}

impl BookUpdateMetrics {
    pub fn new(
        metrics: &mut Metrics,
        symbol: &str,
        binance_depth_received: IntCounter,
        aster_depth_received: IntCounter,
        updates: ChannelSenderMetrics,
        update_queue_depth: Gauge,
    ) -> prometheus::Result<Self> {
        let duration = metrics
            .histogram_vec(
                "processing_duration_nanoseconds",
                "Duration of parsing, book update, or signal calculation work in nanoseconds.",
                &["stage"],
                buckets::PROCESSING_BUCKETS,
            )?
            .with_label_values(&["book_update"]);
        Ok(Self {
            duration,
            binance: BookMetrics::new(metrics, "binance", symbol)?,
            aster: BookMetrics::new(metrics, "aster", symbol)?,
            binance_depth_received,
            aster_depth_received,
            updates,
            update_queue_depth,
        })
    }
}
