use std::time::Duration;

use prometheus::{Gauge, Histogram, IntCounter};

use crate::entity::exchange::Exchange;
use crate::metrics::{Metrics, buckets};
use crate::util::channel_metrics::ChannelSenderMetrics;

#[derive(Clone)]
pub struct SignalMetrics {
    calculations: [IntCounter; 7],
    threshold: Gauge,
    sell_binance_buy_aster: Gauge,
    sell_aster_buy_binance: Gauge,
    binance_input_age: Histogram,
    aster_input_age: Histogram,
    input_skew: Histogram,
    pub duration: Histogram,
    pub binance_input_usable: Gauge,
    pub aster_input_usable: Gauge,
    pub updates_received: IntCounter,
    pub results: ChannelSenderMetrics,
}

#[derive(Clone, Copy)]
pub enum SignalCalculation {
    LongAsterShortBinance,
    LongBinanceShortAster,
    HoldNoBook,
    HoldFeedUnavailable,
    HoldStale,
    HoldInvalidQuotes,
    HoldThreshold,
}

impl SignalMetrics {
    pub fn new(
        metrics: &mut Metrics,
        symbol: &str,
        threshold_bps: f64,
        updates_received: IntCounter,
        results: ChannelSenderMetrics,
    ) -> prometheus::Result<Self> {
        let input_usable = metrics.gauge_vec(
            "book_usable",
            "Whether this exchange book is initialized, current-session, fresh, and quote-valid.",
            &["exchange", "symbol"],
        )?;
        let calculations = metrics.counter_vec(
            "signal_calculations_total",
            "Calculated signal outcomes, including fixed Hold reasons.",
            &["result"],
        )?;
        let threshold = metrics.register(Gauge::with_opts(prometheus::Opts::new(
            "signal_threshold_basis_points",
            "Configured strict spread threshold in basis points for this process.",
        ))?)?;
        let spreads = metrics.gauge_vec(
            "signal_spread_basis_points",
            "Last calculated cross-exchange spread in basis points; NaN means unusable inputs.",
            &["direction"],
        )?;
        let input_age = metrics.histogram_vec(
            "signal_input_age_nanoseconds",
            "Age of each stored book snapshot from its local monotonic receive instant at signal calculation in nanoseconds.",
            &["exchange"],
            buckets::AGE_BUCKETS,
        )?;
        let input_skew = metrics.histogram(
            "signal_input_skew_nanoseconds",
            "Absolute difference between local monotonic receive instants of the two book snapshots used by a calculation.",
            buckets::AGE_BUCKETS,
        )?;
        let duration = metrics
            .histogram_vec(
                "processing_duration_nanoseconds",
                "Duration of parsing, book update, or signal calculation work in nanoseconds.",
                &["stage"],
                buckets::PROCESSING_BUCKETS,
            )?
            .with_label_values(&["signal"]);

        let instance = Self {
            calculations: [
                calculations.with_label_values(&["long_aster_short_binance"]),
                calculations.with_label_values(&["long_binance_short_aster"]),
                calculations.with_label_values(&["hold_no_book"]),
                calculations.with_label_values(&["hold_feed_unavailable"]),
                calculations.with_label_values(&["hold_stale"]),
                calculations.with_label_values(&["hold_invalid_quotes"]),
                calculations.with_label_values(&["hold_threshold"]),
            ],
            threshold,
            sell_binance_buy_aster: spreads.with_label_values(&["sell_binance_buy_aster"]),
            sell_aster_buy_binance: spreads.with_label_values(&["sell_aster_buy_binance"]),
            binance_input_age: input_age.with_label_values(&["binance"]),
            aster_input_age: input_age.with_label_values(&["aster"]),
            input_skew,
            duration,
            binance_input_usable: input_usable.with_label_values(&["binance", symbol]),
            aster_input_usable: input_usable.with_label_values(&["aster", symbol]),
            updates_received,
            results,
        };
        instance.binance_input_usable.set(0.0);
        instance.aster_input_usable.set(0.0);
        instance.threshold.set(threshold_bps);
        instance.set_unavailable_spreads();
        Ok(instance)
    }

    pub fn record_calculation(&self, result: SignalCalculation) {
        self.calculations[match result {
            SignalCalculation::LongAsterShortBinance => 0,
            SignalCalculation::LongBinanceShortAster => 1,
            SignalCalculation::HoldNoBook => 2,
            SignalCalculation::HoldFeedUnavailable => 3,
            SignalCalculation::HoldStale => 4,
            SignalCalculation::HoldInvalidQuotes => 5,
            SignalCalculation::HoldThreshold => 6,
        }]
        .inc();
    }

    pub fn set_spreads(&self, sell_binance_buy_aster: f64, sell_aster_buy_binance: f64) {
        self.sell_binance_buy_aster.set(sell_binance_buy_aster);
        self.sell_aster_buy_binance.set(sell_aster_buy_binance);
    }

    pub fn set_unavailable_spreads(&self) {
        self.set_spreads(f64::NAN, f64::NAN);
    }

    pub fn record_input_age(&self, exchange: Exchange, age: Duration) {
        let histogram = match exchange {
            Exchange::BinanceFutures => &self.binance_input_age,
            Exchange::AsterPerpetuals => &self.aster_input_age,
        };
        histogram.observe(age.as_nanos() as f64);
    }

    pub fn record_input_skew(&self, skew: Duration) {
        self.input_skew.observe(skew.as_nanos() as f64);
    }
}
