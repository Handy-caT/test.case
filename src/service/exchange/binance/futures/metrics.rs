use prometheus::Histogram;

use crate::metrics::{Metrics, buckets};
use crate::service::exchange::metrics::DepthFeedMetrics;

#[derive(Clone)]
pub struct BinanceMetrics {
    pub depth: DepthFeedMetrics,
    pub parsing: Histogram,
}

impl BinanceMetrics {
    pub fn new(metrics: &mut Metrics) -> prometheus::Result<Self> {
        let depth = DepthFeedMetrics::new(metrics, "binance")?;
        let parsing = metrics
            .histogram_vec(
                "processing_duration_nanoseconds",
                "Duration of parsing, book update, or signal calculation work in nanoseconds.",
                &["stage"],
                buckets::PROCESSING_BUCKETS,
            )?
            .with_label_values(&["binance_parse"]);
        Ok(Self { depth, parsing })
    }
}
