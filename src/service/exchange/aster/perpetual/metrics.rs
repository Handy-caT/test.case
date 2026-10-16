use prometheus::Histogram;

use crate::metrics::{Metrics, buckets};
use crate::service::exchange::metrics::DepthFeedMetrics;

#[derive(Clone)]
pub struct AsterMetrics {
    pub depth: DepthFeedMetrics,
    pub parsing: Histogram,
}

impl AsterMetrics {
    pub fn new(metrics: &mut Metrics) -> prometheus::Result<Self> {
        let depth = DepthFeedMetrics::new(metrics, "aster")?;
        let parsing = metrics
            .histogram_vec(
                "processing_duration_nanoseconds",
                "Duration of parsing, book update, or signal calculation work in nanoseconds.",
                &["stage"],
                buckets::PROCESSING_BUCKETS,
            )?
            .with_label_values(&["aster_parse"]);
        Ok(Self { depth, parsing })
    }
}
