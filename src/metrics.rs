use std::collections::HashMap;

use prometheus::{
    Encoder, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry,
    TextEncoder, core::Collector,
};

pub mod buckets;

pub struct Metrics {
    registry: Registry,
    counter_vecs: HashMap<String, IntCounterVec>,
    gauge_vecs: HashMap<String, GaugeVec>,
    histogram_vecs: HashMap<String, HistogramVec>,
}

impl Metrics {
    pub fn new() -> prometheus::Result<Self> {
        let registry = Registry::new();

        #[cfg(target_os = "linux")]
        registry.register(Box::new(
            prometheus::process_collector::ProcessCollector::for_self(),
        ))?;

        Ok(Self {
            registry,
            counter_vecs: HashMap::new(),
            gauge_vecs: HashMap::new(),
            histogram_vecs: HashMap::new(),
        })
    }

    pub fn counter_vec(
        &mut self,
        name: &str,
        help: &str,
        labels: &[&str],
    ) -> prometheus::Result<IntCounterVec> {
        if let Some(vector) = self.counter_vecs.get(name) {
            return Ok(vector.clone());
        }

        let vector = IntCounterVec::new(Opts::new(name, help), labels)?;
        self.registry.register(Box::new(vector.clone()))?;
        self.counter_vecs.insert(name.to_owned(), vector.clone());
        Ok(vector)
    }

    pub fn gauge_vec(
        &mut self,
        name: &str,
        help: &str,
        labels: &[&str],
    ) -> prometheus::Result<GaugeVec> {
        if let Some(vector) = self.gauge_vecs.get(name) {
            return Ok(vector.clone());
        }

        let vector = GaugeVec::new(Opts::new(name, help), labels)?;
        self.registry.register(Box::new(vector.clone()))?;
        self.gauge_vecs.insert(name.to_owned(), vector.clone());
        Ok(vector)
    }

    pub fn histogram_vec(
        &mut self,
        name: &str,
        help: &str,
        labels: &[&str],
        buckets: &[f64],
    ) -> prometheus::Result<HistogramVec> {
        if let Some(vector) = self.histogram_vecs.get(name) {
            return Ok(vector.clone());
        }

        let options = HistogramOpts::new(name, help).buckets(buckets.to_vec());
        let vector = HistogramVec::new(options, labels)?;
        self.registry.register(Box::new(vector.clone()))?;
        self.histogram_vecs.insert(name.to_owned(), vector.clone());
        Ok(vector)
    }

    pub fn histogram(
        &mut self,
        name: &str,
        help: &str,
        buckets: &[f64],
    ) -> prometheus::Result<Histogram> {
        let histogram =
            Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets.to_vec()))?;
        self.register(histogram)
    }

    pub fn register<C>(&mut self, collector: C) -> prometheus::Result<C>
    where
        C: Collector + Clone + 'static,
    {
        self.registry.register(Box::new(collector.clone()))?;
        Ok(collector)
    }

    pub fn encode(&self) -> prometheus::Result<Vec<u8>> {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&metric_families, &mut buffer)?;
        Ok(buffer)
    }
}
