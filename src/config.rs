use std::net::SocketAddr;
use std::time::{Duration, Instant};

use eyre::{ensure, eyre};
use serde::Deserialize;
use serde_with::{DisplayFromStr, DurationMilliSeconds, PickFirst, serde_as};
use smart_default::SmartDefault;
use url::Url;

use crate::util::retry_stream::strategies::ExpBackoffStrategy;

mod loader;

#[serde_as]
#[derive(Clone, Debug, SmartDefault, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    #[default = "ETHUSDT"]
    pub symbol: String,
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    #[default = 5.0]
    pub signal_threshold_bps: f64,
    #[default(
        _code = "Url::parse(\"wss://fstream.binance.com/public/stream\").expect(\"valid default Binance URL\")"
    )]
    pub binance_ws_base_url: Url,
    #[default(
        _code = "Url::parse(\"wss://fstream.asterdex.com/stream\").expect(\"valid default Aster URL\")"
    )]
    pub aster_ws_base_url: Url,
    pub feed: FeedConfig,
    pub channels: ChannelConfig,
    pub metrics: MetricsConfig,
    pub log: LogConfig,
}

// Buffered numeric adapters prevent config from rounding floats or accepting booleans as integers.
#[serde_as]
#[derive(Clone, Copy, Debug, SmartDefault, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FeedConfig {
    #[default = 10]
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    pub max_attempts: usize,
    #[serde(rename = "connect_timeout_ms")]
    #[serde_as(as = "PickFirst<(DurationMilliSeconds<u64>, DurationMilliSeconds<String>)>")]
    #[default(Duration::from_secs(10))]
    pub connect_timeout: Duration,
    #[serde(rename = "subscription_timeout_ms")]
    #[serde_as(as = "PickFirst<(DurationMilliSeconds<u64>, DurationMilliSeconds<String>)>")]
    #[default(Duration::from_secs(5))]
    pub subscription_timeout: Duration,
    #[serde(rename = "useful_data_timeout_ms")]
    #[serde_as(as = "PickFirst<(DurationMilliSeconds<u64>, DurationMilliSeconds<String>)>")]
    #[default(Duration::from_secs(10))]
    pub useful_data_timeout: Duration,
    #[serde(rename = "published_book_max_age_ms")]
    #[serde_as(as = "PickFirst<(DurationMilliSeconds<u64>, DurationMilliSeconds<String>)>")]
    #[default(Duration::from_secs(3))]
    pub published_book_max_age: Duration,
    #[serde(rename = "ping_interval_ms")]
    #[serde_as(as = "PickFirst<(DurationMilliSeconds<u64>, DurationMilliSeconds<String>)>")]
    #[default(Duration::from_secs(15))]
    pub ping_interval: Duration,
    #[serde(rename = "ping_timeout_ms")]
    #[serde_as(as = "PickFirst<(DurationMilliSeconds<u64>, DurationMilliSeconds<String>)>")]
    #[default(Duration::from_secs(5))]
    pub ping_timeout: Duration,
    #[serde(rename = "ping_write_timeout_ms")]
    #[serde_as(as = "PickFirst<(DurationMilliSeconds<u64>, DurationMilliSeconds<String>)>")]
    #[default(Duration::from_secs(1))]
    pub ping_write_timeout: Duration,
    pub backoff: ExpBackoffStrategy,
}

#[serde_as]
#[derive(Clone, Copy, Debug, SmartDefault, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChannelConfig {
    #[default = 10]
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    pub binance_depth: usize,
    #[default = 10]
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    pub aster_depth: usize,
    #[default = 3]
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    pub book_updates: usize,
    #[default = 3]
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    pub signal_results: usize,
    #[serde(rename = "sampling_interval_ms")]
    #[serde_as(as = "PickFirst<(DurationMilliSeconds<u64>, DurationMilliSeconds<String>)>")]
    #[default(Duration::from_secs(1))]
    pub sampling_interval: Duration,
}

#[serde_as]
#[derive(Clone, Copy, Debug, SmartDefault, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsConfig {
    #[default(_code = "SocketAddr::from(([127, 0, 0, 1], 9898))")]
    pub listen_addr: SocketAddr,
    #[serde(rename = "request_timeout_ms")]
    #[serde_as(as = "PickFirst<(DurationMilliSeconds<u64>, DurationMilliSeconds<String>)>")]
    #[default(Duration::from_secs(2))]
    pub request_timeout: Duration,
}

#[serde_as]
#[derive(Clone, Debug, SmartDefault, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogConfig {
    #[default = "warn,test_case=info"]
    pub filter: String,
    #[default = 1024]
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    pub queue_capacity: usize,
    #[serde(rename = "shutdown_timeout_ms")]
    #[serde_as(as = "PickFirst<(DurationMilliSeconds<u64>, DurationMilliSeconds<String>)>")]
    #[default(Duration::from_millis(100))]
    pub shutdown_timeout: Duration,
}

impl Config {
    fn validate(&mut self) -> eyre::Result<()> {
        ensure!(
            !self.symbol.is_empty()
                && self.symbol.is_ascii()
                && self.symbol.bytes().all(|byte| byte.is_ascii_alphanumeric()),
            "symbol must contain only ASCII letters and digits"
        );
        self.symbol.make_ascii_uppercase();
        ensure!(
            self.signal_threshold_bps.is_finite() && self.signal_threshold_bps >= 0.0,
            "signal_threshold_bps must be finite and nonnegative"
        );
        for (name, url) in [
            ("binance_ws_base_url", &self.binance_ws_base_url),
            ("aster_ws_base_url", &self.aster_ws_base_url),
        ] {
            let authority = url
                .as_str()
                .split_once("://")
                .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or_default())
                .ok_or_else(|| eyre!("{name} must be an absolute URL"))?;
            ensure!(
                url.scheme() == "wss"
                    && url.host_str().is_some()
                    && !authority.contains('@')
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "{name} must be a wss URL without credentials, query, or fragment"
            );
        }
        self.feed.validate()?;
        self.channels.validate()?;
        self.log.validate()?;
        ensure!(
            !self.metrics.request_timeout.is_zero()
                && Instant::now()
                    .checked_add(self.metrics.request_timeout)
                    .is_some(),
            "metrics.request_timeout_ms must be positive and fit the monotonic clock"
        );
        Ok(())
    }
}

impl FeedConfig {
    fn validate(&self) -> eyre::Result<()> {
        ensure!(self.max_attempts > 0, "feed.max_attempts must be positive");
        for (name, value) in [
            ("connect_timeout_ms", self.connect_timeout),
            ("subscription_timeout_ms", self.subscription_timeout),
            ("useful_data_timeout_ms", self.useful_data_timeout),
            ("published_book_max_age_ms", self.published_book_max_age),
            ("ping_interval_ms", self.ping_interval),
            ("ping_timeout_ms", self.ping_timeout),
            ("ping_write_timeout_ms", self.ping_write_timeout),
        ] {
            ensure!(
                !value.is_zero() && Instant::now().checked_add(value).is_some(),
                "feed.{name} must be positive and fit the monotonic clock"
            );
        }
        ensure!(
            self.ping_interval > self.ping_timeout && self.ping_write_timeout < self.ping_timeout,
            "feed.ping_interval_ms must exceed feed.ping_timeout_ms, and feed.ping_write_timeout_ms must be smaller"
        );
        ensure!(
            !self.backoff.min_delay.is_zero()
                && self.backoff.min_delay <= self.backoff.max_delay
                && Instant::now().checked_add(self.backoff.max_delay).is_some(),
            "feed.backoff delays must be positive, min_delay_ms must not exceed max_delay_ms, and delays must fit the monotonic clock"
        );
        ensure!(
            self.backoff.factor.is_finite() && self.backoff.factor >= 1.0,
            "feed.backoff.factor must be finite and at least 1"
        );
        ensure!(
            self.backoff.jitter.is_finite() && (0.0..=1.0).contains(&self.backoff.jitter),
            "feed.backoff.jitter must be finite and between 0 and 1"
        );
        Ok(())
    }
}

impl ChannelConfig {
    fn validate(&self) -> eyre::Result<()> {
        for (name, value) in [
            ("binance_depth", self.binance_depth),
            ("aster_depth", self.aster_depth),
            ("book_updates", self.book_updates),
            ("signal_results", self.signal_results),
        ] {
            ensure!(
                value > 0 && u32::try_from(value).is_ok(),
                "channels.{name} must be between 1 and {} (Crossfire capacity limit)",
                u32::MAX
            );
        }
        ensure!(
            !self.sampling_interval.is_zero()
                && Instant::now().checked_add(self.sampling_interval).is_some(),
            "channels.sampling_interval_ms must be positive and fit the monotonic clock"
        );
        Ok(())
    }
}

impl LogConfig {
    fn validate(&self) -> eyre::Result<()> {
        ensure!(
            self.queue_capacity > 0 && u32::try_from(self.queue_capacity).is_ok(),
            "log.queue_capacity must be positive and fit the Crossfire capacity limit"
        );
        ensure!(
            !self.shutdown_timeout.is_zero()
                && Instant::now().checked_add(self.shutdown_timeout).is_some(),
            "log.shutdown_timeout_ms must be positive and fit the monotonic clock"
        );
        Ok(())
    }
}
