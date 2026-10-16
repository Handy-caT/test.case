use std::time::Duration;

use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::Deserialize;
use serde_with::{DisplayFromStr, DurationMilliSeconds, PickFirst, serde_as};
use smart_default::SmartDefault;

#[serde_as]
#[derive(Clone, Copy, Debug, Deserialize, SmartDefault)]
#[serde(default, deny_unknown_fields)]
pub struct ExpBackoffStrategy {
    #[serde(rename = "min_delay_ms")]
    #[serde_as(as = "PickFirst<(DurationMilliSeconds<u64>, DurationMilliSeconds<String>)>")]
    #[default(Duration::from_millis(100))]
    pub min_delay: Duration,
    #[serde(rename = "max_delay_ms")]
    #[serde_as(as = "PickFirst<(DurationMilliSeconds<u64>, DurationMilliSeconds<String>)>")]
    #[default(Duration::from_millis(1_800_000))]
    pub max_delay: Duration,
    #[default(1.1)]
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    pub factor: f64,
    #[default(0.05)]
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    pub jitter: f64,
}

impl IntoIterator for ExpBackoffStrategy {
    type Item = Duration;
    type IntoIter = ExpBackoffIter;

    fn into_iter(self) -> Self::IntoIter {
        let init = self.min_delay.as_secs_f64();
        let rng = StdRng::from_entropy();
        ExpBackoffIter {
            strategy: self,
            init,
            pow: 0,
            rng,
        }
    }
}

pub struct ExpBackoffIter {
    strategy: ExpBackoffStrategy,
    init: f64,
    pow: u32,
    rng: StdRng,
}

impl Iterator for ExpBackoffIter {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        let max_seconds = self.strategy.max_delay.as_secs_f64();
        let base = self.init * self.strategy.factor.powf(self.pow as f64);
        let base = if base.is_finite() {
            base.min(max_seconds)
        } else {
            max_seconds
        };
        self.pow = self.pow.saturating_add(1);

        let jitter = self.strategy.jitter.clamp(0.0, 1.0);
        let multiplier = if jitter == 0.0 {
            1.0
        } else {
            self.rng.gen_range(1.0 - jitter, 1.0 + jitter)
        };
        let seconds = (base * multiplier).min(max_seconds);
        Some(Duration::from_secs_f64(seconds))
    }
}
