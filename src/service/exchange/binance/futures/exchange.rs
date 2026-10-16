use std::time::Duration;

use crossfire::{TrySendError, mpsc};
use futures::StreamExt;
use prometheus::Gauge;
use tokio::time::{self, MissedTickBehavior};
use tracing::Instrument;
use url::Url;

use crate::config::FeedConfig;
use crate::entity::exchange::response::PartialOrderBookDepth;
use crate::entity::symbol::Symbol;
use crate::service::exchange::binance::futures::exchange::ws::partial_order_book_depth_stream::PartialOrderBookDepthStream;
use crate::service::exchange::binance::futures::metrics::BinanceMetrics;
use crate::service::exchange::depth::DepthSubscription;
use crate::service::exchange::status::FeedStatusSender;
use crate::service::exchange::transport::metrics::FeedTransportMetrics;
use crate::task::builder::TaskBuilder;
use crate::util::channel_metrics::ChannelSenderMetrics;

pub mod ws;

pub struct BinanceFuturesExchange {
    ws_url: Url,
    settings: FeedConfig,
    queue_sampling_interval: Duration,
}

impl BinanceFuturesExchange {
    pub fn new(ws_url: Url, settings: FeedConfig, queue_sampling_interval: Duration) -> Self {
        Self {
            ws_url,
            settings,
            queue_sampling_interval,
        }
    }

    pub fn subscribe_depth(
        &self,
        symbol: Symbol,
        metrics: BinanceMetrics,
        transport_metrics: FeedTransportMetrics,
        channel: ChannelSenderMetrics,
        queue_depth: Gauge,
        capacity: usize,
    ) -> DepthSubscription {
        let (depth_sender, depth_receiver) = mpsc::bounded_async::<PartialOrderBookDepth>(capacity);
        let (status_sender, status_receiver) = FeedStatusSender::new();
        let task_status_sender = status_sender.clone();
        let task_status_receiver = status_receiver.clone();
        let endpoint = self.ws_url.clone();
        let settings = self.settings;
        let queue_sampling_interval = self.queue_sampling_interval;
        let span = tracing::error_span!("feed", exchange = "binance", symbol = %symbol);
        let task = TaskBuilder::new("BinanceFuturesDepth")
            .with_task(async move {
                let mut stream = PartialOrderBookDepthStream::connect(
                    endpoint,
                    symbol,
                    settings,
                    metrics,
                    transport_metrics,
                    task_status_sender,
                    task_status_receiver,
                )
                .await?;
                let mut sample_queue = time::interval(queue_sampling_interval);
                sample_queue.set_missed_tick_behavior(MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        item = stream.next() => {
                            let depth = item.ok_or_else(|| eyre::eyre!("Binance depth WebSocket exhausted its retry attempts"))??;
                            match depth_sender.try_send(depth) {
                                Ok(()) => channel.sent.inc(),
                                Err(TrySendError::Full(_)) => {
                                    channel.failures_full.inc();
                                    log::trace!("Depth channel full; dropping new snapshot");
                                },
                                Err(TrySendError::Disconnected(_)) => {
                                    channel.failures_disconnected.inc();
                                    eyre::bail!("Binance depth receiver disconnected");
                                }
                            }
                        }
                        _ = sample_queue.tick() => queue_depth.set(depth_sender.len() as f64),
                    }
                }
            }.instrument(span))
            .spawn();
        let stream = depth_receiver.into_stream();
        DepthSubscription {
            task,
            stream,
            status_sender,
            status_receiver,
        }
    }
}
