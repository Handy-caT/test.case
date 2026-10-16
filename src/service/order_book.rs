use std::sync::Arc;
use std::time::{Duration, Instant};

use crossfire::mpsc::Array;
use crossfire::stream::AsyncStream;
use crossfire::{MAsyncTx, TrySendError};
use futures::StreamExt;
use tokio::time;

use crate::db::strategy::common::order_book::{OrderBookRow, OrderBookStorage};
use crate::entity::exchange::response::PartialOrderBookDepth;
use crate::entity::symbol::Symbol;
use crate::service::exchange::status::FeedStatusSender;
use crate::service::order_book::metrics::{BookMetrics, BookUpdateMetrics};
use crate::task::Task;
use crate::task::builder::TaskBuilder;

pub mod metrics;

#[derive(Clone, Copy, Debug)]
pub struct OrderBookUpdated;

pub struct OrderBookUpdateTask {
    pub symbol: Symbol,
    pub storage: Arc<OrderBookStorage>,
    pub binance_depth: AsyncStream<Array<PartialOrderBookDepth>>,
    pub aster_depth: AsyncStream<Array<PartialOrderBookDepth>>,
    pub book_updates: MAsyncTx<Array<OrderBookUpdated>>,
    pub binance_feed_status: FeedStatusSender,
    pub aster_feed_status: FeedStatusSender,
    pub metrics: BookUpdateMetrics,
    pub queue_sampling_interval: Duration,
}

impl OrderBookUpdateTask {
    pub fn spawn(mut self) -> Task {
        TaskBuilder::new("OrderBookUpdates")
            .with_task(async move {
                let mut queue_sampling = time::interval(self.queue_sampling_interval);
                queue_sampling.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

                loop {
                    tokio::select! {
                        depth = self.binance_depth.next() => {
                            let depth = depth.ok_or_else(|| eyre::eyre!("Binance depth stream ended"))?;
                            self.metrics.binance_depth_received.inc();
                            self.process_depth(depth, &self.binance_feed_status, &self.metrics.binance)?;
                        }
                        depth = self.aster_depth.next() => {
                            let depth = depth.ok_or_else(|| eyre::eyre!("Aster depth stream ended"))?;
                            self.metrics.aster_depth_received.inc();
                            self.process_depth(depth, &self.aster_feed_status, &self.metrics.aster)?;
                        }
                        _ = queue_sampling.tick() => {
                            self.metrics.update_queue_depth.set(self.book_updates.len() as f64);
                        }
                    }
                }
            })
            .spawn()
    }

    fn process_depth(
        &self,
        depth: PartialOrderBookDepth,
        feed_status: &FeedStatusSender,
        book_metrics: &BookMetrics,
    ) -> eyre::Result<()> {
        let started = Instant::now();
        if depth.symbol != self.symbol {
            book_metrics.validation_errors.inc();
            self.record_book_update_duration(started);
            eyre::bail!(
                "Received order-book depth for `{}`, expected `{}`",
                depth.symbol,
                self.symbol
            );
        }

        let received_instant = depth.received_instant;
        let row = match OrderBookRow::from_partial_depth(&depth) {
            Ok(row) => row,
            Err(error) => {
                log::debug!(
                    "Rejected order-book row: exchange={:?}, symbol={}, error={error:#}",
                    depth.exchange,
                    depth.symbol
                );
                book_metrics.validation_errors.inc();
                self.record_book_update_duration(started);
                return Ok(());
            }
        };
        let received_at = row
            .received_at
            .expect("published rows carry a receive timestamp");
        let row = Arc::new(row);
        let session_id = row.session_id.expect("published rows carry a feed session");
        let bid_levels = usize::from(row.bids_size);
        let ask_levels = usize::from(row.asks_size);

        let mut replacement_result = None;
        let published = feed_status.publish_if_connected(session_id, || {
            replacement_result = Some(self.storage.replace(row));
        });
        if !published {
            log::trace!(
                "Discarding order-book row from inactive session: exchange={:?}, session_id={session_id}",
                depth.exchange
            );
            self.record_book_update_duration(started);
            return Ok(());
        }
        if let Err(error) =
            replacement_result.expect("a matching connected session replaces its book")
        {
            self.record_book_update_duration(started);
            return Err(error);
        }

        self.record_book_update_duration(started);
        book_metrics.record_publication(
            received_at,
            bid_levels,
            ask_levels,
            received_instant.elapsed(),
        );

        match self.book_updates.try_send(OrderBookUpdated) {
            Ok(()) => self.metrics.updates.sent.inc(),
            Err(TrySendError::Full(_)) => {
                self.metrics.updates.failures_full.inc();
                log::trace!("Book-update channel full; coalescing notification");
            }
            Err(TrySendError::Disconnected(_)) => {
                self.metrics.updates.failures_disconnected.inc();
                eyre::bail!("Signal generator update receiver disconnected");
            }
        }

        Ok(())
    }

    fn record_book_update_duration(&self, started: Instant) {
        self.metrics
            .duration
            .observe(started.elapsed().as_nanos() as f64);
    }
}
