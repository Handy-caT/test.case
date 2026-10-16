use std::future::pending;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossfire::mpsc::Array;
use crossfire::{AsyncRx, MAsyncTx, TrySendError};
use tokio::sync::{Notify, watch};
use tokio::time;

use crate::db::strategy::common::order_book::OrderBookStorage;
use crate::entity::exchange::Exchange;
use crate::entity::order_book::BidAskAcrossExchanges;
use crate::entity::symbol::Symbol;
use crate::service::exchange::status::FeedStatus;
use crate::service::order_book::OrderBookUpdated;
use crate::service::strategy::signal_generator::metrics::{SignalCalculation, SignalMetrics};
use crate::service::strategy::signal_generator::signal::{
    HoldReason, SignalDecision, SignalResult,
};
use crate::task::Task;
use crate::task::builder::TaskBuilder;
use crate::util::time::utc_timestamp_nanoseconds;

pub struct SignalGenerator {
    pub threshold_bps: f64,
    pub symbol: Symbol,
    pub storage: Arc<OrderBookStorage>,
    pub binance_status: watch::Receiver<FeedStatus>,
    pub aster_status: watch::Receiver<FeedStatus>,
    pub max_book_age: Duration,
    pub metrics: SignalMetrics,
}

struct SignalEvaluation {
    result: SignalResult,
    next_expiry: Option<Instant>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum BookState {
    Missing,
    FeedUnavailable,
    Stale,
    InvalidQuotes,
    Usable,
}

impl SignalGenerator {
    pub fn spawn_signal_generation_task(
        self: Arc<Self>,
        update_receiver: AsyncRx<Array<OrderBookUpdated>>,
        result_sender: MAsyncTx<Array<SignalResult>>,
        result_space: Arc<Notify>,
    ) -> Task {
        TaskBuilder::new("SignalGenerator")
            .with_task(async move {
                let mut binance_status = self.binance_status.clone();
                let mut aster_status = self.aster_status.clone();
                let mut result_pending = false;

                let mut next_expiry = self.recalculate_and_send(&result_sender, &mut result_pending)?;

                loop {
                    let expiry = async move {
                        if let Some(deadline) = next_expiry {
                            time::sleep_until(deadline.into()).await;
                        } else {
                            pending::<()>().await;
                        }
                    };
                    tokio::pin!(expiry);

                    tokio::select! {
                        update = update_receiver.recv() => {
                            update.map_err(|_| eyre::eyre!("Order-book update channel closed"))?;
                            self.metrics.updates_received.inc();
                            next_expiry = self.recalculate_and_send(&result_sender, &mut result_pending)?;
                        }
                        changed = binance_status.changed() => {
                            changed.map_err(|_| eyre::eyre!("Binance feed status channel closed"))?;
                            let _ = binance_status.borrow_and_update();
                            next_expiry = self.recalculate_and_send(&result_sender, &mut result_pending)?;
                        }
                        changed = aster_status.changed() => {
                            changed.map_err(|_| eyre::eyre!("Aster feed status channel closed"))?;
                            let _ = aster_status.borrow_and_update();
                            next_expiry = self.recalculate_and_send(&result_sender, &mut result_pending)?;
                        }
                        _ = &mut expiry => {
                            next_expiry = self.recalculate_and_send(&result_sender, &mut result_pending)?;
                        }
                        _ = result_space.notified() => {
                            if result_pending {
                                next_expiry = self.recalculate_and_send(&result_sender, &mut result_pending)?;
                            }
                        }
                    }
                }
            })
            .spawn()
    }

    fn recalculate_and_send(
        &self,
        result_sender: &MAsyncTx<Array<SignalResult>>,
        result_pending: &mut bool,
    ) -> eyre::Result<Option<Instant>> {
        let evaluation = self.calculate_signal();
        self.try_send_result(result_sender, evaluation.result, result_pending)?;
        Ok(evaluation.next_expiry)
    }

    fn try_send_result(
        &self,
        result_sender: &MAsyncTx<Array<SignalResult>>,
        result: SignalResult,
        result_pending: &mut bool,
    ) -> eyre::Result<()> {
        match result_sender.try_send(result) {
            Ok(()) => {
                self.metrics.results.sent.inc();
                *result_pending = false;
            }
            Err(TrySendError::Full(_)) => {
                self.metrics.results.failures_full.inc();
                log::trace!(
                    "Signal-result channel full; scheduling recalculation when space is available"
                );
                *result_pending = true;
            }
            Err(TrySendError::Disconnected(_)) => {
                self.metrics.results.failures_disconnected.inc();
                eyre::bail!("Signal result receiver disconnected");
            }
        }
        Ok(())
    }

    fn calculate_signal(&self) -> SignalEvaluation {
        let started = Instant::now();
        let pair = match self
            .storage
            .read_pair(Exchange::BinanceFutures, Exchange::AsterPerpetuals)
        {
            Ok(pair) => pair,
            Err(_) => {
                self.metrics.binance_input_usable.set(0.0);
                self.metrics.aster_input_usable.set(0.0);
                self.metrics.set_unavailable_spreads();
                return self.finish(
                    SignalResult::hold_without_quotes(
                        self.symbol.clone(),
                        self.threshold_bps,
                        HoldReason::OrderBookUnavailable {
                            exchanges: vec![Exchange::BinanceFutures, Exchange::AsterPerpetuals],
                        },
                    ),
                    SignalCalculation::HoldNoBook,
                    started,
                    None,
                );
            }
        };

        let (binance_status, aster_status) =
            (*self.binance_status.borrow(), *self.aster_status.borrow());
        let now = Instant::now();
        let next_expiry = Self::next_book_expiry(&pair, self.max_book_age, now);
        let binance_age = Self::input_age(pair.left_received_instant, now);
        let aster_age = Self::input_age(pair.right_received_instant, now);
        Self::record_input_age(&self.metrics, Exchange::BinanceFutures, binance_age);
        Self::record_input_age(&self.metrics, Exchange::AsterPerpetuals, aster_age);
        if let (Some(binance_at), Some(aster_at)) =
            (pair.left_received_instant, pair.right_received_instant)
        {
            self.metrics.record_input_skew(if binance_at >= aster_at {
                binance_at.saturating_duration_since(aster_at)
            } else {
                aster_at.saturating_duration_since(binance_at)
            });
        }

        let binance_state = Self::book_state(
            binance_age,
            pair.left_session_id,
            pair.left_initialized,
            (pair.left_bid_price, pair.left_ask_price),
            binance_status,
            self.max_book_age,
        );
        let aster_state = Self::book_state(
            aster_age,
            pair.right_session_id,
            pair.right_initialized,
            (pair.right_bid_price, pair.right_ask_price),
            aster_status,
            self.max_book_age,
        );
        self.metrics
            .binance_input_usable
            .set(f64::from(binance_state == BookState::Usable));
        self.metrics
            .aster_input_usable
            .set(f64::from(aster_state == BookState::Usable));

        let unavailable = [
            (Exchange::BinanceFutures, binance_state),
            (Exchange::AsterPerpetuals, aster_state),
        ];
        if unavailable
            .iter()
            .any(|(_, state)| *state == BookState::Missing)
        {
            self.metrics.set_unavailable_spreads();
            let exchanges = unavailable
                .iter()
                .filter_map(|(exchange, state)| (*state == BookState::Missing).then_some(*exchange))
                .collect();
            return self.finish(
                SignalResult::hold_without_quotes(
                    self.symbol.clone(),
                    self.threshold_bps,
                    HoldReason::OrderBookUnavailable { exchanges },
                ),
                SignalCalculation::HoldNoBook,
                started,
                next_expiry,
            );
        }

        if unavailable
            .iter()
            .any(|(_, state)| *state == BookState::FeedUnavailable)
        {
            self.metrics.set_unavailable_spreads();
            let exchanges = unavailable
                .iter()
                .filter_map(|(exchange, state)| {
                    (*state == BookState::FeedUnavailable).then_some(*exchange)
                })
                .collect();
            return self.finish(
                SignalResult::hold_with_quotes(
                    self.symbol.clone(),
                    pair,
                    self.threshold_bps,
                    HoldReason::FeedUnavailable { exchanges },
                ),
                SignalCalculation::HoldFeedUnavailable,
                started,
                next_expiry,
            );
        }

        if unavailable
            .iter()
            .any(|(_, state)| *state == BookState::Stale)
        {
            self.metrics.set_unavailable_spreads();
            return self.finish(
                SignalResult::hold_with_quotes(
                    self.symbol.clone(),
                    pair,
                    self.threshold_bps,
                    HoldReason::StaleBook {
                        binance_age: binance_age.expect("stale book has a receive instant"),
                        aster_age: aster_age.expect("stale book has a receive instant"),
                        max_age: self.max_book_age,
                    },
                ),
                SignalCalculation::HoldStale,
                started,
                next_expiry,
            );
        }

        let quotes = pair;
        let spreads = match quotes.calculate_spreads() {
            Ok(spreads) => spreads,
            Err(reason) => {
                self.metrics.set_unavailable_spreads();
                return self.finish(
                    SignalResult::hold_with_quotes(
                        self.symbol.clone(),
                        quotes,
                        self.threshold_bps,
                        HoldReason::InvalidQuotes { reason },
                    ),
                    SignalCalculation::HoldInvalidQuotes,
                    started,
                    next_expiry,
                );
            }
        };

        self.metrics.set_spreads(
            spreads.sell_binance_buy_aster_bps,
            spreads.sell_aster_buy_binance_bps,
        );
        let decision = if spreads.sell_binance_buy_aster_bps > self.threshold_bps {
            SignalDecision::LongAsterShortBinance
        } else if spreads.sell_aster_buy_binance_bps > self.threshold_bps {
            SignalDecision::LongBinanceShortAster
        } else {
            SignalDecision::Hold
        };
        let calculation = match decision {
            SignalDecision::LongAsterShortBinance => SignalCalculation::LongAsterShortBinance,
            SignalDecision::LongBinanceShortAster => SignalCalculation::LongBinanceShortAster,
            SignalDecision::Hold => SignalCalculation::HoldThreshold,
        };
        let hold_reason =
            (decision == SignalDecision::Hold).then_some(HoldReason::ThresholdNotExceeded);

        self.finish(
            SignalResult {
                symbol: self.symbol.clone(),
                calculated_at: Instant::now(),
                calculated_at_timestamp_nanoseconds: utc_timestamp_nanoseconds(),
                decision,
                quotes: Some(quotes),
                spreads: Some(spreads),
                threshold_bps: self.threshold_bps,
                hold_reason,
            },
            calculation,
            started,
            next_expiry,
        )
    }

    fn finish(
        &self,
        result: SignalResult,
        calculation: SignalCalculation,
        started: Instant,
        next_expiry: Option<Instant>,
    ) -> SignalEvaluation {
        self.metrics.record_calculation(calculation);
        self.metrics
            .duration
            .observe(started.elapsed().as_nanos() as f64);
        SignalEvaluation {
            result,
            next_expiry,
        }
    }

    fn input_age(received_instant: Option<Instant>, now: Instant) -> Option<Duration> {
        received_instant.map(|received_instant| now.saturating_duration_since(received_instant))
    }

    fn record_input_age(metrics: &SignalMetrics, exchange: Exchange, age: Option<Duration>) {
        let Some(age) = age else {
            return;
        };
        metrics.record_input_age(exchange, age);
    }

    fn book_state(
        age: Option<Duration>,
        session_id: Option<u64>,
        initialized: bool,
        (bid, ask): (f64, f64),
        status: FeedStatus,
        max_age: Duration,
    ) -> BookState {
        let (Some(age), Some(session_id)) = (age, session_id) else {
            return BookState::Missing;
        };
        if !initialized {
            return BookState::Missing;
        }
        if !status.connected || status.session_id != session_id {
            return BookState::FeedUnavailable;
        }
        if age >= max_age {
            return BookState::Stale;
        }
        if !bid.is_finite() || bid <= 0.0 || !ask.is_finite() || ask <= 0.0 || bid > ask {
            return BookState::InvalidQuotes;
        }
        BookState::Usable
    }

    fn next_book_expiry(
        pair: &BidAskAcrossExchanges,
        max_age: Duration,
        evaluated_at: Instant,
    ) -> Option<Instant> {
        // Only a calculation acknowledges expiry; rebuilding select! must retain overdue deadlines.
        [pair.left_received_instant, pair.right_received_instant]
            .into_iter()
            .flatten()
            .filter_map(|received_at| received_at.checked_add(max_age))
            .filter(|deadline| *deadline > evaluated_at)
            .min()
    }
}
