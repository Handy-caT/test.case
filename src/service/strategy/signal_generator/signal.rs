use std::fmt::{self, Formatter};
use std::time::{Duration, Instant};

use derive_more::Display;

use crate::entity::exchange::Exchange;
use crate::entity::order_book::{BidAskAcrossExchanges, CrossExchangeSpreads, InvalidQuoteReason};
use crate::entity::symbol::Symbol;
use crate::util::time::utc_timestamp_nanoseconds;

#[derive(Clone, Copy, Debug, Display, Eq, PartialEq)]
pub enum SignalDecision {
    #[display("LongAsterShortBinance")]
    LongAsterShortBinance,
    #[display("LongBinanceShortAster")]
    LongBinanceShortAster,
    #[display("Hold")]
    Hold,
}

#[derive(Clone, Debug, Display)]
pub enum HoldReason {
    #[display("order book is not initialized for {:?}", exchanges)]
    OrderBookUnavailable { exchanges: Vec<Exchange> },
    #[display("feed is not connected to the session used by {:?}", exchanges)]
    FeedUnavailable { exchanges: Vec<Exchange> },
    #[display(
        "order-book age exceeds limit: binance={:?}, aster={:?}, limit={:?}",
        binance_age,
        aster_age,
        max_age
    )]
    StaleBook {
        binance_age: Duration,
        aster_age: Duration,
        max_age: Duration,
    },
    #[display("invalid quotes: {}", reason)]
    InvalidQuotes { reason: InvalidQuoteReason },
    #[display("neither spread is strictly above the threshold")]
    ThresholdNotExceeded,
}

#[derive(Clone, Debug)]
pub struct SignalResult {
    pub symbol: Symbol,
    pub calculated_at: Instant,
    pub calculated_at_timestamp_nanoseconds: u64,
    pub decision: SignalDecision,
    pub quotes: Option<BidAskAcrossExchanges>,
    pub spreads: Option<CrossExchangeSpreads>,
    pub threshold_bps: f64,
    pub hold_reason: Option<HoldReason>,
}

impl SignalResult {
    pub fn is_expired(&self, now: Instant, max_book_age: Duration) -> bool {
        if now.saturating_duration_since(self.calculated_at) >= max_book_age {
            return true;
        }
        if matches!(
            self.hold_reason,
            Some(
                HoldReason::OrderBookUnavailable { .. }
                    | HoldReason::FeedUnavailable { .. }
                    | HoldReason::StaleBook { .. }
            )
        ) {
            return false;
        }
        self.quotes.as_ref().is_none_or(|quotes| {
            [quotes.left_received_instant, quotes.right_received_instant]
                .into_iter()
                .any(|received_at| {
                    received_at.is_none_or(|at| now.saturating_duration_since(at) >= max_book_age)
                })
        })
    }

    pub fn hold_without_quotes(
        symbol: Symbol,
        threshold_bps: f64,
        hold_reason: HoldReason,
    ) -> Self {
        Self {
            symbol,
            calculated_at: Instant::now(),
            calculated_at_timestamp_nanoseconds: utc_timestamp_nanoseconds(),
            decision: SignalDecision::Hold,
            quotes: None,
            spreads: None,
            threshold_bps,
            hold_reason: Some(hold_reason),
        }
    }

    pub fn hold_with_quotes(
        symbol: Symbol,
        quotes: BidAskAcrossExchanges,
        threshold_bps: f64,
        reason: HoldReason,
    ) -> Self {
        Self {
            symbol,
            calculated_at: Instant::now(),
            calculated_at_timestamp_nanoseconds: utc_timestamp_nanoseconds(),
            decision: SignalDecision::Hold,
            quotes: Some(quotes),
            spreads: None,
            threshold_bps,
            hold_reason: Some(reason),
        }
    }
}

impl std::fmt::Display for SignalResult {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} {} | calculated_at={}ns",
            self.symbol, self.decision, self.calculated_at_timestamp_nanoseconds
        )?;

        if let Some(quotes) = &self.quotes {
            write!(
                formatter,
                " | quotes: {} bid={:?} @ {:?}, ask={:?} @ {:?} ts={}ms, {} bid={:?} @ {:?}, ask={:?} @ {:?} ts={}ms",
                quotes.exchange_left,
                quotes.left_bid_price,
                quotes.left_bid_volume,
                quotes.left_ask_price,
                quotes.left_ask_volume,
                quotes.left_timestamp_ms,
                quotes.exchange_right,
                quotes.right_bid_price,
                quotes.right_bid_volume,
                quotes.right_ask_price,
                quotes.right_ask_volume,
                quotes.right_timestamp_ms,
            )?;
        } else {
            formatter.write_str(" | quotes unavailable")?;
        }

        if let Some(spreads) = self.spreads {
            write!(
                formatter,
                " | spreads: sell_binance_buy_aster={:?} bps, sell_aster_buy_binance={:?} bps",
                spreads.sell_binance_buy_aster_bps, spreads.sell_aster_buy_binance_bps,
            )?;
        } else {
            formatter.write_str(" | spreads unavailable")?;
        }

        write!(formatter, " | threshold={:?} bps", self.threshold_bps)?;
        if let Some(reason) = &self.hold_reason {
            write!(formatter, " | reason: {reason}")?;
        }

        Ok(())
    }
}
