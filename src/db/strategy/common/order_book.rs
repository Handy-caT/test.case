use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use derive_more::{Display, Error};
use eyre::eyre;

use crate::entity::exchange::Exchange;
use crate::entity::exchange::response::PartialOrderBookDepth;
use crate::entity::order_book::BidAskAcrossExchanges;

const ORDER_BOOK_DEPTH: usize = 20;
type OrderBookDepth = [f64; ORDER_BOOK_DEPTH];

#[derive(Debug, Display, Error)]
pub enum OrderBookReadError {
    #[display("Order-book storage has no row for {}", exchange)]
    MissingRow { exchange: Exchange },
}

#[derive(Clone, Debug, Default)]
pub struct OrderBookRow {
    pub exchange_id: u8,
    pub timestamp_ms: i64,
    pub received_at: Option<u64>,
    pub received_instant: Option<Instant>,
    pub session_id: Option<u64>,
    pub bids_size: u8,
    pub bid_prices: OrderBookDepth,
    pub bid_sizes: OrderBookDepth,
    pub asks_size: u8,
    pub ask_prices: OrderBookDepth,
    pub ask_sizes: OrderBookDepth,
}

impl OrderBookRow {
    pub fn from_partial_depth(book: &PartialOrderBookDepth) -> eyre::Result<Self> {
        let PartialOrderBookDepth {
            exchange,
            time,
            bids,
            asks,
            received_at,
            received_instant,
            session_id,
            ..
        } = book;
        let timestamp = i64::try_from(*time)
            .map_err(|_| eyre!("Exchange timestamp {time} does not fit in i64"))?;
        let bids_size = bids.len().min(ORDER_BOOK_DEPTH);
        let asks_size = asks.len().min(ORDER_BOOK_DEPTH);

        if bids_size == 0 {
            return Err(eyre!("No bid data received"));
        }
        if asks_size == 0 {
            return Err(eyre!("No ask data received"));
        }

        let mut row = Self {
            exchange_id: *exchange as u8,
            timestamp_ms: timestamp,
            received_at: Some(*received_at),
            received_instant: Some(*received_instant),
            session_id: Some(*session_id),
            ..Self::default()
        };

        for (index, bid) in bids.iter().take(ORDER_BOOK_DEPTH).enumerate() {
            row.bid_prices[index] = bid.price;
            row.bid_sizes[index] = bid.quantity;
        }
        for (index, ask) in asks.iter().take(ORDER_BOOK_DEPTH).enumerate() {
            row.ask_prices[index] = ask.price;
            row.ask_sizes[index] = ask.quantity;
        }

        row.bids_size = bids_size as u8;
        row.asks_size = asks_size as u8;

        Ok(row)
    }
}

#[derive(Debug)]
pub struct OrderBookStorage {
    rows: Vec<ArcSwap<OrderBookRow>>,
}

impl Default for OrderBookStorage {
    fn default() -> Self {
        let rows = (0..=Exchange::TOTAL)
            .map(|exchange_id| {
                ArcSwap::from_pointee(OrderBookRow {
                    exchange_id: exchange_id as u8,
                    ..OrderBookRow::default()
                })
            })
            .collect();
        Self { rows }
    }
}

impl OrderBookStorage {
    pub fn replace(&self, row: Arc<OrderBookRow>) -> eyre::Result<()> {
        let target = self
            .rows
            .get(usize::from(row.exchange_id))
            .ok_or_else(|| eyre!("Invalid exchange id {}", row.exchange_id))?;
        target.store(row);
        Ok(())
    }

    pub fn select(&self, exchange_id: u8) -> Option<Arc<OrderBookRow>> {
        self.rows
            .get(usize::from(exchange_id))
            .map(ArcSwap::load_full)
    }

    pub fn read_pair(
        &self,
        exchange_left: Exchange,
        exchange_right: Exchange,
    ) -> Result<BidAskAcrossExchanges, OrderBookReadError> {
        let left_row = self
            .select(exchange_left as u8)
            .ok_or(OrderBookReadError::MissingRow {
                exchange: exchange_left,
            })?;
        let right_row =
            self.select(exchange_right as u8)
                .ok_or(OrderBookReadError::MissingRow {
                    exchange: exchange_right,
                })?;

        Ok(BidAskAcrossExchanges {
            exchange_left,
            left_bid_price: left_row.bid_prices[0],
            left_bid_volume: left_row.bid_sizes[0],
            left_ask_price: left_row.ask_prices[0],
            left_ask_volume: left_row.ask_sizes[0],
            left_timestamp_ms: left_row.timestamp_ms,
            left_received_instant: left_row.received_instant,
            left_session_id: left_row.session_id,
            left_initialized: left_row.bids_size > 0 && left_row.asks_size > 0,
            exchange_right,
            right_bid_price: right_row.bid_prices[0],
            right_bid_volume: right_row.bid_sizes[0],
            right_ask_price: right_row.ask_prices[0],
            right_ask_volume: right_row.ask_sizes[0],
            right_timestamp_ms: right_row.timestamp_ms,
            right_received_instant: right_row.received_instant,
            right_session_id: right_row.session_id,
            right_initialized: right_row.bids_size > 0 && right_row.asks_size > 0,
        })
    }
}
