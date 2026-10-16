use std::time::Instant;

use crate::entity::exchange::Exchange;
use crate::entity::exchange::price_quantity::PriceQuantity;
use crate::entity::symbol::Symbol;

#[derive(Clone, Debug, PartialEq)]
pub struct PartialOrderBookDepth {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub bids: Vec<PriceQuantity>,
    pub asks: Vec<PriceQuantity>,
    pub time: u64,
    pub update_id: u64,
    pub received_at: u64,
    pub received_instant: Instant,
    pub session_id: u64,
}
