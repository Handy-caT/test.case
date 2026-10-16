use std::time::Instant;

use derive_more::Display;

use crate::entity::exchange::Exchange;
use crate::util::bp::get_basis_point;

#[derive(Debug, Clone)]
pub struct BidAskAcrossExchanges {
    pub exchange_left: Exchange,
    pub left_bid_price: f64,
    pub left_bid_volume: f64,
    pub left_ask_price: f64,
    pub left_ask_volume: f64,
    pub left_timestamp_ms: i64,
    pub left_received_instant: Option<Instant>,
    pub left_session_id: Option<u64>,
    pub left_initialized: bool,
    pub exchange_right: Exchange,
    pub right_bid_price: f64,
    pub right_bid_volume: f64,
    pub right_ask_price: f64,
    pub right_ask_volume: f64,
    pub right_timestamp_ms: i64,
    pub right_received_instant: Option<Instant>,
    pub right_session_id: Option<u64>,
    pub right_initialized: bool,
}

impl BidAskAcrossExchanges {
    pub fn calculate_spreads(&self) -> Result<CrossExchangeSpreads, InvalidQuoteReason> {
        self.validate_prices()?;

        let sell_binance_buy_aster_bps = get_basis_point(self.left_bid_price, self.right_ask_price);
        if !sell_binance_buy_aster_bps.is_finite() {
            return Err(InvalidQuoteReason::NonFiniteSpread {
                sell_exchange: self.exchange_left,
                buy_exchange: self.exchange_right,
            });
        }

        let sell_aster_buy_binance_bps = get_basis_point(self.right_bid_price, self.left_ask_price);
        if !sell_aster_buy_binance_bps.is_finite() {
            return Err(InvalidQuoteReason::NonFiniteSpread {
                sell_exchange: self.exchange_right,
                buy_exchange: self.exchange_left,
            });
        }

        Ok(CrossExchangeSpreads {
            sell_binance_buy_aster_bps,
            sell_aster_buy_binance_bps,
        })
    }

    fn validate_prices(&self) -> Result<(), InvalidQuoteReason> {
        Self::validate_price(self.exchange_left, QuoteSide::Bid, self.left_bid_price)?;
        Self::validate_price(self.exchange_left, QuoteSide::Ask, self.left_ask_price)?;
        Self::validate_price(self.exchange_right, QuoteSide::Bid, self.right_bid_price)?;
        Self::validate_price(self.exchange_right, QuoteSide::Ask, self.right_ask_price)?;

        if self.left_bid_price > self.left_ask_price {
            return Err(InvalidQuoteReason::CrossedBook {
                exchange: self.exchange_left,
                bid_price: self.left_bid_price,
                ask_price: self.left_ask_price,
            });
        }
        if self.right_bid_price > self.right_ask_price {
            return Err(InvalidQuoteReason::CrossedBook {
                exchange: self.exchange_right,
                bid_price: self.right_bid_price,
                ask_price: self.right_ask_price,
            });
        }

        Self::validate_denominator(
            self.exchange_left,
            self.left_bid_price,
            self.exchange_right,
            self.right_ask_price,
        )?;
        Self::validate_denominator(
            self.exchange_right,
            self.right_bid_price,
            self.exchange_left,
            self.left_ask_price,
        )
    }

    fn validate_price(
        exchange: Exchange,
        side: QuoteSide,
        price: f64,
    ) -> Result<(), InvalidQuoteReason> {
        if !price.is_finite() || price <= 0.0 {
            return Err(InvalidQuoteReason::InvalidPrice {
                exchange,
                side,
                price,
            });
        }

        Ok(())
    }

    fn validate_denominator(
        sell_exchange: Exchange,
        sell_price: f64,
        buy_exchange: Exchange,
        buy_price: f64,
    ) -> Result<(), InvalidQuoteReason> {
        let denominator = (sell_price + buy_price) / 2.0;
        if !denominator.is_finite() || denominator <= 0.0 {
            return Err(InvalidQuoteReason::InvalidDenominator {
                sell_exchange,
                buy_exchange,
            });
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CrossExchangeSpreads {
    pub sell_binance_buy_aster_bps: f64,
    pub sell_aster_buy_binance_bps: f64,
}

#[derive(Clone, Copy, Debug, Display, Eq, PartialEq)]
pub enum QuoteSide {
    #[display("bid")]
    Bid,
    #[display("ask")]
    Ask,
}

#[derive(Clone, Debug, Display, PartialEq)]
pub enum InvalidQuoteReason {
    #[display(
        "{} {} price must be finite and positive (got {})",
        exchange,
        side,
        price
    )]
    InvalidPrice {
        exchange: Exchange,
        side: QuoteSide,
        price: f64,
    },
    #[display("{} best bid {} exceeds best ask {}", exchange, bid_price, ask_price)]
    CrossedBook {
        exchange: Exchange,
        bid_price: f64,
        ask_price: f64,
    },
    #[display(
        "midpoint for selling on {} and buying on {} is not finite and positive",
        sell_exchange,
        buy_exchange
    )]
    InvalidDenominator {
        sell_exchange: Exchange,
        buy_exchange: Exchange,
    },
    #[display(
        "spread for selling on {} and buying on {} is not finite",
        sell_exchange,
        buy_exchange
    )]
    NonFiniteSpread {
        sell_exchange: Exchange,
        buy_exchange: Exchange,
    },
}
