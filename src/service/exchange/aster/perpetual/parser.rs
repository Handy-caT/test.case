use crate::entity::exchange::Exchange;
use crate::entity::exchange::price_quantity::PriceQuantity;
use crate::entity::exchange::response::PartialOrderBookDepth;
use crate::entity::symbol::Symbol;
use crate::service::exchange::aster::perpetual::response::PartialOrderBookDepthResponse;
use crate::service::exchange::depth::DepthParseError;
use crate::service::exchange::ws_message::WsMessage;

const DEPTH_LIMIT: usize = 20;
const DEPTH_STREAM_KIND: &str = "depth20@100ms";

#[derive(Debug)]
pub struct Parser;

impl Parser {
    pub fn parse_partial_order_book_depth(
        text: &str,
        received_at: u64,
        received_instant: Instant,
        session_id: u64,
    ) -> Result<PartialOrderBookDepth, DepthParseError> {
        let message: WsMessage<PartialOrderBookDepthResponse> =
            serde_json::from_str(text).map_err(|_| DepthParseError::Malformed)?;

        let stream = message
            .stream
            .as_deref()
            .ok_or(DepthParseError::Malformed)?;
        let symbol = Self::parse_symbol_from_stream(stream)?;
        if message.data.event != "depthUpdate" {
            return Err(DepthParseError::Invalid);
        }
        let message_symbol = Symbol::from(message.data.symbol.as_ref().to_ascii_uppercase());
        if message_symbol != symbol {
            return Err(DepthParseError::Invalid);
        }

        let depth = message.data;
        if i64::try_from(depth.transaction_time_ms).is_err() {
            return Err(DepthParseError::Invalid);
        }
        Ok(PartialOrderBookDepth {
            exchange: Exchange::AsterPerpetuals,
            symbol,
            bids: Self::parse_side(depth.bids, true)?,
            asks: Self::parse_side(depth.asks, false)?,
            time: depth.transaction_time_ms,
            update_id: depth.update_id,
            received_at,
            received_instant,
            session_id,
        })
    }

    fn parse_symbol_from_stream(stream: &str) -> Result<Symbol, DepthParseError> {
        let Some((stream_symbol, suffix)) = stream.split_once('@') else {
            return Err(DepthParseError::Malformed);
        };
        if suffix != DEPTH_STREAM_KIND
            || stream_symbol.is_empty()
            || !stream_symbol.is_ascii()
            || !stream_symbol
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric())
            || stream_symbol.bytes().any(|byte| byte.is_ascii_uppercase())
        {
            return Err(DepthParseError::Malformed);
        }

        Ok(Symbol::from(stream_symbol.to_ascii_uppercase()))
    }

    fn parse_side(
        levels: Vec<[f64; 2]>,
        is_bid: bool,
    ) -> Result<Vec<PriceQuantity>, DepthParseError> {
        if levels.is_empty() || levels.len() > DEPTH_LIMIT {
            return Err(DepthParseError::Invalid);
        }

        let mut levels = levels
            .into_iter()
            .map(|[price, quantity]| {
                if !price.is_finite() || price <= 0.0 || !quantity.is_finite() || quantity <= 0.0 {
                    return Err(DepthParseError::Invalid);
                }

                Ok(PriceQuantity { price, quantity })
            })
            .collect::<Result<Vec<_>, _>>()?;
        levels.sort_unstable_by(|left, right| {
            let ordering = left.price.total_cmp(&right.price);
            if is_bid { ordering.reverse() } else { ordering }
        });

        Ok(levels)
    }
}
use std::time::Instant;
