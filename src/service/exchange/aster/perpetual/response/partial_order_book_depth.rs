use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};

use crate::entity::symbol::Symbol;

#[serde_as]
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct PartialOrderBookDepthResponse {
    #[serde(rename = "e")]
    pub event: String,
    #[serde(rename = "E")]
    pub event_time_ms: u64,
    #[serde(rename = "T")]
    pub transaction_time_ms: u64,
    #[serde(rename = "s")]
    pub symbol: Symbol,
    #[serde(rename = "U")]
    pub first_update_id: u64,
    #[serde(rename = "u")]
    pub update_id: u64,
    #[serde(rename = "pu")]
    pub previous_update_id: u64,
    #[serde_as(as = "Vec<[DisplayFromStr; 2]>")]
    #[serde(rename = "b")]
    pub bids: Vec<[f64; 2]>,
    #[serde_as(as = "Vec<[DisplayFromStr; 2]>")]
    #[serde(rename = "a")]
    pub asks: Vec<[f64; 2]>,
}
