use derive_more::Display;
use serde::Deserialize;

pub mod price_quantity;
pub mod response;

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Display, Eq, Hash, Ord, PartialEq, PartialOrd,
)]
#[repr(u8)]
pub enum Exchange {
    #[default]
    #[display("BIN-futures")]
    BinanceFutures = 1,
    #[display("AST-perps")]
    AsterPerpetuals = 2,
}

impl Exchange {
    pub const TOTAL: usize = 2;
}
