use derive_more::{Display, From};

use crate::entity::symbol::Symbol;

#[derive(Clone, Debug, Display, From)]
#[display("{}@depth20@100ms", _0.as_ref().to_ascii_lowercase())]
pub struct PartialOrderBookDepthRequest(pub Symbol);
