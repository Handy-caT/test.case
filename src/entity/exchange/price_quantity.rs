use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Copy, Clone, Debug, Default, PartialEq)]
pub struct PriceQuantity {
    pub price: f64,
    pub quantity: f64,
}
