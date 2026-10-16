use derive_more::{AsRef, Display, From};
use serde::{Deserialize, Serialize};

#[derive(
    Default,
    Serialize,
    Deserialize,
    Display,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    From,
    AsRef,
    PartialOrd,
    Ord,
)]
#[from(String, &str)]
#[as_ref(str)]
pub struct Symbol(String);

impl PartialEq<str> for Symbol {
    fn eq(&self, other: &str) -> bool {
        self.0.as_str() == other
    }
}
