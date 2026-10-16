pub use config::ReconnectOptions;
pub use stream::{
    ConnectionAttemptResult, ConnectionSeries, DisconnectReason, ReconnectControl, ReconnectStream,
    UnderlyingStream,
};

pub mod config;
pub mod strategies;
mod stream;
pub mod tungstenite;
