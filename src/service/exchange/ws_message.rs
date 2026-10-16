use serde::Deserialize;

#[derive(Deserialize)]
pub struct WsMessage<T> {
    pub data: T,
    pub stream: Option<String>,
}
