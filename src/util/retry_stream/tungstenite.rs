use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures::SinkExt;
use tokio::io;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::http::Uri;
use tokio_tungstenite::tungstenite::{Message, error::Error as WsError};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::util::retry_stream::{DisconnectReason, ReconnectStream, UnderlyingStream};

pub struct RetryableWsStream;

impl UnderlyingStream<Uri, Result<Message, WsError>, WsError> for RetryableWsStream {
    type Stream = WebSocketStream<MaybeTlsStream<TcpStream>>;

    fn establish(
        uri: Uri,
        subscribe: Vec<Message>,
        connect_timeout: Duration,
        subscription_timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Stream, WsError>> + Send>> {
        Box::pin(async move {
            let mut ws_connection = tokio::time::timeout(connect_timeout, connect_async(&uri))
                .await
                .map_err(|_| Self::timeout_error())??
                .0;
            for message in subscribe {
                tokio::time::timeout(
                    subscription_timeout,
                    SinkExt::send(&mut ws_connection, message),
                )
                .await
                .map_err(|_| Self::timeout_error())??;
            }
            Ok(ws_connection)
        })
    }

    fn is_write_disconnect_error(error: &WsError) -> bool {
        matches!(
            error,
            WsError::ConnectionClosed
                | WsError::AlreadyClosed
                | WsError::Io(_)
                | WsError::Tls(_)
                | WsError::Protocol(_)
        )
    }

    fn read_disconnect_reason(item: &Result<Message, WsError>) -> Option<DisconnectReason> {
        match item {
            Err(_) => Some(DisconnectReason::ReadError),
            Ok(Message::Close(_)) => Some(DisconnectReason::Close),
            _ => None,
        }
    }

    fn is_timeout_error(error: &WsError) -> bool {
        matches!(error, WsError::Io(io_error) if io_error.kind() == io::ErrorKind::TimedOut)
    }

    fn exhaust_err() -> WsError {
        WsError::Io(io::Error::other("Exhausted"))
    }
}

impl RetryableWsStream {
    fn timeout_error() -> WsError {
        WsError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "websocket connection attempt timed out",
        ))
    }
}

pub type ReconnectWs = ReconnectStream<RetryableWsStream, Uri, Result<Message, WsError>, WsError>;
