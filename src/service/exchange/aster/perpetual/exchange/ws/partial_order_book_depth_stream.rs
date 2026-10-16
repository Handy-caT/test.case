use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use futures::Stream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::http::Uri;
use url::Url;

use crate::config::FeedConfig;
use crate::entity::exchange::response::PartialOrderBookDepth;
use crate::entity::symbol::Symbol;
use crate::service::exchange::aster::perpetual::metrics::AsterMetrics;
use crate::service::exchange::aster::perpetual::parser::Parser;
use crate::service::exchange::aster::perpetual::request::partial_order_book_depth::PartialOrderBookDepthRequest;
use crate::service::exchange::metrics::{DepthErrorKind, FeedMessageResult};
use crate::service::exchange::status::{FeedStatus, FeedStatusSender};
use crate::service::exchange::transport::metrics::FeedTransportMetrics;
use crate::service::exchange::transport::{FeedTransport, TransportFrame};

pub struct PartialOrderBookDepthStream {
    transport: FeedTransport,
    symbol: Symbol,
    metrics: AsterMetrics,
    last_update_id: Option<u64>,
    session_id: Option<u64>,
}

impl PartialOrderBookDepthStream {
    pub async fn connect(
        mut endpoint: Url,
        symbol: Symbol,
        settings: FeedConfig,
        metrics: AsterMetrics,
        transport_metrics: FeedTransportMetrics,
        status_sender: FeedStatusSender,
        status_receiver: tokio::sync::watch::Receiver<FeedStatus>,
    ) -> eyre::Result<Self> {
        endpoint.query_pairs_mut().append_pair(
            "streams",
            &PartialOrderBookDepthRequest::from(symbol.clone()).to_string(),
        );
        let uri = endpoint.as_str().parse::<Uri>()?;
        let transport = FeedTransport::connect(
            uri,
            settings,
            transport_metrics,
            status_sender,
            status_receiver,
        )
        .await?;
        Ok(Self {
            transport,
            symbol,
            metrics,
            last_update_id: None,
            session_id: None,
        })
    }

    fn parse_frame(
        &mut self,
        frame: TransportFrame,
    ) -> Option<eyre::Result<PartialOrderBookDepth>> {
        if self.session_id != Some(frame.session_id) {
            self.session_id = Some(frame.session_id);
            self.last_update_id = None;
        }
        match frame.message {
            Message::Text(text) => {
                let started = Instant::now();
                let result = Parser::parse_partial_order_book_depth(
                    text.as_str(),
                    frame.received_at,
                    frame.received_instant,
                    frame.session_id,
                );
                self.metrics
                    .parsing
                    .observe(started.elapsed().as_nanos() as f64);
                let depth = match result {
                    Ok(depth) => depth,
                    Err(error) => {
                        log::debug!("depth frame rejected: parse/validation error={error:?}");
                        self.metrics.depth.record_parse_error(error);
                        return None;
                    }
                };
                if depth.symbol != self.symbol {
                    log::debug!(
                        "depth frame rejected: expected_symbol={} received_symbol={}",
                        self.symbol,
                        depth.symbol
                    );
                    self.metrics
                        .depth
                        .record_message(FeedMessageResult::WrongSymbol);
                    self.metrics.depth.record_error(DepthErrorKind::Validation);
                    return None;
                }
                if let Some(last) = self.last_update_id {
                    if depth.update_id == last {
                        log::trace!(
                            "duplicate depth update ignored: update_id={} session_id={}",
                            depth.update_id,
                            frame.session_id
                        );
                        self.metrics
                            .depth
                            .record_message(FeedMessageResult::Duplicate);
                        return None;
                    }
                    if depth.update_id < last {
                        log::trace!(
                            "outdated depth update ignored: update_id={} last_update_id={} session_id={}",
                            depth.update_id,
                            last,
                            frame.session_id
                        );
                        self.metrics
                            .depth
                            .record_message(FeedMessageResult::Outdated);
                        return None;
                    }
                }
                if !self.transport.accepted_update(frame.session_id) {
                    return None;
                }
                self.last_update_id = Some(depth.update_id);
                self.metrics
                    .depth
                    .set_last_valid_update_timestamp(frame.received_at);
                self.metrics
                    .depth
                    .record_message(FeedMessageResult::Accepted);
                Some(Ok(depth))
            }
            Message::Pong(_)
            | Message::Ping(_)
            | Message::Close(_)
            | Message::Binary(_)
            | Message::Frame(_) => {
                self.metrics
                    .depth
                    .record_message(FeedMessageResult::Control);
                None
            }
        }
    }
}

impl Stream for PartialOrderBookDepthStream {
    type Item = eyre::Result<PartialOrderBookDepth>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.transport).poll_next(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(depth) = self.parse_frame(frame) {
                    Poll::Ready(Some(depth))
                } else {
                    context.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
    }
}
