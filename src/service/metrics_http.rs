use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use eyre::WrapErr;
use tokio::net::TcpListener;
use tower_http::timeout::TimeoutLayer;

use crate::config::MetricsConfig;
use crate::metrics::Metrics;
use crate::task::Task;
use crate::task::builder::TaskBuilder;

pub struct MetricsHttpServer {
    listener: TcpListener,
    metrics: Arc<Metrics>,
    request_timeout: Duration,
}

impl MetricsHttpServer {
    pub async fn spawn(config: MetricsConfig, metrics: Arc<Metrics>) -> eyre::Result<Task> {
        let address = config.listen_addr;
        let listener = TcpListener::bind(address)
            .await
            .wrap_err_with(|| format!("Could not bind Prometheus metrics listener at {address}"))?;
        log::info!("Prometheus metrics listening on {}", listener.local_addr()?);

        Ok(TaskBuilder::new("MetricsHttpServer")
            .with_task(async move {
                Self {
                    listener,
                    metrics,
                    request_timeout: config.request_timeout,
                }
                .serve()
                .await
            })
            .spawn())
    }

    async fn serve(self) -> eyre::Result<()> {
        let router = Router::new()
            .route("/metrics", get(Self::export))
            .with_state(self.metrics)
            .layer(TimeoutLayer::with_status_code(
                StatusCode::REQUEST_TIMEOUT,
                self.request_timeout,
            ));
        axum::serve(self.listener, router).await?;
        Ok(())
    }

    async fn export(State(metrics): State<Arc<Metrics>>) -> Response {
        match metrics.encode() {
            Ok(body) => (
                [(
                    header::CONTENT_TYPE,
                    "text/plain; version=0.0.4; charset=utf-8",
                )],
                body,
            )
                .into_response(),
            Err(error) => {
                log::error!("Could not encode Prometheus metrics: {error:#}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "metrics encoding failed\n",
                )
                    .into_response()
            }
        }
    }
}
