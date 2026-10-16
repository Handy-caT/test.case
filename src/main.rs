use std::process::ExitCode;
use std::sync::Arc;

use crossfire::mpsc::{self, Array};
use tokio::sync::Notify;
use tokio::time;

use crate::config::Config;
use crate::db::strategy::common::order_book::OrderBookStorage;
use crate::entity::symbol::Symbol;
use crate::service::exchange::aster::perpetual::exchange::AsterPerpetualExchange;
use crate::service::exchange::aster::perpetual::metrics::AsterMetrics;
use crate::service::exchange::binance::futures::exchange::BinanceFuturesExchange;
use crate::service::exchange::binance::futures::metrics::BinanceMetrics;
use crate::service::exchange::transport::metrics::FeedTransportMetrics;
use crate::service::metrics_http::MetricsHttpServer;
use crate::service::order_book::metrics::BookUpdateMetrics;
use crate::service::order_book::{OrderBookUpdateTask, OrderBookUpdated};
use crate::service::signal_output::SignalOutput;
use crate::service::strategy::signal_generator::metrics::SignalMetrics;
use crate::service::strategy::signal_generator::{SignalGenerator, SignalResult};
use crate::task::{Task, TaskState};
use crate::util::channel_metrics::ChannelMetrics;

mod config;
mod db;
mod entity;
mod metrics;
mod service;
mod task;
mod util;

#[tokio::main]
async fn main() -> ExitCode {
    let config = match Config::load() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("Could not load configuration: {error:#}");
            return ExitCode::FAILURE;
        }
    };
    let _logging = match config.log.setup() {
        Ok(guard) => guard,
        Err(error) => {
            eprintln!("Could not start logging: {error:#}");
            return ExitCode::FAILURE;
        }
    };
    match run(config).await {
        Ok(()) => {
            log::info!("Application stopped");
            ExitCode::SUCCESS
        }
        Err(error) => {
            log::error!("Application failed: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(config: Config) -> eyre::Result<()> {
    log::info!(
        "Starting market depth service: symbol={}, signal_threshold_bps={}",
        &config.symbol,
        config.signal_threshold_bps
    );
    let symbol = Symbol::from(config.symbol.as_str());
    let mut metrics = metrics::Metrics::new()?;
    let binance_metrics = BinanceMetrics::new(&mut metrics)?;
    let aster_metrics = AsterMetrics::new(&mut metrics)?;
    let binance_transport_metrics = FeedTransportMetrics::new(&mut metrics, "binance")?;
    let aster_transport_metrics = FeedTransportMetrics::new(&mut metrics, "aster")?;
    let binance_channel =
        ChannelMetrics::new(&mut metrics, "binance_depth", config.channels.binance_depth)?;
    let aster_channel =
        ChannelMetrics::new(&mut metrics, "aster_depth", config.channels.aster_depth)?;
    let update_channel =
        ChannelMetrics::new(&mut metrics, "book_updates", config.channels.book_updates)?;
    let result_channel = ChannelMetrics::new(
        &mut metrics,
        "signal_results",
        config.channels.signal_results,
    )?;
    let book_update_metrics = BookUpdateMetrics::new(
        &mut metrics,
        &config.symbol,
        binance_channel.received,
        aster_channel.received,
        update_channel.sender,
        update_channel.queue_depth,
    )?;
    let signal_metrics = SignalMetrics::new(
        &mut metrics,
        &config.symbol,
        config.signal_threshold_bps,
        update_channel.received,
        result_channel.sender,
    )?;
    let result_queue_depth = result_channel.queue_depth;
    let metrics = Arc::new(metrics);
    let order_book_storage = Arc::new(OrderBookStorage::default());

    let (update_sender, update_receiver) =
        mpsc::bounded_async::<OrderBookUpdated>(config.channels.book_updates);
    let (signal_sender, signal_receiver) =
        mpsc::bounded_async_blocking::<SignalResult>(config.channels.signal_results);
    let signal_sender_weak = signal_sender.downgrade();
    let signal_result_space = Arc::new(Notify::new());
    SignalOutput {
        receiver: signal_receiver,
        result_space: Arc::clone(&signal_result_space),
        received: result_channel.received,
        max_book_age: config.feed.published_book_max_age,
    }
    .spawn()?;

    let metrics_task = MetricsHttpServer::spawn(config.metrics, Arc::clone(&metrics)).await?;
    let binance = BinanceFuturesExchange::new(
        config.binance_ws_base_url.clone(),
        config.feed,
        config.channels.sampling_interval,
    )
    .subscribe_depth(
        symbol.clone(),
        binance_metrics,
        binance_transport_metrics,
        binance_channel.sender,
        binance_channel.queue_depth,
        config.channels.binance_depth,
    );
    let aster = AsterPerpetualExchange::new(
        config.aster_ws_base_url.clone(),
        config.feed,
        config.channels.sampling_interval,
    )
    .subscribe_depth(
        symbol.clone(),
        aster_metrics,
        aster_transport_metrics,
        aster_channel.sender,
        aster_channel.queue_depth,
        config.channels.aster_depth,
    );
    let signal_generator = Arc::new(SignalGenerator {
        threshold_bps: config.signal_threshold_bps,
        symbol: symbol.clone(),
        storage: Arc::clone(&order_book_storage),
        binance_status: binance.status_receiver,
        aster_status: aster.status_receiver,
        max_book_age: config.feed.published_book_max_age,
        metrics: signal_metrics,
    });

    let mut tasks = ApplicationTasks {
        binance: Some(binance.task),
        aster: Some(aster.task),
        order_book_updates: Some(
            OrderBookUpdateTask {
                symbol,
                storage: order_book_storage,
                binance_depth: binance.stream,
                aster_depth: aster.stream,
                book_updates: update_sender,
                binance_feed_status: binance.status_sender,
                aster_feed_status: aster.status_sender,
                metrics: book_update_metrics,
                queue_sampling_interval: config.channels.sampling_interval,
            }
            .spawn(),
        ),
        signal_generator: Some(Arc::clone(&signal_generator).spawn_signal_generation_task(
            update_receiver,
            signal_sender,
            Arc::clone(&signal_result_space),
        )),
        metrics_server: Some(metrics_task),
    };
    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);
    let mut signal_queue_sampling = time::interval(config.channels.sampling_interval);
    signal_queue_sampling.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    let result = loop {
        tokio::select! {
            result = tasks.binance.as_mut().expect("Binance task is monitored").join_blocking() => {
                tasks.binance = None;
                break ApplicationTasks::unexpected_end("Binance depth source", result);
            },
            result = tasks.aster.as_mut().expect("Aster task is monitored").join_blocking() => {
                tasks.aster = None;
                break ApplicationTasks::unexpected_end("Aster depth source", result);
            },
            result = tasks.order_book_updates.as_mut().expect("Order-book updates are monitored").join_blocking() => {
                tasks.order_book_updates = None;
                break ApplicationTasks::unexpected_end("Order-book updates", result);
            },
            result = tasks.signal_generator.as_mut().expect("Signal generator is monitored").join_blocking() => {
                tasks.signal_generator = None;
                break ApplicationTasks::unexpected_end("Signal generator", result);
            },
            result = tasks.metrics_server.as_mut().expect("Metrics HTTP server is monitored").join_blocking() => {
                tasks.metrics_server = None;
                break ApplicationTasks::unexpected_end("Metrics HTTP server", result);
            },
            _ = signal_queue_sampling.tick() => {
                if let Some(signal_sender) = signal_sender_weak
                    .upgrade::<crossfire::MAsyncTx<Array<SignalResult>>>()
                {
                    result_queue_depth.set(signal_sender.len() as f64);
                }
            },
            signal = &mut shutdown_signal => {
                if signal.is_ok() {
                    log::info!("Shutdown requested; stopping application tasks");
                }
                break signal.map_err(Into::into);
            }
        }
    };

    match (result, tasks.cancel_and_wait().await) {
        (Err(error), _) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(shutdown_error)) => Err(shutdown_error),
    }
}

struct ApplicationTasks {
    binance: Option<Task>,
    aster: Option<Task>,
    order_book_updates: Option<Task>,
    signal_generator: Option<Task>,
    metrics_server: Option<Task>,
}

impl ApplicationTasks {
    fn unexpected_end(name: &str, result: eyre::Result<TaskState>) -> eyre::Result<()> {
        match result {
            Ok(state) => Err(eyre::eyre!("{name} ended unexpectedly in state {state:?}")),
            Err(error) => Err(error.wrap_err(format!("{name} task could not be joined"))),
        }
    }

    async fn cancel_and_wait(&mut self) -> eyre::Result<()> {
        for task in [
            &mut self.binance,
            &mut self.aster,
            &mut self.order_book_updates,
            &mut self.signal_generator,
            &mut self.metrics_server,
        ]
        .into_iter()
        .flatten()
        {
            task.cancel();
        }

        let mut shutdown_error = None;
        for task in [
            &mut self.binance,
            &mut self.aster,
            &mut self.order_book_updates,
            &mut self.signal_generator,
            &mut self.metrics_server,
        ] {
            let Some(mut task) = task.take() else {
                continue;
            };

            match task.join_blocking().await {
                Ok(TaskState::Cancelled | TaskState::Finished) => {}
                Ok(state) => {
                    shutdown_error.get_or_insert_with(|| {
                        eyre::eyre!("{} ended during shutdown in state {state:?}", task.name)
                    });
                }
                Err(error) => {
                    shutdown_error.get_or_insert_with(|| {
                        error.wrap_err(format!(
                            "{} task could not be joined during shutdown",
                            task.name
                        ))
                    });
                }
            }
        }

        shutdown_error.map_or(Ok(()), Err)
    }
}
