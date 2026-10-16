use std::io::{self, IsTerminal, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crossfire::mpsc::{self, Array};
use crossfire::{MTx, Rx};
use eyre::WrapErr;
use tracing::Level;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::writer::MakeWriterExt;

use crate::config::LogConfig;

enum LogMessage {
    Line(Vec<u8>),
    Shutdown,
}

#[derive(Clone)]
struct LogWriter {
    sender: MTx<Array<LogMessage>>,
}

struct LogWorker {
    sender: MTx<Array<LogMessage>>,
    completed: Rx<Array<()>>,
    stopping: Arc<AtomicBool>,
}

pub struct LoggingGuard {
    stdout: LogWorker,
    stderr: LogWorker,
    shutdown_timeout: Duration,
}

impl LogConfig {
    pub fn setup(&self) -> eyre::Result<LoggingGuard> {
        let filter = EnvFilter::builder()
            .with_regex(false)
            .parse(&self.filter)
            .wrap_err("Invalid log.filter")?;
        let (stdout, stdout_worker) =
            LogWorker::start(io::stdout(), "log-stdout", self.queue_capacity)?;
        let (stderr, stderr_worker) =
            LogWorker::start(io::stderr(), "log-stderr", self.queue_capacity)?;
        let guard = LoggingGuard {
            stdout: stdout_worker,
            stderr: stderr_worker,
            shutdown_timeout: self.shutdown_timeout,
        };

        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(stderr.with_max_level(Level::WARN).or_else(stdout))
            .with_ansi(io::stdout().is_terminal() && io::stderr().is_terminal())
            .try_init()
            .map_err(|error| eyre::eyre!("Could not initialize logging: {error}"))?;
        Ok(guard)
    }
}

impl LogWorker {
    fn start<W>(mut output: W, name: &str, capacity: usize) -> eyre::Result<(LogWriter, Self)>
    where
        W: Write + Send + 'static,
    {
        let (sender, receiver) = mpsc::bounded_blocking(capacity);
        let (done_sender, completed) = mpsc::bounded_blocking(1);
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                while let Ok(LogMessage::Line(bytes)) = receiver.recv() {
                    if output.write_all(&bytes).is_err() {
                        break;
                    }
                    if worker_stopping.load(Ordering::Acquire) && receiver.is_empty() {
                        break;
                    }
                }
                let _ = output.flush();
                let _ = done_sender.try_send(());
            })
            .wrap_err_with(|| format!("Could not start {name} writer"))?;
        let writer = LogWriter {
            sender: sender.clone(),
        };
        Ok((
            writer,
            Self {
                sender,
                completed,
                stopping,
            },
        ))
    }
}

impl Drop for LoggingGuard {
    fn drop(&mut self) {
        let deadline = Instant::now() + self.shutdown_timeout;
        for worker in [&self.stderr, &self.stdout] {
            worker.stopping.store(true, Ordering::Release);
            let _ = worker.sender.try_send(LogMessage::Shutdown);
        }
        // A blocked OS pipe cannot be joined safely; wait only within the shared flush budget.
        for worker in [&self.stderr, &self.stdout] {
            let _ = worker
                .completed
                .recv_timeout(deadline.saturating_duration_since(Instant::now()));
        }
    }
}

impl<'a> MakeWriter<'a> for LogWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let _ = self.sender.try_send(LogMessage::Line(bytes.to_vec()));
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
