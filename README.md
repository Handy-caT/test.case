# test-case

A Rust service that reads public ETHUSDT perpetual order books from Binance and Aster and calculates a cross-exchange spread signal. It uses no API keys, private endpoints, or order execution.

## Data sources

Both sources use the `ethusdt@depth20@100ms` stream:

| Exchange | Market | WebSocket URL |
| --- | --- | --- |
| Binance | USD-M futures | `wss://fstream.binance.com/public/stream?streams=ethusdt@depth20@100ms` |
| Aster | Perpetual futures | `wss://fstream.asterdex.com/stream?streams=ethusdt@depth20@100ms` |

Each message contains a complete partial snapshot of the top 20 levels. A new snapshot replaces the previous one, so this mode does not require REST snapshot/diff reconstruction.

## Storage and synchronization

Independent feed tasks send snapshots through bounded Crossfire channels using nonblocking `try_send`. One book-update task writes both books. Each book is an immutable row published through `ArcSwap`: a concurrent reader sees a complete old or new row. The two books are read independently, with their own receipt times and session IDs.

Feed state uses Tokio `watch`; publication is serialized with session changes to reject snapshots from disconnected or superseded sessions. An unavailable feed reconnects with backoff while the other continues. Useful-data deadlines and Ping/Pong detect stalled connections. Exhausting the configured attempt limit ends the application with an error. Logs and per-exchange metrics expose connection and book status.

## Signal

The generator reads best bids and asks from storage and evaluates both directions:

```text
spread_bps = 10_000 * (sell_bid - buy_ask) / ((sell_bid + buy_ask) / 2)
```

Selling on Binance and buying on Aster above the threshold produces `LongAsterShortBinance`; the reverse produces `LongBinanceShortAster`. Otherwise the result is `Hold`. Missing, stale, disconnected, or invalid inputs also produce `Hold`. Defaults are a strict 5 bps threshold and a maximum book age of 3 seconds, measured with a monotonic clock.

Results go to stdout with the calculation time, available source quotes, spreads, and Hold reason. Expired queued results are discarded before writing. The signal ignores fees, slippage, and funding.

## Run with Docker and Grafana

Requires Docker with Compose. From the repository root:

```sh
./monitoring/start.sh
```

This builds the service and starts it with Prometheus and Grafana. On first launch, the script generates a Grafana password in `.env` unless `GRAFANA_ADMIN_PASSWORD` is already supplied. Sign in as `admin` using that password.

- [Grafana dashboard](http://127.0.0.1:3000/d/test-case-runtime/test-case-runtime)
- [Prometheus](http://127.0.0.1:9090)
- [Service metrics](http://127.0.0.1:9898/metrics)

View service output or stop the stack:

```sh
docker compose logs -f app
docker compose down
```

## Run the service natively

Requires Rust stable. To use the native service with Grafana and Prometheus in Docker, start the service in one terminal:

```sh
TEST_CASE__METRICS__LISTEN_ADDR=0.0.0.0:9898 \
  cargo run --release --locked -- --config config.example.toml
```

Start monitoring in another terminal:

```sh
./monitoring/start.sh native
```

Use the same Grafana URL and credentials as above. Stop the native service with Ctrl+C. For the service alone, run the Cargo command without the environment override; metrics then bind to `127.0.0.1:9898`.

## Configuration

Settings are loaded from Rust defaults, the optional `--config` TOML file, and then `TEST_CASE__...` environment overrides. See [config.example.toml](config.example.toml); for example, `TEST_CASE__SIGNAL_THRESHOLD_BPS=7.5` changes the signal threshold. Compose mounts this file and forwards the supported overrides. Recreate the app after changing its configuration.
