FROM rust:1.92.0-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release --locked

FROM debian:bookworm-slim AS runtime

RUN groupadd --system --gid 10001 test-case \
    && useradd --system --uid 10001 --gid test-case --no-create-home --shell /usr/sbin/nologin test-case

COPY --from=builder --chown=10001:10001 /app/target/release/test-case /usr/local/bin/test-case

USER 10001:10001
EXPOSE 9898
ENV TEST_CASE__METRICS__LISTEN_ADDR=0.0.0.0:9898

ENTRYPOINT ["/usr/local/bin/test-case"]
