FROM rust:1.77 AS builder

RUN apt-get update && apt-get install -y cmake pkg-config libssl-dev

WORKDIR /app

# Cache dependencies by copying manifests first
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release && rm -rf src

COPY src ./src
# Touch main.rs so cargo knows to recompile it
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/broker-benchmarker /usr/local/bin/broker-benchmarker

ENTRYPOINT ["broker-benchmarker"]
