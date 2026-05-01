FROM rust:latest AS builder

RUN apt-get update && apt-get install -y cmake pkg-config libssl-dev protobuf-compiler

WORKDIR /app

# Cache dependencies by copying manifests and build script first
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release && rm -rf src

COPY src ./src
# Touch main.rs so cargo knows to recompile it
RUN touch src/main.rs && cargo build --release

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/broker-benchmarker /usr/local/bin/broker-benchmarker

ENTRYPOINT ["broker-benchmarker"]
