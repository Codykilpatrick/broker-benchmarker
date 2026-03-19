# Broker Benchmarker

Stress-tests **Redpanda** and **NATS JetStream** under identical workloads.
Emits CSV to stdout — capture it with `kubectl logs`.

---

## Quick-start (local smoke test)

```bash
# Start NATS with JetStream
docker run -d --name nats -p 4222:4222 nats:latest -js

# Terminal 1 — consumer (start first)
ROLE=consumer BROKER_TYPE=nats BROKER_ENDPOINT=localhost:4222 \
  GBPS_TARGET=0.01 RUN_MODE=1kb RUN_DURATION_SECS=30 \
  cargo run --release 2>/dev/null

# Terminal 2 — producer
ROLE=producer BROKER_TYPE=nats BROKER_ENDPOINT=localhost:4222 \
  GBPS_TARGET=0.01 RUN_MODE=1kb RUN_DURATION_SECS=30 \
  PRODUCER_START_DELAY_SECS=5 \
  cargo run --release
```

For Redpanda:

```bash
docker run -d --name redpanda -p 9092:9092 \
  redpandadata/redpanda:latest redpanda start --overprovisioned \
  --smp 1 --memory 1G --reserve-memory 0M \
  --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr localhost:9092

ROLE=consumer BROKER_TYPE=redpanda BROKER_ENDPOINT=localhost:9092 \
  GBPS_TARGET=0.01 RUN_MODE=1kb RUN_DURATION_SECS=30 \
  cargo run --release 2>/dev/null
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ROLE` | `consumer` | `producer` or `consumer` |
| `BROKER_TYPE` | `nats` | `nats` or `redpanda` |
| `BROKER_ENDPOINT` | `localhost:4222` / `localhost:9092` | Broker address |
| `GBPS_TARGET` | `1.0` | Target throughput in Gbps (1–10) |
| `RUN_MODE` | `combined` | `combined`, `1kb`, `64kb`, `1mb`, `32mb` |
| `TOPIC_PREFIX` | `benchmark` | Prefix for topic/subject names |
| `RUN_DURATION_SECS` | `120` | How long to run (seconds) |
| `PRODUCER_START_DELAY_SECS` | `10` | Producer waits this long before sending (producer only) |

### Run modes

| Mode | Active streams | Allocation |
|------|---------------|------------|
| `combined` | 1 KB + 64 KB + 1 MB + 32 MB | 25% of target each |
| `1kb` | 1 KB only | 100% of target |
| `64kb` | 64 KB only | 100% of target |
| `1mb` | 1 MB only | 100% of target |
| `32mb` | 32 MB only | 100% of target |

---

## Build

### Local

```bash
# Requires: cmake, pkg-config, libssl-dev (for rdkafka static build)
cargo build --release
cargo test
```

### Docker (multi-stage, no runtime deps)

```bash
docker build -t broker-benchmarker:latest .
```

For an airgapped environment, pre-pull the builder image and push to your internal registry:

```bash
docker pull rust:1.77
docker tag rust:1.77 <internal-registry>/rust:1.77
docker push <internal-registry>/rust:1.77
# Then update the FROM line in Dockerfile accordingly
```

---

## Deploy to Kubernetes

1. Edit `k8s/consumer.yaml` and `k8s/producer.yaml`:
   - Replace `<registry>` with your image registry path
   - Set `BROKER_TYPE`, `BROKER_ENDPOINT`, `GBPS_TARGET`, `RUN_MODE`

2. Deploy **consumer first** so it is ready to receive before the producer starts:

```bash
kubectl apply -f k8s/consumer.yaml
kubectl apply -f k8s/producer.yaml
```

3. Capture CSV output:

```bash
kubectl logs -f deployment/benchmark-consumer > results.csv
```

4. (Optional) also capture producer diagnostics:

```bash
kubectl logs -f deployment/benchmark-producer > producer.log
```

---

## Benchmark Scenarios

### Scenario 1: NATS JetStream — Combined load, 5 Gbps

```bash
# consumer.yaml env:
BROKER_TYPE=nats
BROKER_ENDPOINT=nats.default.svc.cluster.local:4222
GBPS_TARGET=5
RUN_MODE=combined
RUN_DURATION_SECS=120
```

### Scenario 2: Redpanda — 1 MB messages, 10 Gbps

```bash
BROKER_TYPE=redpanda
BROKER_ENDPOINT=redpanda.default.svc.cluster.local:9092
GBPS_TARGET=10
RUN_MODE=1mb
RUN_DURATION_SECS=120
```

### Scenario 3: Redpanda — 32 MB messages (large payload)

```bash
BROKER_TYPE=redpanda
BROKER_ENDPOINT=redpanda.default.svc.cluster.local:9092
GBPS_TARGET=1
RUN_MODE=32mb
RUN_DURATION_SECS=120
```

---

## CSV Output Format

The consumer writes one header row and one data row per active stream to stdout:

```
broker_type,run_mode,gbps_target,message_size_bytes,run_duration_secs,messages_sent,messages_received,messages_lost,achieved_throughput_gbps,latency_p50_ms,latency_p95_ms,latency_p99_ms,latency_max_ms
```

Example output (combined mode, NATS):

```
broker_type,run_mode,gbps_target,message_size_bytes,run_duration_secs,messages_sent,messages_received,messages_lost,achieved_throughput_gbps,latency_p50_ms,latency_p95_ms,latency_p99_ms,latency_max_ms
nats,combined,5.000,1024,120,18432000,18431200,800,1.249312,0.512,1.024,2.048,15.360
nats,combined,5.000,65536,120,288000,287950,50,1.248701,0.768,1.536,3.072,20.480
nats,combined,5.000,1048576,120,18000,17990,10,1.249100,1.024,2.048,4.096,32.768
nats,combined,5.000,33554432,120,563,560,3,1.246000,5.120,10.240,20.480,102.400
```

`messages_sent` is inferred by the consumer from sequence numbers (`received + lost`).
Diagnostic logs (stderr) are separate from CSV (stdout) — `2>/dev/null` suppresses them.

---

## Message Wire Format

Each message begins with a 24-byte header followed by zero-padded bytes:

| Bytes | Field | Type |
|-------|-------|------|
| 0–7 | producer timestamp (ns since epoch) | u64 big-endian |
| 8–15 | sequence number (per stream, zero-based) | u64 big-endian |
| 16–23 | stream ID (= message size in bytes) | u64 big-endian |
| 24–N | zero padding | — |

---

## Architecture Notes

- **Single binary**, behavior determined by `ROLE` env var.
- **rdkafka** (`cmake-build` feature) compiles librdkafka from source for static linking — no shared library deps at runtime.
- **NATS JetStream** uses ordered push consumers for low-overhead consumption.
- Rate limiting uses `tokio::time::interval` with batching: at very high rates (e.g., 1 KB @ 10 Gbps ≈ 1.25M msg/s) messages are batched per tick rather than one per tick.
- Latency measured as end-to-end wall clock (producer embed timestamp → consumer receive timestamp). Requires clock synchronization between pods (NTP/PTP).
- Loss detection via per-stream sequence number gaps.
