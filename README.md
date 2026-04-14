# Broker Benchmarker

Stress-tests **Redpanda** and **NATS** under identical workloads.
Emits CSV results to a file — capture it with `kubectl logs` or read directly from disk.

---

## Quick-start (local smoke test)

> **Note:** Set environment variables with `export` before running `cargo run`.
> Inline env vars on the same line as `cargo run` can be misinterpreted by some shells
> and silently fall back to defaults.

### Redpanda (with monitoring stack)

```bash
# Start Redpanda + Prometheus + Grafana + Runbooks
cd monitoring && docker compose up -d

# Terminal 1 — consumer (start first)
export ROLE=consumer
export BROKER_TYPE=redpanda
export BROKER_ENDPOINT=localhost:9092
export GBPS_TARGET=10
export RUN_MODE=combined
export RUN_DURATION_SECS=60
export PARTITIONS=16
export CONSUMER_TASKS=16
export REPORT_INTERVAL_SECS=5
cargo run --release

# Terminal 2 — producer
export ROLE=producer
export BROKER_TYPE=redpanda
export BROKER_ENDPOINT=localhost:9092
export GBPS_TARGET=10
export RUN_MODE=combined
export RUN_DURATION_SECS=60
export PARTITIONS=16
export PRODUCER_TASKS=16
export REPORT_INTERVAL_SECS=5
cargo run --release
```

Grafana dashboard: http://localhost:3000 (admin/admin)
Prometheus: http://localhost:9090
Runbooks: http://localhost:8090

### NATS

```bash
docker run -d --name nats -p 4222:4222 nats:latest -js

export ROLE=consumer
export BROKER_TYPE=nats
export BROKER_ENDPOINT=localhost:4222
export GBPS_TARGET=0.01
export RUN_MODE=64kb
export RUN_DURATION_SECS=30
cargo run --release

export ROLE=producer
export BROKER_TYPE=nats
export BROKER_ENDPOINT=localhost:4222
export GBPS_TARGET=0.01
export RUN_MODE=64kb
export RUN_DURATION_SECS=30
export PRODUCER_START_DELAY_SECS=5
cargo run --release
```

---

## Monitoring Stack

The `monitoring/` directory contains a Docker Compose stack with:

| Service | Port | Description |
|---------|------|-------------|
| Redpanda | 9092 | Kafka-compatible broker |
| Prometheus | 9090 | Scrapes Redpanda metrics every 5s |
| Grafana | 3000 | Dashboards and alerting (admin/admin) |
| Runbooks | 8090 | Static HTML remediation guides |

```bash
cd monitoring
docker compose up -d    # start all services
docker compose down     # stop all services
```

### Grafana Dashboards

All dashboards are auto-provisioned under the **Redpanda** folder:

- **Redpanda Overview** — bytes in/out, latency, CPU, memory, disk, partitions, alert list
- **Alerts** — full-page alert list (suitable for sharing with non-technical viewers)
- **Redpanda Ops Dashboard** — official Redpanda ops/health dashboard
- **Kafka Topic Metrics** — per-topic throughput, offsets, partition skew

### Alert Rules

Four alert rules are provisioned as code in `monitoring/grafana/provisioning/alerting/redpanda-alerts.yaml`:

| Alert | Threshold | Severity | Runbook |
|-------|-----------|----------|---------|
| High Request Latency (p99) | > 500ms for 2m | warning | http://localhost:8090/high-latency.html |
| High CPU Usage | > 80% for 2m | warning | http://localhost:8090/high-cpu.html |
| Low Disk Space | < 20% free for 5m | warning | http://localhost:8090/low-disk.html |
| Under-Replicated Partitions | > 0 for 1m | critical | http://localhost:8090/under-replicated.html |

Each alert includes a `runbook_url` annotation linking to step-by-step remediation instructions. To add runbooks for new alerts, drop HTML files into `monitoring/runbooks/` and reference them in the alert YAML.

### User Access

- **Viewer role** — can view dashboards and alerts, cannot edit anything
- **Editor role** — required to silence alerts

Create users in **Administration → Users** in Grafana.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ROLE` | `consumer` | `producer` or `consumer` |
| `BROKER_TYPE` | `nats` | `nats`, `redpanda`, `kafka`, or `grpc` |
| `BROKER_ENDPOINT` | `localhost:4222` / `localhost:9092` | Broker address |
| `GBPS_TARGET` | `1.0` | Target throughput in Gbps |
| `RUN_MODE` | `combined` | `combined`, `64kb`, `1mb`, `12mb`, or `32mb` |
| `PARTITIONS` | `16` | Number of Kafka/Redpanda partitions per topic |
| `PRODUCER_TASKS` | `$PARTITIONS` | Parallel producer tasks (independent of partition count) |
| `CONSUMER_TASKS` | `$PARTITIONS` | Parallel consumer instances (each assigned a partition slice) |
| `PAYLOAD_TYPE` | `float32` | `float32` (correlated sensor data, LZ4-compressible) or `random_bytes` (entropy baseline) |
| `REPORT_INTERVAL_SECS` | `5` | Seconds between real-time throughput prints to stderr (0 = off) |
| `TOPIC_PREFIX` | `benchmark` | Prefix for topic/subject names |
| `RUN_DURATION_SECS` | `120` | How long to run (seconds) |
| `PRODUCER_START_DELAY_SECS` | `10` | Producer waits this long before sending |
| `RUN_ID` | unix timestamp | Identifier appended to output filenames |
| `OUTPUT_DIR` | `.` | Directory to write CSV result files |

### Run modes

| Mode | Active streams | Allocation |
|------|---------------|------------|
| `combined` | 64 KB + 1 MB + 12 MB + 32 MB | 25% of target each |
| `64kb` | 64 KB only | 100% of target |
| `1mb` | 1 MB only | 100% of target |
| `12mb` | 12 MB only | 100% of target |
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

### Scenario 1: NATS — Combined load, 5 Gbps

```bash
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

Results are written to `<OUTPUT_DIR>/<role>_<broker_type>_out_<RUN_ID>.csv`. One row per active stream:

```
role,broker_type,run_mode,gbps_target,message_size_bytes,run_duration_secs,messages_sent,messages_received,messages_lost,achieved_throughput_gbps,latency_p50_ms,latency_p95_ms,latency_p99_ms,latency_max_ms
```

Example output (combined mode, Redpanda, consumer):

```
role,broker_type,run_mode,gbps_target,message_size_bytes,run_duration_secs,messages_sent,messages_received,messages_lost,achieved_throughput_gbps,latency_p50_ms,latency_p95_ms,latency_p99_ms,latency_max_ms
consumer,redpanda,combined,1.000,65536,60,,20389,0,0.178162,11.722,33.341,163.391,265.055
consumer,redpanda,combined,1.000,1048576,60,,1220,0,0.170568,21.756,39.183,151.671,226.959
consumer,redpanda,combined,1.000,12582912,60,,104,0,0.174483,70.259,207.175,249.735,263.519
consumer,redpanda,combined,1.000,33554432,60,,40,0,0.178957,169.631,223.167,250.735,250.735
```

The producer CSV records `messages_sent` but not latency. The consumer CSV records `messages_received`, `messages_lost`, latency, and achieved throughput.

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
- **NATS** uses ordered push consumers for low-overhead consumption.
- Rate limiting uses `tokio::time::interval` with batching: at very high rates messages are batched per tick rather than one per tick.
- Latency measured as end-to-end wall clock (producer embed timestamp → consumer receive timestamp). Requires clock synchronization between pods (NTP/PTP).
- Loss detection via per-stream sequence number gaps.
