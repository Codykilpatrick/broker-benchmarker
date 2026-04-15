# Broker Benchmarker

Stress-tests **Redpanda** and **NATS** under identical workloads.
Produces a markdown benchmark report and CSV results after each run.

---

## Quick-start

### Redpanda — one command (recommended)

The `bench.sh` script starts the monitoring stack, builds the benchmarker image, and runs producer + consumer inside the Docker network so traffic never crosses the host↔VM boundary.

```bash
# Start Redpanda + Prometheus + Grafana + Runbooks (first time only)
cd monitoring && docker compose up -d && cd ..

# Run a full benchmark — results land in ./results/
./bench.sh --docker --gbps 10 --mode combined --duration 60 --partitions 16 --payload float32
```

Results are written to `results/report_<run_id>.md` and `results/consumer_redpanda_out_<run_id>.csv`.

Example report output:

```
## Results
| Message Size | Target Gbps | Achieved Gbps | Efficiency | Messages | Lost | p50 ms | p95 ms | p99 ms | max ms |
|---|---|---|---|---|---|---|---|---|---|
| 24b    | 2.50 | 0.000 | 0%  | 3713 | 0 |   958 |  1530 |  3306 | 10216 |
| 256b   | 2.50 | 0.000 | 0%  | 4208 | 0 |   943 |  1269 |  3312 |  3318 |
| 4kb    | 2.50 | 0.002 | 0%  | 4318 | 0 |   943 |  1276 |  3318 |  3332 |
| 64kb   | 2.50 | 0.037 | 1%  | 4252 | 0 |   960 |  1444 |  3340 |  3394 |
| 512kb  | 2.50 | 0.176 | 7%  | 2516 | 0 |  1000 |  3028 |  6528 | 34990 |
| 4mb    | 2.50 | 0.962 | 38% | 1721 | 0 |  1406 |  4690 | 10275 | 32743 |
| 16mb   | 2.50 | 0.960 | 38% |  429 | 0 |  2621 | 17192 | 25736 | 26507 |
| 80mb   | 2.50 | 1.163 | 46% |  104 | 0 |  7941 | 34365 | 47073 | 50680 |

## Summary
| Metric          | Value     |
| Target Gbps     | 10.0      |
| Achieved Gbps   | 3.301     |
| Efficiency      | 33%       |
| Messages lost   | 0         |
```

> **Docker Desktop on macOS** caps container-to-container throughput at ~3–4 Gbps due to VM overhead. The same workload on bare-metal Linux reaches 6–10+ Gbps.

### bench.sh options

```
./bench.sh [options]

  --gbps N            Target throughput in Gbps                     (default: 10)
  --mode MODE         combined|24b|256b|4kb|64kb|512kb|4mb|16mb|80mb (default: combined)
  --duration N        Run duration in seconds                        (default: 60)
  --partitions N      Partitions, producer & consumer tasks          (default: 16)
  --producer-tasks N  Producer task count                            (default: PARTITIONS)
  --consumer-tasks N  Consumer task count                            (default: PARTITIONS)
  --payload TYPE      float32|random_bytes                           (default: float32)
  --endpoint HOST:PORT Broker address                                (default: localhost:9092)
  --output-dir DIR    Where to write CSV + report                    (default: .)
  --docker            Run benchmarker inside Docker network (no host↔VM hop)
```

### Redpanda — manual (native, no Docker for benchmarker)

> **Note:** Use `export VAR=value` rather than inline env vars — zsh silently drops inline assignments before `cargo run`.

```bash
# Start monitoring stack
cd monitoring && docker compose up -d

# Terminal 1 — consumer (start first)
export ROLE=consumer BROKER_TYPE=redpanda BROKER_ENDPOINT=localhost:9092
export GBPS_TARGET=10 RUN_MODE=combined RUN_DURATION_SECS=60
export PARTITIONS=16 CONSUMER_TASKS=16 REPORT_INTERVAL_SECS=5
cargo run --release

# Terminal 2 — producer
export ROLE=producer BROKER_TYPE=redpanda BROKER_ENDPOINT=localhost:9092
export GBPS_TARGET=10 RUN_MODE=combined RUN_DURATION_SECS=60
export PARTITIONS=16 PRODUCER_TASKS=16 REPORT_INTERVAL_SECS=5
cargo run --release
```

Grafana dashboard: http://localhost:3000 (admin/admin)
Prometheus: http://localhost:9090
Runbooks: http://localhost:8090

### NATS

```bash
docker run -d --name nats -p 4222:4222 nats:latest -js

export ROLE=consumer BROKER_TYPE=nats BROKER_ENDPOINT=localhost:4222
export GBPS_TARGET=0.01 RUN_MODE=64kb RUN_DURATION_SECS=30
cargo run --release

export ROLE=producer BROKER_TYPE=nats BROKER_ENDPOINT=localhost:4222
export GBPS_TARGET=0.01 RUN_MODE=64kb RUN_DURATION_SECS=30
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
| `RUN_MODE` | `combined` | `combined`, `24b`, `256b`, `4kb`, `64kb`, `512kb`, `4mb`, `16mb`, or `80mb` |
| `PARTITIONS` | `16` | Number of Kafka/Redpanda partitions per topic |
| `PRODUCER_TASKS` | `$PARTITIONS` | Parallel producer tasks (independent of partition count) |
| `CONSUMER_TASKS` | `$PARTITIONS` | Parallel consumer instances (each assigned a partition slice) |
| `PAYLOAD_TYPE` | `float32` | `float32` (correlated sensor data, LZ4-compressible) or `random_bytes` (entropy baseline) |
| `MAX_MSG_RATE` | `100` | Max messages/sec per stream; water-filling redistributes unused bandwidth to larger streams |
| `REPORT_INTERVAL_SECS` | `5` | Seconds between real-time throughput prints to stderr (0 = off) |
| `TOPIC_PREFIX` | `benchmark` | Prefix for topic/subject names |
| `RUN_DURATION_SECS` | `120` | How long to run (seconds) |
| `PRODUCER_START_DELAY_SECS` | `10` | Producer waits this long before sending |
| `RUN_ID` | unix timestamp | Identifier appended to output filenames |
| `OUTPUT_DIR` | `.` | Directory to write CSV result files |

### Run modes

Bandwidth is allocated via water-filling: small-message streams are capped at `MAX_MSG_RATE` msg/s and any
unallocated bandwidth is redistributed to larger-message streams.

| Mode | Active streams |
|------|---------------|
| `combined` | all 8 sizes (24 B → 80 MB) |
| `24b` | 24 B only |
| `256b` | 256 B only |
| `4kb` | 4 KB only |
| `64kb` | 64 KB only |
| `512kb` | 512 KB only |
| `4mb` | 4 MB only |
| `16mb` | 16 MB only |
| `80mb` | 80 MB only |

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
docker pull rust:latest
docker tag rust:latest <internal-registry>/rust:latest
docker push <internal-registry>/rust:latest
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
export BROKER_TYPE=nats
export BROKER_ENDPOINT=nats.default.svc.cluster.local:4222
export GBPS_TARGET=5
export RUN_MODE=combined
export RUN_DURATION_SECS=120
```

### Scenario 2: Redpanda — 4 MB messages, 10 Gbps

```bash
export BROKER_TYPE=redpanda
export BROKER_ENDPOINT=redpanda.default.svc.cluster.local:9092
export GBPS_TARGET=10
export RUN_MODE=4mb
export RUN_DURATION_SECS=120
```

### Scenario 3: Redpanda — Full combined range (24 B → 80 MB), 10 Gbps

```bash
./bench.sh --docker --gbps 10 --mode combined --duration 120 --partitions 16 --payload float32
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
consumer,redpanda,combined,10.000,24,60,,3713,0,0.000,958.0,1529.8,3306.0,10215.9
consumer,redpanda,combined,10.000,256,60,,4208,0,0.000,942.5,1269.4,3312.0,3317.8
consumer,redpanda,combined,10.000,4096,60,,4318,0,0.002,942.9,1275.5,3317.9,3331.8
consumer,redpanda,combined,10.000,65536,60,,4252,0,0.037,960.2,1443.8,3340.3,3393.8
consumer,redpanda,combined,10.000,524288,60,,2516,0,0.176,1000.4,3027.5,6527.7,34990.1
consumer,redpanda,combined,10.000,4194304,60,,1721,0,0.962,1406.0,4689.9,10274.8,32743.4
consumer,redpanda,combined,10.000,16777216,60,,429,0,0.960,2620.9,17191.9,25736.2,26507.3
consumer,redpanda,combined,10.000,83886080,60,,104,0,1.163,7940.9,34365.4,47073.3,50679.8
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
