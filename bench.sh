#!/usr/bin/env bash
# bench.sh — run a Redpanda benchmark (producer + consumer in parallel)
#
# Usage:
#   ./bench.sh                        # 10 Gbps, combined mode, 60s, 16 partitions
#   ./bench.sh --gbps 5               # change target
#   ./bench.sh --mode 1mb             # single message-size mode
#   ./bench.sh --duration 120         # longer run
#   ./bench.sh --partitions 32        # more partitions
#   ./bench.sh --payload random_bytes # entropy baseline (no LZ4 benefit)
#   ./bench.sh --help

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
BROKER_ENDPOINT="${BROKER_ENDPOINT:-localhost:9092}"
GBPS_TARGET="${GBPS_TARGET:-10}"
RUN_MODE="${RUN_MODE:-combined}"
RUN_DURATION_SECS="${RUN_DURATION_SECS:-60}"
PARTITIONS="${PARTITIONS:-16}"
PRODUCER_TASKS="${PRODUCER_TASKS:-$PARTITIONS}"
CONSUMER_TASKS="${CONSUMER_TASKS:-$PARTITIONS}"
REPORT_INTERVAL_SECS="${REPORT_INTERVAL_SECS:-5}"
PAYLOAD_TYPE="${PAYLOAD_TYPE:-float32}"
PRODUCER_START_DELAY_SECS="${PRODUCER_START_DELAY_SECS:-5}"
MAX_MSG_RATE="${MAX_MSG_RATE:-100}"
OUTPUT_DIR="${OUTPUT_DIR:-.}"
DOCKER_MODE=false

# ── Arg parsing ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --gbps)        GBPS_TARGET="$2";             shift 2 ;;
    --mode)        RUN_MODE="$2";                shift 2 ;;
    --duration)    RUN_DURATION_SECS="$2";       shift 2 ;;
    --partitions)  PARTITIONS="$2"; PRODUCER_TASKS="$2"; CONSUMER_TASKS="$2"; shift 2 ;;
    --producer-tasks) PRODUCER_TASKS="$2";       shift 2 ;;
    --consumer-tasks) CONSUMER_TASKS="$2";       shift 2 ;;
    --payload)     PAYLOAD_TYPE="$2";            shift 2 ;;
    --max-msg-rate) MAX_MSG_RATE="$2";           shift 2 ;;
    --endpoint)    BROKER_ENDPOINT="$2";         shift 2 ;;
    --output-dir)  OUTPUT_DIR="$2";              shift 2 ;;
    --docker)      DOCKER_MODE=true;             shift ;;
    --help|-h)
      echo "Usage: $0 [options]"
      echo ""
      echo "  --gbps N            Target throughput in Gbps        (default: 10)"
      echo "  --mode MODE         combined|24b|256b|4kb|64kb|512kb|4mb|16mb|80mb  (default: combined)"
      echo "  --duration N        Run duration in seconds          (default: 60)"
      echo "  --partitions N      Partitions, producer & consumer  (default: 16)"
      echo "  --producer-tasks N  Producer task count              (default: PARTITIONS)"
      echo "  --consumer-tasks N  Consumer task count              (default: PARTITIONS)"
      echo "  --payload TYPE      float32|random_bytes             (default: float32)"
      echo "  --max-msg-rate N    Max messages/sec per stream      (default: 100)"
      echo "  --endpoint HOST:PORT Broker address                  (default: localhost:9092)"
      echo "  --output-dir DIR    Where to write CSV + report      (default: .)"
      echo "  --docker            Run benchmarker inside Docker network (no host↔VM hop)"
      exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Banner ────────────────────────────────────────────────────────────────────
echo "=================================================="
echo " Broker Benchmarker — Redpanda"
echo "=================================================="
echo "  Endpoint       : $BROKER_ENDPOINT"
echo "  Target         : ${GBPS_TARGET} Gbps"
echo "  Mode           : $RUN_MODE"
echo "  Duration       : ${RUN_DURATION_SECS}s"
echo "  Partitions     : $PARTITIONS"
echo "  Producer tasks : $PRODUCER_TASKS"
echo "  Consumer tasks : $CONSUMER_TASKS"
echo "  Payload        : $PAYLOAD_TYPE"
echo "  Max msg rate   : ${MAX_MSG_RATE} msg/s/stream"
echo "  Output dir     : $OUTPUT_DIR"
echo "=================================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Docker mode ───────────────────────────────────────────────────────────────
if [[ "$DOCKER_MODE" == true ]]; then
  echo ""
  echo "[bench] building Docker image (first run may take a few minutes)..."
  mkdir -p "$SCRIPT_DIR/results"

  # Export vars so docker compose can read them as ${VAR} substitutions
  export GBPS_TARGET RUN_MODE RUN_DURATION_SECS PARTITIONS PRODUCER_TASKS \
         CONSUMER_TASKS PAYLOAD_TYPE REPORT_INTERVAL_SECS PRODUCER_START_DELAY_SECS \
         MAX_MSG_RATE

  cd "$SCRIPT_DIR/monitoring"

  echo "[bench] starting consumer and producer inside Docker network..."
  docker compose --profile bench up --build --abort-on-container-exit \
    bench-consumer bench-producer

  echo ""
  echo "[bench] done — results in $SCRIPT_DIR/results/"
  echo ""
  ls -1t "$SCRIPT_DIR/results"/*.csv "$SCRIPT_DIR/results"/report_*.md 2>/dev/null | head -6
  exit 0
fi

# ── Native mode ───────────────────────────────────────────────────────────────
echo ""
echo "[bench] building..."
cargo build --release --quiet

# ── Shared env ────────────────────────────────────────────────────────────────
COMMON_ENV=(
  BROKER_TYPE=redpanda
  BROKER_ENDPOINT="$BROKER_ENDPOINT"
  GBPS_TARGET="$GBPS_TARGET"
  RUN_MODE="$RUN_MODE"
  RUN_DURATION_SECS="$RUN_DURATION_SECS"
  PARTITIONS="$PARTITIONS"
  REPORT_INTERVAL_SECS="$REPORT_INTERVAL_SECS"
  PAYLOAD_TYPE="$PAYLOAD_TYPE"
  MAX_MSG_RATE="$MAX_MSG_RATE"
  OUTPUT_DIR="$OUTPUT_DIR"
)

# ── Start consumer ────────────────────────────────────────────────────────────
echo ""
echo "[bench] starting consumer..."
env "${COMMON_ENV[@]}" \
  ROLE=consumer \
  CONSUMER_TASKS="$CONSUMER_TASKS" \
  ./target/release/broker-benchmarker &
CONSUMER_PID=$!

# ── Start producer (after delay) ──────────────────────────────────────────────
echo "[bench] starting producer (delay: ${PRODUCER_START_DELAY_SECS}s)..."
env "${COMMON_ENV[@]}" \
  ROLE=producer \
  PRODUCER_TASKS="$PRODUCER_TASKS" \
  PRODUCER_START_DELAY_SECS="$PRODUCER_START_DELAY_SECS" \
  ./target/release/broker-benchmarker &
PRODUCER_PID=$!

# ── Wait ──────────────────────────────────────────────────────────────────────
echo "[bench] running — press Ctrl+C to abort early"
echo ""

trap 'echo ""; echo "[bench] interrupted — killing producer and consumer"; kill $PRODUCER_PID $CONSUMER_PID 2>/dev/null; exit 1' INT

wait $PRODUCER_PID
wait $CONSUMER_PID

echo ""
echo "[bench] done — results in $OUTPUT_DIR/"
echo ""
ls -1t "$OUTPUT_DIR"/*.csv "$OUTPUT_DIR"/report_*.md 2>/dev/null | head -6
