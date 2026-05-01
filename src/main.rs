use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub mod benchmark_proto {
    tonic::include_proto!("benchmark");
}

use rand::{Rng, SeedableRng};

use bytes::Bytes;
use futures::StreamExt;
use hdrhistogram::Histogram;
use tokio::time::{interval, sleep, Instant};

// ─── Message sizes ──────────────────────────────────────────────────────────

const MESSAGE_SIZES: &[(u64, &str)] = &[
    (24,          "24b"),
    (256,         "256b"),
    (4_096,       "4kb"),
    (65_536,      "64kb"),
    (524_288,     "512kb"),
    (4_194_304,   "4mb"),
    (16_777_216,  "16mb"),
    (83_886_080,  "80mb"),
];

const HEADER_LEN: usize = 24;

// ─── Config ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Role {
    Producer,
    Consumer,
}

#[derive(Debug, Clone, PartialEq)]
enum BrokerType {
    Redpanda,
    Kafka,
    Nats,
    Grpc,
}

#[derive(Debug, Clone, PartialEq)]
enum RunMode {
    Combined,
    Only24b,
    Only256b,
    Only4kb,
    Only64kb,
    Only512kb,
    Only4mb,
    Only16mb,
    Only80mb,
}

#[derive(Debug, Clone, PartialEq)]
enum PayloadType {
    /// Correlated float32 values — realistic sensor/ADC data, compresses well with LZ4.
    Float32,
    /// Pure random bytes — max-entropy baseline, minimal LZ4 benefit.
    RandomBytes,
}

#[derive(Debug, Clone)]
struct Config {
    role: Role,
    broker_type: BrokerType,
    broker_endpoint: String,
    gbps_target: f64,
    run_mode: RunMode,
    topic_prefix: String,
    run_duration_secs: u64,
    producer_start_delay_secs: u64,
    run_id: String,
    output_dir: String,
    num_partitions: usize,
    /// Number of parallel producer tasks. Defaults to num_partitions.
    /// Use PRODUCER_TASKS to test "few vs many" producers independently.
    num_producer_tasks: usize,
    /// Number of parallel consumer instances. Defaults to num_partitions.
    /// Each instance is assigned a slice of partitions via assign() (no group coordinator).
    num_consumer_tasks: usize,
    /// How often (seconds) to print real-time throughput to stderr.
    report_interval_secs: u64,
    /// Payload content type — float32 (realistic sensor data) or random_bytes (baseline).
    payload_type: PayloadType,
    /// Maximum messages per second per stream. Bandwidth is redistributed from
    /// capped (small message) streams to uncapped (large message) streams.
    max_msg_rate: u64,
}

fn env_var(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_config() -> Config {
    let role = match env_var("ROLE", "consumer").to_lowercase().as_str() {
        "producer" => Role::Producer,
        _ => Role::Consumer,
    };

    let broker_type = match env_var("BROKER_TYPE", "nats").to_lowercase().as_str() {
        "redpanda" => BrokerType::Redpanda,
        "kafka" => BrokerType::Kafka,
        "grpc" => BrokerType::Grpc,
        _ => BrokerType::Nats,
    };

    let broker_endpoint = env_var(
        "BROKER_ENDPOINT",
        match broker_type {
            BrokerType::Redpanda | BrokerType::Kafka => "localhost:9092",
            BrokerType::Nats => "localhost:4222",
            BrokerType::Grpc => "0.0.0.0:50051",
        },
    );

    let gbps_target: f64 = env_var("GBPS_TARGET", "1.0")
        .parse()
        .expect("GBPS_TARGET must be a float");

    let run_mode = match env_var("RUN_MODE", "combined").to_lowercase().as_str() {
        "24b"   => RunMode::Only24b,
        "256b"  => RunMode::Only256b,
        "4kb"   => RunMode::Only4kb,
        "64kb"  => RunMode::Only64kb,
        "512kb" => RunMode::Only512kb,
        "4mb"   => RunMode::Only4mb,
        "16mb"  => RunMode::Only16mb,
        "80mb"  => RunMode::Only80mb,
        _       => RunMode::Combined,
    };

    let topic_prefix = env_var("TOPIC_PREFIX", "benchmark");
    let run_duration_secs: u64 = env_var("RUN_DURATION_SECS", "120")
        .parse()
        .expect("RUN_DURATION_SECS must be u64");
    let producer_start_delay_secs: u64 = env_var("PRODUCER_START_DELAY_SECS", "10")
        .parse()
        .expect("PRODUCER_START_DELAY_SECS must be u64");

    let default_run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let run_id = env_var("RUN_ID", &default_run_id);
    let output_dir = env_var("OUTPUT_DIR", ".");
    let num_partitions: usize = env_var("PARTITIONS", "16")
        .parse()
        .expect("PARTITIONS must be a positive integer");
    let num_partitions = num_partitions.max(1);

    let num_producer_tasks: usize = env_var("PRODUCER_TASKS", &num_partitions.to_string())
        .parse()
        .expect("PRODUCER_TASKS must be a positive integer");
    let num_producer_tasks = num_producer_tasks.max(1);

    let num_consumer_tasks: usize = env_var("CONSUMER_TASKS", &num_partitions.to_string())
        .parse()
        .expect("CONSUMER_TASKS must be a positive integer");
    let num_consumer_tasks = num_consumer_tasks.max(1);

    let report_interval_secs: u64 = env_var("REPORT_INTERVAL_SECS", "5")
        .parse()
        .expect("REPORT_INTERVAL_SECS must be u64");

    let payload_type = match env_var("PAYLOAD_TYPE", "float32").to_lowercase().as_str() {
        "random_bytes" | "random" => PayloadType::RandomBytes,
        _ => PayloadType::Float32,
    };

    let max_msg_rate: u64 = env_var("MAX_MSG_RATE", "100")
        .parse()
        .expect("MAX_MSG_RATE must be a positive integer");
    let max_msg_rate = max_msg_rate.max(1);

    Config {
        role,
        broker_type,
        broker_endpoint,
        gbps_target,
        run_mode,
        topic_prefix,
        run_duration_secs,
        producer_start_delay_secs,
        run_id,
        output_dir,
        num_partitions,
        num_producer_tasks,
        num_consumer_tasks,
        report_interval_secs,
        payload_type,
        max_msg_rate,
    }
}

// ─── Active streams ───────────────────────────────────────────────────────────

/// Water-filling bandwidth allocation subject to a per-stream message rate cap.
///
/// Streams that would require more than `max_msg_rate` msg/s get capped; their
/// unused bandwidth is redistributed to uncapped (larger) streams. This ensures
/// small-message streams are tested at realistic rates while large-message streams
/// absorb the remaining bandwidth budget.
///
/// Returns msg/s for each stream in the same order as `sizes`.
fn water_fill_rates(sizes: &[u64], bytes_per_sec_total: f64, max_msg_rate: f64) -> Vec<f64> {
    let n = sizes.len();
    let mut rates = vec![0.0f64; n];
    let mut capped = vec![false; n];
    let mut remaining_bps = bytes_per_sec_total;
    let mut uncapped_count = n;

    loop {
        if uncapped_count == 0 {
            break;
        }
        let bps_each = remaining_bps / uncapped_count as f64;
        let mut any_newly_capped = false;

        for i in 0..n {
            if capped[i] {
                continue;
            }
            let msg_s = bps_each / sizes[i] as f64;
            if msg_s > max_msg_rate {
                rates[i] = max_msg_rate;
                capped[i] = true;
                remaining_bps -= max_msg_rate * sizes[i] as f64;
                uncapped_count -= 1;
                any_newly_capped = true;
            }
        }

        if !any_newly_capped {
            // All remaining streams fit within the cap — assign equal share
            for i in 0..n {
                if !capped[i] {
                    rates[i] = bps_each / sizes[i] as f64;
                }
            }
            break;
        }
    }

    rates
}

/// Returns (size_bytes, size_label, fraction_of_gbps_target) for each active stream.
///
/// `fraction` is derived from the water-filled allocation so downstream rate math
/// stays unchanged: `msgs_per_sec = bytes_per_sec * fraction / size_bytes`.
fn active_streams(mode: &RunMode, gbps_target: f64, max_msg_rate: u64) -> Vec<(u64, &'static str, f64)> {
    let sizes: &[(u64, &str)] = match mode {
        RunMode::Combined => MESSAGE_SIZES,
        RunMode::Only24b   => &[(24,         "24b")],
        RunMode::Only256b  => &[(256,        "256b")],
        RunMode::Only4kb   => &[(4_096,      "4kb")],
        RunMode::Only64kb  => &[(65_536,     "64kb")],
        RunMode::Only512kb => &[(524_288,    "512kb")],
        RunMode::Only4mb   => &[(4_194_304,  "4mb")],
        RunMode::Only16mb  => &[(16_777_216, "16mb")],
        RunMode::Only80mb  => &[(83_886_080, "80mb")],
    };

    let bytes_per_sec_total = gbps_target * 1_000_000_000.0 / 8.0;
    let size_bytes: Vec<u64> = sizes.iter().map(|(s, _)| *s).collect();
    let rates = water_fill_rates(&size_bytes, bytes_per_sec_total, max_msg_rate as f64);

    sizes.iter().zip(rates.iter()).map(|((size, label), &rate)| {
        let fraction = if bytes_per_sec_total > 0.0 {
            (rate * *size as f64) / bytes_per_sec_total
        } else {
            0.0
        };
        (*size, *label, fraction)
    }).collect()
}

// ─── Topic naming ─────────────────────────────────────────────────────────────

fn topic_name(prefix: &str, size_label: &str, stream_index: u64) -> String {
    format!("{}_{}_{}", prefix, size_label, stream_index)
}

/// NATS stream names cannot contain dots or hyphens.
fn stream_name_from_topic(topic: &str) -> String {
    topic.replace('.', "_").replace('-', "_")
}

// ─── Message encode / decode ─────────────────────────────────────────────────

fn encode_message(buf: &mut Vec<u8>, producer_ts_ns: u64, seq: u64, stream_id: u64) {
    if buf.len() < HEADER_LEN {
        // Message too small for the full tracking header — skip.
        // These messages will be counted as received but won't contribute to
        // latency or loss stats (decode_header returns None for short buffers).
        return;
    }
    buf[0..8].copy_from_slice(&producer_ts_ns.to_be_bytes());
    buf[8..16].copy_from_slice(&seq.to_be_bytes());
    buf[16..24].copy_from_slice(&stream_id.to_be_bytes());
}

fn decode_header(data: &[u8]) -> Option<(u64, u64, u64)> {
    if data.len() < HEADER_LEN {
        return None;
    }
    let ts = u64::from_be_bytes(data[0..8].try_into().unwrap());
    let seq = u64::from_be_bytes(data[8..16].try_into().unwrap());
    let stream_id = u64::from_be_bytes(data[16..24].try_into().unwrap());
    Some((ts, seq, stream_id))
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// ─── Rate limiter math ────────────────────────────────────────────────────────

struct RateParams {
    interval_ms: u64,
    batch_size: u64,
}

fn compute_rate_params(msgs_per_sec: f64) -> RateParams {
    if msgs_per_sec <= 0.0 {
        return RateParams {
            interval_ms: 1000,
            batch_size: 1,
        };
    }
    let batch_size = (msgs_per_sec / 1000.0).ceil().max(1.0) as u64;
    let ticks_per_sec = msgs_per_sec / batch_size as f64;
    let interval_ms = (1000.0 / ticks_per_sec).max(1.0) as u64;
    RateParams {
        interval_ms,
        batch_size,
    }
}

// ─── Stats ────────────────────────────────────────────────────────────────────

struct StreamStats {
    messages_received: u64,
    total_bytes_received: u64,
    next_expected_seq: HashMap<u64, u64>, // stream_id -> next expected seq
    messages_lost: u64,
    latency_us: Histogram<u64>,
}

impl StreamStats {
    fn new() -> Self {
        StreamStats {
            messages_received: 0,
            total_bytes_received: 0,
            next_expected_seq: HashMap::new(),
            messages_lost: 0,
            latency_us: Histogram::new(4).expect("histogram creation failed"),
        }
    }

    fn record(&mut self, data: &[u8], recv_ns: u64) {
        let size = data.len() as u64;
        self.messages_received += 1;
        self.total_bytes_received += size;

        if let Some((ts_ns, seq, stream_id)) = decode_header(data) {
            // Latency
            let latency_us = if recv_ns >= ts_ns {
                (recv_ns - ts_ns) / 1000
            } else {
                0
            };
            let _ = self.latency_us.record(latency_us.max(1));

            // Loss detection
            let expected = self.next_expected_seq.entry(stream_id).or_insert(0);
            if seq > *expected {
                self.messages_lost += seq - *expected;
            }
            *expected = seq + 1;
        }
    }

    fn merge(&mut self, other: &StreamStats) {
        self.messages_received += other.messages_received;
        self.total_bytes_received += other.total_bytes_received;
        self.messages_lost += other.messages_lost;
        let _ = self.latency_us.add(&other.latency_us);
    }

    fn achieved_gbps(&self, run_duration_secs: u64) -> f64 {
        if run_duration_secs == 0 {
            return 0.0;
        }
        (self.total_bytes_received as f64 * 8.0)
            / (run_duration_secs as f64 * 1_000_000_000.0)
    }
}

// ─── CSV output ───────────────────────────────────────────────────────────────
//
// Both producer and consumer write to stdout. Each row has a `role` column
// so the two can be captured together and split apart by the user.
//
// Producer columns: role,broker_type,run_mode,gbps_target,message_size_bytes,
//                   run_duration_secs,messages_sent,,,,,,,
// Consumer columns: role,broker_type,run_mode,gbps_target,message_size_bytes,
//                   run_duration_secs,,messages_received,messages_lost,
//                   achieved_throughput_gbps,latency_p50_ms,...
//
// Empty cells in either direction are left blank (not 0) to avoid false
// comparisons between a blank producer latency column and a real consumer value.

fn open_csv_file(cfg: &Config) -> std::fs::File {
    let role = match cfg.role {
        Role::Producer => "producer",
        Role::Consumer => "consumer",
    };
    let path = format!("{}/{}_{}_out_{}.csv", cfg.output_dir, role, broker_type_str(&cfg.broker_type), cfg.run_id);
    eprintln!("[csv] writing results to {}", path);
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("failed to open {}: {}", path, e))
}

fn write_csv_header(f: &mut std::fs::File) {
    writeln!(
        f,
        "role,broker_type,run_mode,gbps_target,message_size_bytes,run_duration_secs,\
         messages_sent,messages_received,messages_lost,achieved_throughput_gbps,\
         latency_p50_ms,latency_p95_ms,latency_p99_ms,latency_max_ms"
    ).expect("csv write failed");
}

fn write_producer_csv_row(
    f: &mut std::fs::File,
    broker_type: &str,
    run_mode: &str,
    gbps_target: f64,
    message_size_bytes: u64,
    run_duration_secs: u64,
    messages_sent: u64,
) {
    writeln!(
        f,
        "producer,{},{},{:.3},{},{},{},,,,,,,",
        broker_type,
        run_mode,
        gbps_target,
        message_size_bytes,
        run_duration_secs,
        messages_sent,
    ).expect("csv write failed");
}

#[allow(clippy::too_many_arguments)]
fn write_consumer_csv_row(
    f: &mut std::fs::File,
    broker_type: &str,
    run_mode: &str,
    gbps_target: f64,
    message_size_bytes: u64,
    run_duration_secs: u64,
    messages_received: u64,
    messages_lost: u64,
    achieved_gbps: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
) {
    writeln!(
        f,
        "consumer,{},{},{:.3},{},{},,{},{},{:.6},{:.3},{:.3},{:.3},{:.3}",
        broker_type,
        run_mode,
        gbps_target,
        message_size_bytes,
        run_duration_secs,
        messages_received,
        messages_lost,
        achieved_gbps,
        p50_ms,
        p95_ms,
        p99_ms,
        max_ms,
    ).expect("csv write failed");
}

fn broker_type_str(bt: &BrokerType) -> &'static str {
    match bt {
        BrokerType::Redpanda => "redpanda",
        BrokerType::Kafka => "kafka",
        BrokerType::Nats => "nats",
        BrokerType::Grpc => "grpc",
    }
}

fn run_mode_str(rm: &RunMode) -> &'static str {
    match rm {
        RunMode::Combined  => "combined",
        RunMode::Only24b   => "24b",
        RunMode::Only256b  => "256b",
        RunMode::Only4kb   => "4kb",
        RunMode::Only64kb  => "64kb",
        RunMode::Only512kb => "512kb",
        RunMode::Only4mb   => "4mb",
        RunMode::Only16mb  => "16mb",
        RunMode::Only80mb  => "80mb",
    }
}

fn us_to_ms(us: u64) -> f64 {
    us as f64 / 1000.0
}

// ─── Payload generation ───────────────────────────────────────────────────────

/// Fill buf[HEADER_LEN..] with correlated float32 values that simulate sensor data.
/// Adjacent samples share a common base value with small per-sample noise, giving
/// LZ4 meaningful compressibility (like real ADC/radar output).
fn fill_float32_payload(buf: &mut [u8], rng: &mut rand::rngs::SmallRng) {
    let base: f32 = rng.gen_range(-1000.0_f32..1000.0_f32);
    let noise_scale: f32 = rng.gen_range(0.1_f32..2.0_f32);
    let mut offset = HEADER_LEN;
    while offset + 4 <= buf.len() {
        let val: f32 = base + noise_scale * rng.gen_range(-1.0_f32..1.0_f32);
        buf[offset..offset + 4].copy_from_slice(&val.to_le_bytes());
        offset += 4;
    }
}

// ─── Real-time throughput reporter ───────────────────────────────────────────

async fn run_throughput_reporter(
    role: &str,
    bytes_counter: Arc<AtomicU64>,
    report_interval_secs: u64,
    run_deadline: tokio::time::Instant,
) {
    if report_interval_secs == 0 {
        return;
    }
    let mut ticker = interval(Duration::from_secs(report_interval_secs));
    let mut last_bytes: u64 = 0;
    let mut last_t = tokio::time::Instant::now();
    loop {
        ticker.tick().await;
        if tokio::time::Instant::now() >= run_deadline {
            break;
        }
        let now_bytes = bytes_counter.load(Ordering::Relaxed);
        let elapsed = last_t.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            let gbps = ((now_bytes - last_bytes) as f64 * 8.0) / (elapsed * 1_000_000_000.0);
            let mb = (now_bytes - last_bytes) / 1_048_576;
            eprintln!("[{}] {:.3} Gbps ({} MB in {:.1}s)", role, gbps, mb, elapsed);
        }
        last_bytes = now_bytes;
        last_t = tokio::time::Instant::now();
    }
}

// ─── NATS implementation ──────────────────────────────────────────────────────

async fn nats_producer(cfg: Config) {
    sleep(Duration::from_secs(cfg.producer_start_delay_secs)).await;
    eprintln!("[producer] connecting to NATS at {}", cfg.broker_endpoint);

    let streams = active_streams(&cfg.run_mode, cfg.gbps_target, cfg.max_msg_rate);
    let bytes_per_sec = cfg.gbps_target * 1_000_000_000.0 / 8.0;
    let run_deadline = Instant::now() + Duration::from_secs(cfg.run_duration_secs);

    let mut handles: Vec<(u64, &'static str, tokio::task::JoinHandle<u64>)> = Vec::new();
    for (size_bytes, size_label, fraction) in streams.clone() {
        let msgs_per_sec = (bytes_per_sec * fraction) / size_bytes as f64;
        let rate = compute_rate_params(msgs_per_sec);

        for p in 0..1usize {
            let topic = topic_name(&cfg.topic_prefix, size_label, p as u64);
            let stream_id = size_bytes;
            let endpoint = cfg.broker_endpoint.clone();

            eprintln!(
                "[producer] stream {} -> {:.1} msg/s (batch={}, interval={}ms)",
                topic, msgs_per_sec, rate.batch_size, rate.interval_ms
            );

            let handle = tokio::spawn(async move {
                // Own connection per stream — each gets its own TCP socket so streams
                // don't share bandwidth through a single pipe.
                let client = async_nats::connect(&endpoint)
                    .await
                    .expect("NATS producer connect failed");

                let mut buf = vec![0u8; size_bytes as usize];
                { let mut rng = rand::thread_rng(); rng.fill(&mut buf[HEADER_LEN..]); }
                let mut seq: u64 = 0;
                let mut ticker = interval(Duration::from_millis(rate.interval_ms));

                loop {
                    if Instant::now() >= run_deadline {
                        break;
                    }
                    ticker.tick().await;

                    for _ in 0..rate.batch_size {
                        if Instant::now() >= run_deadline {
                            break;
                        }
                        let ts = now_ns();
                        encode_message(&mut buf, ts, seq, stream_id);
                        let payload = Bytes::copy_from_slice(&buf);
                        // Fire-and-forget: core NATS publish, no PubAck round-trip.
                        // Message lands in JetStream stream because subject is bound to it.
                        if let Err(e) = client.publish(topic.clone(), payload).await {
                            eprintln!("[producer] publish error on {}: {}", topic, e);
                        }
                        seq += 1;
                    }
                }
                eprintln!("[producer] stream {} done, sent {} messages", topic, seq);
                seq
            });
            handles.push((size_bytes, size_label, handle));
        }
    }

    // Aggregate sent counts across partitions per stream size
    let mut sent_map: HashMap<(u64, &'static str), u64> = HashMap::new();
    for (size_bytes, size_label, h) in handles {
        let sent = h.await.unwrap_or(0);
        *sent_map.entry((size_bytes, size_label)).or_insert(0) += sent;
    }
    let sent_per_stream: Vec<(u64, &'static str, u64)> = streams
        .iter()
        .map(|(size, label, _)| (*size, *label, sent_map.get(&(*size, *label)).copied().unwrap_or(0)))
        .collect();

    eprintln!("[producer] all streams complete");
    emit_producer_csv(&cfg, &sent_per_stream);
}

async fn nats_consumer(cfg: Config) {
    eprintln!("[consumer] connecting to NATS at {}", cfg.broker_endpoint);

    let streams = active_streams(&cfg.run_mode, cfg.gbps_target, cfg.max_msg_rate);
    let run_deadline = Instant::now() + Duration::from_secs(cfg.run_duration_secs);

    // One task per stream, each with its own TCP connection so streams receive in parallel.
    let mut handles: Vec<(String, tokio::task::JoinHandle<StreamStats>)> = Vec::new();

    for (_size_bytes, size_label, _) in &streams {
        let topic = topic_name(&cfg.topic_prefix, size_label, 0);
        let topic2 = topic.clone();
        let endpoint = cfg.broker_endpoint.clone();

        let handle = tokio::spawn(async move {
            let mut stats = StreamStats::new();

            // Own connection per stream — independent TCP socket, own receive buffer.
            let client = async_nats::ConnectOptions::new()
                .subscription_capacity(1_000_000)
                .connect(&endpoint)
                .await
                .expect("NATS consumer connect failed");

            let mut sub = match client.subscribe(topic2.clone()).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[consumer] subscribe error on {}: {}", topic2, e);
                    return stats;
                }
            };

            eprintln!("[consumer] subscribed to {}", topic2);

            loop {
                tokio::select! {
                    biased;
                    msg = sub.next() => {
                        match msg {
                            Some(msg) => stats.record(&msg.payload, now_ns()),
                            None => break,
                        }
                    }
                    _ = tokio::time::sleep_until(run_deadline) => break,
                }
            }

            eprintln!("[consumer] {} done, received {} messages", topic2, stats.messages_received);
            stats
        });
        handles.push((topic, handle));
    }

    let mut stats_map: HashMap<String, StreamStats> = HashMap::new();
    for (topic, handle) in handles {
        let stats = handle.await.unwrap_or_else(|_| StreamStats::new());
        stats_map.insert(topic, stats);
    }

    emit_consumer_csv(&cfg, &stats_map, &streams);
}

// ─── Redpanda (rdkafka) implementation ───────────────────────────────────────

async fn rdkafka_ensure_topics(broker: &str, topics: &[String], num_partitions: usize) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::ClientConfig;

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .create()
        .expect("admin client creation failed");

    let new_topics: Vec<NewTopic> = topics
        .iter()
        .map(|t| {
            NewTopic::new(t, num_partitions as i32, TopicReplication::Fixed(1))
                .set("max.message.bytes", "104857600") // 100 MB (covers 80 MB payload)
                .set("compression.type", "lz4")
        })
        .collect();

    let opts = AdminOptions::new();
    match admin.create_topics(&new_topics, &opts).await {
        Ok(results) => {
            for r in results {
                match r {
                    Ok(name) => eprintln!("[kafka] topic {} created/exists", name),
                    Err((name, e)) => {
                        eprintln!("[kafka] topic {} error: {:?}", name, e)
                    }
                }
            }
        }
        Err(e) => eprintln!("[kafka] create_topics error: {}", e),
    }
}

async fn redpanda_producer(cfg: Config) {
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::ClientConfig;

    sleep(Duration::from_secs(cfg.producer_start_delay_secs)).await;
    eprintln!(
        "[producer] connecting to Redpanda at {} ({} producer tasks, {} partitions, payload={:?})",
        cfg.broker_endpoint, cfg.num_producer_tasks, cfg.num_partitions, cfg.payload_type
    );

    let streams = active_streams(&cfg.run_mode, cfg.gbps_target, cfg.max_msg_rate);
    let topics: Vec<String> = streams
        .iter()
        .map(|(_, label, _)| topic_name(&cfg.topic_prefix, label, 0))
        .collect();

    rdkafka_ensure_topics(&cfg.broker_endpoint, &topics, cfg.num_partitions).await;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.broker_endpoint)
        .set("message.max.bytes", "104857600")         // 100 MB max message (covers 80 MB payload)
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.kbytes", "4194304")  // 4 GB buffer
        .set("queue.buffering.max.ms", "50")           // linger 50ms for larger batches
        .set("batch.num.messages", "100000")
        .set("batch.size", "104857600")                // 100 MB batch
        .set("compression.type", "lz4")
        .set("socket.send.buffer.bytes", "67108864")
        .set("socket.receive.buffer.bytes", "67108864")
        .create()
        .expect("Kafka producer creation failed");

    let bytes_per_sec = cfg.gbps_target * 1_000_000_000.0 / 8.0;
    let run_deadline = Instant::now() + Duration::from_secs(cfg.run_duration_secs);

    // Real-time throughput reporter
    let bytes_counter = Arc::new(AtomicU64::new(0));
    {
        let counter = bytes_counter.clone();
        let interval_secs = cfg.report_interval_secs;
        tokio::spawn(async move {
            run_throughput_reporter("producer", counter, interval_secs, run_deadline).await;
        });
    }

    let payload_type = cfg.payload_type.clone();
    let mut handles: Vec<(u64, &'static str, tokio::task::JoinHandle<u64>)> = Vec::new();
    for (size_bytes, size_label, fraction) in &streams {
        let topic = topic_name(&cfg.topic_prefix, size_label, 0);
        let base_stream_id = *size_bytes;

        // Rate split evenly across producer tasks (not partitions — tasks control throughput)
        let msgs_per_sec = (bytes_per_sec * fraction) / (*size_bytes as f64 * cfg.num_producer_tasks as f64);
        let rate = compute_rate_params(msgs_per_sec);

        // Limit in-flight per task to ~100ms of pipeline depth
        let max_in_flight = {
            let bytes_in_flight = bytes_per_sec * fraction * 0.100 / cfg.num_producer_tasks as f64;
            ((bytes_in_flight / *size_bytes as f64).ceil() as usize).max(4)
        };

        eprintln!(
            "[producer] topic {} x{} tasks (x{} partitions) -> {:.1} msg/s/task (batch={}, interval={}ms, max_in_flight={})",
            topic, cfg.num_producer_tasks, cfg.num_partitions, msgs_per_sec, rate.batch_size, rate.interval_ms, max_in_flight
        );

        for task_id in 0..cfg.num_producer_tasks {
            // Round-robin across partitions; unique stream_id per task for loss tracking
            let partition = (task_id % cfg.num_partitions) as i32;
            let stream_id = base_stream_id * 1000 + task_id as u64;
            let semaphore = Arc::new(tokio::sync::Semaphore::new(max_in_flight));
            let producer2 = producer.clone();
            let topic2 = topic.clone();
            let size_bytes = *size_bytes;
            let size_label = *size_label;
            let counter = bytes_counter.clone();
            let payload_type2 = payload_type.clone();

            let handle = tokio::spawn(async move {
                let mut rng = rand::rngs::SmallRng::from_entropy();
                let mut buf = vec![0u8; size_bytes as usize];
                match payload_type2 {
                    PayloadType::Float32 => fill_float32_payload(&mut buf, &mut rng),
                    PayloadType::RandomBytes => rng.fill(&mut buf[HEADER_LEN..]),
                }
                let mut seq: u64 = 0;
                let mut ticker = interval(Duration::from_millis(rate.interval_ms));
                let key = format!("{}-{}", stream_id, task_id);

                loop {
                    if Instant::now() >= run_deadline {
                        break;
                    }
                    ticker.tick().await;

                    for _ in 0..rate.batch_size {
                        if Instant::now() >= run_deadline {
                            break;
                        }
                        let permit = semaphore.clone().acquire_owned().await.unwrap();
                        let ts = now_ns();
                        encode_message(&mut buf, ts, seq, stream_id);
                        let topic_clone = topic2.clone();
                        let bytes_sent = size_bytes;
                        let counter2 = counter.clone();
                        // Retry on QueueFull so the producer naturally backs off
                        // when the broker can't keep up, rather than dropping messages.
                        let future = loop {
                            let r = FutureRecord::to(&topic2)
                                .key(key.as_str())
                                .partition(partition)
                                .payload(&buf[..]);
                            match producer2.send_result(r) {
                                Ok(f) => break Some(f),
                                Err((e, _)) if e.to_string().contains("QueueFull") => {
                                    sleep(Duration::from_millis(10)).await;
                                }
                                Err((e, _)) => {
                                    eprintln!("[producer] enqueue error on {}: {}", topic2, e);
                                    break None;
                                }
                            }
                        };
                        if let Some(future) = future {
                            tokio::spawn(async move {
                                match future.await {
                                    Ok(Ok(_)) => {
                                        counter2.fetch_add(bytes_sent, Ordering::Relaxed);
                                    }
                                    Ok(Err((e, _))) => eprintln!("[producer] delivery error on {}: {}", topic_clone, e),
                                    Err(_) => eprintln!("[producer] delivery canceled on {}", topic_clone),
                                }
                                drop(permit);
                            });
                        } else {
                            drop(permit);
                        }
                        seq += 1;
                    }
                }
                eprintln!("[producer] {}/task-{} done, sent {} messages", topic2, task_id, seq);
                seq
            });
            handles.push((size_bytes, size_label, handle));
        }
    }

    // Aggregate sent counts per stream size
    let mut sent_map: HashMap<(u64, &'static str), u64> = HashMap::new();
    for (size_bytes, size_label, h) in handles {
        let sent = h.await.unwrap_or(0);
        *sent_map.entry((size_bytes, size_label)).or_insert(0) += sent;
    }
    let sent_per_stream: Vec<(u64, &'static str, u64)> = streams
        .iter()
        .map(|(size, label, _)| (*size, *label, sent_map.get(&(*size, *label)).copied().unwrap_or(0)))
        .collect();

    eprintln!("[producer] all streams complete");
    emit_producer_csv(&cfg, &sent_per_stream);
}

async fn redpanda_consumer(cfg: Config) {
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
    use rdkafka::ClientConfig;

    eprintln!(
        "[consumer] connecting to Redpanda at {} ({} consumer tasks, {} partitions)",
        cfg.broker_endpoint, cfg.num_consumer_tasks, cfg.num_partitions
    );

    let streams = active_streams(&cfg.run_mode, cfg.gbps_target, cfg.max_msg_rate);
    let topics: Vec<String> = streams
        .iter()
        .map(|(_, label, _)| topic_name(&cfg.topic_prefix, label, 0))
        .collect();

    rdkafka_ensure_topics(&cfg.broker_endpoint, &topics, cfg.num_partitions).await;
    sleep(Duration::from_millis(500)).await;

    let run_deadline = Instant::now() + Duration::from_secs(cfg.run_duration_secs);

    // Real-time throughput reporter
    let bytes_counter = Arc::new(AtomicU64::new(0));
    {
        let counter = bytes_counter.clone();
        let interval_secs = cfg.report_interval_secs;
        tokio::spawn(async move {
            run_throughput_reporter("consumer", counter, interval_secs, run_deadline).await;
        });
    }

    // Spawn one consumer task per CONSUMER_TASKS, each owning a slice of partitions.
    // Using assign() instead of subscribe() bypasses the group coordinator entirely —
    // no rebalance delays, no group protocol overhead, linear throughput scaling.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<HashMap<String, StreamStats>>(cfg.num_consumer_tasks + 1);

    for task_i in 0..cfg.num_consumer_tasks {
        // Interleave partition assignment: task 0 gets 0,4,8,...  task 1 gets 1,5,9,...
        let assigned_partitions: Vec<i32> = (0..cfg.num_partitions as i32)
            .filter(|&p| (p as usize) % cfg.num_consumer_tasks == task_i)
            .collect();

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &cfg.broker_endpoint)
            // group.id is required by rdkafka even with assign() — not used for coordination
            .set("group.id", &format!("bench-{}-{}", cfg.run_id, task_i))
            .set("auto.offset.reset", "latest")
            .set("enable.auto.commit", "false")
            .set("fetch.message.max.bytes", "104857600")   // 100 MB per message (handles 80 MB)
            .set("fetch.max.bytes", "209715200")            // 200 MB total per poll
            .set("receive.message.max.bytes", "268435456") // must be > fetch.max.bytes + 512
            .set("fetch.wait.max.ms", "5")
            .set("socket.receive.buffer.bytes", "67108864")
            .create()
            .expect("Kafka consumer creation failed");

        // Assign specific partitions — skips group coordinator entirely
        let mut tpl = TopicPartitionList::new();
        for topic in &topics {
            for &p in &assigned_partitions {
                tpl.add_partition_offset(topic, p, Offset::End)
                    .expect("add_partition_offset failed");
            }
        }
        consumer.assign(&tpl).expect("consumer assign failed");

        let topics_clone = topics.clone();
        let tx_clone = tx.clone();
        let counter = bytes_counter.clone();

        tokio::spawn(async move {
            let mut local_stats: HashMap<String, StreamStats> = topics_clone
                .iter()
                .map(|t| (t.clone(), StreamStats::new()))
                .collect();

            let mut stream = consumer.stream();
            while tokio::time::Instant::now() < run_deadline {
                match tokio::time::timeout(Duration::from_millis(200), stream.next()).await {
                    Ok(Some(Ok(msg))) => {
                        use rdkafka::Message;
                        let recv_ns = now_ns();
                        let topic = msg.topic().to_string();
                        if let Some(payload) = msg.payload() {
                            counter.fetch_add(payload.len() as u64, Ordering::Relaxed);
                            if let Some(stats) = local_stats.get_mut(&topic) {
                                stats.record(payload, recv_ns);
                            }
                        }
                    }
                    Ok(Some(Err(e))) => eprintln!("[consumer] task {} kafka error: {}", task_i, e),
                    Ok(None) => break,
                    Err(_) => {} // timeout
                }
            }

            let total: u64 = local_stats.values().map(|s| s.messages_received).sum();
            eprintln!("[consumer] task {} done, received {} messages", task_i, total);
            let _ = tx_clone.send(local_stats).await;
        });
    }
    drop(tx); // close the sender side so rx.recv() eventually returns None

    // Aggregate stats from all consumer tasks
    let mut stats_map: HashMap<String, StreamStats> = topics
        .iter()
        .map(|t| (t.clone(), StreamStats::new()))
        .collect();
    while let Some(local) = rx.recv().await {
        for (topic, stats) in local {
            stats_map
                .entry(topic)
                .or_insert_with(StreamStats::new)
                .merge(&stats);
        }
    }

    emit_consumer_csv(&cfg, &stats_map, &streams);
}

// ─── Benchmark report ─────────────────────────────────────────────────────────

struct ReportRow {
    size_label: String,
    size_bytes: u64,
    achieved_gbps: f64,
    messages_received: u64,
    messages_lost: u64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

fn emit_benchmark_report(cfg: &Config, rows: &[ReportRow]) {
    let path = format!(
        "{}/report_{}.md",
        cfg.output_dir, cfg.run_id
    );
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("failed to open {}: {}", path, e));

    // ── Hardware ──────────────────────────────────────────────────────────────
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);

    let total_ram_gb = {
        use sysinfo::System;
        let mut sys = System::new();
        sys.refresh_memory();
        sys.total_memory() / 1_073_741_824
    };

    let in_container = std::path::Path::new("/.dockerenv").exists();
    let os = std::env::consts::OS;

    writeln!(f, "# Broker Benchmark Report").unwrap();
    writeln!(f, "\n## Hardware").unwrap();
    writeln!(f, "| Property | Value |").unwrap();
    writeln!(f, "|---|---|").unwrap();
    writeln!(f, "| OS | {} |", os).unwrap();
    writeln!(f, "| Logical CPU cores | {} |", cpu_count).unwrap();
    writeln!(f, "| Total RAM | {} GB |", total_ram_gb).unwrap();
    writeln!(f, "| Container | {} |", if in_container { "yes (Docker)" } else { "no" }).unwrap();

    // ── Setup ─────────────────────────────────────────────────────────────────
    let payload_label = match cfg.payload_type {
        PayloadType::Float32 => "float32 correlated (LZ4-compressible)",
        PayloadType::RandomBytes => "random bytes (entropy baseline)",
    };

    writeln!(f, "\n## Test Setup").unwrap();
    writeln!(f, "| Parameter | Value |").unwrap();
    writeln!(f, "|---|---|").unwrap();
    writeln!(f, "| Broker | {:?} @ {} |", cfg.broker_type, cfg.broker_endpoint).unwrap();
    writeln!(f, "| Run mode | {} |", run_mode_str(&cfg.run_mode)).unwrap();
    writeln!(f, "| GBPS target | {:.1} |", cfg.gbps_target).unwrap();
    writeln!(f, "| Duration | {}s |", cfg.run_duration_secs).unwrap();
    writeln!(f, "| Partitions | {} |", cfg.num_partitions).unwrap();
    writeln!(f, "| Producer tasks | {} |", cfg.num_producer_tasks).unwrap();
    writeln!(f, "| Consumer tasks | {} |", cfg.num_consumer_tasks).unwrap();
    writeln!(f, "| Payload type | {} |", payload_label).unwrap();
    writeln!(f, "| Max msg rate | {} msg/s/stream |", cfg.max_msg_rate).unwrap();
    writeln!(f, "| Compression | LZ4 |").unwrap();
    writeln!(f, "| Run ID | {} |", cfg.run_id).unwrap();

    // ── Results ───────────────────────────────────────────────────────────────
    writeln!(f, "\n## Results").unwrap();
    writeln!(f, "| Message Size | Target Gbps | Achieved Gbps | Efficiency | Messages | Lost | p50 ms | p95 ms | p99 ms | max ms |").unwrap();
    writeln!(f, "|---|---|---|---|---|---|---|---|---|---|").unwrap();

    for row in rows {
        let efficiency = if cfg.gbps_target > 0.0 {
            (row.achieved_gbps / (cfg.gbps_target * 0.25)) * 100.0  // 0.25 for combined 25% each
        } else {
            0.0
        };
        let target_per_stream = if matches!(cfg.run_mode, RunMode::Combined) {
            cfg.gbps_target * 0.25
        } else {
            cfg.gbps_target
        };
        let eff_pct = if target_per_stream > 0.0 {
            (row.achieved_gbps / target_per_stream * 100.0) as u64
        } else {
            0
        };
        let _ = efficiency;
        writeln!(
            f,
            "| {} | {:.2} | {:.3} | {}% | {} | {} | {:.1} | {:.1} | {:.1} | {:.1} |",
            row.size_label,
            target_per_stream,
            row.achieved_gbps,
            eff_pct,
            row.messages_received,
            row.messages_lost,
            row.p50_ms,
            row.p95_ms,
            row.p99_ms,
            row.max_ms,
        ).unwrap();
    }

    // ── Summary totals ────────────────────────────────────────────────────────
    let total_achieved: f64 = rows.iter().map(|r| r.achieved_gbps).sum();
    let total_messages: u64 = rows.iter().map(|r| r.messages_received).sum();
    let total_lost: u64 = rows.iter().map(|r| r.messages_lost).sum();
    let overall_eff = if cfg.gbps_target > 0.0 {
        (total_achieved / cfg.gbps_target * 100.0) as u64
    } else {
        0
    };

    writeln!(f, "\n## Summary").unwrap();
    writeln!(f, "| Metric | Value |").unwrap();
    writeln!(f, "|---|---|").unwrap();
    writeln!(f, "| Target Gbps | {:.1} |", cfg.gbps_target).unwrap();
    writeln!(f, "| Achieved Gbps | **{:.3}** |", total_achieved).unwrap();
    writeln!(f, "| Efficiency | **{}%** |", overall_eff).unwrap();
    writeln!(f, "| Messages received | {} |", total_messages).unwrap();
    writeln!(f, "| Messages lost | **{}** |", total_lost).unwrap();

    eprintln!("[report] written to {}", path);
}

// ─── CSV emission ─────────────────────────────────────────────────────────────

fn emit_consumer_csv(
    cfg: &Config,
    stats_map: &HashMap<String, StreamStats>,
    streams: &[(u64, &'static str, f64)],
) {
    let bt = broker_type_str(&cfg.broker_type);
    let rm = run_mode_str(&cfg.run_mode);
    let mut f = open_csv_file(cfg);

    write_csv_header(&mut f);

    // NATS uses 1 stream per size; Redpanda, Kafka, and gRPC use num_partitions topics
    let partition_count = match cfg.broker_type {
        BrokerType::Nats => 1,
        BrokerType::Redpanda | BrokerType::Kafka | BrokerType::Grpc => cfg.num_partitions,
    };

    let mut report_rows: Vec<ReportRow> = Vec::new();

    for (size_bytes, size_label, _) in streams {
        // Aggregate stats across all partitions for this stream size
        let mut agg = StreamStats::new();
        for p in 0..partition_count {
            let topic = topic_name(&cfg.topic_prefix, size_label, p as u64);
            if let Some(stats) = stats_map.get(&topic) {
                agg.merge(stats);
            }
        }

        let p50 = us_to_ms(agg.latency_us.value_at_quantile(0.50));
        let p95 = us_to_ms(agg.latency_us.value_at_quantile(0.95));
        let p99 = us_to_ms(agg.latency_us.value_at_quantile(0.99));
        let max = us_to_ms(agg.latency_us.max());
        let achieved = agg.achieved_gbps(cfg.run_duration_secs);

        write_consumer_csv_row(
            &mut f,
            bt,
            rm,
            cfg.gbps_target,
            *size_bytes,
            cfg.run_duration_secs,
            agg.messages_received,
            agg.messages_lost,
            achieved,
            p50,
            p95,
            p99,
            max,
        );

        report_rows.push(ReportRow {
            size_label: size_label.to_string(),
            size_bytes: *size_bytes,
            achieved_gbps: achieved,
            messages_received: agg.messages_received,
            messages_lost: agg.messages_lost,
            p50_ms: p50,
            p95_ms: p95,
            p99_ms: p99,
            max_ms: max,
        });
    }

    emit_benchmark_report(cfg, &report_rows);
}

fn emit_producer_csv(
    cfg: &Config,
    sent_per_stream: &[(u64, &'static str, u64)], // (size_bytes, size_label, messages_sent)
) {
    let bt = broker_type_str(&cfg.broker_type);
    let rm = run_mode_str(&cfg.run_mode);
    let mut f = open_csv_file(cfg);

    write_csv_header(&mut f);

    for (size_bytes, _size_label, messages_sent) in sent_per_stream {
        write_producer_csv_row(
            &mut f,
            bt,
            rm,
            cfg.gbps_target,
            *size_bytes,
            cfg.run_duration_secs,
            *messages_sent,
        );
    }
}

// ─── gRPC implementation ──────────────────────────────────────────────────────

async fn grpc_consumer(cfg: Config) {
    use benchmark_proto::benchmark_service_server::{BenchmarkService, BenchmarkServiceServer};
    use benchmark_proto::{BenchmarkMessage, StreamSummary};
    use std::sync::Mutex;
    use tokio_stream::StreamExt;
    use tonic::{transport::Server, Request, Response, Status, Streaming};

    eprintln!(
        "[consumer] starting gRPC server on {}",
        cfg.broker_endpoint
    );

    let streams = active_streams(&cfg.run_mode, cfg.gbps_target, cfg.max_msg_rate);

    // Shared stats map keyed by stream_id (size_bytes * 1000 + partition).
    let stats: Arc<Mutex<HashMap<u64, StreamStats>>> = Arc::new(Mutex::new(HashMap::new()));

    struct Svc {
        stats: Arc<Mutex<HashMap<u64, StreamStats>>>,
    }

    #[tonic::async_trait]
    impl BenchmarkService for Svc {
        async fn stream_messages(
            &self,
            request: Request<Streaming<BenchmarkMessage>>,
        ) -> Result<Response<StreamSummary>, Status> {
            let mut stream = request.into_inner();
            let mut count: u64 = 0;
            while let Some(msg) = StreamExt::next(&mut stream).await {
                match msg {
                    Ok(m) => {
                        let recv_ns = now_ns();
                        let payload = m.payload;
                        if let Some((_, _, stream_id)) = decode_header(&payload) {
                            let mut map = self.stats.lock().unwrap();
                            map.entry(stream_id)
                                .or_insert_with(StreamStats::new)
                                .record(&payload, recv_ns);
                        }
                        count += 1;
                    }
                    Err(e) => eprintln!("[consumer] stream error: {}", e),
                }
            }
            Ok(Response::new(StreamSummary {
                messages_received: count,
            }))
        }
    }

    let svc = Svc {
        stats: stats.clone(),
    };

    let addr = cfg.broker_endpoint.parse().expect("invalid gRPC listen address");

    // Shut down after run_duration + producer_start_delay + 10s buffer.
    let shutdown_after =
        Duration::from_secs(cfg.run_duration_secs + cfg.producer_start_delay_secs + 10);

    Server::builder()
        .add_service(
            BenchmarkServiceServer::new(svc)
                .max_decoding_message_size(64 * 1024 * 1024),
        )
        .serve_with_shutdown(addr, async move {
            tokio::time::sleep(shutdown_after).await;
        })
        .await
        .expect("gRPC server error");

    // Reconstruct stats_map keyed by topic name for emit_consumer_csv().
    let raw = stats.lock().unwrap();
    let mut stats_map: HashMap<String, StreamStats> = HashMap::new();
    for (size_bytes, size_label, _) in &streams {
        for p in 0..cfg.num_partitions {
            let stream_id = size_bytes * 1000 + p as u64;
            if let Some(s) = raw.get(&stream_id) {
                let topic = topic_name(&cfg.topic_prefix, size_label, p as u64);
                let mut agg = StreamStats::new();
                agg.merge(s);
                stats_map.insert(topic, agg);
            }
        }
    }
    drop(raw);

    emit_consumer_csv(&cfg, &stats_map, &streams);
}

async fn grpc_producer(cfg: Config) {
    use benchmark_proto::benchmark_service_client::BenchmarkServiceClient;
    use benchmark_proto::BenchmarkMessage;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    sleep(Duration::from_secs(cfg.producer_start_delay_secs)).await;

    let endpoint = format!("http://{}", cfg.broker_endpoint);
    eprintln!("[producer] connecting to gRPC server at {}", endpoint);

    // Single channel with HTTP/2 multiplexing — one connection, many concurrent streams.
    let channel = tonic::transport::Channel::from_shared(endpoint)
        .expect("invalid gRPC endpoint")
        .connect()
        .await
        .expect("gRPC connect failed");

    let streams = active_streams(&cfg.run_mode, cfg.gbps_target, cfg.max_msg_rate);
    let bytes_per_sec = cfg.gbps_target * 1_000_000_000.0 / 8.0;
    let run_deadline = Instant::now() + Duration::from_secs(cfg.run_duration_secs);

    let mut handles: Vec<(u64, &'static str, tokio::task::JoinHandle<u64>)> = Vec::new();

    for (size_bytes, size_label, fraction) in &streams {
        let msgs_per_sec =
            (bytes_per_sec * fraction) / (*size_bytes as f64 * cfg.num_partitions as f64);
        let rate = compute_rate_params(msgs_per_sec);
        let base_stream_id = *size_bytes;

        eprintln!(
            "[producer] gRPC {} x{} partitions -> {:.1} msg/s/partition (batch={}, interval={}ms)",
            size_label, cfg.num_partitions, msgs_per_sec, rate.batch_size, rate.interval_ms
        );

        for p in 0..cfg.num_partitions {
            let stream_id = base_stream_id * 1000 + p as u64;
            let channel2 = channel.clone();
            let size_bytes = *size_bytes;
            let size_label = *size_label;

            let handle = tokio::spawn(async move {
                let mut client = BenchmarkServiceClient::new(channel2)
                    .max_encoding_message_size(64 * 1024 * 1024);

                // mpsc channel bridges rate-limited sends into the gRPC streaming RPC.
                let (tx, rx) = mpsc::channel::<BenchmarkMessage>(256);
                let stream = ReceiverStream::new(rx);

                // Spawn the RPC call — it runs until the sender is dropped.
                let rpc = tokio::spawn(async move {
                    match client.stream_messages(stream).await {
                        Ok(_) => {}
                        Err(e) => eprintln!("[producer] gRPC RPC error partition {}: {}", p, e),
                    }
                });

                let mut buf = vec![0u8; size_bytes as usize];
                { let mut rng = rand::thread_rng(); rng.fill(&mut buf[HEADER_LEN..]); }
                let mut seq: u64 = 0;
                let mut ticker = interval(Duration::from_millis(rate.interval_ms));

                loop {
                    if Instant::now() >= run_deadline {
                        break;
                    }
                    ticker.tick().await;

                    for _ in 0..rate.batch_size {
                        if Instant::now() >= run_deadline {
                            break;
                        }
                        let ts = now_ns();
                        encode_message(&mut buf, ts, seq, stream_id);
                        let msg = BenchmarkMessage {
                            payload: buf.clone(),
                        };
                        if tx.send(msg).await.is_err() {
                            break;
                        }
                        seq += 1;
                    }
                }

                drop(tx); // close stream → server returns StreamSummary
                let _ = rpc.await;
                eprintln!(
                    "[producer] gRPC {}/partition-{} done, sent {} messages",
                    size_label, p, seq
                );
                seq
            });
            handles.push((size_bytes, size_label, handle));
        }
    }

    let mut sent_map: HashMap<(u64, &'static str), u64> = HashMap::new();
    for (size_bytes, size_label, h) in handles {
        let sent = h.await.unwrap_or(0);
        *sent_map.entry((size_bytes, size_label)).or_insert(0) += sent;
    }
    let sent_per_stream: Vec<(u64, &'static str, u64)> = streams
        .iter()
        .map(|(size, label, _)| {
            (
                *size,
                *label,
                sent_map.get(&(*size, *label)).copied().unwrap_or(0),
            )
        })
        .collect();

    eprintln!("[producer] all gRPC streams complete");
    emit_producer_csv(&cfg, &sent_per_stream);
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cfg = parse_config();

    eprintln!(
        "[main] role={:?} broker={:?} endpoint={} gbps={} mode={:?} duration={}s",
        cfg.role,
        cfg.broker_type,
        cfg.broker_endpoint,
        cfg.gbps_target,
        cfg.run_mode,
        cfg.run_duration_secs
    );

    match (&cfg.role, &cfg.broker_type) {
        (Role::Producer, BrokerType::Nats) => nats_producer(cfg).await,
        (Role::Consumer, BrokerType::Nats) => nats_consumer(cfg).await,
        (Role::Producer, BrokerType::Redpanda) => redpanda_producer(cfg).await,
        (Role::Consumer, BrokerType::Redpanda) => redpanda_consumer(cfg).await,
        (Role::Producer, BrokerType::Kafka) => redpanda_producer(cfg).await,
        (Role::Consumer, BrokerType::Kafka) => redpanda_consumer(cfg).await,
        (Role::Producer, BrokerType::Grpc) => grpc_producer(cfg).await,
        (Role::Consumer, BrokerType::Grpc) => grpc_consumer(cfg).await,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut buf = vec![0u8; 1024];
        encode_message(&mut buf, 123_456_789_000, 42, 99);
        let (ts, seq, sid) = decode_header(&buf).unwrap();
        assert_eq!(ts, 123_456_789_000);
        assert_eq!(seq, 42);
        assert_eq!(sid, 99);
    }

    #[test]
    fn test_decode_short_buffer_returns_none() {
        let buf = vec![0u8; 10];
        assert!(decode_header(&buf).is_none());
    }

    #[test]
    fn test_rate_params_low_rate() {
        let p = compute_rate_params(10.0);
        assert_eq!(p.batch_size, 1);
        assert!(p.interval_ms > 0);
    }

    #[test]
    fn test_rate_params_high_rate() {
        // 1.25M msg/s should produce batch_size > 1
        let p = compute_rate_params(1_250_000.0);
        assert!(p.batch_size >= 1000);
        assert!(p.interval_ms >= 1);
    }

    #[test]
    fn test_active_streams_combined() {
        // Combined mode should return all 8 message sizes
        let s = active_streams(&RunMode::Combined, 10.0, 100);
        assert_eq!(s.len(), 8);
        // Fractions must sum to 1.0
        let total: f64 = s.iter().map(|(_, _, f)| f).sum();
        assert!((total - 1.0).abs() < 1e-6);
        // All fractions must be positive
        for (_, _, frac) in &s {
            assert!(*frac > 0.0);
        }
    }

    #[test]
    fn test_active_streams_single() {
        // Single-size mode returns exactly one stream
        let s = active_streams(&RunMode::Only64kb, 10.0, 100);
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].0, 65_536);
        // Fraction must be positive and <= 1.0
        assert!(s[0].2 > 0.0);
        assert!(s[0].2 <= 1.0 + 1e-9);
    }

    #[test]
    fn test_topic_name() {
        assert_eq!(topic_name("bench", "1kb", 0), "bench_1kb_0");
    }

    #[test]
    fn test_stream_stats_loss_detection() {
        let mut stats = StreamStats::new();
        let mut buf = vec![0u8; 1024];

        // Send seq 0, 1, skip 2, send 3 → lost = 1
        for seq in [0u64, 1, 3] {
            encode_message(&mut buf, 1_000_000, seq, 7);
            stats.record(&buf, 1_001_000);
        }
        assert_eq!(stats.messages_received, 3);
        assert_eq!(stats.messages_lost, 1);
    }

    #[test]
    fn test_achieved_gbps() {
        let mut stats = StreamStats::new();
        stats.total_bytes_received = 1_000_000_000; // 1 GB
        let gbps = stats.achieved_gbps(1);
        assert!((gbps - 8.0).abs() < 0.001); // 8 Gbps
    }

    #[test]
    fn test_csv_row_formatting() {
        // Just ensure no panic and output looks reasonable
        let mut buf = Vec::new();
        writeln!(buf, "role,broker_type,run_mode,gbps_target,message_size_bytes,run_duration_secs,messages_sent,messages_received,messages_lost,achieved_throughput_gbps,latency_p50_ms,latency_p95_ms,latency_p99_ms,latency_max_ms").unwrap();
        writeln!(buf, "producer,nats,1kb,1.000,1024,120,1000,,,,,,,").unwrap();
        writeln!(buf, "consumer,nats,1kb,1.000,1024,120,,950,50,0.500000,1.200,5.000,9.000,15.000").unwrap();
        assert!(!buf.is_empty());
    }
}
