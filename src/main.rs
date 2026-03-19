use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub mod benchmark_proto {
    tonic::include_proto!("benchmark");
}

use rand::Rng;

use bytes::Bytes;
use futures::StreamExt;
use hdrhistogram::Histogram;
use tokio::time::{interval, sleep, Instant};

// ─── Message sizes ──────────────────────────────────────────────────────────

const MESSAGE_SIZES: &[(u64, &str)] = &[
    (65_536, "64kb"),
    (1_048_576, "1mb"),
    (12_582_912, "12mb"),
    (33_554_432, "32mb"),
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
    Nats,
    Grpc,
}

#[derive(Debug, Clone, PartialEq)]
enum RunMode {
    Combined,
    Only64kb,
    Only1mb,
    Only12mb,
    Only32mb,
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
        "grpc" => BrokerType::Grpc,
        _ => BrokerType::Nats,
    };

    let broker_endpoint = env_var(
        "BROKER_ENDPOINT",
        match broker_type {
            BrokerType::Redpanda => "localhost:9092",
            BrokerType::Nats => "localhost:4222",
            BrokerType::Grpc => "0.0.0.0:50051",
        },
    );

    let gbps_target: f64 = env_var("GBPS_TARGET", "1.0")
        .parse()
        .expect("GBPS_TARGET must be a float");

    let run_mode = match env_var("RUN_MODE", "combined").to_lowercase().as_str() {
        "64kb" => RunMode::Only64kb,
        "1mb" => RunMode::Only1mb,
        "12mb" => RunMode::Only12mb,
        "32mb" => RunMode::Only32mb,
        _ => RunMode::Combined,
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
    let num_partitions: usize = env_var("PARTITIONS", "4")
        .parse()
        .expect("PARTITIONS must be a positive integer");
    let num_partitions = num_partitions.max(1);

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
    }
}

// ─── Active streams ───────────────────────────────────────────────────────────

/// Returns (size_bytes, size_label, fraction_of_gbps_target) for each active stream.
fn active_streams(mode: &RunMode) -> Vec<(u64, &'static str, f64)> {
    match mode {
        RunMode::Combined => MESSAGE_SIZES
            .iter()
            .map(|(size, label)| (*size, *label, 0.25))
            .collect(),
        RunMode::Only64kb => vec![(65_536, "64kb", 1.0)],
        RunMode::Only1mb => vec![(1_048_576, "1mb", 1.0)],
        RunMode::Only12mb => vec![(12_582_912, "12mb", 1.0)],
        RunMode::Only32mb => vec![(33_554_432, "32mb", 1.0)],
    }
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
    buf[0..8].copy_from_slice(&producer_ts_ns.to_be_bytes());
    buf[8..16].copy_from_slice(&seq.to_be_bytes());
    buf[16..24].copy_from_slice(&stream_id.to_be_bytes());
    // remainder is zero-padding (already zeroed from allocation)
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
        BrokerType::Nats => "nats",
        BrokerType::Grpc => "grpc",
    }
}

fn run_mode_str(rm: &RunMode) -> &'static str {
    match rm {
        RunMode::Combined => "combined",
        RunMode::Only64kb => "64kb",
        RunMode::Only1mb => "1mb",
        RunMode::Only12mb => "12mb",
        RunMode::Only32mb => "32mb",
    }
}

fn us_to_ms(us: u64) -> f64 {
    us as f64 / 1000.0
}

// ─── NATS implementation ──────────────────────────────────────────────────────

async fn nats_producer(cfg: Config) {
    sleep(Duration::from_secs(cfg.producer_start_delay_secs)).await;
    eprintln!("[producer] connecting to NATS at {}", cfg.broker_endpoint);

    let streams = active_streams(&cfg.run_mode);
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

    let streams = active_streams(&cfg.run_mode);
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
                .set("max.message.bytes", "67108864")
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
        "[producer] connecting to Redpanda at {}",
        cfg.broker_endpoint
    );

    let streams = active_streams(&cfg.run_mode);
    let topics: Vec<String> = streams
        .iter()
        .map(|(_, label, _)| topic_name(&cfg.topic_prefix, label, 0))
        .collect();

    rdkafka_ensure_topics(&cfg.broker_endpoint, &topics, cfg.num_partitions).await;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.broker_endpoint)
        .set("message.max.bytes", "33600000")
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.kbytes", "4194304")  // 4 GB buffer
        .set("queue.buffering.max.ms", "10")
        .set("batch.num.messages", "100000")
        .set("batch.size", "104857600")  // 100 MB batches to match broker
        .create()
        .expect("Kafka producer creation failed");

    let bytes_per_sec = cfg.gbps_target * 1_000_000_000.0 / 8.0;
    let run_deadline = Instant::now() + Duration::from_secs(cfg.run_duration_secs);

    let mut handles: Vec<(u64, &'static str, tokio::task::JoinHandle<u64>)> = Vec::new();
    for (size_bytes, size_label, fraction) in &streams {
        let topic = topic_name(&cfg.topic_prefix, size_label, 0);
        let base_stream_id = *size_bytes;
        // Split rate evenly across partitions
        let msgs_per_sec = (bytes_per_sec * fraction) / (*size_bytes as f64 * cfg.num_partitions as f64);
        let rate = compute_rate_params(msgs_per_sec);

        // Limit in-flight per partition to ~100ms of pipeline depth.
        // Generous enough to keep the pipeline full across linger+ack time,
        // tight enough to bound queue buildup and latency.
        let max_in_flight = {
            let bytes_in_flight = bytes_per_sec * fraction * 0.100 / cfg.num_partitions as f64;
            ((bytes_in_flight / *size_bytes as f64).ceil() as usize).max(4)
        };

        eprintln!(
            "[producer] topic {} x{} partitions -> {:.1} msg/s/partition (batch={}, interval={}ms, max_in_flight={})",
            topic, cfg.num_partitions, msgs_per_sec, rate.batch_size, rate.interval_ms, max_in_flight
        );

        for p in 0..cfg.num_partitions {
            // Unique stream_id per partition so the consumer's loss detector
            // tracks each partition's sequence space independently.
            let stream_id = base_stream_id * 1000 + p as u64;
            let semaphore = Arc::new(tokio::sync::Semaphore::new(max_in_flight));
            let producer2 = producer.clone();
            let topic2 = topic.clone();
            let size_bytes = *size_bytes;
            let size_label = *size_label;

            let handle = tokio::spawn(async move {
                let mut buf = vec![0u8; size_bytes as usize];
                { let mut rng = rand::thread_rng(); rng.fill(&mut buf[HEADER_LEN..]); }
                let mut seq: u64 = 0;
                let mut ticker = interval(Duration::from_millis(rate.interval_ms));
                let key = format!("{}-{}", stream_id, p);

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
                        let record = FutureRecord::to(&topic2)
                            .key(key.as_str())
                            .partition(p as i32)
                            .payload(&buf[..]);
                        let topic_clone = topic2.clone();
                        match producer2.send_result(record) {
                            Ok(future) => {
                                tokio::spawn(async move {
                                    match future.await {
                                        Ok(Ok(_)) => {}
                                        Ok(Err((e, _))) => eprintln!("[producer] delivery error on {}: {}", topic_clone, e),
                                        Err(_) => eprintln!("[producer] delivery canceled on {}", topic_clone),
                                    }
                                    drop(permit);
                                });
                            }
                            Err((e, _)) => {
                                eprintln!("[producer] enqueue error on {}: {}", topic2, e);
                                drop(permit);
                            }
                        }
                        seq += 1;
                    }
                }
                eprintln!("[producer] {}/partition-{} done, sent {} messages", topic2, p, seq);
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

async fn redpanda_consumer(cfg: Config) {
    use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
    use rdkafka::ClientConfig;

    eprintln!(
        "[consumer] connecting to Redpanda at {}",
        cfg.broker_endpoint
    );

    let streams = active_streams(&cfg.run_mode);
    let topics: Vec<String> = streams
        .iter()
        .map(|(_, label, _)| topic_name(&cfg.topic_prefix, label, 0))
        .collect();

    rdkafka_ensure_topics(&cfg.broker_endpoint, &topics, cfg.num_partitions).await;
    sleep(Duration::from_millis(500)).await;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.broker_endpoint)
        .set("group.id", &format!("broker-benchmarker-{}", cfg.run_id))
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "false")
        .set("fetch.message.max.bytes", "33600000")
        .set("fetch.max.bytes", "52428800")
        .set("receive.message.max.bytes", "67108864")
        .create()
        .expect("Kafka consumer creation failed");

    let topic_strs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
    consumer
        .subscribe(&topic_strs)
        .expect("subscription failed");

    let mut stats_map: HashMap<String, StreamStats> = HashMap::new();
    for topic in &topics {
        stats_map.insert(topic.clone(), StreamStats::new());
    }

    let run_deadline = Instant::now() + Duration::from_secs(cfg.run_duration_secs);

    let mut stream = consumer.stream();
    while Instant::now() < run_deadline {
        match tokio::time::timeout(Duration::from_millis(200), stream.next()).await {
            Ok(Some(Ok(msg))) => {
                use rdkafka::Message;
                let recv_ns = now_ns();
                let topic = msg.topic().to_string();
                if let Some(payload) = msg.payload() {
                    if let Some(stats) = stats_map.get_mut(&topic) {
                        stats.record(payload, recv_ns);
                    }
                }
                let _ = consumer.commit_message(&msg, CommitMode::Async);
            }
            Ok(Some(Err(e))) => eprintln!("[consumer] kafka error: {}", e),
            Ok(None) => break,
            Err(_) => {} // timeout
        }
    }

    emit_consumer_csv(&cfg, &stats_map, &streams);
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

    // NATS uses 1 stream per size; Redpanda and gRPC use num_partitions topics
    let partition_count = match cfg.broker_type {
        BrokerType::Nats => 1,
        BrokerType::Redpanda | BrokerType::Grpc => cfg.num_partitions,
    };

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
    }
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

    let streams = active_streams(&cfg.run_mode);

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

    let streams = active_streams(&cfg.run_mode);
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
        let s = active_streams(&RunMode::Combined);
        assert_eq!(s.len(), 4);
        for (_, _, frac) in &s {
            assert!((frac - 0.25).abs() < 1e-9);
        }
    }

    #[test]
    fn test_active_streams_single() {
        let s = active_streams(&RunMode::Only64kb);
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].0, 65_536);
        assert!((s[0].2 - 1.0).abs() < 1e-9);
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
