use anyhow::{bail, Context, Result};
use clap::Parser;
use homekv::data_plane::{
    decode_prefix, decode_response, encode_request, CodecLimits, FrameKind, Request as CompactRequest,
    RequestBody, Status as CompactStatus, FRAME_PREFIX_LEN,
};
use homekv::storage::shard_for_key;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tonic::transport::Channel;

use homekv_service::home_kv_service_client::HomeKvServiceClient;
use homekv_service::{DelRequest, GetRequest, Record, SetRequest};

mod homekv_service {
    tonic::include_proto!("homekv_service");
}

#[derive(Debug, Parser)]
#[command(name = "hkvm2bench", about = "HomeKV M2 compact-vs-gRPC local benchmark")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct BenchConfig {
    schema_version: u32,
    seed: u64,
    grpc_endpoint: String,
    compact_endpoint: String,
    key_size: usize,
    value_size: usize,
    dataset_cardinality: usize,
    pipeline_depths: Vec<usize>,
    workloads: Vec<String>,
    warmup_operations: usize,
    measured_operations: usize,
}

#[derive(Debug, Clone, Serialize)]
struct EnvironmentMetadata {
    homekv_git_sha: String,
    rustc_version: String,
    os: String,
    arch: String,
    kernel: String,
    cpu_model: String,
    logical_cpus: usize,
}

#[derive(Debug, Serialize)]
struct LatencyPercentiles {
    p50: u64,
    p95: u64,
    p99: u64,
}

#[derive(Debug, Serialize)]
struct BenchResult {
    protocol: String,
    workload: String,
    key_size: usize,
    value_size: usize,
    dataset_cardinality: usize,
    pipeline_depth: usize,
    warmup_operations: usize,
    attempted_operations: usize,
    measured_operations: usize,
    elapsed_ns: u64,
    throughput_ops_sec: f64,
    latency_ns: LatencyPercentiles,
    failures: u64,
    endpoint: String,
    environment: EnvironmentMetadata,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ResultBundle {
    schema_version: u32,
    mode: &'static str,
    generated_at_unix_ms: u64,
    authoritative_performance_result: bool,
    results: Vec<BenchResult>,
}

#[derive(Clone)]
struct OperationPlan {
    key: String,
    value: Vec<u8>,
    kind: OpKind,
}

#[derive(Clone, Copy)]
enum OpKind {
    Get,
    Set,
    Delete,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let text = fs::read_to_string(&args.config)
        .with_context(|| format!("failed to read {}", args.config.display()))?;
    let config: BenchConfig = serde_json::from_str(&text)
        .with_context(|| format!("invalid benchmark config {}", args.config.display()))?;
    validate_config(&config)?;

    let keys = make_keys(config.seed, config.dataset_cardinality, config.key_size);
    let values = make_values(config.seed, config.dataset_cardinality, config.value_size);
    let environment = collect_environment();

    let mut grpc = HomeKvServiceClient::connect(config.grpc_endpoint.clone())
        .await
        .with_context(|| format!("failed to connect to {}", config.grpc_endpoint))?;
    preload_grpc(&mut grpc, &keys, &values).await?;

    let mut results = Vec::new();
    for &depth in &config.pipeline_depths {
        for workload in &config.workloads {
            results.push(
                run_grpc_case(
                    &config,
                    depth,
                    workload,
                    grpc.clone(),
                    &keys,
                    &values,
                    environment.clone(),
                )
                .await?,
            );
            results.push(
                run_compact_case(
                    &config,
                    depth,
                    workload,
                    &keys,
                    &values,
                    environment.clone(),
                )
                .await?,
            );
        }
    }

    let bundle = ResultBundle {
        schema_version: 1,
        mode: "m2-comparative",
        generated_at_unix_ms: unix_ms(),
        authoritative_performance_result: false,
        results,
    };
    let encoded = serde_json::to_string_pretty(&bundle)?;
    if let Some(path) = args.output {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        fs::write(&path, &encoded)?;
    }
    println!("{encoded}");
    Ok(())
}

fn validate_config(config: &BenchConfig) -> Result<()> {
    if config.schema_version != 1 {
        bail!("unsupported schema_version {}", config.schema_version);
    }
    if config.grpc_endpoint.trim().is_empty() || config.compact_endpoint.trim().is_empty() {
        bail!("both endpoints must be configured");
    }
    if config.key_size == 0 || config.value_size == 0 || config.dataset_cardinality == 0 {
        bail!("key/value sizes and dataset cardinality must be positive");
    }
    if config.pipeline_depths.is_empty() || config.pipeline_depths.iter().any(|&d| d == 0) {
        bail!("pipeline_depths must contain positive values");
    }
    if !config.pipeline_depths.contains(&1) || !config.pipeline_depths.contains(&32) {
        bail!("M2 comparison requires pipeline depths 1 and 32");
    }
    if config.measured_operations < 10_000 {
        bail!("M2 comparison requires at least 10000 measured operations per cell");
    }
    if config.workloads.is_empty() {
        bail!("workloads must not be empty");
    }
    for workload in &config.workloads {
        match workload.as_str() {
            "get" | "set" | "delete" | "read80_write20" => {}
            other => bail!("unsupported workload {other}"),
        }
    }
    Ok(())
}

async fn preload_grpc(
    client: &mut HomeKvServiceClient<Channel>,
    keys: &[String],
    values: &[Vec<u8>],
) -> Result<()> {
    const BATCH: usize = 1024;
    for start in (0..keys.len()).step_by(BATCH) {
        let end = (start + BATCH).min(keys.len());
        let records = (start..end)
            .map(|i| Record {
                key: keys[i].clone(),
                value: Some(values[i].clone()),
            })
            .collect();
        let response = client.set(SetRequest { records }).await?.into_inner();
        if !response.succ {
            bail!("preload SET returned succ=false");
        }
    }
    Ok(())
}

async fn run_grpc_case(
    config: &BenchConfig,
    depth: usize,
    workload: &str,
    client: HomeKvServiceClient<Channel>,
    keys: &[String],
    values: &[Vec<u8>],
    environment: EnvironmentMetadata,
) -> Result<BenchResult> {
    run_grpc_operations(
        config,
        depth,
        workload,
        client.clone(),
        keys,
        values,
        config.warmup_operations,
        false,
    )
    .await?;

    let started = Instant::now();
    let (mut samples, failures) = run_grpc_operations(
        config,
        depth,
        workload,
        client,
        keys,
        values,
        config.measured_operations,
        true,
    )
    .await?;
    let elapsed = started.elapsed();
    samples.sort_unstable();
    Ok(make_result(
        "grpc",
        workload,
        depth,
        config,
        samples,
        failures,
        elapsed.as_nanos().min(u64::MAX as u128) as u64,
        config.grpc_endpoint.clone(),
        environment,
        "gRPC uses the existing Tonic/HTTP2 compatibility path with the same local M1 shard engine",
    ))
}

async fn run_grpc_operations(
    config: &BenchConfig,
    depth: usize,
    workload: &str,
    client: HomeKvServiceClient<Channel>,
    keys: &[String],
    values: &[Vec<u8>],
    operations: usize,
    record: bool,
) -> Result<(Vec<u64>, u64)> {
    let mut samples = Vec::with_capacity(if record { operations } else { 0 });
    let mut failures = 0u64;
    let mut ordinal = 0usize;
    while ordinal < operations {
        let width = depth.min(operations - ordinal);
        let mut handles = Vec::with_capacity(width);
        for offset in 0..width {
            let plan = operation_plan(config, workload, ordinal + offset, keys, values);
            let mut worker = client.clone();
            handles.push(tokio::spawn(async move {
                let started = Instant::now();
                let result = execute_grpc(&mut worker, plan).await;
                let latency = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                (result, latency)
            }));
        }
        for handle in handles {
            let (result, latency) = handle.await.context("gRPC benchmark worker join failed")?;
            if result.is_ok() {
                if record {
                    samples.push(latency);
                }
            } else {
                failures = failures.saturating_add(1);
            }
        }
        ordinal += width;
    }
    Ok((samples, failures))
}

async fn execute_grpc(client: &mut HomeKvServiceClient<Channel>, plan: OperationPlan) -> Result<()> {
    match plan.kind {
        OpKind::Get => {
            let response = client
                .get(GetRequest { keys: vec![plan.key] })
                .await?
                .into_inner();
            if response.records.len() != 1 || response.records[0].value.is_none() {
                bail!("GET did not return one populated record");
            }
        }
        OpKind::Set => {
            let response = client
                .set(SetRequest {
                    records: vec![Record {
                        key: plan.key,
                        value: Some(plan.value),
                    }],
                })
                .await?
                .into_inner();
            if !response.succ {
                bail!("SET returned succ=false");
            }
        }
        OpKind::Delete => {
            let response = client
                .del(DelRequest { keys: vec![plan.key] })
                .await?
                .into_inner();
            if !response.succ {
                bail!("DELETE returned succ=false");
            }
        }
    }
    Ok(())
}

async fn run_compact_case(
    config: &BenchConfig,
    depth: usize,
    workload: &str,
    keys: &[String],
    values: &[Vec<u8>],
    environment: EnvironmentMetadata,
) -> Result<BenchResult> {
    let mut stream = TcpStream::connect(&config.compact_endpoint)
        .await
        .with_context(|| format!("failed to connect to {}", config.compact_endpoint))?;
    let limits = CodecLimits::default();
    let mut request_id = 1u64;
    run_compact_operations(
        &mut stream,
        limits,
        config,
        depth,
        workload,
        keys,
        values,
        config.warmup_operations,
        &mut request_id,
        false,
    )
    .await?;

    let started = Instant::now();
    let (mut samples, failures) = run_compact_operations(
        &mut stream,
        limits,
        config,
        depth,
        workload,
        keys,
        values,
        config.measured_operations,
        &mut request_id,
        true,
    )
    .await?;
    let elapsed = started.elapsed();
    samples.sort_unstable();
    Ok(make_result(
        "compact",
        workload,
        depth,
        config,
        samples,
        failures,
        elapsed.as_nanos().min(u64::MAX as u128) as u64,
        config.compact_endpoint.clone(),
        environment,
        "compact uses one TCP connection with true request pipelining and request-id response correlation",
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_compact_operations(
    stream: &mut TcpStream,
    limits: CodecLimits,
    config: &BenchConfig,
    depth: usize,
    workload: &str,
    keys: &[String],
    values: &[Vec<u8>],
    operations: usize,
    next_request_id: &mut u64,
    record: bool,
) -> Result<(Vec<u64>, u64)> {
    let mut samples = Vec::with_capacity(if record { operations } else { 0 });
    let mut failures = 0u64;
    let mut ordinal = 0usize;
    while ordinal < operations {
        let width = depth.min(operations - ordinal);
        let mut starts = HashMap::with_capacity(width);
        for offset in 0..width {
            let plan = operation_plan(config, workload, ordinal + offset, keys, values);
            let id = *next_request_id;
            *next_request_id = next_request_id.wrapping_add(1).max(1);
            let key = plan.key.into_bytes();
            let body = match plan.kind {
                OpKind::Get => RequestBody::Get { key: key.clone() },
                OpKind::Set => RequestBody::Set {
                    key: key.clone(),
                    value: plan.value,
                },
                OpKind::Delete => RequestBody::Delete { key: key.clone() },
            };
            let request = CompactRequest {
                request_id: id,
                shard_id: shard_for_key(&key).as_u16(),
                body,
            };
            let frame = encode_request(&request, limits)?;
            let started = Instant::now();
            stream.write_all(&frame).await?;
            starts.insert(id, started);
        }
        stream.flush().await?;
        for _ in 0..width {
            let response = read_compact_response(stream, limits).await?;
            let started = starts
                .remove(&response.request_id)
                .context("response request_id was not outstanding")?;
            if response.status == CompactStatus::Ok {
                if record {
                    samples.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
                }
            } else {
                failures = failures.saturating_add(1);
            }
        }
        if !starts.is_empty() {
            bail!("compact pipeline chunk ended with unresolved request ids");
        }
        ordinal += width;
    }
    Ok((samples, failures))
}

async fn read_compact_response(stream: &mut TcpStream, limits: CodecLimits) -> Result<homekv::data_plane::Response> {
    let mut prefix_bytes = [0u8; FRAME_PREFIX_LEN];
    stream.read_exact(&mut prefix_bytes).await?;
    let prefix = decode_prefix(&prefix_bytes, limits)?;
    if prefix.kind != FrameKind::Response {
        bail!("compact server returned unexpected frame kind {:?}", prefix.kind);
    }
    let mut frame = Vec::with_capacity(FRAME_PREFIX_LEN + prefix.payload_len as usize);
    frame.extend_from_slice(&prefix_bytes);
    let mut payload = vec![0u8; prefix.payload_len as usize];
    stream.read_exact(&mut payload).await?;
    frame.extend_from_slice(&payload);
    Ok(decode_response(&frame, limits)?)
}

fn operation_plan(
    config: &BenchConfig,
    workload: &str,
    ordinal: usize,
    keys: &[String],
    values: &[Vec<u8>],
) -> OperationPlan {
    let mixed = workload == "read80_write20";
    let kind = match workload {
        "get" => OpKind::Get,
        "set" => OpKind::Set,
        "delete" => OpKind::Delete,
        "read80_write20" if ordinal % 5 == 4 => OpKind::Set,
        "read80_write20" => OpKind::Get,
        _ => unreachable!("validated workload"),
    };
    let idx = ((ordinal as u64)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(config.seed) as usize)
        % keys.len();
    let key = if matches!(kind, OpKind::Delete) {
        "z".repeat(config.key_size)
    } else {
        keys[idx].clone()
    };
    let value = if mixed || matches!(kind, OpKind::Set) {
        values[(idx + 1) % values.len()].clone()
    } else {
        Vec::new()
    };
    OperationPlan { key, value, kind }
}

fn make_result(
    protocol: &str,
    workload: &str,
    depth: usize,
    config: &BenchConfig,
    samples: Vec<u64>,
    failures: u64,
    elapsed_ns: u64,
    endpoint: String,
    environment: EnvironmentMetadata,
    protocol_note: &str,
) -> BenchResult {
    let measured = samples.len();
    let attempted = measured.saturating_add(failures as usize);
    let elapsed_seconds = elapsed_ns as f64 / 1_000_000_000.0;
    BenchResult {
        protocol: protocol.to_string(),
        workload: workload.to_string(),
        key_size: config.key_size,
        value_size: config.value_size,
        dataset_cardinality: config.dataset_cardinality,
        pipeline_depth: depth,
        warmup_operations: config.warmup_operations,
        attempted_operations: attempted,
        measured_operations: measured,
        elapsed_ns,
        throughput_ops_sec: if elapsed_seconds > 0.0 {
            measured as f64 / elapsed_seconds
        } else {
            0.0
        },
        latency_ns: LatencyPercentiles {
            p50: percentile(&samples, 50.0),
            p95: percentile(&samples, 95.0),
            p99: percentile(&samples, 99.0),
        },
        failures,
        endpoint,
        environment,
        notes: vec![
            protocol_note.to_string(),
            "single-node local M1 semantics only; no replicated durability or distributed linearizability claim".to_string(),
            "engineering comparison only; authoritative_performance_result=false".to_string(),
        ],
    }
}

fn make_keys(seed: u64, count: usize, size: usize) -> Vec<String> {
    (0..count)
        .map(|i| {
            let mut s = format!("{:016x}", (i as u64) ^ seed);
            while s.len() < size {
                s.push('k');
            }
            s.truncate(size);
            s
        })
        .collect()
}

fn make_values(seed: u64, count: usize, size: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            (0..size)
                .map(|j| ((seed.wrapping_add(i as u64).wrapping_add(j as u64)) % 251) as u8)
                .collect()
        })
        .collect()
}

fn percentile(samples: &[u64], pct: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let rank = ((pct / 100.0) * (samples.len().saturating_sub(1) as f64)).round() as usize;
    samples[rank.min(samples.len() - 1)]
}

fn collect_environment() -> EnvironmentMetadata {
    EnvironmentMetadata {
        homekv_git_sha: command_output("git", &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into()),
        rustc_version: command_output("rustc", &["--version"]).unwrap_or_else(|| "unknown".into()),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        kernel: command_output("uname", &["-sr"]).unwrap_or_else(|| "unknown".into()),
        cpu_model: cpu_model(),
        logical_cpus: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
    }
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn cpu_model() -> String {
    fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("model name\t: ").map(str::to_string))
        })
        .unwrap_or_else(|| "unknown".into())
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
