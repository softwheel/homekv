use anyhow::{bail, Context, Result};
use clap::Parser;
use homekv::storage::{BTreeStore, Mvcc, Store};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use serde_derive::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Parser)]
#[command(
    name = "hkvbench",
    about = "Reproducible HomeKV prototype benchmark harness"
)]
struct Args {
    /// JSON benchmark configuration.
    #[arg(long)]
    config: PathBuf,

    /// Optional file for the JSON result bundle.
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
struct PayloadProfile {
    key_size: usize,
    value_size: usize,
}

#[derive(Debug, Deserialize)]
struct BenchConfig {
    schema_version: u32,
    mode: String,
    seed: u64,
    profiles: Vec<PayloadProfile>,
    dataset_cardinalities: Vec<usize>,
    workloads: Vec<String>,
    warmup_operations: usize,
    storage_operations: usize,
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
    memory_bytes: Option<u64>,
    process_rss_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
struct LatencyPercentiles {
    p50: u64,
    p95: u64,
    p99: u64,
}

#[derive(Debug, Serialize)]
struct BenchResult {
    layer: &'static str,
    workload: String,
    seed: u64,
    key_size: usize,
    value_size: usize,
    dataset_cardinality: usize,
    concurrency: usize,
    warmup_operations: usize,
    measured_operations: usize,
    elapsed_ns: u64,
    throughput_ops_sec: f64,
    latency_ns: LatencyPercentiles,
    failures: u64,
    environment: EnvironmentMetadata,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ResultBundle {
    schema_version: u32,
    mode: String,
    generated_at_unix_ms: u64,
    authoritative_performance_result: bool,
    results: Vec<BenchResult>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config_text = fs::read_to_string(&args.config)
        .with_context(|| format!("failed to read {}", args.config.display()))?;
    let config: BenchConfig = serde_json::from_str(&config_text)
        .with_context(|| format!("invalid benchmark config {}", args.config.display()))?;
    validate_config(&config)?;

    let environment = collect_environment();
    let mut results = Vec::new();

    for profile in &config.profiles {
        for &dataset_cardinality in &config.dataset_cardinalities {
            for workload in &config.workloads {
                let result = run_storage_case(
                    &config,
                    profile,
                    dataset_cardinality,
                    workload,
                    environment.clone(),
                )
                .await?;
                results.push(result);
            }
        }
    }

    let bundle = ResultBundle {
        schema_version: 1,
        mode: config.mode.clone(),
        generated_at_unix_ms: unix_ms(),
        // M0 is explicitly a single-node prototype baseline. Even a full M0 run is
        // not a distributed/strong-consistency release performance result.
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
        fs::write(&path, &encoded)
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    println!("{encoded}");
    Ok(())
}

fn validate_config(config: &BenchConfig) -> Result<()> {
    if config.schema_version != 1 {
        bail!("unsupported config schema_version {}", config.schema_version);
    }
    if config.mode != "smoke" && config.mode != "baseline" {
        bail!("mode must be 'smoke' or 'baseline'");
    }
    if config.profiles.is_empty() {
        bail!("profiles must not be empty");
    }
    if config.dataset_cardinalities.is_empty()
        || config.dataset_cardinalities.iter().any(|&n| n == 0)
    {
        bail!("dataset_cardinalities must contain positive values");
    }
    if config.storage_operations == 0 {
        bail!("storage_operations must be positive");
    }
    for profile in &config.profiles {
        if profile.key_size == 0 || profile.value_size == 0 {
            bail!("key_size and value_size must be positive");
        }
    }
    if config.workloads.is_empty() {
        bail!("workloads must not be empty");
    }
    for workload in &config.workloads {
        match workload.as_str() {
            "get" | "set" | "delete" | "read80_write20" => {}
            other => bail!("unsupported workload '{other}'"),
        }
    }
    Ok(())
}

async fn run_storage_case(
    config: &BenchConfig,
    profile: &PayloadProfile,
    dataset_cardinality: usize,
    workload: &str,
    mut environment: EnvironmentMetadata,
) -> Result<BenchResult> {
    let (store, keys, values) =
        populated_store(profile, dataset_cardinality, config.seed).await?;
    let missing_key = vec![b'z'; profile.key_size];
    let mut rng = SmallRng::seed_from_u64(
        config.seed
            ^ (dataset_cardinality as u64).rotate_left(17)
            ^ workload_discriminator(workload),
    );

    for _ in 0..config.warmup_operations {
        execute_storage_operation(
            &store,
            &keys,
            &values,
            &missing_key,
            workload,
            &mut rng,
        )
        .await?;
    }

    let mut samples = Vec::with_capacity(config.storage_operations);
    let measured_start = Instant::now();
    for _ in 0..config.storage_operations {
        let op_start = Instant::now();
        execute_storage_operation(
            &store,
            &keys,
            &values,
            &missing_key,
            workload,
            &mut rng,
        )
        .await?;
        samples.push(duration_ns(op_start.elapsed()));
    }
    let elapsed = measured_start.elapsed();
    samples.sort_unstable();

    environment.process_rss_bytes = process_rss_bytes();
    let measured_operations = samples.len();
    let elapsed_seconds = elapsed.as_secs_f64();
    let throughput_ops_sec = if elapsed_seconds > 0.0 {
        measured_operations as f64 / elapsed_seconds
    } else {
        0.0
    };

    let mut notes = vec![
        "single-node prototype storage baseline; not a distributed consistency result".to_string(),
        "storage concurrency is 1 because the current MVCC writer path is serialized".to_string(),
    ];
    if workload == "delete" {
        notes.push(
            "DELETE targets a guaranteed-missing key so dataset cardinality remains stable while measuring COW mutation cost"
                .to_string(),
        );
    }

    Ok(BenchResult {
        layer: "storage",
        workload: workload.to_string(),
        seed: config.seed,
        key_size: profile.key_size,
        value_size: profile.value_size,
        dataset_cardinality,
        concurrency: 1,
        warmup_operations: config.warmup_operations,
        measured_operations,
        elapsed_ns: duration_ns(elapsed),
        throughput_ops_sec,
        latency_ns: LatencyPercentiles {
            p50: percentile(&samples, 50.0),
            p95: percentile(&samples, 95.0),
            p99: percentile(&samples, 99.0),
        },
        failures: 0,
        environment,
        notes,
    })
}

async fn populated_store(
    profile: &PayloadProfile,
    dataset_cardinality: usize,
    seed: u64,
) -> Result<(Mvcc<BTreeStore>, Vec<Vec<u8>>, Vec<Vec<u8>>)> {
    let keys = (0..dataset_cardinality)
        .map(|i| deterministic_bytes(seed, i as u64, profile.key_size))
        .collect::<Vec<_>>();
    let values = (0..dataset_cardinality)
        .map(|i| {
            deterministic_bytes(
                seed ^ 0xa5a5_a5a5_a5a5_a5a5,
                i as u64,
                profile.value_size,
            )
        })
        .collect::<Vec<_>>();

    let store = Mvcc::new(BTreeStore::new());
    let mut write_txn = store.write().await;
    {
        let inner = write_txn.get_mut();
        for (key, value) in keys.iter().zip(values.iter()) {
            inner.set(key, value.clone())?;
        }
    }
    write_txn.commit().await;
    Ok((store, keys, values))
}

async fn execute_storage_operation(
    store: &Mvcc<BTreeStore>,
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
    missing_key: &[u8],
    workload: &str,
    rng: &mut SmallRng,
) -> Result<()> {
    let idx = rng.gen_range(0..keys.len());
    match workload {
        "get" => storage_get(store, &keys[idx]).await,
        "set" => storage_set(store, &keys[idx], values[(idx + 1) % values.len()].clone()).await,
        "delete" => storage_delete(store, missing_key).await,
        "read80_write20" => {
            if rng.gen_range(0..100_u32) < 80 {
                storage_get(store, &keys[idx]).await
            } else {
                storage_set(store, &keys[idx], values[(idx + 1) % values.len()].clone()).await
            }
        }
        _ => unreachable!("validated workload"),
    }
}

async fn storage_get(store: &Mvcc<BTreeStore>, key: &[u8]) -> Result<()> {
    let read_txn = store.read().await;
    let value = read_txn.get(key)?;
    if value.is_none() {
        bail!("benchmark expected populated key");
    }
    Ok(())
}

async fn storage_set(store: &Mvcc<BTreeStore>, key: &[u8], value: Vec<u8>) -> Result<()> {
    let mut write_txn = store.write().await;
    write_txn.get_mut().set(key, value)?;
    write_txn.commit().await;
    Ok(())
}

async fn storage_delete(store: &Mvcc<BTreeStore>, key: &[u8]) -> Result<()> {
    let mut write_txn = store.write().await;
    write_txn.get_mut().delete(key)?;
    write_txn.commit().await;
    Ok(())
}

fn deterministic_bytes(seed: u64, ordinal: u64, len: usize) -> Vec<u8> {
    let mut result = Vec::with_capacity(len);
    let mut block = 0_u64;
    while result.len() < len {
        let mixed = splitmix64(
            seed ^ ordinal.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ block.rotate_left(29),
        );
        let chunk = format!("{mixed:016x}");
        result.extend_from_slice(chunk.as_bytes());
        block = block.wrapping_add(1);
    }
    result.truncate(len);
    result
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn workload_discriminator(workload: &str) -> u64 {
    match workload {
        "get" => 0x01,
        "set" => 0x02,
        "delete" => 0x03,
        "read80_write20" => 0x04,
        _ => 0xff,
    }
}

fn percentile(sorted_samples: &[u64], pct: f64) -> u64 {
    if sorted_samples.is_empty() {
        return 0;
    }
    let rank = ((pct / 100.0) * sorted_samples.len() as f64).ceil() as usize;
    sorted_samples[rank.saturating_sub(1).min(sorted_samples.len() - 1)]
}

fn duration_ns(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn collect_environment() -> EnvironmentMetadata {
    EnvironmentMetadata {
        homekv_git_sha: command_output("git", &["rev-parse", "HEAD"])
            .or_else(|| std::env::var("GITHUB_SHA").ok())
            .unwrap_or_else(|| "unknown".to_string()),
        rustc_version: command_output("rustc", &["--version"])
            .unwrap_or_else(|| "unknown".to_string()),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        kernel: command_output("uname", &["-r"]).unwrap_or_else(|| "unknown".to_string()),
        cpu_model: linux_field(Path::new("/proc/cpuinfo"), "model name")
            .unwrap_or_else(|| "unknown".to_string()),
        logical_cpus: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        memory_bytes: linux_kib_field(Path::new("/proc/meminfo"), "MemTotal"),
        process_rss_bytes: process_rss_bytes(),
    }
}

fn process_rss_bytes() -> Option<u64> {
    linux_kib_field(Path::new("/proc/self/status"), "VmRSS")
}

fn linux_field(path: &Path, key: &str) -> Option<String> {
    let text = fs::read_to_string(path).ok()?;
    text.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.trim() == key).then(|| value.trim().to_string())
    })
}

fn linux_kib_field(path: &Path, key: &str) -> Option<u64> {
    let value = linux_field(path, key)?;
    let number = value.split_whitespace().next()?.parse::<u64>().ok()?;
    number.checked_mul(1024)
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    Some(value.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_uses_nearest_rank() {
        let samples = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        assert_eq!(percentile(&samples, 50.0), 5);
        assert_eq!(percentile(&samples, 95.0), 10);
        assert_eq!(percentile(&samples, 99.0), 10);
    }

    #[test]
    fn deterministic_generator_is_stable_and_sized() {
        let a = deterministic_bytes(42, 7, 16);
        let b = deterministic_bytes(42, 7, 16);
        let c = deterministic_bytes(42, 8, 16);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 16);
        assert_eq!(deterministic_bytes(42, 7, 32).len(), 32);
    }

    #[test]
    fn guaranteed_missing_key_cannot_match_hex_generated_keys() {
        let key = deterministic_bytes(42, 1, 16);
        assert!(!key.contains(&b'z'));
        assert_ne!(key, vec![b'z'; 16]);
    }
}
