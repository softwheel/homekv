use anyhow::{bail, Context, Result};
use clap::Parser;
use homekv::storage::{BTreeStore, Mvcc, Store};
use serde_derive::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::net::{TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::transport::Channel;

use homekv_service::home_kv_service_client::HomeKvServiceClient;
use homekv_service::{Record, SetRequest};

mod homekv_service {
    tonic::include_proto!("homekv_service");
}

#[derive(Debug, Parser)]
#[command(
    name = "hkvmem",
    about = "Low-intrusion HomeKV M0 process/RSS memory accounting probe"
)]
struct Args {
    /// Existing hkvbench JSON configuration. Storage/server dimensions are reused.
    #[arg(long)]
    config: PathBuf,

    /// Optional file for the JSON result bundle.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Path to a pre-built HomeKV server binary for server-layer memory probes.
    #[arg(long)]
    homekv_bin: Option<PathBuf>,

    /// Settle interval before taking RSS samples.
    #[arg(long, default_value_t = 250)]
    settle_ms: u64,

    /// Internal isolated storage worker case: key_size,value_size,cardinality.
    #[arg(long, hide = true)]
    storage_worker: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct PayloadProfile {
    key_size: usize,
    value_size: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct ServerCase {
    key_size: usize,
    value_size: usize,
    dataset_cardinality: usize,
    concurrency: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct ServerConfig {
    #[serde(default = "default_preload_batch_size")]
    preload_batch_size: usize,
    cases: Vec<ServerCase>,
}

#[derive(Debug, Deserialize)]
struct BenchConfig {
    schema_version: u32,
    mode: String,
    #[serde(default = "default_layer")]
    layer: String,
    seed: u64,
    #[serde(default)]
    profiles: Vec<PayloadProfile>,
    #[serde(default)]
    dataset_cardinalities: Vec<usize>,
    server: Option<ServerConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MemoryMeasurement {
    layer: String,
    target: String,
    target_pid: u32,
    key_size: usize,
    value_size: usize,
    dataset_cardinality: usize,
    logical_payload_bytes: u64,
    rss_before_bytes: Option<u64>,
    rss_after_population_bytes: Option<u64>,
    rss_delta_bytes: Option<i64>,
    rss_bytes_per_key: Option<f64>,
    rss_over_logical_payload: Option<f64>,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct MemoryBundle {
    schema_version: u32,
    mode: String,
    generated_at_unix_ms: u64,
    authoritative_performance_result: bool,
    homekv_git_sha: String,
    rustc_version: String,
    os: String,
    kernel: String,
    measurements: Vec<MemoryMeasurement>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config_text = fs::read_to_string(&args.config)
        .with_context(|| format!("failed to read {}", args.config.display()))?;
    let config: BenchConfig = serde_json::from_str(&config_text)
        .with_context(|| format!("invalid benchmark config {}", args.config.display()))?;
    validate_config(&config)?;

    if let Some(worker) = &args.storage_worker {
        let (key_size, value_size, dataset_cardinality) = parse_storage_worker(worker)?;
        let measurement = measure_storage_case(
            config.seed,
            key_size,
            value_size,
            dataset_cardinality,
            args.settle_ms,
        )
        .await?;
        println!("{}", serde_json::to_string(&measurement)?);
        return Ok(());
    }

    let measurements = match config.layer.as_str() {
        "storage" => run_storage_matrix(&args, &config)?,
        "server" => run_server_matrix(&args, &config).await?,
        _ => unreachable!("validated layer"),
    };

    let bundle = MemoryBundle {
        schema_version: 1,
        mode: config.mode,
        generated_at_unix_ms: unix_ms(),
        authoritative_performance_result: false,
        homekv_git_sha: command_output("git", &["rev-parse", "HEAD"])
            .or_else(|| std::env::var("GITHUB_SHA").ok())
            .unwrap_or_else(|| "unknown".to_string()),
        rustc_version: command_output("rustc", &["--version"])
            .unwrap_or_else(|| "unknown".to_string()),
        os: std::env::consts::OS.to_string(),
        kernel: command_output("uname", &["-r"]).unwrap_or_else(|| "unknown".to_string()),
        measurements,
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

fn default_layer() -> String {
    "storage".to_string()
}

fn default_preload_batch_size() -> usize {
    8192
}

fn validate_config(config: &BenchConfig) -> Result<()> {
    if config.schema_version != 1 {
        bail!("unsupported config schema_version {}", config.schema_version);
    }
    if config.mode != "smoke" && config.mode != "baseline" {
        bail!("mode must be 'smoke' or 'baseline'");
    }
    match config.layer.as_str() {
        "storage" => {
            if config.profiles.is_empty() {
                bail!("storage profiles must not be empty");
            }
            if config.dataset_cardinalities.is_empty()
                || config.dataset_cardinalities.iter().any(|&n| n == 0)
            {
                bail!("storage dataset_cardinalities must contain positive values");
            }
        }
        "server" => {
            let server = config
                .server
                .as_ref()
                .context("server configuration is required for server memory probes")?;
            if server.preload_batch_size == 0 || server.cases.is_empty() {
                bail!("server preload_batch_size and cases must be non-empty/positive");
            }
        }
        other => bail!("unsupported benchmark layer '{other}'"),
    }
    Ok(())
}

fn run_storage_matrix(args: &Args, config: &BenchConfig) -> Result<Vec<MemoryMeasurement>> {
    let exe = std::env::current_exe().context("failed to locate hkvmem executable")?;
    let mut measurements = Vec::new();

    for profile in &config.profiles {
        for &dataset_cardinality in &config.dataset_cardinalities {
            let worker = format!(
                "{},{},{}",
                profile.key_size, profile.value_size, dataset_cardinality
            );
            let output = Command::new(&exe)
                .arg("--config")
                .arg(&args.config)
                .arg("--settle-ms")
                .arg(args.settle_ms.to_string())
                .arg("--storage-worker")
                .arg(&worker)
                .output()
                .with_context(|| format!("failed to launch isolated storage worker {worker}"))?;
            if !output.status.success() {
                bail!(
                    "storage worker {worker} failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            let measurement: MemoryMeasurement = serde_json::from_slice(&output.stdout)
                .with_context(|| format!("storage worker {worker} emitted invalid JSON"))?;
            measurements.push(measurement);
        }
    }
    Ok(measurements)
}

async fn measure_storage_case(
    seed: u64,
    key_size: usize,
    value_size: usize,
    dataset_cardinality: usize,
    settle_ms: u64,
) -> Result<MemoryMeasurement> {
    let pid = std::process::id();
    tokio::time::sleep(Duration::from_millis(settle_ms)).await;
    let before = process_rss_bytes(pid);

    let store = Mvcc::new(BTreeStore::new());
    let mut write_txn = store.write().await;
    {
        let inner = write_txn.get_mut();
        for i in 0..dataset_cardinality {
            let key = deterministic_bytes(seed, i as u64, key_size);
            let value = deterministic_bytes(
                seed ^ 0xa5a5_a5a5_a5a5_a5a5,
                i as u64,
                value_size,
            );
            inner.set(&key, value)?;
        }
    }
    write_txn.commit().await;

    tokio::time::sleep(Duration::from_millis(settle_ms)).await;
    std::hint::black_box(&store);
    let after = process_rss_bytes(pid);
    Ok(build_measurement(
        "storage",
        "isolated hkvmem process containing Mvcc<BTreeStore>",
        pid,
        key_size,
        value_size,
        dataset_cardinality,
        before,
        after,
        vec![
            "each storage cell runs in a fresh process to reduce allocator high-water contamination between dataset sizes".to_string(),
            "RSS delta is process-level evidence, not exact heap allocation accounting".to_string(),
            "temporary deterministic key/value generation can affect allocator retention; bytes/key is therefore approximate".to_string(),
        ],
    ))
}

async fn run_server_matrix(args: &Args, config: &BenchConfig) -> Result<Vec<MemoryMeasurement>> {
    let server = config
        .server
        .as_ref()
        .context("validated server configuration")?;
    let homekv_bin = resolve_homekv_bin(args)?;

    let unique_cases = server
        .cases
        .iter()
        .map(|case| (case.key_size, case.value_size, case.dataset_cardinality))
        .collect::<BTreeSet<_>>();

    let mut measurements = Vec::new();
    for (key_size, value_size, dataset_cardinality) in unique_cases {
        measurements.push(
            measure_server_case(
                &homekv_bin,
                config.seed,
                key_size,
                value_size,
                dataset_cardinality,
                server.preload_batch_size,
                args.settle_ms,
            )
            .await?,
        );
    }
    Ok(measurements)
}

async fn measure_server_case(
    homekv_bin: &Path,
    seed: u64,
    key_size: usize,
    value_size: usize,
    dataset_cardinality: usize,
    preload_batch_size: usize,
    settle_ms: u64,
) -> Result<MemoryMeasurement> {
    let port = free_tcp_port()?;
    let gossip_port = free_udp_port()?;
    let endpoint = format!("http://127.0.0.1:{port}");

    let mut child = Command::new(homekv_bin)
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--public_host")
        .arg("127.0.0.1")
        .arg("--gossip_port")
        .arg(gossip_port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to start {}", homekv_bin.display()))?;
    let pid = child.id();

    let result = async {
        let mut client = wait_for_server(&endpoint, &mut child).await?;
        tokio::time::sleep(Duration::from_millis(settle_ms)).await;
        let before = process_rss_bytes(pid);

        preload_server_dataset(
            &mut client,
            seed,
            key_size,
            value_size,
            dataset_cardinality,
            preload_batch_size,
        )
        .await?;

        tokio::time::sleep(Duration::from_millis(settle_ms)).await;
        let after = process_rss_bytes(pid);
        Ok::<_, anyhow::Error>(build_measurement(
            "server",
            "fresh unmodified HomeKV Tonic/Tokio server process",
            pid,
            key_size,
            value_size,
            dataset_cardinality,
            before,
            after,
            vec![
                "each server memory cell starts a fresh HomeKV process; client concurrency is intentionally deduplicated because it does not change the preloaded dataset".to_string(),
                "server stdout/stderr are suppressed only to prevent historical per-request logging from contaminating the measurement driver; server code is unchanged".to_string(),
                "RSS delta includes runtime/storage allocator behavior and is not exact heap allocation accounting".to_string(),
            ],
        ))
    }
    .await;

    let _ = child.kill();
    let _ = child.wait();
    result
}

async fn wait_for_server(
    endpoint: &str,
    child: &mut std::process::Child,
) -> Result<HomeKvServiceClient<Channel>> {
    let mut last_error = None;
    for _ in 0..80 {
        if let Some(status) = child.try_wait().context("failed to inspect HomeKV child")? {
            bail!("HomeKV server exited before becoming ready: {status}");
        }
        match HomeKvServiceClient::connect(endpoint.to_string()).await {
            Ok(client) => return Ok(client),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    bail!(
        "HomeKV server did not become ready at {endpoint}: {:?}",
        last_error
    )
}

async fn preload_server_dataset(
    client: &mut HomeKvServiceClient<Channel>,
    seed: u64,
    key_size: usize,
    value_size: usize,
    dataset_cardinality: usize,
    batch_size: usize,
) -> Result<()> {
    for start in (0..dataset_cardinality).step_by(batch_size) {
        let end = (start + batch_size).min(dataset_cardinality);
        let records = (start..end)
            .map(|idx| Record {
                key: deterministic_key(seed, idx as u64, key_size),
                value: Some(deterministic_bytes(
                    seed ^ 0xa5a5_a5a5_a5a5_a5a5,
                    idx as u64,
                    value_size,
                )),
            })
            .collect();
        let response = client
            .set(SetRequest { records })
            .await
            .context("server memory preload SET RPC failed")?
            .into_inner();
        if !response.succ {
            bail!("server memory preload SET returned succ=false");
        }
    }
    Ok(())
}

fn resolve_homekv_bin(args: &Args) -> Result<PathBuf> {
    if let Some(path) = &args.homekv_bin {
        if path.exists() {
            return Ok(path.clone());
        }
        bail!("--homekv-bin does not exist: {}", path.display());
    }

    let current = std::env::current_exe().context("failed to locate hkvmem executable")?;
    #[cfg(windows)]
    let candidate = current.with_file_name("homekv.exe");
    #[cfg(not(windows))]
    let candidate = current.with_file_name("homekv");
    if !candidate.exists() {
        bail!(
            "HomeKV server binary not found at {}. Build both binaries first or pass --homekv-bin.",
            candidate.display()
        );
    }
    Ok(candidate)
}

fn build_measurement(
    layer: &str,
    target: &str,
    target_pid: u32,
    key_size: usize,
    value_size: usize,
    dataset_cardinality: usize,
    before: Option<u64>,
    after: Option<u64>,
    notes: Vec<String>,
) -> MemoryMeasurement {
    let logical_payload_bytes = logical_payload_bytes(key_size, value_size, dataset_cardinality);
    let delta = rss_delta(before, after);
    let positive_delta = delta.filter(|value| *value > 0).map(|value| value as f64);
    let rss_bytes_per_key = positive_delta.map(|value| value / dataset_cardinality as f64);
    let rss_over_logical_payload = positive_delta.and_then(|value| {
        (logical_payload_bytes > 0).then_some(value / logical_payload_bytes as f64)
    });

    MemoryMeasurement {
        layer: layer.to_string(),
        target: target.to_string(),
        target_pid,
        key_size,
        value_size,
        dataset_cardinality,
        logical_payload_bytes,
        rss_before_bytes: before,
        rss_after_population_bytes: after,
        rss_delta_bytes: delta,
        rss_bytes_per_key,
        rss_over_logical_payload,
        notes,
    }
}

fn logical_payload_bytes(key_size: usize, value_size: usize, count: usize) -> u64 {
    (key_size as u64)
        .saturating_add(value_size as u64)
        .saturating_mul(count as u64)
}

fn rss_delta(before: Option<u64>, after: Option<u64>) -> Option<i64> {
    let before = before? as i128;
    let after = after? as i128;
    i64::try_from(after - before).ok()
}

fn process_rss_bytes(pid: u32) -> Option<u64> {
    linux_kib_field(&PathBuf::from(format!("/proc/{pid}/status")), "VmRSS")
}

fn linux_kib_field(path: &Path, key: &str) -> Option<u64> {
    let text = fs::read_to_string(path).ok()?;
    let value = text.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.trim() == key).then(|| value.trim().to_string())
    })?;
    let number = value.split_whitespace().next()?.parse::<u64>().ok()?;
    number.checked_mul(1024)
}

fn free_tcp_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

fn free_udp_port() -> Result<u16> {
    let socket = UdpSocket::bind("127.0.0.1:0")?;
    Ok(socket.local_addr()?.port())
}

fn parse_storage_worker(value: &str) -> Result<(usize, usize, usize)> {
    let parts = value
        .split(',')
        .map(str::parse::<usize>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("storage worker must be key_size,value_size,cardinality")?;
    if parts.len() != 3 || parts.iter().any(|value| *value == 0) {
        bail!("storage worker must contain three positive integers");
    }
    Ok((parts[0], parts[1], parts[2]))
}

fn deterministic_key(seed: u64, ordinal: u64, len: usize) -> String {
    String::from_utf8(deterministic_bytes(seed, ordinal, len))
        .expect("deterministic benchmark keys are ASCII hex")
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

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
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
    fn worker_case_parser_requires_three_positive_dimensions() {
        assert_eq!(parse_storage_worker("16,64,1000").unwrap(), (16, 64, 1000));
        assert!(parse_storage_worker("16,64").is_err());
        assert!(parse_storage_worker("16,0,1000").is_err());
    }

    #[test]
    fn logical_payload_is_key_plus_value_times_count() {
        assert_eq!(logical_payload_bytes(16, 64, 1000), 80_000);
    }

    #[test]
    fn rss_delta_preserves_negative_measurements_instead_of_fabricating_zero() {
        assert_eq!(rss_delta(Some(4096), Some(8192)), Some(4096));
        assert_eq!(rss_delta(Some(8192), Some(4096)), Some(-4096));
        assert_eq!(rss_delta(None, Some(4096)), None);
    }
}
