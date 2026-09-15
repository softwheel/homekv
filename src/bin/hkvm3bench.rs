use anyhow::{bail, Context, Result};
use clap::Parser;
use homekv::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftCommand, RaftNode};
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, TestLinkController, ThreeNodeBootstrap};
use openraft::raft::Raft;
use openraft::{Config, ServerState};
use serde_derive::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Barrier;

#[derive(Debug, Parser)]
#[command(
    name = "hkvm3bench",
    about = "RF=3 quorum-durable, linearizable HomeKV engineering benchmark"
)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, Deserialize)]
struct BenchConfig {
    schema_version: u32,
    mode: String,
    seed: u64,
    key_size: usize,
    value_size: usize,
    dataset_cardinality: usize,
    warmup_operations: usize,
    operations_per_cell: usize,
    concurrencies: Vec<usize>,
    workloads: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct Environment {
    homekv_git_sha: String,
    rustc_version: String,
    os: String,
    arch: String,
    kernel: String,
    cpu_model: String,
    logical_cpus: usize,
    memory_bytes: Option<u64>,
    filesystem: String,
}

#[derive(Debug, Serialize)]
struct Latency {
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
}

#[derive(Debug, Serialize)]
struct CellResult {
    workload: String,
    concurrency: usize,
    key_size: usize,
    value_size: usize,
    dataset_cardinality: usize,
    warmup_operations: usize,
    attempted_operations: usize,
    successful_operations: usize,
    elapsed_ns: u64,
    throughput_ops_sec: f64,
    latency: Latency,
    failures: u64,
}

#[derive(Debug, Serialize)]
struct ResultBundle {
    schema_version: u32,
    mode: String,
    generated_at_unix_ms: u64,
    authoritative_performance_result: bool,
    replication_factor: u8,
    consistency_mode: &'static str,
    durability_mode: &'static str,
    openraft_version: &'static str,
    seed: u64,
    environment: Environment,
    results: Vec<CellResult>,
    notes: Vec<&'static str>,
}

struct Cluster {
    root: PathBuf,
    nodes: BTreeMap<u64, Raft<HomeKvRaftConfig>>,
    state_machines: BTreeMap<u64, HomeKvStateMachine>,
}

impl Cluster {
    async fn start() -> Result<Self> {
        let root = unique_dir();
        fs::create_dir_all(&root)?;
        let bootstrap = ThreeNodeBootstrap::new(
            "homekv-m3-rf3-benchmark",
            [
                BootstrapNode {
                    id: 1,
                    raft_endpoint: "127.0.0.1:19801".into(),
                },
                BootstrapNode {
                    id: 2,
                    raft_endpoint: "127.0.0.1:19802".into(),
                },
                BootstrapNode {
                    id: 3,
                    raft_endpoint: "127.0.0.1:19803".into(),
                },
            ],
        )?;
        let membership = BTreeMap::from([
            (1, RaftNode::new("127.0.0.1:19801")),
            (2, RaftNode::new("127.0.0.1:19802")),
            (3, RaftNode::new("127.0.0.1:19803")),
        ]);
        let config = Arc::new(
            Config {
                cluster_name: "homekv-m3-rf3-benchmark".into(),
                heartbeat_interval: 50,
                election_timeout_min: 500,
                election_timeout_max: 1_000,
                ..Default::default()
            }
            .validate()?,
        );
        let links = TestLinkController::default();
        let mut factories = BTreeMap::new();
        let mut nodes = BTreeMap::new();
        let mut state_machines = BTreeMap::new();
        for id in 1..=3 {
            let factory =
                HomeKvRaftNetworkFactory::new(id, bootstrap.clone(), 64, links.clone())?;
            let store = HomeKvRaftLogStore::open(root.join(format!("node-{id}.raft")))?;
            let sm = HomeKvStateMachine::default();
            let raft = Raft::new(
                id,
                config.clone(),
                factory.clone(),
                store,
                sm.clone(),
            )
            .await?;
            factories.insert(id, factory);
            nodes.insert(id, raft);
            state_machines.insert(id, sm);
        }
        for factory in factories.values() {
            for (id, raft) in &nodes {
                factory.register_handler(*id, Arc::new(raft.clone()))?;
            }
        }
        nodes[&1].initialize(membership).await?;
        Ok(Self {
            root,
            nodes,
            state_machines,
        })
    }

    async fn leader(&self) -> Result<u64> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let snapshots: Vec<_> = self
                .nodes
                .iter()
                .map(|(id, raft)| (*id, raft.metrics().borrow().clone()))
                .collect();
            let leaders: Vec<_> = snapshots
                .iter()
                .filter_map(|(id, metrics)| {
                    (metrics.state == ServerState::Leader).then_some(*id)
                })
                .collect();
            let known: BTreeSet<_> = snapshots
                .iter()
                .filter_map(|(_, metrics)| metrics.current_leader)
                .collect();
            if leaders.len() == 1 && known == BTreeSet::from([leaders[0]]) {
                let candidate = leaders[0];
                tokio::time::sleep(Duration::from_millis(500)).await;
                let settled: Vec<_> = self
                    .nodes
                    .iter()
                    .map(|(id, raft)| (*id, raft.metrics().borrow().clone()))
                    .collect();
                let still_leader = settled.iter().any(|(id, metrics)| {
                    *id == candidate && metrics.state == ServerState::Leader
                });
                let settled_known: BTreeSet<_> = settled
                    .iter()
                    .filter_map(|(_, metrics)| metrics.current_leader)
                    .collect();
                if still_leader && settled_known == BTreeSet::from([candidate]) {
                    self.nodes[&candidate].ensure_linearizable().await?;
                    return Ok(candidate);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("three-node benchmark cluster did not converge: {snapshots:?}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn shutdown(self) -> Result<()> {
        for raft in self.nodes.values() {
            tokio::time::timeout(Duration::from_secs(5), raft.shutdown())
                .await
                .context("benchmark cluster shutdown timed out")??;
        }
        fs::remove_dir_all(self.root)?;
        Ok(())
    }
}

#[derive(Default)]
struct WorkerResult {
    samples: Vec<u64>,
    failures: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let raw = fs::read_to_string(&args.config)
        .with_context(|| format!("failed to read {}", args.config.display()))?;
    let config: BenchConfig = serde_json::from_str(&raw)
        .with_context(|| format!("invalid benchmark config {}", args.config.display()))?;
    validate(&config)?;

    let cluster = Cluster::start().await?;
    let leader = cluster.leader().await?;
    let raft = cluster.nodes[&leader].clone();
    let sm = cluster.state_machines[&leader].clone();
    preload(&config, &raft).await?;

    let mut results = Vec::new();
    for concurrency in &config.concurrencies {
        for workload in &config.workloads {
            results.push(
                run_cell(&config, workload, *concurrency, raft.clone(), sm.clone()).await?,
            );
        }
    }
    cluster.shutdown().await?;

    let total_failures: u64 = results.iter().map(|result| result.failures).sum();
    let bundle = ResultBundle {
        schema_version: 1,
        mode: config.mode.clone(),
        generated_at_unix_ms: unix_ms(),
        authoritative_performance_result: false,
        replication_factor: 3,
        consistency_mode: "quorum-backed-linearizable",
        durability_mode: "quorum-durable-plus-leader-apply",
        openraft_version: "0.9.25",
        seed: config.seed,
        environment: environment(),
        results,
        notes: vec![
            "Engineering/regression characterization only; not a public release performance claim.",
            "GET and the read side of 80/20 traverse OpenRaft ensure_linearizable before state access.",
            "SET/DELETE success is returned only by OpenRaft client_write after quorum durability and leader apply.",
            "DELETE targets deterministic absent keys to preserve dataset cardinality across cells.",
        ],
    };
    let encoded = serde_json::to_string_pretty(&bundle)?;
    if let Some(parent) = args.output.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(&args.output, &encoded)
        .with_context(|| format!("failed to write {}", args.output.display()))?;
    println!("{encoded}");
    if total_failures != 0 {
        bail!("benchmark run recorded {total_failures} operation failures");
    }
    Ok(())
}

fn validate(config: &BenchConfig) -> Result<()> {
    if config.schema_version != 1 {
        bail!("unsupported schema_version {}", config.schema_version);
    }
    if config.mode != "engineering" {
        bail!("mode must be 'engineering'");
    }
    if config.key_size < 8
        || config.value_size == 0
        || config.dataset_cardinality == 0
        || config.warmup_operations == 0
        || config.operations_per_cell < 100
    {
        bail!("invalid payload, dataset, warmup, or operation count");
    }
    if config.concurrencies.len() < 2
        || config.concurrencies.iter().any(|value| *value == 0)
        || !config.concurrencies.iter().any(|value| *value == 1)
        || !config.concurrencies.iter().any(|value| *value > 1)
    {
        bail!("concurrencies must include low (1) and moderate (>1) levels");
    }
    let required = ["get", "set", "delete", "read80_write20"];
    if config.workloads.len() != required.len()
        || required
            .iter()
            .any(|required| !config.workloads.iter().any(|actual| actual == required))
    {
        bail!("workloads must contain exactly get, set, delete, and read80_write20");
    }
    Ok(())
}

async fn preload(config: &BenchConfig, raft: &Raft<HomeKvRaftConfig>) -> Result<()> {
    for index in 0..config.dataset_cardinality {
        raft.client_write(RaftCommand::Set {
            key: bytes(0x4b, index, config.key_size),
            value: bytes(0x56, index ^ config.seed as usize, config.value_size),
        })
        .await?;
    }
    Ok(())
}

async fn run_cell(
    config: &BenchConfig,
    workload: &str,
    concurrency: usize,
    raft: Raft<HomeKvRaftConfig>,
    sm: HomeKvStateMachine,
) -> Result<CellResult> {
    for index in 0..config.warmup_operations {
        perform(config, workload, index, &raft, &sm).await?;
    }

    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = Vec::new();
    let per_worker = config.operations_per_cell / concurrency;
    let remainder = config.operations_per_cell % concurrency;
    for worker in 0..concurrency {
        let count = per_worker + usize::from(worker < remainder);
        let start_index = worker * per_worker + worker.min(remainder);
        let barrier = barrier.clone();
        let raft = raft.clone();
        let sm = sm.clone();
        let workload = workload.to_string();
        let owned = OwnedConfig {
            seed: config.seed,
            key_size: config.key_size,
            value_size: config.value_size,
            dataset_cardinality: config.dataset_cardinality,
        };
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut result = WorkerResult::default();
            for offset in 0..count {
                let started = Instant::now();
                match perform_owned(&owned, &workload, start_index + offset, &raft, &sm).await {
                    Ok(()) => result.samples.push(nanos(started.elapsed())),
                    Err(_) => result.failures += 1,
                }
            }
            result
        }));
    }

    let started = Instant::now();
    barrier.wait().await;
    let mut samples = Vec::with_capacity(config.operations_per_cell);
    let mut failures = 0;
    for task in tasks {
        let worker = task.await.context("benchmark worker panicked")?;
        samples.extend(worker.samples);
        failures += worker.failures;
    }
    let elapsed = started.elapsed();
    samples.sort_unstable();
    let successful = samples.len();
    let elapsed_ns = nanos(elapsed);
    Ok(CellResult {
        workload: workload.to_string(),
        concurrency,
        key_size: config.key_size,
        value_size: config.value_size,
        dataset_cardinality: config.dataset_cardinality,
        warmup_operations: config.warmup_operations,
        attempted_operations: config.operations_per_cell,
        successful_operations: successful,
        elapsed_ns,
        throughput_ops_sec: if elapsed_ns == 0 {
            0.0
        } else {
            successful as f64 * 1_000_000_000.0 / elapsed_ns as f64
        },
        latency: Latency {
            p50_ns: percentile(&samples, 50),
            p95_ns: percentile(&samples, 95),
            p99_ns: percentile(&samples, 99),
        },
        failures,
    })
}

struct OwnedConfig {
    seed: u64,
    key_size: usize,
    value_size: usize,
    dataset_cardinality: usize,
}

async fn perform(
    config: &BenchConfig,
    workload: &str,
    index: usize,
    raft: &Raft<HomeKvRaftConfig>,
    sm: &HomeKvStateMachine,
) -> Result<()> {
    perform_owned(
        &OwnedConfig {
            seed: config.seed,
            key_size: config.key_size,
            value_size: config.value_size,
            dataset_cardinality: config.dataset_cardinality,
        },
        workload,
        index,
        raft,
        sm,
    )
    .await
}

async fn perform_owned(
    config: &OwnedConfig,
    workload: &str,
    index: usize,
    raft: &Raft<HomeKvRaftConfig>,
    sm: &HomeKvStateMachine,
) -> Result<()> {
    let key_index = index % config.dataset_cardinality;
    let key = bytes(0x4b, key_index, config.key_size);
    match workload {
        "get" => {
            raft.ensure_linearizable().await?;
            let _ = sm.get(&key).await;
        }
        "set" => {
            raft.client_write(RaftCommand::Set {
                key,
                value: bytes(
                    0x56,
                    index ^ config.seed as usize,
                    config.value_size,
                ),
            })
            .await?;
        }
        "delete" => {
            raft.client_write(RaftCommand::Delete {
                key: bytes(
                    0x44,
                    key_index + config.dataset_cardinality,
                    config.key_size,
                ),
            })
            .await?;
        }
        "read80_write20" if index % 5 == 0 => {
            raft.client_write(RaftCommand::Set {
                key,
                value: bytes(
                    0x4d,
                    index ^ config.seed as usize,
                    config.value_size,
                ),
            })
            .await?;
        }
        "read80_write20" => {
            raft.ensure_linearizable().await?;
            let _ = sm.get(&key).await;
        }
        other => bail!("unsupported workload {other}"),
    }
    Ok(())
}

fn bytes(prefix: u8, index: usize, size: usize) -> Vec<u8> {
    let mut result = vec![prefix; size];
    let encoded = (index as u64).to_be_bytes();
    let count = encoded.len().min(size);
    result[size - count..].copy_from_slice(&encoded[encoded.len() - count..]);
    result
}

fn percentile(samples: &[u64], percent: usize) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let rank = (samples.len() * percent).div_ceil(100);
    samples[rank.saturating_sub(1)]
}

fn nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn unique_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "homekv-m3-rf3-benchmark-{}-{nonce}",
        std::process::id()
    ))
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn command(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .unwrap_or_else(|| "unavailable".to_string())
}

fn environment() -> Environment {
    let cpu_model = fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("model name\t: ").map(str::to_string))
        })
        .unwrap_or_else(|| "unavailable".to_string());
    let memory_bytes = fs::read_to_string("/proc/meminfo").ok().and_then(|text| {
        text.lines().find_map(|line| {
            line.strip_prefix("MemTotal:")
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|kb| kb.parse::<u64>().ok())
                .map(|kb| kb * 1024)
        })
    });
    Environment {
        homekv_git_sha: command("git", &["rev-parse", "HEAD"]),
        rustc_version: command("rustc", &["--version", "--verbose"]),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        kernel: command("uname", &["-a"]),
        cpu_model,
        logical_cpus: std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(0),
        memory_bytes,
        filesystem: command("stat", &["-f", "-c", "%T", "."]),
    }
}
