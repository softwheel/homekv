// hkvm4bench: many-group RF=3 engineering benchmark.
//
// Stands up G independent Raft groups (RF=3, real OpenRaft consensus over the
// in-process test transport), elects a leader per group, then runs workload
// cells (idle / uniform / skew x get / set / delete / 80-20 x concurrencies)
// plus fault-isolation cells (unavailable group, delayed group, membership
// movement on one group while the rest serve).
//
// Writes use `client_write` (quorum-durable + leader apply). Reads use
// `ensure_linearizable` before state access (quorum-backed linearizable).
// This is an engineering/regression characterization, not a public release
// performance claim (REQ-M4-PERF-005).

use anyhow::{bail, Context, Result};
use clap::Parser;
use homekv::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftCommand, RaftNode};
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, LinkRule, TestLinkController, ThreeNodeBootstrap};
use openraft::raft::Raft;
use openraft::{Config, ServerState, SnapshotPolicy};
use serde_derive::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Barrier, Semaphore};

#[derive(Debug, Parser)]
#[command(
    name = "hkvm4bench",
    about = "Many-group RF=3 quorum-durable, linearizable HomeKV engineering benchmark"
)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    output: PathBuf,
    /// Number of Raft groups (1, 64, 256, 1024).
    #[arg(long)]
    groups: usize,
    /// "probe" (stand up, elect, smoke ops, report) or "bench" (full cells).
    #[arg(long, default_value = "bench")]
    mode: String,
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
    selections: Vec<String>,
    #[serde(default = "default_true")]
    fault_cells: bool,
    #[serde(default = "default_idle_secs")]
    idle_secs: u64,
}

fn default_true() -> bool {
    true
}

fn default_idle_secs() -> u64 {
    5
}

// ---------------------------------------------------------------------------
// Environment identity
// ---------------------------------------------------------------------------

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
}

fn environment() -> Environment {
    let git_sha = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let rustc_version = [
        "rustc",
        &format!(
            "{}/.cargo/bin/rustc",
            std::env::var("HOME").unwrap_or_default()
        ),
    ]
    .into_iter()
    .filter_map(|bin| {
        Command::new(bin)
            .arg("--version")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
    })
    .next()
    .map(|s| s.trim().to_string())
    .unwrap_or_else(|| "unknown".to_string());
    let cpu_model = fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|line| line.starts_with("model name"))
                .and_then(|line| line.split(':').nth(1))
                .map(|s| s.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());
    let memory_bytes = fs::read_to_string("/proc/meminfo").ok().and_then(|text| {
        text.lines()
            .find(|line| line.starts_with("MemTotal:"))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|kb| kb.parse::<u64>().ok())
            .map(|kb| kb * 1024)
    });
    let kernel = Command::new("uname")
        .arg("-r")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    Environment {
        homekv_git_sha: git_sha,
        rustc_version,
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        kernel,
        cpu_model,
        logical_cpus: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        memory_bytes,
    }
}

/// Current process RSS in bytes (Linux).
fn rss_bytes() -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|line| line.starts_with("VmRSS:"))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|kb| kb.parse::<u64>().ok())
                .map(|kb| kb * 1024)
        })
}

fn tokio_workers() -> usize {
    tokio::runtime::Handle::try_current()
        .map(|handle| handle.metrics().num_workers())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Deterministic PRNG (xorshift64*) — no extra dependencies.
// ---------------------------------------------------------------------------

struct XorShift64(u64);

impl XorShift64 {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn bytes(prefix: u8, index: usize, size: usize) -> Vec<u8> {
    let mut out = vec![prefix; size.max(8)];
    out[1..9].copy_from_slice(&(index as u64).to_le_bytes());
    out.truncate(size);
    out
}

fn nanos(duration: Duration) -> u64 {
    duration.as_secs() * 1_000_000_000 + u64::from(duration.subsec_nanos())
}

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (pct / 100.0 * sorted.len() as f64).ceil() as usize;
    sorted[rank.max(1).min(sorted.len()) - 1]
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct Latency {
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
}

#[derive(Debug, Serialize)]
struct CellResult {
    cell: String,
    workload: String,
    selection: String,
    concurrency: usize,
    groups: usize,
    attempted_operations: usize,
    successful_operations: usize,
    failures: u64,
    elapsed_ns: u64,
    throughput_ops_sec: f64,
    latency: Latency,
}

#[derive(Debug, Serialize)]
struct FaultCellResult {
    cell: String,
    description: String,
    healthy_groups_ops: usize,
    healthy_groups_failures: u64,
    healthy_groups_p99_ns: u64,
    affected_group_ops: usize,
    affected_group_failures: u64,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ResourceSample {
    label: String,
    rss_bytes: Option<u64>,
    tokio_workers: usize,
    groups: usize,
    raft_instances: usize,
}

#[derive(Debug, Serialize)]
struct ResultBundle {
    schema_version: u32,
    mode: String,
    generated_at_unix_ms: u64,
    authoritative_performance_result: bool,
    groups: usize,
    replication_factor: u32,
    consistency_mode: &'static str,
    durability_mode: &'static str,
    openraft_version: &'static str,
    seed: u64,
    environment: Environment,
    resource_samples: Vec<ResourceSample>,
    cells: Vec<CellResult>,
    fault_cells: Vec<FaultCellResult>,
    notes: Vec<String>,
}

// ---------------------------------------------------------------------------
// Many-group cluster
// ---------------------------------------------------------------------------

type GroupRaft = Raft<HomeKvRaftConfig>;

struct Group {
    nodes: BTreeMap<u64, GroupRaft>,
    state_machines: BTreeMap<u64, HomeKvStateMachine>,
    links: TestLinkController,
    leader: u64,
}

struct ManyGroupCluster {
    root: PathBuf,
    groups: Vec<Group>,
}

impl ManyGroupCluster {
    async fn start(group_count: usize, root: PathBuf) -> Result<Self> {
        fs::create_dir_all(&root)?;
        // Cap concurrent Raft construction to bound memory spikes.
        let semaphore = Arc::new(Semaphore::new(64));
        let mut tasks = Vec::with_capacity(group_count);
        for group_index in 0..group_count {
            let permit = semaphore.clone().acquire_owned().await?;
            let group_root = root.join(format!("group-{group_index:04}"));
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                build_group(group_index, group_root).await
            }));
        }
        let mut groups = Vec::with_capacity(group_count);
        for task in tasks {
            groups.push(task.await??);
        }
        // Initialize each group's membership on its node 1. The member
        // addresses must match the bootstrap endpoints exactly (the network
        // layer rejects RPCs on endpoint mismatch).
        for (group_index, group) in groups.iter().enumerate() {
            let membership = BTreeMap::from([
                (1, RaftNode::new(group_endpoint(group_index, 1))),
                (2, RaftNode::new(group_endpoint(group_index, 2))),
                (3, RaftNode::new(group_endpoint(group_index, 3))),
            ]);
            group.nodes[&1].initialize(membership).await?;
        }
        Ok(Self { root, groups })
    }

    /// Wait until every group has exactly one stable leader known to all
    /// three members. Returns per-group leaders.
    async fn wait_for_leaders(&mut self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let mut debug_next = Instant::now();
        // Poll all groups; check convergence in rounds to avoid O(G^2) work.
        loop {
            let mut pending = 0;
            for group in self.groups.iter_mut() {
                if group.leader != 0 {
                    continue;
                }
                let snapshots: Vec<(u64, ServerState, Option<u64>)> = group
                    .nodes
                    .iter()
                    .map(|(id, raft)| {
                        let m = raft.metrics().borrow().clone();
                        (*id, m.state, m.current_leader)
                    })
                    .collect();
                let leaders: Vec<u64> = snapshots
                    .iter()
                    .filter(|(_, state, _)| *state == ServerState::Leader)
                    .map(|(id, _, _)| *id)
                    .collect();
                let known: BTreeMap<u64, usize> = {
                    let mut counts = BTreeMap::new();
                    for (_, _, leader) in &snapshots {
                        if let Some(id) = leader {
                            *counts.entry(*id).or_insert(0) += 1;
                        }
                    }
                    counts
                };
                if leaders.len() == 1 && known.get(&leaders[0]) == Some(&3) {
                    group.leader = leaders[0];
                } else {
                    pending += 1;
                }
            }
            if pending > 0 && Instant::now() >= debug_next {
                eprintln!(
                    "elect: {}/{} groups converged",
                    self.groups.len() - pending,
                    self.groups.len()
                );
                debug_next = Instant::now() + Duration::from_secs(5);
            }
            if pending == 0 {
                // Settle: confirm leaders are still leaders (no churn).
                tokio::time::sleep(Duration::from_millis(500)).await;
                let mut churned = 0;
                for group in self.groups.iter_mut() {
                    let m = group.nodes[&group.leader].metrics().borrow().clone();
                    if m.state != ServerState::Leader {
                        group.leader = 0;
                        churned += 1;
                    }
                }
                if churned == 0 {
                    // Linearizability check on every leader before serving.
                    // Parallelize: 1024 sequential quorum reads are too slow.
                    let semaphore = Arc::new(Semaphore::new(64));
                    let mut tasks = Vec::new();
                    for group in &self.groups {
                        let permit = semaphore.clone().acquire_owned().await?;
                        let raft = group.nodes[&group.leader].clone();
                        tasks.push(tokio::spawn(async move {
                            let _permit = permit;
                            raft.ensure_linearizable().await
                        }));
                    }
                    for task in tasks {
                        task.await.context("linearizability check panicked")??;
                    }
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                bail!(
                    "{pending} of {} groups did not converge on a leader",
                    self.groups.len()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Reset leader tracking and re-converge. Used before fault cells to
    /// establish a known-good baseline after intentional disruption.
    async fn refresh_leaders(&mut self, timeout: Duration) -> Result<()> {
        for group in self.groups.iter_mut() {
            group.leader = 0;
        }
        self.wait_for_leaders(timeout).await
    }

    fn leader_handles(&self) -> Vec<(GroupRaft, HomeKvStateMachine)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.nodes[&group.leader].clone(),
                    group.state_machines[&group.leader].clone(),
                )
            })
            .collect()
    }

    /// A few quorum writes + linearizable reads per group to prove every
    /// group serves before the benchmark cells run. Parallelized across groups.
    async fn smoke(&self, seed: u64) -> Result<(usize, u64)> {
        let semaphore = Arc::new(Semaphore::new(64));
        let mut tasks = Vec::new();
        for (index, group) in self.groups.iter().enumerate() {
            let permit = semaphore.clone().acquire_owned().await?;
            let raft = group.nodes[&group.leader].clone();
            let sm = group.state_machines[&group.leader].clone();
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                let mut ops = 0usize;
                let mut failures = 0u64;
                for round in 0..4 {
                    let key = bytes(0x4b, index * 16 + round, 16);
                    let value = bytes(0x56, (seed as usize) ^ index ^ round, 32);
                    match raft
                        .client_write(RaftCommand::Set {
                            key: key.clone(),
                            value,
                        })
                        .await
                    {
                        Ok(_) => ops += 1,
                        Err(error) => {
                            failures += 1;
                            eprintln!(
                                "smoke write failed: group={index} round={round} error={error:#}"
                            );
                        }
                    }
                    match raft.ensure_linearizable().await {
                        Ok(_) => {
                            ops += 1;
                            let _ = sm.get(&key).await;
                        }
                        Err(error) => {
                            failures += 1;
                            eprintln!(
                                "smoke read failed: group={index} round={round} error={error:#}"
                            );
                        }
                    }
                }
                (ops, failures)
            }));
        }
        let mut total_ops = 0usize;
        let mut total_failures = 0u64;
        for task in tasks {
            let (ops, failures) = task.await.context("smoke task panicked")?;
            total_ops += ops;
            total_failures += failures;
        }
        Ok((total_ops, total_failures))
    }

    async fn shutdown(self) -> Result<()> {
        // Best-effort shutdown: 3,072 Raft instances shutting down at once
        // can trigger election storms; bound the total time and never fail
        // the benchmark on cleanup. The temp dir is removed regardless and
        // the process exits afterwards.
        let mut tasks = Vec::new();
        for group in self.groups {
            // Shut down followers before the leader to minimize elections.
            let mut node_ids: Vec<u64> = group.nodes.keys().copied().collect();
            node_ids.sort_by_key(|id| usize::from(*id == group.leader));
            for id in node_ids {
                let raft = group.nodes[&id].clone();
                tasks.push(tokio::spawn(async move {
                    let _ = tokio::time::timeout(Duration::from_secs(5), raft.shutdown()).await;
                }));
            }
        }
        let _ = tokio::time::timeout(Duration::from_secs(60), async {
            for task in tasks {
                let _ = task.await;
            }
        })
        .await;
        fs::remove_dir_all(&self.root)?;
        Ok(())
    }
}

fn group_endpoint(group_index: usize, id: u64) -> String {
    format!("127.0.0.1:{}", 21_000 + group_index * 3 + id as usize)
}

async fn build_group(group_index: usize, root: PathBuf) -> Result<Group> {
    fs::create_dir_all(&root)?;
    let cluster_name = format!("homekv-m4-group-{group_index:04}");
    // Endpoints are unique strings; the test transport never dials them.
    let bootstrap = ThreeNodeBootstrap::new(
        cluster_name.clone(),
        [1u64, 2, 3].map(|id| BootstrapNode {
            id,
            raft_endpoint: group_endpoint(group_index, id),
        }),
    )?;
    let config = Arc::new(
        Config {
            cluster_name,
            heartbeat_interval: 100,
            election_timeout_min: 5_000,
            election_timeout_max: 10_000,
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()?,
    );
    let links = TestLinkController::default();
    let mut factories = BTreeMap::new();
    let mut nodes = BTreeMap::new();
    let mut state_machines = BTreeMap::new();
    for id in 1..=3u64 {
        let factory = HomeKvRaftNetworkFactory::new(id, bootstrap.clone(), 64, links.clone())?;
        let store = HomeKvRaftLogStore::open(root.join(format!("node-{id}.raft")))?;
        let sm = HomeKvStateMachine::default();
        let raft = GroupRaft::new(id, config.clone(), factory.clone(), store, sm.clone()).await?;
        factories.insert(id, factory);
        nodes.insert(id, raft);
        state_machines.insert(id, sm);
    }
    for factory in factories.values() {
        for (id, raft) in &nodes {
            factory.register_handler(*id, Arc::new(raft.clone()))?;
        }
    }
    Ok(Group {
        nodes,
        state_machines,
        links,
        leader: 0,
    })
}

// ---------------------------------------------------------------------------
// Workload cells
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct OwnedWorkload {
    seed: u64,
    key_size: usize,
    value_size: usize,
    dataset_cardinality: usize,
}

async fn perform(
    config: &OwnedWorkload,
    workload: &str,
    index: usize,
    raft: &GroupRaft,
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
                value: bytes(0x56, index ^ config.seed as usize, config.value_size),
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
        "read80_write20" if index.is_multiple_of(5) => {
            raft.client_write(RaftCommand::Set {
                key,
                value: bytes(0x4d, index ^ config.seed as usize, config.value_size),
            })
            .await?;
        }
        "read80_write20" => {
            raft.ensure_linearizable().await?;
            let _ = sm.get(&key).await;
        }
        other => bail!("unknown workload {other}"),
    }
    Ok(())
}

/// Deterministic group picker: uniform round-robin, or skew where 80% of
/// operations land on the hottest 20% of groups.
fn pick_group(selection: &str, index: usize, group_count: usize, rng: &mut XorShift64) -> usize {
    match selection {
        "skew" => {
            let hot = (group_count.max(5) / 5).max(1);
            if rng.below(100) < 80 {
                rng.below(hot)
            } else {
                hot + rng.below(group_count - hot)
            }
        }
        _ => index % group_count,
    }
}

#[derive(Default)]
struct WorkerResult {
    samples: Vec<u64>,
    failures: u64,
}

#[allow(clippy::too_many_arguments)]
async fn run_cell(
    config: &BenchConfig,
    cell: &str,
    workload: &str,
    selection: &str,
    concurrency: usize,
    leaders: &[(GroupRaft, HomeKvStateMachine)],
    skip_groups: &[usize],
) -> Result<CellResult> {
    let eligible: Vec<usize> = (0..leaders.len())
        .filter(|index| !skip_groups.contains(index))
        .collect();
    if eligible.is_empty() {
        bail!("no eligible groups for cell {cell}");
    }
    let owned = OwnedWorkload {
        seed: config.seed,
        key_size: config.key_size,
        value_size: config.value_size,
        dataset_cardinality: config.dataset_cardinality,
    };
    // Warmup (serial, spread across eligible groups).
    let mut warm_rng = XorShift64(config.seed ^ 0x9E37_79B9_7F4A_7C15);
    for index in 0..config.warmup_operations {
        let slot = pick_group(selection, index, eligible.len(), &mut warm_rng);
        let group_index = eligible[slot];
        let (raft, sm) = &leaders[group_index];
        perform(&owned, workload, index, raft, sm)
            .await
            .with_context(|| {
                format!("warmup failed: cell={cell} group={group_index} index={index}")
            })?;
    }

    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = Vec::new();
    let per_worker = config.operations_per_cell / concurrency;
    let remainder = config.operations_per_cell % concurrency;
    for worker in 0..concurrency {
        let count = per_worker + usize::from(worker < remainder);
        let start_index = worker * per_worker + worker.min(remainder);
        let barrier = barrier.clone();
        let owned = owned.clone();
        let workload = workload.to_string();
        let selection = selection.to_string();
        let cell_name = cell.to_string();
        let leader_clones = leaders.to_vec();
        let eligible = eligible.clone();
        tasks.push(tokio::spawn(async move {
            let mut rng = XorShift64(owned.seed ^ (worker as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            barrier.wait().await;
            let mut result = WorkerResult::default();
            for offset in 0..count {
                let index = start_index + offset;
                let slot = pick_group(&selection, index, eligible.len(), &mut rng);
                let group_index = eligible[slot];
                let (raft, sm) = &leader_clones[group_index];
                let started = Instant::now();
                match perform(&owned, &workload, index, raft, sm).await {
                    Ok(()) => result.samples.push(nanos(started.elapsed())),
                    Err(error) => {
                        result.failures += 1;
                        eprintln!(
                            "cell={cell_name} workload={workload} group={group_index} index={index} error={error:#}"
                        );
                    }
                }
            }
            result
        }));
    }

    let started = Instant::now();
    barrier.wait().await;
    let mut samples = Vec::with_capacity(config.operations_per_cell);
    let mut failures = 0u64;
    for task in tasks {
        let worker = task.await.context("cell worker panicked")?;
        samples.extend(worker.samples);
        failures += worker.failures;
    }
    let elapsed = started.elapsed();
    samples.sort_unstable();
    let successful = samples.len();
    let elapsed_ns = nanos(elapsed);
    Ok(CellResult {
        cell: cell.to_string(),
        workload: workload.to_string(),
        selection: selection.to_string(),
        concurrency,
        groups: eligible.len(),
        attempted_operations: config.operations_per_cell,
        successful_operations: successful,
        failures,
        elapsed_ns,
        throughput_ops_sec: if elapsed_ns == 0 {
            0.0
        } else {
            successful as f64 * 1_000_000_000.0 / elapsed_ns as f64
        },
        latency: Latency {
            p50_ns: percentile(&samples, 50.0),
            p95_ns: percentile(&samples, 95.0),
            p99_ns: percentile(&samples, 99.0),
            p999_ns: percentile(&samples, 99.9),
        },
    })
}

// ---------------------------------------------------------------------------
// Fault-isolation cells (REQ-M4-PERF-004)
// ---------------------------------------------------------------------------

/// Partition one group (all links dropped) and prove the remaining groups
/// keep serving with zero unexpected failures.
async fn fault_cell_unavailable(
    config: &BenchConfig,
    cluster: &ManyGroupCluster,
    leaders: &[(GroupRaft, HomeKvStateMachine)],
) -> Result<FaultCellResult> {
    if cluster.groups.len() < 2 {
        bail!("unavailable-group cell needs at least 2 groups");
    }
    let victim = 0usize;
    let victim_links = &cluster.groups[victim].links;
    for from in 1..=3u64 {
        for to in 1..=3u64 {
            if from != to {
                victim_links.set_rule(from, to, LinkRule::Drop);
            }
        }
    }
    let skip = [victim];
    let healthy = run_cell(
        config,
        "fault-unavailable",
        "read80_write20",
        "uniform",
        4,
        leaders,
        &skip,
    )
    .await?;
    // Direct probes at the partitioned group must fail (expected), and only there.
    let (victim_raft, _) = &leaders[victim];
    let mut victim_failures = 0u64;
    let mut victim_ops = 0usize;
    for index in 0..20 {
        victim_ops += 1;
        let attempt = tokio::time::timeout(
            Duration::from_secs(3),
            victim_raft.client_write(RaftCommand::Set {
                key: bytes(0x4b, index, 16),
                value: bytes(0x56, index, 16),
            }),
        )
        .await;
        match attempt {
            Ok(Ok(_)) => {}
            _ => victim_failures += 1,
        }
    }
    for from in 1..=3u64 {
        for to in 1..=3u64 {
            if from != to {
                victim_links.set_rule(from, to, LinkRule::Pass);
            }
        }
    }
    Ok(FaultCellResult {
        cell: "fault-unavailable-group".to_string(),
        description: "group 0 fully partitioned (links dropped) while remaining groups serve read80_write20".to_string(),
        healthy_groups_ops: healthy.successful_operations,
        healthy_groups_failures: healthy.failures,
        healthy_groups_p99_ns: healthy.latency.p99_ns,
        affected_group_ops: victim_ops,
        affected_group_failures: victim_failures,
        notes: vec![
            format!("healthy groups: {} (expected 0 unexpected failures)", healthy.failures),
            format!("partitioned group 0: {victim_failures}/{victim_ops} ops failed (expected: quorum lost)"),
        ],
    })
}

/// Delay one group's links and prove the remaining groups' tail latency is
/// unaffected.
async fn fault_cell_delayed(
    config: &BenchConfig,
    cluster: &ManyGroupCluster,
    leaders: &[(GroupRaft, HomeKvStateMachine)],
) -> Result<FaultCellResult> {
    if cluster.groups.len() < 2 {
        bail!("delayed-group cell needs at least 2 groups");
    }
    let victim = 1usize.min(cluster.groups.len() - 1);
    let victim_links = &cluster.groups[victim].links;
    for from in 1..=3u64 {
        for to in 1..=3u64 {
            if from != to {
                victim_links.set_rule(from, to, LinkRule::Delay(Duration::from_millis(250)));
            }
        }
    }
    let skip = [victim];
    let healthy = run_cell(config, "fault-delayed", "get", "uniform", 4, leaders, &skip).await?;
    for from in 1..=3u64 {
        for to in 1..=3u64 {
            if from != to {
                victim_links.set_rule(from, to, LinkRule::Pass);
            }
        }
    }
    Ok(FaultCellResult {
        cell: "fault-delayed-group".to_string(),
        description: format!(
            "group {victim} links delayed 250ms while remaining groups serve linearizable gets"
        ),
        healthy_groups_ops: healthy.successful_operations,
        healthy_groups_failures: healthy.failures,
        healthy_groups_p99_ns: healthy.latency.p99_ns,
        affected_group_ops: 0,
        affected_group_failures: 0,
        notes: vec![format!(
            "healthy groups p99 = {} ms with one group delayed 250ms (isolation holds if p99 stays near baseline)",
            healthy.latency.p99_ns / 1_000_000
        )],
    })
}

/// Run a real membership movement (3 -> 2 -> 3 voters) on one group while the
/// rest serve, proving unrelated groups keep making progress.
async fn fault_cell_moving(
    config: &BenchConfig,
    cluster: &ManyGroupCluster,
    leaders: &[(GroupRaft, HomeKvStateMachine)],
) -> Result<FaultCellResult> {
    if cluster.groups.len() < 2 {
        bail!("moving-group cell needs at least 2 groups");
    }
    let victim = 2usize.min(cluster.groups.len() - 1);
    let (victim_raft, _) = &leaders[victim];
    let mut notes = Vec::new();
    let shrink: BTreeSet<u64> = BTreeSet::from([1, 2]);
    let restore: BTreeSet<u64> = BTreeSet::from([1, 2, 3]);
    let movement = tokio::spawn({
        let raft = victim_raft.clone();
        let shrink = shrink.clone();
        let restore = restore.clone();
        async move {
            raft.change_membership(shrink, true).await?;
            tokio::time::sleep(Duration::from_secs(2)).await;
            raft.change_membership(restore, true).await?;
            anyhow::Ok(())
        }
    });
    let skip = [victim];
    let healthy = run_cell(
        config,
        "fault-moving",
        "read80_write20",
        "uniform",
        4,
        leaders,
        &skip,
    )
    .await?;
    match movement.await {
        Ok(Ok(())) => {
            notes.push("membership movement 3->2->3 on victim group completed".to_string())
        }
        Ok(Err(error)) => notes.push(format!("membership movement failed: {error:#}")),
        Err(error) => notes.push(format!("membership movement task panicked: {error}")),
    }
    Ok(FaultCellResult {
        cell: "fault-moving-group".to_string(),
        description: format!(
            "group {victim} undergoes membership movement 3->2->3 while remaining groups serve"
        ),
        healthy_groups_ops: healthy.successful_operations,
        healthy_groups_failures: healthy.failures,
        healthy_groups_p99_ns: healthy.latency.p99_ns,
        affected_group_ops: 0,
        affected_group_failures: 0,
        notes,
    })
}

// ---------------------------------------------------------------------------
// Config validation + preload + main
// ---------------------------------------------------------------------------

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
        || config.concurrencies.contains(&0)
        || !config.concurrencies.contains(&1)
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
    let selections = ["uniform", "skew"];
    if config.selections.len() != selections.len()
        || selections
            .iter()
            .any(|required| !config.selections.iter().any(|actual| actual == required))
    {
        bail!("selections must contain exactly uniform and skew");
    }
    Ok(())
}

/// Preload a small dataset per group (concurrent across groups).
async fn preload(config: &BenchConfig, leaders: &[(GroupRaft, HomeKvStateMachine)]) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(32));
    let mut tasks = Vec::new();
    for (group_index, (raft, _)) in leaders.iter().enumerate() {
        let permit = semaphore.clone().acquire_owned().await?;
        let raft = raft.clone();
        let config_owned = (
            config.seed,
            config.key_size,
            config.value_size,
            config.dataset_cardinality,
        );
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            for index in 0..config_owned.3 {
                raft.client_write(RaftCommand::Set {
                    key: bytes(0x4b, index, config_owned.1),
                    value: bytes(0x56, index ^ config_owned.0 as usize, config_owned.2),
                })
                .await
                .with_context(|| format!("preload failed on group {group_index} key {index}"))?;
            }
            anyhow::Ok(())
        }));
    }
    for task in tasks {
        task.await??;
    }
    Ok(())
}

fn resource_sample(label: &str, groups: usize, samples: &mut Vec<ResourceSample>) {
    samples.push(ResourceSample {
        label: label.to_string(),
        rss_bytes: rss_bytes(),
        tokio_workers: tokio_workers(),
        groups,
        raft_instances: groups * 3,
    });
}

fn write_bundle(bundle: &ResultBundle, output: &PathBuf) -> Result<()> {
    let encoded = serde_json::to_string_pretty(bundle)?;
    if let Some(parent) = output.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(output, &encoded).with_context(|| format!("failed to write {}", output.display()))?;
    println!("{encoded}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.groups == 0 || args.groups > 2048 {
        bail!("--groups must be in 1..=2048");
    }
    if args.mode != "probe" && args.mode != "bench" {
        bail!("--mode must be 'probe' or 'bench'");
    }
    let raw = fs::read_to_string(&args.config)
        .with_context(|| format!("failed to read {}", args.config.display()))?;
    let config: BenchConfig = serde_json::from_str(&raw)?;
    validate(&config)?;

    let environment = environment();
    let mut resource_samples = Vec::new();
    let mut notes = vec![
        "Writes use openraft client_write (quorum-durable, leader-applied).".to_string(),
        "Reads use ensure_linearizable before state access (quorum-backed linearizable)."
            .to_string(),
        "Each group is an independent 3-voter Raft cluster with ids {1,2,3} per M3 bootstrap contract."
            .to_string(),
        "Results are engineering/regression characterization only, not public release performance claims (REQ-M4-PERF-005)."
            .to_string(),
    ];

    resource_sample("process-start", args.groups, &mut resource_samples);
    let root = std::env::temp_dir().join(format!("hkvm4bench-{}-{}", args.groups, unix_ms()));
    let cluster_start = Instant::now();
    let mut cluster = ManyGroupCluster::start(args.groups, root).await?;
    resource_sample("cluster-built", args.groups, &mut resource_samples);
    notes.push(format!(
        "cluster build: {} groups ({} raft instances) in {:.1}s",
        args.groups,
        args.groups * 3,
        cluster_start.elapsed().as_secs_f64()
    ));

    let elect_start = Instant::now();
    cluster.wait_for_leaders(Duration::from_secs(300)).await?;
    resource_sample("leaders-elected", args.groups, &mut resource_samples);
    notes.push(format!(
        "leader election converged for {} groups in {:.1}s",
        args.groups,
        elect_start.elapsed().as_secs_f64()
    ));

    let leaders = cluster.leader_handles();
    let (smoke_ops, smoke_failures) = cluster.smoke(config.seed).await?;
    notes.push(format!(
        "smoke: {smoke_ops} quorum writes + linearizable reads across {} groups, {smoke_failures} failures",
        args.groups
    ));
    if smoke_failures != 0 {
        bail!("smoke recorded {smoke_failures} failures");
    }

    let mut cells = Vec::new();
    let mut fault_cells = Vec::new();

    if args.mode == "bench" {
        preload(&config, &leaders).await?;
        resource_sample("preloaded", args.groups, &mut resource_samples);

        // Idle cell: no workload, verify leaders stay stable and sample RSS.
        let idle_leaders: Vec<u64> = cluster.groups.iter().map(|group| group.leader).collect();
        tokio::time::sleep(Duration::from_secs(config.idle_secs)).await;
        let mut idle_churn = 0;
        for (group, previous) in cluster.groups.iter().zip(idle_leaders.iter()) {
            let current = group.nodes[previous].metrics().borrow().clone();
            if current.state != ServerState::Leader {
                idle_churn += 1;
            }
        }
        resource_sample("idle-complete", args.groups, &mut resource_samples);
        notes.push(format!(
            "idle cell ({}s, no workload): {idle_churn} leader changes across {} groups",
            config.idle_secs, args.groups
        ));

        let empty: [usize; 0] = [];
        for selection in &config.selections {
            for workload in &config.workloads {
                for concurrency in &config.concurrencies {
                    let cell = format!("{selection}-{workload}-c{concurrency}");
                    let result = run_cell(
                        &config,
                        &cell,
                        workload,
                        selection,
                        *concurrency,
                        &leaders,
                        &empty,
                    )
                    .await?;
                    println!(
                        "cell={cell} ok={}/{} failures={} throughput={:.0} ops/s p50={:.2}ms p99={:.2}ms",
                        result.successful_operations,
                        result.attempted_operations,
                        result.failures,
                        result.throughput_ops_sec,
                        result.latency.p50_ns as f64 / 1_000_000.0,
                        result.latency.p99_ns as f64 / 1_000_000.0,
                    );
                    cells.push(result);
                }
            }
        }
        resource_sample(
            "workload-cells-complete",
            args.groups,
            &mut resource_samples,
        );

        if config.fault_cells {
            // Re-establish a known-good leader baseline: the workload cells
            // ran for minutes and the fault cells intentionally disrupt the
            // cluster, so stale leader handles must not be used.
            cluster.refresh_leaders(Duration::from_secs(120)).await?;
            let leaders = cluster.leader_handles();
            fault_cells.push(fault_cell_unavailable(&config, &cluster, &leaders).await?);
            cluster.refresh_leaders(Duration::from_secs(120)).await?;
            let leaders = cluster.leader_handles();
            fault_cells.push(fault_cell_delayed(&config, &cluster, &leaders).await?);
            cluster.refresh_leaders(Duration::from_secs(120)).await?;
            let leaders = cluster.leader_handles();
            fault_cells.push(fault_cell_moving(&config, &cluster, &leaders).await?);
            resource_sample("fault-cells-complete", args.groups, &mut resource_samples);
        }
    }

    let bundle = ResultBundle {
        schema_version: 1,
        mode: args.mode.clone(),
        generated_at_unix_ms: unix_ms(),
        authoritative_performance_result: false,
        groups: args.groups,
        replication_factor: 3,
        consistency_mode:
            "linearizable reads via ensure_linearizable; quorum-durable writes via client_write",
        durability_mode: "quorum replication; disk-backed per-node log store",
        openraft_version: "0.9 (pinned via Cargo.lock)",
        seed: config.seed,
        environment,
        resource_samples,
        cells,
        fault_cells,
        notes,
    };
    write_bundle(&bundle, &args.output)?;

    let total_failures: u64 = bundle.cells.iter().map(|cell| cell.failures).sum::<u64>()
        + bundle
            .fault_cells
            .iter()
            .map(|cell| cell.healthy_groups_failures)
            .sum::<u64>();
    cluster.shutdown().await?;
    if total_failures != 0 {
        bail!("benchmark run recorded {total_failures} unexpected operation failures");
    }
    Ok(())
}
