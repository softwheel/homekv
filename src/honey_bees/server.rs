use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::lookup_host;

use rand::prelude::*;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time;
use tracing::{debug, error, info, warn};

use super::message::GossipMessage;
use super::transport::{Socket, Transport};
use super::{Command, GossipConfig, GossipHandle, HoneyBees};

/// Number of nodes picked for random gossip.
const GOSSIP_COUNT: usize = 3;
/// DNS Refreshing Interval.
const DNS_POLLING_DURATION: Duration = Duration::from_secs(60);

async fn dns_refresh_loop(
    seed_hosts_requiring_dns: HashSet<String>,
    seed_addrs_not_requiring_resolution: HashSet<SocketAddr>,
    seed_addrs_tx: watch::Sender<HashSet<SocketAddr>>,
) {
    let mut interval = time::interval(DNS_POLLING_DURATION);
    // We actually do not want to run the polling loop right away, hence this tick.
    interval.tick().await;
    while seed_addrs_tx.receiver_count() > 0 {
        interval.tick().await;
        let mut seed_addrs = seed_addrs_not_requiring_resolution.clone();
        for seed_host in &seed_hosts_requiring_dns {
            resolve_seed_host(seed_host, &mut seed_addrs).await;
        }
        if seed_addrs_tx.send(seed_addrs).is_err() {
            return;
        }
    }
}

async fn resolve_seed_host(seed_host: &str, seed_addrs: &mut HashSet<SocketAddr>) {
    if let Ok(resolved_seed_addrs) = lookup_host(seed_host).await {
        for seed_addr in resolved_seed_addrs {
            if seed_addrs.contains(&seed_addr) {
                continue;
            }
            debug!(seed_host=seed_host, seed_addr=%seed_addr, "seed-addr-from_dns");
            seed_addrs.insert(seed_addr);
        }
    } else {
        warn!(seed_host=%seed_host, "Failed to lookup host");
    }
}

/// The seed nodes address can be string representing a socket addr directly, or hostname:port.
/// The latter is especially important when relying on a headless service in k8s or when using
/// DNS in general. In that case, we do not want to perform the resolution once and forall.
///
/// We want to periodically retry DNS resolution, in order to avoid having a split cluster.
///
/// The newcomers are supposed to chime in too, so there is no need to refresh it too often,
/// especially if it is not empty.
async fn spawn_dns_refresh_loop(seeds: &[String]) -> watch::Receiver<HashSet<SocketAddr>> {
    let mut seed_addrs_not_requiring_resolution: HashSet<SocketAddr> = Default::default();
    let mut first_round_seed_resolution: HashSet<SocketAddr> = Default::default();
    let mut seed_requiring_dns: HashSet<String> = Default::default();
    for seed in seeds {
        if let Ok(seed_addr) = seed.parse() {
            seed_addrs_not_requiring_resolution.insert(seed_addr);
        } else {
            seed_requiring_dns.insert(seed.clone());
            resolve_seed_host(seed, &mut first_round_seed_resolution).await;
        }
    }

    let initial_seed_addrs: HashSet<SocketAddr> = seed_addrs_not_requiring_resolution
        .union(&first_round_seed_resolution)
        .cloned()
        .collect();

    info!(initial_seed_addrs=?initial_seed_addrs);

    let (seed_addrs_tx, seed_addrs_rx) = watch::channel(initial_seed_addrs);
    if !seed_requiring_dns.is_empty() {
        tokio::task::spawn(dns_refresh_loop(
            seed_requiring_dns,
            seed_addrs_not_requiring_resolution,
            seed_addrs_tx,
        ));
    }
    seed_addrs_rx
}

pub async fn spawn_gossip(
    config: GossipConfig,
    initial_key_values: Vec<(String, String)>,
    transport: &dyn Transport,
) -> anyhow::Result<GossipHandle> {
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let seed_addrs: watch::Receiver<HashSet<SocketAddr>> =
        spawn_dns_refresh_loop(&config.seed_nodes).await;
    let socket = transport.open(config.listen_addr).await?;
    let node = config.node.clone();
    let gossiper = HoneyBees::with_node_id_and_seeds(config, seed_addrs, initial_key_values);
    let gossiper_arc = Arc::new(Mutex::new(gossiper));
    let gossiper_arc_clone = gossiper_arc.clone();
    let join_handle = tokio::spawn(async move {
        Server::new(command_rx, gossiper_arc_clone, socket)
            .await
            .run()
            .await
    });

    Ok(GossipHandle {
        node,
        command_tx,
        honey_bees: gossiper_arc,
        join_handle,
    })
}

struct Server {
    command_rx: UnboundedReceiver<Command>,
    gossiper: Arc<Mutex<HoneyBees>>,
    transport: Box<dyn Socket>,
    rng: SmallRng,
}

impl Server {
    async fn new(
        command_rx: UnboundedReceiver<Command>,
        gossiper: Arc<Mutex<HoneyBees>>,
        transport: Box<dyn Socket>,
    ) -> Self {
        let rng = SmallRng::from_rng(thread_rng()).expect("Failed to seed random generator");
        Self {
            gossiper,
            command_rx,
            transport,
            rng,
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let gossip_interval = self.gossiper.lock().await.config.gossip_interval;
        let mut gossip_interval = time::interval(gossip_interval);
        loop {
            tokio::select! {
                result = self.transport.recv() => match result {
                    Ok((from_addr, message)) => {
                        let _ = self.handle_message(from_addr, message).await;
                    }
                    Err(err) => return Err(err),
                },
                _ = gossip_interval.tick() => {
                    self.gossip_multiple().await;
                },
                command = self.command_rx.recv() => match command {
                    Some(Command::Gossip(addr)) => {
                        let _ = self.gossip(addr).await;
                    },
                    Some(Command::Shutdown) | None => break,
                }
            }
        }
        Ok(())
    }

    /// Process a single UDP packet.
    async fn handle_message(
        &mut self,
        from_addr: SocketAddr,
        message: GossipMessage,
    ) -> anyhow::Result<()> {
        let response = self.gossiper.lock().await.process_message(message);
        if let Some(message) = response {
            self.transport.send(from_addr, message).await?;
        }
        Ok(())
    }

    /// Gossip to multiple randomly chosen nodes.
    async fn gossip_multiple(&mut self) {
        let mut gossiper_guard = self.gossiper.lock().await;
        let cluster_state = gossiper_guard.cluster_state();

        let peer_nodes = cluster_state
            .nodes()
            .filter(|node| *node != gossiper_guard.self_node())
            .map(|node| node.gossip_address)
            .collect::<HashSet<_>>();
        let live_nodes = gossiper_guard
            .live_nodes()
            .map(|node| node.gossip_address)
            .collect::<HashSet<_>>();
        let dead_nodes = gossiper_guard
            .dead_nodes()
            .map(|node| node.gossip_address)
            .collect::<HashSet<_>>();
        let seed_nodes: HashSet<SocketAddr> = gossiper_guard.seed_addrs();
        let (selected_nodes, random_dead_node_opt, random_seed_node_opt) = select_gossip_nodes(
            &mut self.rng,
            peer_nodes,
            live_nodes,
            dead_nodes,
            seed_nodes,
        );

        gossiper_guard.update_heartbeat();

        // Drop lock to prevent deadlock in [`UdpSocket::gossip`]
        drop(gossiper_guard);

        for node in selected_nodes {
            let result = self.gossip(node).await;
            if result.is_err() {
                error!(node = ?node, "Gossip error with a live node.");
            }
        }

        if let Some(random_dead_node) = random_dead_node_opt {
            let result = self.gossip(random_dead_node).await;
            if result.is_err() {
                error!(node = ?random_dead_node, "Gossip error with a dead node.")
            }
        }

        if let Some(random_seed_node) = random_seed_node_opt {
            let result = self.gossip(random_seed_node).await;
            if result.is_err() {
                error!(node = ?random_seed_node, "Gossip error with a seed node.")
            }
        }

        // Update node liveliness
        let mut gossiper_guard = self.gossiper.lock().await;
        gossiper_guard.update_nodes_liveness();
    }

    async fn gossip(&mut self, addr: SocketAddr) -> anyhow::Result<()> {
        let syn = self.gossiper.lock().await.create_syn_message();
        self.transport.send(addr, syn).await?;
        Ok(())
    }
}

fn select_gossip_nodes<R>(
    rng: &mut R,
    peer_nodes: HashSet<SocketAddr>,
    live_nodes: HashSet<SocketAddr>,
    dead_nodes: HashSet<SocketAddr>,
    seed_nodes: HashSet<SocketAddr>,
) -> (Vec<SocketAddr>, Option<SocketAddr>, Option<SocketAddr>)
where
    R: Rng + ?Sized,
{
    let live_nodes_c = live_nodes.len();
    let dead_nodes_c = dead_nodes.len();

    // Select `GOSSIP_COUNT` number of live nodes.
    // On startup, select from cluster nodes since we don't know any live node yet.
    let nodes = if live_nodes_c == 0 {
        peer_nodes
    } else {
        live_nodes
    }
    .iter()
    .cloned()
    .choose_multiple(rng, GOSSIP_COUNT);

    // gossip with seed
    let mut has_g_w_seed = false;
    for node_id in &nodes {
        if seed_nodes.contains(node_id) {
            has_g_w_seed = true;
            break;
        }
    }

    let dead_node: Option<SocketAddr> =
        select_random_node(rng, &dead_nodes, live_nodes_c, dead_nodes_c);
    // Prevent from network partition caused by the count of seeds.
    // see https://issues.apache.org/jira/browse/CASSANDRA-150
    let seed_node: Option<SocketAddr> = if !has_g_w_seed || live_nodes_c < seed_nodes.len() {
        select_random_node(rng, &seed_nodes, live_nodes_c, dead_nodes_c)
    } else {
        None
    };

    (nodes, dead_node, seed_node)
}

fn select_random_node<R>(
    rng: &mut R,
    nodes: &HashSet<SocketAddr>,
    live_nodes_count: usize,
    dead_nodes_count: usize,
) -> Option<SocketAddr>
where
    R: Rng + ?Sized,
{
    let p = dead_nodes_count as f64 / (live_nodes_count + 1) as f64;
    if p > rng.gen::<f64>() {
        return nodes.iter().choose(rng).cloned();
    }
    None
}
