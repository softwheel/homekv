use std::sync::Arc;
use std::time::Duration;

use atomic_counter::{AtomicCounter, RelaxedCounter};
use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use homekv::data_plane::CodecLimits;
use homekv::data_plane_adapter::ShardRequestHandler;
use homekv::data_plane_runtime::{serve_listener, RuntimeLimits};
use homekv::honey_bees::failure_detector::FailureDetectorConfig;
use homekv::honey_bees::server::spawn_gossip;
use homekv::honey_bees::transport::UdpTransport;
use homekv::honey_bees::{GossipConfig, HoneyBee, HoneyBees};
use homekv::storage::ShardStore;

// GRPC Service
use homekv_service::home_kv_service_server::{HomeKvService, HomeKvServiceServer};
use homekv_service::*;

mod homekv_service {
    tonic::include_proto!("homekv_service");
}

#[derive(Debug)]
pub struct StoreStatus {
    // RelaxedCounter is more suitable for counting metrics
    cmds_count: Arc<RelaxedCounter>,
}

impl StoreStatus {
    fn new() -> Self {
        StoreStatus {
            cmds_count: Arc::new(RelaxedCounter::new(0)),
        }
    }
}

pub struct HomeKvServer {
    store: ShardStore,
    status: StoreStatus,
    _honey_bees: Option<Arc<Mutex<HoneyBees>>>,
}

impl HomeKvServer {
    pub fn with_store(store: ShardStore) -> Self {
        HomeKvServer {
            store,
            status: StoreStatus::new(),
            _honey_bees: None,
        }
    }

    pub fn with_store_and_honey_bees(
        store: ShardStore,
        honey_bees: Arc<Mutex<HoneyBees>>,
    ) -> Self {
        HomeKvServer {
            store,
            status: StoreStatus::new(),
            _honey_bees: Some(honey_bees),
        }
    }

    pub fn with_honey_bees(honey_bees: Arc<Mutex<HoneyBees>>) -> Self {
        Self::with_store_and_honey_bees(ShardStore::spawn_default(), honey_bees)
    }

    fn storage_error() -> Status {
        Status::new(Code::Internal, "Internal Storage Error")
    }
}

#[tonic::async_trait]
impl HomeKvService for HomeKvServer {
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> std::result::Result<Response<GetResponse>, Status> {
        self.status.cmds_count.inc();

        println!("Got a request: {:?}", request);
        let keys = request.into_inner().keys;
        let raw_keys: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let values = self
            .store
            .get_many(&raw_keys)
            .await
            .map_err(|_| Self::storage_error())?;
        let records = keys
            .into_iter()
            .zip(values.into_iter())
            .map(|(key, value)| Record { key, value })
            .collect();

        Ok(Response::new(GetResponse { records }))
    }

    async fn set(
        &self,
        request: Request<SetRequest>,
    ) -> std::result::Result<Response<SetResponse>, Status> {
        self.status.cmds_count.inc();

        println!("Got a request: {:?}", request);
        let records = request.into_inner().records;
        let mutations = records
            .into_iter()
            .map(|record| (record.key.into_bytes(), record.value))
            .collect();

        self.store
            .set_many(mutations)
            .await
            .map_err(|_| Self::storage_error())?;

        Ok(Response::new(SetResponse { succ: true }))
    }

    async fn del(
        &self,
        request: Request<DelRequest>,
    ) -> std::result::Result<Response<DelResponse>, Status> {
        self.status.cmds_count.inc();

        println!("Got a request: {:?}", request);
        let keys = request
            .into_inner()
            .keys
            .into_iter()
            .map(String::into_bytes)
            .collect();

        self.store
            .delete_many(keys)
            .await
            .map_err(|_| Self::storage_error())?;

        Ok(Response::new(DelResponse { succ: true }))
    }

    #[allow(unused_variables)]
    async fn metrics(
        &self,
        request: Request<()>,
    ) -> std::result::Result<Response<MetricsResponse>, Status> {
        println!("Got a metrics request");
        let storage = self
            .store
            .metrics()
            .await
            .map_err(|_| Self::storage_error())?;
        Ok(Response::new(MetricsResponse {
            metrics: Some(Metrics {
                keys_count: storage.key_count as u32,
                values_size_in_bytes: storage.logical_bytes as u64,
                cmds_count: self.status.cmds_count.get() as u64,
            }),
        }))
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "HOMEKV Server",
    version,
    about = "Highly Optimized Memory Efficient KV Store"
)]
struct Opt {
    // Defines the server host
    #[arg(long = "host", default_value = "127.0.0.1")]
    host: String,
    // Defines the server port
    #[arg(long = "port", default_value = "20001")]
    port: u32,
    // Defines the compact data-plane bind host.
    #[arg(long = "compact_host", default_value = "127.0.0.1")]
    compact_host: String,
    // Defines the compact data-plane port.
    #[arg(long = "compact_port", default_value = "20003")]
    compact_port: u32,
    #[arg(long = "compact_max_frame", default_value = "8388608")]
    compact_max_frame: usize,
    #[arg(long = "compact_max_key", default_value = "65536")]
    compact_max_key: usize,
    #[arg(long = "compact_max_value", default_value = "4194304")]
    compact_max_value: usize,
    #[arg(long = "compact_max_batch_mutations", default_value = "1024")]
    compact_max_batch_mutations: usize,
    #[arg(long = "compact_max_batch_payload", default_value = "8388608")]
    compact_max_batch_payload: usize,
    #[arg(long = "compact_max_in_flight", default_value = "256")]
    compact_max_in_flight: usize,
    #[arg(long = "compact_response_queue_capacity", default_value = "256")]
    compact_response_queue_capacity: usize,
    // Defines the public host, which other servers will use to
    // reach to this server.
    #[arg(long = "public_host")]
    public_host: String,
    // Defines the gossip port
    #[arg(long = "gossip_port", default_value = "20002")]
    gossip_port: u32,
    // Defines the seed nodes list for gossip
    #[arg(long = "gossip_seeds", default_value = "")]
    gossip_seeds: Vec<String>,
    // Defines the gossip sync interval
    #[arg(long = "gossip_interval", default_value = "500")]
    gossip_interval: u64,
    /// Run the placement-node composition instead of the legacy server:
    /// catalog Raft group, data Raft groups, and per-shard MovementDrivers
    /// with a background drive loop (spec-0006 production wiring).
    #[arg(long = "placement", default_value = "false")]
    placement: bool,
    /// Data directory for the placement node's Raft state.
    #[arg(long = "placement_data_dir", default_value = "./homekv-placement")]
    placement_data_dir: String,
    /// Shards hosted by this placement node, e.g. "0,1,2" or "0-7".
    #[arg(long = "placement_shards", default_value = "0")]
    placement_shards: String,
    /// Node identities hosted by this process, e.g. "1,2,3,4".
    #[arg(long = "placement_node_ids", default_value = "1,2,3,4")]
    placement_node_ids: String,
    /// 16-byte cluster identity committed to the catalog at bootstrap.
    #[arg(long = "placement_cluster_id", default_value = "homekv-place0001")]
    placement_cluster_id: String,
    /// Drive-loop catalog poll interval in milliseconds.
    #[arg(long = "placement_drive_interval_ms", default_value = "1000")]
    placement_drive_interval_ms: u64,
    /// Local-replica reconciliation interval in milliseconds.
    #[arg(long = "placement_reconcile_interval_ms", default_value = "5000")]
    placement_reconcile_interval_ms: u64,
}

fn compact_limits(opt: &Opt) -> Result<(CodecLimits, RuntimeLimits), Box<dyn std::error::Error>> {
    if opt.compact_max_frame == 0
        || opt.compact_max_key == 0
        || opt.compact_max_value == 0
        || opt.compact_max_batch_mutations == 0
        || opt.compact_max_batch_payload == 0
    {
        return Err("compact codec limits must be positive".into());
    }
    let codec_limits = CodecLimits {
        max_frame: opt.compact_max_frame,
        max_key: opt.compact_max_key,
        max_value: opt.compact_max_value,
        max_batch_mutations: opt.compact_max_batch_mutations,
        max_batch_payload: opt.compact_max_batch_payload,
    };
    let runtime_limits = RuntimeLimits {
        max_in_flight: opt.compact_max_in_flight,
        response_queue_capacity: opt.compact_response_queue_capacity,
    }
    .validate()?;
    Ok((codec_limits, runtime_limits))
}

/// Parse a shard list like "0,1,2" or "0-7" into sorted unique shard ids.
fn parse_shards(spec: &str) -> Result<Vec<u16>, Box<dyn std::error::Error>> {
    let mut shards = std::collections::BTreeSet::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((lo, hi)) = part.split_once('-') {
            let lo: u16 = lo.trim().parse()?;
            let hi: u16 = hi.trim().parse()?;
            if lo > hi {
                return Err(format!("invalid shard range {part:?}").into());
            }
            for s in lo..=hi {
                shards.insert(s);
            }
        } else {
            shards.insert(part.parse::<u16>()?);
        }
    }
    if shards.is_empty() {
        return Err("placement_shards is empty".into());
    }
    Ok(shards.into_iter().collect())
}

fn parse_node_ids(spec: &str) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let mut ids = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        ids.push(part.parse::<u64>()?);
    }
    if ids.len() < 3 {
        return Err("placement_node_ids needs at least 3 identities".into());
    }
    Ok(ids)
}

/// `--placement` mode: run the production placement-node composition
/// (catalog group + data groups + movement drivers + drive loop) until
/// Ctrl-C.
async fn run_placement(opt: Opt) -> Result<(), Box<dyn std::error::Error>> {
    use homekv::placement_node::{PlacementNode, PlacementNodeConfig};
    use std::time::Duration;

    let cluster_bytes = opt.placement_cluster_id.as_bytes();
    if cluster_bytes.len() != 16 {
        return Err("placement_cluster_id must be exactly 16 bytes".into());
    }
    let mut cluster_id = [0u8; 16];
    cluster_id.copy_from_slice(cluster_bytes);

    let node_ids = parse_node_ids(&opt.placement_node_ids)?;
    let config = PlacementNodeConfig {
        node_ids: node_ids.clone(),
        catalog_voters: [node_ids[0], node_ids[1], node_ids[2]],
        data_dir: opt.placement_data_dir.into(),
        cluster_id,
        shards: parse_shards(&opt.placement_shards)?,
        drive_interval: Duration::from_millis(opt.placement_drive_interval_ms),
        reconcile_interval: Duration::from_millis(opt.placement_reconcile_interval_ms),
        ..PlacementNodeConfig::default()
    };
    config.validate()?;

    let node = PlacementNode::start(config).await?;
    eprintln!("homekv placement node running; press Ctrl-C to stop");
    // The tokio "signal" feature is not enabled in this workspace; block
    // until killed. All Raft state is disk-backed, so a kill is crash-safe
    // and recovery replays from the log on restart.
    std::future::pending::<()>().await;
    node.shutdown();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::parse();

    if opt.placement {
        return run_placement(opt).await;
    }

    let server_addr = format!("{}:{}", opt.host, opt.port).parse()?;
    let compact_addr: std::net::SocketAddr =
        format!("{}:{}", opt.compact_host, opt.compact_port).parse()?;
    let gossip_addr = format!("{}:{}", opt.public_host, opt.gossip_port).parse()?;
    let (codec_limits, runtime_limits) = compact_limits(&opt)?;

    let node = HoneyBee::new(gossip_addr);
    let config = GossipConfig {
        node,
        cluster_id: "HOMEKV-1".to_string(),
        gossip_interval: Duration::from_millis(opt.gossip_interval),
        listen_addr: gossip_addr,
        seed_nodes: opt.gossip_seeds.clone(),
        failure_detector_config: FailureDetectorConfig::default(),
        is_ready_predicate: None,
    };
    let gossip_handler = spawn_gossip(config, Vec::new(), &UdpTransport).await?;
    let honey_bees = gossip_handler.honey_bees();

    let store = ShardStore::spawn_default();
    let homekv = HomeKvServer::with_store_and_honey_bees(store.clone(), honey_bees);
    let compact_handler = Arc::new(ShardRequestHandler::local(store.clone()));
    let compact_listener = TcpListener::bind(compact_addr).await?;
    let compact_task = tokio::spawn(serve_listener(
        compact_listener,
        compact_handler,
        codec_limits,
        runtime_limits,
    ));

    let grpc_result = Server::builder()
        .add_service(HomeKvServiceServer::new(homekv))
        .serve(server_addr)
        .await;

    compact_task.abort();
    let _ = compact_task.await;
    let shutdown_result = store.shutdown().await;

    grpc_result?;
    shutdown_result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use homekv::data_plane::{Request as CompactRequest, RequestBody, Status as CompactStatus};
    use homekv::data_plane_runtime::RequestHandler;
    use homekv::storage::shard_for_key;

    #[tokio::test]
    async fn grpc_and_compact_handlers_share_one_shard_store() {
        let store = ShardStore::spawn(8);
        let grpc = HomeKvServer::with_store(store.clone());
        let compact = ShardRequestHandler::local(store.clone());
        let key = "shared-protocol-key".to_string();
        let value = b"shared-value".to_vec();

        grpc.set(Request::new(SetRequest {
            records: vec![Record {
                key: key.clone(),
                value: Some(value.clone()),
            }],
        }))
        .await
        .unwrap();

        let compact_get = compact
            .handle(CompactRequest {
                request_id: 1,
                shard_id: shard_for_key(key.as_bytes()).as_u16(),
                body: RequestBody::Get {
                    key: key.as_bytes().to_vec(),
                },
            })
            .await;
        assert_eq!(compact_get.status, CompactStatus::Ok);
        assert_eq!(compact_get.body, value);

        store.shutdown().await.unwrap();
    }

    #[test]
    fn compact_runtime_bounds_reject_response_queue_larger_than_in_flight() {
        let limits = RuntimeLimits {
            max_in_flight: 4,
            response_queue_capacity: 5,
        };
        assert!(limits.validate().is_err());
    }
}
