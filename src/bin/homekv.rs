use std::sync::Arc;
use std::time::Duration;

use atomic_counter::{AtomicCounter, RelaxedCounter};
use structopt::StructOpt;
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

#[derive(Debug, StructOpt)]
#[structopt(
    name = "HOMEKV Server",
    about = "Highly Optimized Memory Efficient KV Store"
)]
struct Opt {
    // Defines the server host
    #[structopt(long = "host", default_value = "127.0.0.1")]
    host: String,
    // Defines the server port
    #[structopt(long = "port", default_value = "20001")]
    port: u32,
    // Defines the compact data-plane bind host.
    #[structopt(long = "compact_host", default_value = "127.0.0.1")]
    compact_host: String,
    // Defines the compact data-plane port.
    #[structopt(long = "compact_port", default_value = "20003")]
    compact_port: u32,
    #[structopt(long = "compact_max_frame", default_value = "8388608")]
    compact_max_frame: usize,
    #[structopt(long = "compact_max_key", default_value = "65536")]
    compact_max_key: usize,
    #[structopt(long = "compact_max_value", default_value = "4194304")]
    compact_max_value: usize,
    #[structopt(long = "compact_max_batch_mutations", default_value = "1024")]
    compact_max_batch_mutations: usize,
    #[structopt(long = "compact_max_batch_payload", default_value = "8388608")]
    compact_max_batch_payload: usize,
    #[structopt(long = "compact_max_in_flight", default_value = "256")]
    compact_max_in_flight: usize,
    #[structopt(long = "compact_response_queue_capacity", default_value = "256")]
    compact_response_queue_capacity: usize,
    // Defines the public host, which other servers will use to
    // reach to this server.
    #[structopt(long = "public_host")]
    public_host: String,
    // Defines the gossip port
    #[structopt(long = "gossip_port", default_value = "20002")]
    gossip_port: u32,
    // Defines the seed nodes list for gossip
    #[structopt(long = "gossip_seeds", default_value = "")]
    gossip_seeds: Vec<String>,
    // Defines the gossip sync interval
    #[structopt(long = "gossip_interval", default_value = "500")]
    gossip_interval: u64,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();
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
