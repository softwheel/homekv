use std::sync::Arc;
use std::time::Duration;

use atomic_counter::{AtomicCounter, RelaxedCounter};
use structopt::StructOpt;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

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
    honey_bees: Arc<Mutex<HoneyBees>>,
}

impl HomeKvServer {
    pub fn with_honey_bees(honey_bees: Arc<Mutex<HoneyBees>>) -> Self {
        HomeKvServer {
            store: ShardStore::spawn_default(),
            status: StoreStatus::new(),
            honey_bees,
        }
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();
    let server_addr = format!("{}:{}", opt.host, opt.port).parse()?;
    let gossip_addr = format!("{}:{}", opt.public_host, opt.gossip_port).parse()?;
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
    let homekv = HomeKvServer::with_honey_bees(honey_bees);

    Server::builder()
        .add_service(HomeKvServiceServer::new(homekv))
        .serve(server_addr)
        .await?;

    Ok(())
}
