use std::sync::atomic::{AtomicUsize, Ordering};
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
use homekv::storage::Store;
use homekv::storage::{BTreeStore, Mvcc};

// GRPC Service
use homekv_service::home_kv_service_server::{HomeKvService, HomeKvServiceServer};
use homekv_service::*;

mod homekv_service {
    tonic::include_proto!("homekv_service");
}

#[derive(Debug)]
pub struct StoreStatus {
    // RelaxedCounter is more suitable for counting metrics
    keys_count: Arc<RelaxedCounter>,
    values_size_in_bytes: Arc<AtomicUsize>,
    cmds_count: Arc<RelaxedCounter>,
}

impl StoreStatus {
    fn new() -> Self {
        StoreStatus {
            keys_count: Arc::new(RelaxedCounter::new(0)),
            values_size_in_bytes: Arc::new(AtomicUsize::new(0)),
            cmds_count: Arc::new(RelaxedCounter::new(0)),
        }
    }
}

pub struct HomeKvServer {
    store: Mvcc<BTreeStore>,
    status: StoreStatus,
    honey_bees: Arc<Mutex<HoneyBees>>,
}

impl HomeKvServer {
    pub fn with_honey_bees(honey_bees: Arc<Mutex<HoneyBees>>) -> Self {
        HomeKvServer {
            store: Mvcc::new(BTreeStore::new()),
            status: StoreStatus::new(),
            honey_bees,
        }
    }
}

#[tonic::async_trait]
impl HomeKvService for HomeKvServer {
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> std::result::Result<Response<GetResponse>, Status> {
        // Increase Command Calling Count Status
        self.status.cmds_count.inc();

        println!("Got a request: {:?}", request);

        let keys = request.into_inner().keys;
        // Vector for storing result records
        let mut records: Vec<Record> = Vec::new();
        // Initialize a Read Transaction, which is a reference of the storage
        let read_txn = self.store.read().await;
        for key in keys {
            if let Ok(value) = read_txn.get(key.as_bytes()) {
                records.push(Record { key, value });
            } else {
                return Err(Status::new(Code::Internal, "Internal Storage Error"));
            }
        }

        Ok(Response::new(GetResponse { records }))
    }

    async fn set(
        &self,
        request: Request<SetRequest>,
    ) -> std::result::Result<Response<SetResponse>, Status> {
        // Increase Command Calling Count Status
        self.status.cmds_count.inc();

        println!("Got a request: {:?}", request);
        let records = request.into_inner().records;

        let mut write_txn = self.store.write().await;
        let mut_store = write_txn.get_mut();

        // Local value of status metrics
        let mut keys_count = 0;
        let mut values_size: i32 = 0;

        for record in records {
            let record_key = record.key.as_bytes();

            if let Some(value) = record.value {
                // Pump up the local keys_count metric for a new key,
                // Decrease the local values_size for an existing key
                match mut_store.get(record_key) {
                    Ok(Some(old_value)) => {
                        values_size -= old_value.len() as i32;
                        println!(
                            "Dealing with an existing key: {:?}, old_value_size: {}",
                            record.key,
                            old_value.len()
                        );
                    }
                    Ok(None) => {
                        println!("Dealing with a non-existing key: {:?}", record.key);
                        keys_count += 1;
                        values_size += value.len() as i32;
                    }
                    Err(_) => return Err(Status::new(Code::Internal, "Internal Storage Error")),
                }

                // Upsert
                match mut_store.set(record_key, value) {
                    Err(_) => return Err(Status::new(Code::Internal, "Internal Storage Error")),
                    Ok(()) => (),
                }
            } else {
                // Decrease keys_count and values_size for removing existing key
                match mut_store.get(record_key) {
                    Ok(Some(old_value)) => {
                        keys_count -= 1;
                        values_size -= old_value.len() as i32;
                    }
                    Ok(None) => (),
                    Err(_) => return Err(Status::new(Code::Internal, "Internal Storage Error")),
                }

                // Delete
                match mut_store.delete(record_key) {
                    Err(_) => return Err(Status::new(Code::Internal, "Internal Storage Error")),
                    Ok(()) => (),
                }
            }
        }

        // Commit
        write_txn.commit().await;
        // Commit Success
        // Update Key Count Status
        self.status.keys_count.add(keys_count);
        // Update Value Size Status
        if values_size < 0 {
            self.status
                .values_size_in_bytes
                .fetch_sub(values_size.abs() as usize, Ordering::Relaxed);
        } else {
            self.status
                .values_size_in_bytes
                .fetch_add(values_size as usize, Ordering::Relaxed);
        }

        Ok(Response::new(SetResponse { succ: true }))
    }

    async fn del(
        &self,
        request: Request<DelRequest>,
    ) -> std::result::Result<Response<DelResponse>, Status> {
        // Increase Command Calling Count Status
        self.status.cmds_count.inc();

        println!("Got a request: {:?}", request);
        let keys = request.into_inner().keys;

        let mut write_txn = self.store.write().await;
        let mut_store = write_txn.get_mut();
        let mut keys_count: i32 = 0;
        let mut values_size: i32 = 0;

        for key in keys {
            let record_key = key.as_bytes();

            // Decrease keys_count and values_size for removing existing key
            match mut_store.get(record_key) {
                Ok(Some(old_value)) => {
                    keys_count -= 1;
                    values_size -= old_value.len() as i32;
                }
                Ok(None) => (),
                Err(_) => return Err(Status::new(Code::Internal, "Internal Storage Error")),
            }

            match mut_store.delete(record_key) {
                Ok(()) => (),
                Err(_) => return Err(Status::new(Code::Internal, "Internal Storage Error")),
            }
        }

        write_txn.commit().await;

        // Commit Success
        self.status.keys_count.add(keys_count as usize);
        self.status
            .values_size_in_bytes
            .fetch_add(values_size as usize, Ordering::Relaxed);

        Ok(Response::new(DelResponse { succ: true }))
    }

    #[allow(unused_variables)]
    async fn metrics(
        &self,
        request: Request<()>,
    ) -> std::result::Result<Response<MetricsResponse>, Status> {
        println!("Got a metrics request");
        Ok(Response::new(MetricsResponse {
            metrics: Some(Metrics {
                keys_count: self.status.keys_count.get() as u32,
                values_size_in_bytes: self.status.values_size_in_bytes.load(Ordering::Relaxed)
                    as u64,
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
