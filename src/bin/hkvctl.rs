use homekv_service::home_kv_service_client::HomeKvServiceClient;
use homekv_service::*;

use clap::{Parser, Subcommand};
use tonic::transport::Channel;

pub mod homekv_service {
    tonic::include_proto!("homekv_service");
}

pub struct HomeKvClient {}

impl HomeKvClient {
    pub async fn get(
        &self,
        conn: &mut HomeKvServiceClient<Channel>,
        keys: Vec<String>,
    ) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
        let request = tonic::Request::new(GetRequest { keys });

        println!("Sending request to gRPC Server...");
        let response = conn.get(request).await?;

        Ok(response.into_inner().records)
    }

    pub async fn set(
        &self,
        conn: &mut HomeKvServiceClient<Channel>,
        kvs: Vec<String>,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        println!("Setting kvs: {:?}", kvs);
        // FixMe: kvs pattern needs to be verified.
        let records = kvs
            .iter()
            .map(|kv| kv.split("=").collect::<Vec<_>>())
            .map(|kv_vec| Record {
                key: kv_vec[0].into(),
                value: Some(kv_vec[1].into()),
            })
            .collect();
        let request = tonic::Request::new(SetRequest { records: records });

        println!("Sending request to gRPC Server...");
        let response = conn.set(request).await?;
        Ok(response.into_inner().succ)
    }

    pub async fn del(
        &self,
        conn: &mut HomeKvServiceClient<Channel>,
        keys: Vec<String>,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let request = tonic::Request::new(DelRequest { keys });

        println!("Sending request to gRPC Server...");
        let response = conn.del(request).await?;
        Ok(response.into_inner().succ)
    }

    pub async fn metrics(
        &self,
        conn: &mut HomeKvServiceClient<Channel>,
    ) -> Result<Metrics, Box<dyn std::error::Error>> {
        let request = tonic::Request::new(());

        println!("Sending request to gRPC Server...");
        let response = conn.metrics(request).await?;
        Ok(response.into_inner().metrics.unwrap())
    }
}

#[derive(Subcommand)]
enum CMD {
    GET { keys: Option<Vec<String>> },
    SET { kvs: Option<Vec<String>> },
    DEL { keys: Option<Vec<String>> },
    METRICS,
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Args {
    /// Server Host
    #[clap(short, long, default_value_t= String::from("127.0.0.1"))]
    host: String,

    /// Server Port
    /// FixMe: Add a CONST for default port
    #[clap(short, long, default_value_t = 20001)]
    port: usize,

    /// Command: get, set, del
    #[command(subcommand)]
    cmd: CMD,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut conn =
        HomeKvServiceClient::connect(format!("http://{}:{}", args.host, args.port)).await?;

    let home_kv_client = HomeKvClient {};

    println!("querying...");
    match args.cmd {
        CMD::GET { keys } => {
            let keys = keys.expect("get command must have a list of keys!");
            println!("get keys: {:?}", &keys);

            match home_kv_client.get(&mut conn, keys).await {
                Ok(records) => {
                    for record in records {
                        match record.value {
                            Some(value) => {
                                let value = String::from_utf8(value).unwrap();
                                println!("key={}, value={}\n", record.key, value);
                            }
                            None => {
                                // No such key!
                                println!("key={}, value=None\n", record.key);
                            }
                        }
                        println!()
                    }
                }
                Err(e) => println!("Get runs in error: {:?}", e),
            }
        }
        CMD::SET { kvs } => {
            let kvs = kvs.expect("set command must have a list of key-value pairs");
            println!("set kvs: {:?}", &kvs);

            match home_kv_client.set(&mut conn, kvs).await {
                Ok(succ) => println!("Set SUCCESS? {}", succ),
                Err(e) => println!("Set runs in error: {:?}", e),
            }
        }
        CMD::DEL { keys } => {
            let keys = keys.expect("del command must have a list of keys!");
            println!("del keys: {:?}", &keys);

            match home_kv_client.del(&mut conn, keys).await {
                Ok(succ) => println!("Del SUCCESS? {}", succ),
                Err(e) => println!("Del runs in error: {:?}", e),
            }
        }
        CMD::METRICS => match home_kv_client.metrics(&mut conn).await {
            Ok(metrics) => println!("Metrics: {:?}", metrics),
            Err(e) => println!("Metrics runs in error: {:?}", e),
        },
    }
    Ok(())
}
