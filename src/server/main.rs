use crate::{configs::OmniPaxosKVConfig, server::OmniPaxosServer};
use env_logger;

mod configs;
mod database;
mod network;
mod server;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let server_config = match OmniPaxosKVConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    // println!("{:?}",server_config);
    let mut server = OmniPaxosServer::new(server_config).await;
    server.run().await;
}

// OmniPaxosKVConfig { local: LocalConfig { location: Some("local-1"), server_id: 1, listen_address: "127.0.0.1", listen_port: 8001, num_clients: 1, output_filepath: "./logs/server-1.json" },
//  cluster: ClusterConfig { nodes: [1, 2, 3], node_addrs: ["127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"], initial_leader: 1, initial_flexible_quorum: 
//  Some(FlexibleQuorum { read_quorum_size: 2, write_quorum_size: 2 }) } }