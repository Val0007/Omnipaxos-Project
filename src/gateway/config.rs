// src/gateway_config.rs
use std::env;

use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GatewayConfig {
    /// HTTP listen address, e.g. "127.0.0.1:8080"
    pub listen_addr: String,

    /// TCP addresses of KV servers, e.g. ["127.0.0.1:5001", "127.0.0.1:5002", ...]
    pub server_addrs: Vec<String>,

    /// Timeout for waiting for a decided response
    pub request_timeout_ms: u64,
}

impl GatewayConfig {
    pub fn new() -> Result<Self, ConfigError> {
        //wrt where its being run -src
        let gateway_config_file = "../build_scripts/gateway.toml";

        let cfg = Config::builder() 
            .add_source(File::with_name(&gateway_config_file))
            // Optional env overrides (prefix GATEWAY_)
            // - GATEWAY_LISTEN_ADDR
            // - GATEWAY_SERVER_ADDRS="host:port,host:port"
            // - GATEWAY_REQUEST_TIMEOUT_MS
            .add_source(
                Environment::with_prefix("GATEWAY")
                    .try_parsing(true)
                    .list_separator(",")
                    .with_list_parse_key("server_addrs"),
            )
            .build()?;

        cfg.try_deserialize()
    }
}