mod config;

use crate::config::GatewayConfig;
use std::{collections::HashMap, env, sync::Arc, time::Duration};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use omnipaxos_kv::common::{
    kv::{CommandId, KVCommand},
    messages::{ClientMessage, RegistrationMessage, ServerMessage},
    utils::{frame_clients_connection, frame_registration_connection},
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex},
    time,
};

const NETWORK_BATCH_SIZE: usize = 1024;
const RETRY_SERVER_CONNECTION_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct AppState {
    cmd_id_counter: Arc<std::sync::atomic::AtomicUsize>,
    pending: Arc<Mutex<HashMap<CommandId, oneshot::Sender<ServerMessage>>>>,
    tcp_tx: mpsc::Sender<ClientMessage>,
    request_timeout: Duration,
}

#[derive(Deserialize)]
struct PutRequest {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct DeleteRequest {
    key: String,
}

#[tokio::main]
async fn main() {
    env_logger::init();

let cfg = GatewayConfig::new().expect("Failed to load gateway config");

let listen_addr = cfg.listen_addr.clone();
let server_addrs = cfg.server_addrs.clone();
let request_timeout_ms = cfg.request_timeout_ms;


    if server_addrs.is_empty() {
        panic!("GATEWAY_SERVER_ADDRS must contain at least one address");
    }
println!("GATEWAY START (before connect)");
    let (reader, writer) = connect_and_register(&server_addrs).await;
    println!("GATEWAY CONNECTED (after connect)");
    let (tcp_tx, tcp_rx) = mpsc::channel(NETWORK_BATCH_SIZE);
    let pending: Arc<Mutex<HashMap<CommandId, oneshot::Sender<ServerMessage>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    tokio::spawn(writer_task(writer, tcp_rx));
    tokio::spawn(reader_task(reader, pending.clone()));

    let state = Arc::new(AppState {
        cmd_id_counter: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        pending,
        tcp_tx,
        request_timeout: Duration::from_millis(request_timeout_ms),
    });

    let app = Router::new()
        .route("/kv/put", post(put_handler))
        .route("/kv/delete", post(delete_handler))
        .route("/kv/get/:key", get(get_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .unwrap_or_else(|e| panic!("Failed binding gateway HTTP listener at {listen_addr}: {e}"));

println!("GATEWAY LISTENING on {}", listen_addr);
    axum::serve(listener, app)
        .await
        .unwrap_or_else(|e| panic!("Gateway HTTP server failed: {e}"));
}

async fn connect_and_register(
    server_addrs: &[String],
) -> (
    omnipaxos_kv::common::utils::FromServerConnection,
    omnipaxos_kv::common::utils::ToServerConnection,
) {
    loop {
        for addr in server_addrs {
            match TcpStream::connect(addr).await {
                Ok(stream) => {
                    stream
                        .set_nodelay(true)
                        .expect("Failed setting TCP_NODELAY");
                    let mut registration_connection = frame_registration_connection(stream);
                            println!("Connected to {}", addr);
                    if let Err(e) = registration_connection
                        .send(RegistrationMessage::ClientRegister)
                        .await
                    {
                        warn!("Connected to {addr}, but registration failed: {e}");
                        continue;
                    }
                    info!("Connected gateway TCP client to server at {addr}");
                            println!("Registration sent to {}", addr);
                    let underlying_stream = registration_connection.into_inner().into_inner();
                    return frame_clients_connection(underlying_stream);
                }
                Err(e) => warn!("Unable to connect to server at {addr}: {e}"),
            }
        }
        time::sleep(RETRY_SERVER_CONNECTION_TIMEOUT).await;
    }
}

async fn writer_task(
    mut writer: omnipaxos_kv::common::utils::ToServerConnection,
    mut tcp_rx: mpsc::Receiver<ClientMessage>,
) {
    while let Some(msg) = tcp_rx.recv().await {
        if let Err(e) = writer.send(msg).await {
            error!("Failed sending client message to server: {e}");
            break;
        }
    }
    warn!("Gateway writer task stopped");
}

async fn reader_task(
    mut reader: omnipaxos_kv::common::utils::FromServerConnection,
    pending: Arc<Mutex<HashMap<CommandId, oneshot::Sender<ServerMessage>>>>,
) {
    while let Some(msg) = reader.next().await {
        match msg {
            Ok(server_msg) => match server_msg {
                ServerMessage::Write(cmd_id) => {
                    let sender = pending.lock().await.remove(&cmd_id);
                    if let Some(tx) = sender {
                        let _ = tx.send(ServerMessage::Write(cmd_id));
                    }
                }
                ServerMessage::Read(cmd_id, value) => {
                    let sender = pending.lock().await.remove(&cmd_id);
                    if let Some(tx) = sender {
                        let _ = tx.send(ServerMessage::Read(cmd_id, value));
                    }
                }
                ServerMessage::StartSignal(ts) => {
                    info!("Ignoring start signal from server: {ts}");
                }
            },
            Err(e) => error!("Error reading server message: {e:?}"),
        }
    }

    warn!("Gateway reader task stopped, clearing pending requests");
    pending.lock().await.clear();
}

async fn put_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PutRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let response = submit_command(&state, KVCommand::Put(body.key, body.value)).await?;
    let cmd_id = command_id_from_response(response)?;
    Ok(Json(json!({ "ok": true, "cmd_id": cmd_id })))
}

async fn delete_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<DeleteRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let response = submit_command(&state, KVCommand::Delete(body.key)).await?;
    let cmd_id = command_id_from_response(response)?;
    Ok(Json(json!({ "ok": true, "cmd_id": cmd_id })))
}

async fn get_handler(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let response = submit_command(&state, KVCommand::Get(key)).await?;
    match response {
        ServerMessage::Read(cmd_id, value) => Ok(Json(json!({
            "ok": true,
            "cmd_id": cmd_id,
            "value": value,
        }))),
        ServerMessage::Write(cmd_id) => Ok(Json(json!({ "ok": true, "cmd_id": cmd_id }))),
        ServerMessage::StartSignal(_) => Err(api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Unexpected start signal as command response",
        )),
    }
}

async fn submit_command(
    state: &Arc<AppState>,
    kv_cmd: KVCommand,
) -> Result<ServerMessage, (StatusCode, Json<Value>)> {
    let cmd_id = state
        .cmd_id_counter
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (tx, rx) = oneshot::channel();

    state.pending.lock().await.insert(cmd_id, tx);

    if state
        .tcp_tx
        .send(ClientMessage::Append(cmd_id, kv_cmd))
        .await
        .is_err()
    {
        state.pending.lock().await.remove(&cmd_id);
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "TCP sender unavailable",
        ));
    }

    match time::timeout(state.request_timeout, rx).await {
        Ok(Ok(server_msg)) => Ok(server_msg),
        Ok(Err(_)) => {
            state.pending.lock().await.remove(&cmd_id);
            Err(api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Response channel closed",
            ))
        }
        Err(_) => {
            state.pending.lock().await.remove(&cmd_id);
            Err(api_error(StatusCode::GATEWAY_TIMEOUT, "Server response timeout"))
        }
    }
}

fn command_id_from_response(msg: ServerMessage) -> Result<CommandId, (StatusCode, Json<Value>)> {
    match msg {
        ServerMessage::Write(cmd_id) => Ok(cmd_id),
        ServerMessage::Read(cmd_id, _) => Ok(cmd_id),
        ServerMessage::StartSignal(_) => Err(api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Unexpected start signal as command response",
        )),
    }
}

fn api_error(code: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (code, Json(json!({ "ok": false, "error": message })))
}
