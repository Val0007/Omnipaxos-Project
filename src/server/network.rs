use futures::{SinkExt, StreamExt};
use log::*;
use omnipaxos_kv::common::{
    kv::{ClientId, NodeId},
    messages::*,
    utils::*,
};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{collections::HashMap, str::FromStr};
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::Receiver,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::configs::OmniPaxosKVConfig;


pub enum ReconnectEvent { Success { peer_id: NodeId, conn: NewConnection }, Failed { peer_id: NodeId }}

pub struct Network {
    peers: Vec<NodeId>,
    pub peer_connections: Vec<Option<PeerConnection>>,
    pub client_connections: HashMap<ClientId, ClientConnection>,
    max_client_id: Arc<Mutex<ClientId>>,
    batch_size: usize,
    client_message_sender: Sender<(ClientId, ClientMessage)>,
    cluster_message_sender: Sender<(NodeId, ClusterMessage)>,
    pub cluster_messages: Receiver<(NodeId, ClusterMessage)>,
    pub client_messages: Receiver<(ClientId, ClientMessage)>,

    /// Receives newly established ClientConnections from the background accept task and pushes them into client_connections
    pub new_client_connections: Receiver<ClientConnection>,

    pub reconnect_peers_reciever: Receiver<ReconnectEvent>,
    pub reconnect_peers:Sender<ReconnectEvent>,
    id:u64,
    pub socket_addr:Vec<SocketAddr>,
    pub reconnecting: Vec<bool>,
    pub last_heartbeat_ack: Vec<std::time::Instant>, // ← ADD
}

fn get_addrs(config: OmniPaxosKVConfig) -> (SocketAddr, Vec<SocketAddr>) {
    let listen_address_str = format!(
        "{}:{}",
        config.local.listen_address, config.local.listen_port
    );
    let listen_address = SocketAddr::from_str(&listen_address_str).expect(&format!(
        "{listen_address_str} is an invalid listen address"
    ));
    let node_addresses: Vec<SocketAddr> = config
        .cluster
        .node_addrs
        .into_iter()
        .map(|addr_str| match addr_str.to_socket_addrs() {
            Ok(mut addrs) => addrs.next().unwrap(),
            Err(e) => panic!("Address {addr_str} is invalid: {e}"),
        })
        .collect();
    (listen_address, node_addresses)
}

pub async fn connect_with_retry(peer: &str, peer_address: SocketAddr) -> TcpStream {
            let reconnect_delay = Duration::from_secs(1);
            let mut reconnect_interval: tokio::time::Interval = tokio::time::interval(reconnect_delay);    


    loop {
        println!("TRYING TCP CONNECT To {}",peer);
        reconnect_interval.tick().await;
        match TcpStream::connect(peer_address).await {
            Ok(connection) => {
                info!("Retry TCP connect successfull to {peer}");
                connection.set_nodelay(true).unwrap();
                return connection;
            }
            Err(err) => {
                error!("Establishing connection to node {peer} failed: {err}");
            }
        }
    }
    }

impl Network {
    // Creates a new network with connections to other server nodes in the cluster and any clients.
    // Waits until connections to all peer servers are established before resolving.
    // Client connections are accepted forever in a background task; newly connected clients are
    // delivered via `self.new_client_connections`.
    pub async fn new(config: OmniPaxosKVConfig, batch_size: usize) -> Self {
        let (listen_address, node_addresses) = get_addrs(config.clone());
        let id = config.local.server_id;
        let peer_addresses: Vec<(NodeId, SocketAddr)> = config
            .cluster
            .nodes
            .into_iter()
            .zip(node_addresses.into_iter())
            .filter(|(node_id, _addr)| *node_id != id)
            .collect();
        let mut cluster_connections = vec![];
        cluster_connections.resize_with(peer_addresses.len(), Default::default);
        let (cluster_message_sender, cluster_messages) = tokio::sync::mpsc::channel(batch_size);
        let (client_message_sender, client_messages) = tokio::sync::mpsc::channel(batch_size);
        // Channel used to hand newly accepted ClientConnections back to the Network owner.
        let (new_client_tx, new_client_connections) = tokio::sync::mpsc::channel(batch_size);

        // Channel used to handle reconnections and add the peers reconnected back into the vector
        let (reconnect_peers, reconnect_peers_reciever) = tokio::sync::mpsc::channel(batch_size);


        let mut network = Self {
            peers: peer_addresses.iter().map(|(id, _)| *id).collect(),
            socket_addr : peer_addresses.iter().map(|(_, addr)| *addr).collect(),
            peer_connections: cluster_connections,
            client_connections: HashMap::new(),
            max_client_id: Arc::new(Mutex::new(0)),
            batch_size,
            client_message_sender,
            cluster_message_sender,
            cluster_messages,
            client_messages,
            new_client_connections,
            reconnect_peers_reciever,
            reconnect_peers,
            id,
            reconnecting: vec![false; peer_addresses.len()],
            last_heartbeat_ack: vec![std::time::Instant::now(); peer_addresses.len()],
        };
        network
            .initialize_connections(id, peer_addresses, listen_address, new_client_tx)
            .await;
        network
    }

    async fn initialize_connections(
        &mut self,
        id: NodeId,
        peers: Vec<(NodeId, SocketAddr)>,
        listen_address: SocketAddr,
        new_client_tx: Sender<ClientConnection>,
    ) {
        // Separate channel used only during initialisation to deliver both peer and client
        // connections to this function.
        let (connection_sink, mut connection_source) = mpsc::channel(30);

        // The listener runs forever; it sends peer connections through `connection_sink` during
        // init AND keeps sending client connections through `new_client_tx` forever.
        self.spawn_connection_listener(
            connection_sink.clone(),
            new_client_tx,
            listen_address,
        );

        // Only connect to peers with a lower id (the higher-id side listens).
        self.spawn_peer_connectors(connection_sink.clone(), id, peers);

        // Wait only until all peer connections are established.
        while let Some(new_connection) = connection_source.recv().await {
            match new_connection {
                NewConnection::ToPeer(connection) => {
                    let peer_idx = self.cluster_id_to_idx(connection.peer_id).unwrap();
                    self.peer_connections[peer_idx] = Some(connection);
                }
            }
            let all_cluster_connected = self.peer_connections.iter().all(|c| c.is_some());
            if all_cluster_connected {
                break;
            }
        }
    }

    /// Spawns a TCP listener that runs forever.
    ///
    /// - Peer connections are sent through `peer_connection_sender` (used only during init).
    /// - Client connections are sent through `new_client_tx` and are accepted indefinitely.
    fn spawn_connection_listener(
        &self,
        peer_connection_sender: Sender<NewConnection>,
        new_client_tx: Sender<ClientConnection>,
        listen_address: SocketAddr,
    ) -> tokio::task::JoinHandle<()> {
        let client_sender = self.client_message_sender.clone();
        let cluster_sender = self.cluster_message_sender.clone();
        let max_client_id_handle = self.max_client_id.clone();
        let batch_size = self.batch_size;
        tokio::spawn(async move {
            let listener = TcpListener::bind(listen_address).await.unwrap();
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, socket_addr)) => {
                        info!("New connection from {socket_addr}");
                        tcp_stream.set_nodelay(true).unwrap();
                        tokio::spawn(Self::handle_incoming_connection(
                            tcp_stream,
                            client_sender.clone(),
                            cluster_sender.clone(),
                            peer_connection_sender.clone(),
                            new_client_tx.clone(),
                            max_client_id_handle.clone(),
                            batch_size,
                        ));
                    }
                    Err(e) => error!("Error listening for new connection: {:?}", e),
                }
            }
        })
    }

    /// Handshakes the incoming TCP stream and routes it to the right channel:
    /// - Peer  → `peer_connection_sender` (used during cluster init)
    /// - Client → `new_client_tx` (runs forever, independent of init)
    async fn handle_incoming_connection(
        connection: TcpStream,
        client_message_sender: Sender<(ClientId, ClientMessage)>,
        cluster_message_sender: Sender<(NodeId, ClusterMessage)>,
        peer_connection_sender: Sender<NewConnection>,
        new_client_tx: Sender<ClientConnection>,
        max_client_id_handle: Arc<Mutex<ClientId>>,
        batch_size: usize,
    ) {
        let mut registration_connection = frame_registration_connection(connection);
        let registration_message = registration_connection.next().await;
        match registration_message {
            Some(Ok(RegistrationMessage::NodeRegister(node_id))) => {
                info!("Identified connection from node {node_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                let peer_conn = PeerConnection::new(
                    node_id,
                    underlying_stream,
                    batch_size,
                    cluster_message_sender,
                );

                //only use the connection sink here for clusters , after the functions goes out of scope
                //this also goes out of scope and we dont use it for the clients anymore
               if let Err(err) = peer_connection_sender.send(NewConnection::ToPeer(peer_conn)).await {
    // Receiver is gone (init finished), this is a reconnect — ignore
    info!("Init channel closed, dropping inbound peer connection from {node_id}");
}
            }
            Some(Ok(RegistrationMessage::ClientRegister)) => {
                let next_client_id = {
                    let mut max_client_id = max_client_id_handle.lock().unwrap();
                    *max_client_id += 1;
                    *max_client_id
                };
                info!("Identified connection from client {next_client_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                let client_conn = ClientConnection::new(
                    next_client_id,
                    underlying_stream,
                    batch_size,
                    client_message_sender,
                );
                // Send through the dedicated client channel , using a separate Sender<NewConnection> — this never closes.
                if let Err(err) = new_client_tx.send(client_conn).await {
                    error!("Failed to register client {next_client_id}: {err}");
                }
            }
            Some(Err(err)) => {
                error!("Error deserializing handshake: {:?}", err);
            }
            None => {
                info!("Connection to unidentified source dropped");
            }
        }
    }

    fn spawn_peer_connectors(
        &self,
        connection_sender: Sender<NewConnection>,
        my_id: NodeId,
        peers: Vec<(NodeId, SocketAddr)>,
    ) {
        let peers_to_connect_to = peers.into_iter().filter(|(peer_id, _)| *peer_id < my_id);
        for (peer, peer_address) in peers_to_connect_to {
            let reconnect_delay = Duration::from_secs(1);
            let mut reconnect_interval = tokio::time::interval(reconnect_delay);
            let cluster_sender = self.cluster_message_sender.clone();
            let connection_sender = connection_sender.clone();
            let batch_size = self.batch_size;
            tokio::spawn(async move {
                let peer_connection = loop {
                    reconnect_interval.tick().await;
                    match TcpStream::connect(peer_address).await {
                        Ok(connection) => {
                            info!("New connection to node {peer}");
                            connection.set_nodelay(true).unwrap();
                            break connection;
                        }
                        Err(err) => {
                            error!("Establishing connection to node {peer} failed: {err}")
                        }
                    }
                };
                let mut registration_connection = frame_registration_connection(peer_connection);
                let handshake = RegistrationMessage::NodeRegister(my_id);
                if let Err(err) = registration_connection.send(handshake).await {
                    error!("Error sending handshake to {peer}: {err}");
                    return;
                }
                let underlying_stream = registration_connection.into_inner().into_inner();
                let peer_actor =
                    PeerConnection::new(peer, underlying_stream, batch_size, cluster_sender);
                let new_connection = NewConnection::ToPeer(peer_actor);
                connection_sender.send(new_connection).await.unwrap();
            });
        }
    }



    fn spawn_reconnect(&self, peer: NodeId, idx: usize) {
    let peer_address = self.socket_addr[idx];
    let batch_size = self.batch_size;
    let cluster_sender = self.cluster_message_sender.clone();
    let sender = self.reconnect_peers.clone();
    let my_id = self.id.clone();

    tokio::spawn(async move {
        let peer_connection = connect_with_retry(&peer.to_string(), peer_address).await;

        let mut registration_connection = frame_registration_connection(peer_connection);
        let handshake = RegistrationMessage::NodeRegister(my_id);
        if let Err(err) = registration_connection.send(handshake).await {
            error!("Error sending handshake to {peer}: {err}");
            sender.send(ReconnectEvent::Failed { peer_id: peer }).await.unwrap();
            return;
        }

        let underlying_stream = registration_connection.into_inner().into_inner();
        let peer_actor = PeerConnection::new(peer, underlying_stream, batch_size, cluster_sender);
        let new_connection = NewConnection::ToPeer(peer_actor);
        sender.send(ReconnectEvent::Success { peer_id: peer, conn: new_connection }).await.unwrap();
    });
}

    pub fn check_heartbeat_timeouts(&mut self) {
        println!("CHECK HEARTBEAT TIMEOUTS CALLED");
        let timeout = Duration::from_secs(8);  // ← increase to 8s
        for idx in 0..self.peers.len() {
            let peer_id = self.peers[idx];
            let elapsed = self.last_heartbeat_ack[idx].elapsed();
            println!("  peer {} last ack: {}ms ago (timeout={}ms)",
                     peer_id,
                     elapsed.as_millis(),      // ← milliseconds to see exact value
                     timeout.as_millis()
            );
            if elapsed > timeout {
                if self.peer_connections[idx].is_some() {
                    println!("detecting failure for peer {}", peer_id);
                    warn!("=== PARTITION/FAILURE DETECTED: peer {} not responding for {}s ===",
                    peer_id, elapsed.as_secs());
                    self.peer_connections[idx] = None;
                    if !self.reconnecting[idx] {
                        self.reconnecting[idx] = true;
                        self.spawn_reconnect(peer_id, idx);
                    }
                }
            }
        }
    }

    pub fn update_heartbeat_ack(&mut self, from: NodeId) {
        if let Some(idx) = self.cluster_id_to_idx(from) {
            self.last_heartbeat_ack[idx] = std::time::Instant::now();
        }
    }


    // /// Drains any newly connected clients from the background accept task and registers them.
    // /// Call this periodically from your main loop before processing messages.
    // pub fn register_new_clients(&mut self) {
    //     while let Ok(connection) = self.new_client_connections.try_recv() {
    //         info!("Registering new client {}", connection.client_id);
    //         self.client_connections.insert(connection.client_id, connection);
    //     }
    // }

    pub fn send_to_cluster(&mut self, to: NodeId, msg: ClusterMessage) {
        match self.cluster_id_to_idx(to) {
            Some(idx) => match &mut self.peer_connections[idx] {
                Some(ref mut connection) => {
                    if let Err(err) = connection.send(msg) {
                        println!("Couldn't send msg to peer {to} , will try to reconnect from {}",self.id);
                        self.peer_connections[idx] = None;
                          self.reconnecting[idx] = true;
                        self.spawn_reconnect(to, idx);
                    }
                }
                None => {
                            if !self.reconnecting[idx] {
                                self.reconnecting[idx] = true;
                                self.spawn_reconnect(to, idx);
                            }
                },
            },
            None => error!("Sending to unexpected node {to}"),
        }
    }

    pub fn send_to_client(&mut self, to: ClientId, msg: ServerMessage) {
        match self.client_connections.get_mut(&to) {
            Some(connection) => {
                if let Err(err) = connection.send(msg) {
                    warn!("Couldn't send msg to client {to}: {err}");
                    self.client_connections.remove(&to);
                }
            }
            None => warn!("Not connected to client {to}"),
        }
    }

    #[allow(dead_code)]
    pub fn shutdown(&mut self) {
        for (_, client_connection) in self.client_connections.drain() {
            client_connection.close();
        }
        for peer_connection in self.peer_connections.drain(..) {
            if let Some(connection) = peer_connection {
                connection.close();
            }
        }
        for _ in 0..self.peers.len() {
            self.peer_connections.push(None);
        }
    }

    #[inline]
    pub fn cluster_id_to_idx(&self, id: NodeId) -> Option<usize> {
        self.peers.iter().position(|&p| p == id)
    }
}

// During init only peer connections flow through this enum; client connections
// are delivered directly via `new_client_tx`.
pub enum NewConnection {
    ToPeer(PeerConnection),
}

pub struct PeerConnection {
    peer_id: NodeId,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    outgoing_messages: UnboundedSender<ClusterMessage>,
}

impl PeerConnection {
    pub fn new(
        peer_id: NodeId,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(NodeId, ClusterMessage)>,
    ) -> Self {
        let (reader, mut writer) = frame_cluster_connection(connection);
        let reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => {
                            if let Err(_) = incoming_messages.send((peer_id, m)).await {
                                break;
                            };
                        }
                        Err(err) => {
                            error!("Error deserializing message: {:?}", err);
                        }
                    }
                }
            }
        });
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            while message_rx.recv_many(&mut buffer, batch_size).await != 0 {
                for msg in buffer.drain(..) {
                    if let Err(err) = writer.feed(msg).await {
                        error!("Couldn't send message to node {peer_id}: {err}");
                        break;
                    }
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to node {peer_id}: {err}");
                    break;
                }
            }
            info!("Connection to node {peer_id} closed");
        });
        PeerConnection {
            peer_id,
            reader_task,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: ClusterMessage,
    ) -> Result<(), mpsc::error::SendError<ClusterMessage>> {
        self.outgoing_messages.send(msg)
    }

    pub fn close(self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}

pub struct ClientConnection {
    pub client_id: ClientId,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    outgoing_messages: UnboundedSender<ServerMessage>,
}

impl ClientConnection {
    pub fn new(
        client_id: ClientId,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(ClientId, ClientMessage)>,
    ) -> Self {
        let (reader, mut writer) = frame_servers_connection(connection);
        let reader_task: JoinHandle<()> = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => incoming_messages.send((client_id, m)).await.unwrap(),
                        Err(err) => error!("Error deserializing message: {:?}", err),
                    }
                }
            }
        });
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            while message_rx.recv_many(&mut buffer, batch_size).await != 0 {
                for msg in buffer.drain(..) {
                    if let Err(err) = writer.feed(msg).await {
                        error!("Couldn't send message to client {client_id}: {err}");
                        error!("Killing connection to client {client_id}");
                        return;
                    }
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to client {client_id}: {err}");
                    error!("Killing connection to client {client_id}");
                    return;
                }
            }
        });
        ClientConnection {
            client_id,
            reader_task,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: ServerMessage,
    ) -> Result<(), mpsc::error::SendError<ServerMessage>> {
        self.outgoing_messages.send(msg)
    }

    fn close(self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}