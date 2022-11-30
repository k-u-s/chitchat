use std::collections::HashMap;
use std::net::SocketAddr;

use anyhow::Context;
use async_trait::async_trait;
use tracing::warn;

use quinn::{ClientConfig, Connection, ConnectionError, Endpoint, SendStream, ServerConfig};

use crate::serialize::Serializable;
use crate::transport::{Socket, Transport};
use crate::{ChitchatMessage, MTU};


pub struct QuinnTransport {
    buffer_size: usize,
    server_config: ServerConfig,
    client_config: ClientConfig,
}

impl QuinnTransport {

    pub fn new(buffer_size: usize,
               server_config: ServerConfig,
               client_config: ClientConfig,) -> Self {

        Self {
            buffer_size,
            server_config,
            client_config
        }
    }
}

#[async_trait]
impl Transport for QuinnTransport {
    async fn open(&self, bind_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>> {

        let mut endpoint = Endpoint::server(self.server_config.clone(), bind_addr)?;
        endpoint.set_default_client_config(self.client_config.clone());

        let endpoint_arc = std::sync::Arc::new(endpoint);
        let server_endpoint = endpoint_arc.clone();
        let (tx, recv_rx) = tokio::sync::mpsc::channel(self.buffer_size);

        tokio::task::spawn(async move {
            start_listen_server(server_endpoint, tx).await
        });

        let sockets = HashMap::new();

        Ok(Box::new(QuinnSocket {
            recv_rx,
            sockets,
            buf_send: Vec::new(),
            endpoint: endpoint_arc,
        }))
    }
}

struct QuinnSocket {
    recv_rx: tokio::sync::mpsc::Receiver<(SocketAddr, Vec<u8>)>,
    sockets: HashMap<SocketAddr, SendStream>,
    buf_send: Vec<u8>,
    endpoint: std::sync::Arc<Endpoint>,
}

impl Drop for QuinnSocket {
    fn drop(&mut self) {
        self.endpoint.close(quinn::VarInt::from_u32(500), b"");
        self.sockets.clear();
        self.recv_rx.close();
    }
}

#[async_trait]
impl Socket for QuinnSocket {
    async fn send(&mut self, to_addr: SocketAddr, message: ChitchatMessage) -> anyhow::Result<()> {
        self.buf_send.clear();
        message.serialize(&mut self.buf_send);
        self.send_bytes(to_addr).await?;
        Ok(())
    }

    /// Recv needs to be cancellable.
    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, ChitchatMessage)> {
        loop {
            if let Some(message) = self.receive_one().await? {
                return Ok(message);
            }
        }
    }
}

impl QuinnSocket {
    async fn receive_one(&mut self) -> anyhow::Result<Option<(SocketAddr, ChitchatMessage)>> {
        let (from_addr, data) = self.recv_rx
            .recv()
            .await
            .context("Error while receiving UDP message")?;
        let mut buf = &data[..];
        match ChitchatMessage::deserialize(&mut buf) {
            Ok(msg) => Ok(Some((from_addr, msg))),
            Err(err) => {
                warn!(payload_len=data.len(), from=%from_addr, err=%err, "invalid-chitchat-payload");
                Ok(None)
            }
        }
    }

    pub(crate) async fn send_bytes(
        &mut self,
        to_addr: SocketAddr,
    ) -> anyhow::Result<()> {

        if !self.sockets.contains_key(&to_addr) {

            // TODO: Change trait signature to have server name
            const SERVER_NAME: &str = "localhost";
            let connection_res = self.endpoint.connect(to_addr.clone(), SERVER_NAME)?.await;
            let connection = match connection_res {
                Ok(conn) => conn,
                // Sending to unbound endpoint should be safe
                Err(ConnectionError::TimedOut) => return Ok(()),
                Err(err) => return Err(anyhow::anyhow!(err)),
            };
            let send = connection
                .open_uni()
                .await?;
            self.sockets.insert(to_addr.clone(), send);
        }
        let socket = self.sockets.get_mut(&to_addr).unwrap();

        let payload = &self.buf_send;
        let send_res = socket.write_all(payload)
            .await
            .context("Failed to send chitchat message to target");
        return if send_res.is_err() {

            self.sockets.remove(&to_addr);
            Err(send_res.err().unwrap())
        } else {

            Ok(())
        }
    }
}

async fn start_listen_server(server_endpoint: std::sync::Arc<Endpoint>,
                             tx: tokio::sync::mpsc::Sender<(SocketAddr, Vec<u8>)>,
) -> anyhow::Result<()> {

    loop {
        let conn_res = server_endpoint.accept().await;
        if conn_res.is_none() {
            break;
        }

        let conn = conn_res.unwrap();
        let connection = conn.await?;

        let publisher_tx = tx.clone();
        tokio::task::spawn(async move {
            process_connection(connection, publisher_tx).await
        });
    }

    Ok(())
}

async fn process_connection(connection: Connection,
                            publisher_tx: tokio::sync::mpsc::Sender<(SocketAddr, Vec<u8>)>
) -> anyhow::Result<()> {

    let mut buf = [0u8; MTU];
    let mut recv  = connection.accept_uni().await?;
    let remote_address = connection.remote_address();
    while let Ok(len_option) = recv.read(&mut buf).await {

        if len_option.is_none() {
            break;
        }

        let len = len_option.unwrap();
        let data = buf[..len].to_vec();
        publisher_tx.send((remote_address, data)).await?;
    }

    Ok(())
}