use std::net::SocketAddr;

use async_trait::async_trait;

use crate::message::ChitchatMessage;

mod channel;
mod udp;
mod utils;
#[cfg(feature = "quinn-transport")]
mod quinn;

pub use channel::{ChannelTransport, Statistics};
pub use udp::UdpTransport;
pub use utils::TransportExt;

#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>>;
}

#[async_trait]
pub trait Socket: Send + Sync + 'static {
    // Only returns an error if the transport is broken and may not emit message
    // in the future.
    async fn send(&mut self, to: SocketAddr, msg: ChitchatMessage) -> anyhow::Result<()>;
    // Only returns an error if the transport is broken and may not receive message
    // in the future.
    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, ChitchatMessage)>;
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use tokio::net::UdpSocket;
    use tokio::time::timeout;

    use super::Transport;
    use crate::digest::Digest;
    use crate::message::ChitchatMessage;
    use crate::serialize::Serializable;
    use crate::transport::{ChannelTransport, UdpTransport};

    const IGNORE_TEST_PORT: u16 = 30_300;
    const CANNOT_OPEN_PORT: u16 = 10_100;
    const RECV_WAIT_FOR_MSG_PORT: u16 = 20_200;
    const REL_ON_DROP_PORT: u16 = 15_100;
    const SENDING_TO_UNBOUND_PORT: u16 = 40_400;

    fn sample_syn_msg() -> ChitchatMessage {
        ChitchatMessage::Syn {
            cluster_id: "cluster_id".to_string(),
            digest: Digest::default(),
        }
    }

    #[tokio::test]
    async fn test_udp_transport_ignore_invalid_payload() {
        let recv_addr: SocketAddr = ([127, 0, 0, 1], IGNORE_TEST_PORT).into();
        let send_addr: SocketAddr = ([127, 0, 0, 1], IGNORE_TEST_PORT + 1).into();
        let send_udp_socket: UdpSocket = UdpSocket::bind(send_addr).await.unwrap();
        let mut recv_socket = UdpTransport.open(recv_addr).await.unwrap();
        let invalid_payload = b"junk";
        send_udp_socket
            .send_to(&invalid_payload[..], recv_addr)
            .await
            .unwrap();
        let valid_message = sample_syn_msg();
        let mut valid_payload: Vec<u8> = Vec::new();
        valid_message.serialize(&mut valid_payload);
        send_udp_socket
            .send_to(&valid_payload[..], recv_addr)
            .await
            .unwrap();
        let (send_addr2, received_message) = recv_socket.recv().await.unwrap();
        assert_eq!(send_addr, send_addr2);
        assert_eq!(received_message, valid_message);
    }

    async fn test_transport_cannot_open_twice_aux(transport: &dyn Transport) {
        let addr: SocketAddr = ([127, 0, 0, 1], CANNOT_OPEN_PORT).into();
        let _socket = transport.open(addr).await.unwrap();
        assert!(transport.open(addr).await.is_err());
    }

    async fn test_transport_recv_waits_for_message(transport: &dyn Transport) {
        let addr1: SocketAddr = ([127, 0, 0, 1], RECV_WAIT_FOR_MSG_PORT + 1).into();
        let addr2: SocketAddr = ([127, 0, 0, 1], RECV_WAIT_FOR_MSG_PORT + 2).into();
        let mut socket1 = transport.open(addr1).await.unwrap();
        let mut socket2 = transport.open(addr2).await.unwrap();
        assert!(timeout(Duration::from_millis(200), socket2.recv())
            .await
            .is_err());
        let syn_message = sample_syn_msg();
        let socket_recv_fut = tokio::task::spawn(async move { socket2.recv().await.unwrap() });
        tokio::time::sleep(Duration::from_millis(100)).await;
        socket1.send(addr2, syn_message).await.unwrap();
        let recv_res = timeout(Duration::from_millis(5_000), socket_recv_fut)
            .await
            .unwrap();
        let (exp1, _received_msg) = recv_res.unwrap();
        assert_eq!(addr1, exp1);
    }

    async fn test_transport_socket_released_on_drop(transport: &dyn Transport) {
        let addr: SocketAddr = ([127, 0, 0, 1], REL_ON_DROP_PORT).into();
        let socket = transport.open(addr).await.unwrap();
        std::mem::drop(socket);
        // TODO: find why quinn do not release port immediately
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _new_socket = transport.open(addr).await.unwrap();
    }

    async fn test_transport_sending_to_unbound_addr_is_ok(transport: &dyn Transport) {
        let addr: SocketAddr = ([127, 0, 0, 1], SENDING_TO_UNBOUND_PORT).into();
        let unbound_addr: SocketAddr = ([127, 0, 0, 1], SENDING_TO_UNBOUND_PORT + 1).into();
        let mut socket = transport.open(addr).await.unwrap();
        socket.send(unbound_addr, sample_syn_msg()).await.unwrap()
    }

    async fn test_transport_suite(transport: &dyn Transport) {
        test_transport_cannot_open_twice_aux(transport).await;
        test_transport_socket_released_on_drop(transport).await;
        test_transport_recv_waits_for_message(transport).await;
        test_transport_sending_to_unbound_addr_is_ok(transport).await;
    }

    #[tokio::test]
    async fn test_transport_udp() {
        test_transport_suite(&UdpTransport).await;
    }

    #[tokio::test]
    async fn test_transport_in_mem() {
        test_transport_suite(&ChannelTransport::default()).await;
    }

    #[cfg(feature = "quinn-transport")]
    mod quinn_tests {
        use crate::transport::quinn::QuinnTransport;

        // Implementation of `ServerCertVerifier` that verifies everything as trustworthy.
        struct SkipServerVerification;

        impl SkipServerVerification {
            fn new() -> std::sync::Arc<Self> {
                std::sync::Arc::new(Self)
            }
        }

        impl rustls::client::ServerCertVerifier for SkipServerVerification {
            fn verify_server_cert(
                &self,
                _end_entity: &rustls::Certificate,
                _intermediates: &[rustls::Certificate],
                _server_name: &rustls::ServerName,
                _scts: &mut dyn Iterator<Item = &[u8]>,
                _ocsp_response: &[u8],
                _now: std::time::SystemTime,
            ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
                Ok(rustls::client::ServerCertVerified::assertion())
            }
        }

        fn configure_client() -> quinn::ClientConfig {
            let crypto = rustls::ClientConfig::builder()
                .with_safe_defaults()
                .with_custom_certificate_verifier(SkipServerVerification::new())
                .with_no_client_auth();

            quinn::ClientConfig::new(std::sync::Arc::new(crypto))
        }

        fn generate_self_signed_cert() -> Result<(rustls::Certificate, rustls::PrivateKey), Box<dyn std::error::Error>>
        {
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
            let key = rustls::PrivateKey(cert.serialize_private_key_der());
            Ok((rustls::Certificate(cert.serialize_der()?), key))
        }

        fn create_quinn_transport() -> QuinnTransport {

            let client_config = configure_client();
            let (cert, key) = generate_self_signed_cert().unwrap();
            let certs = vec![cert];
            let server_config = quinn::ServerConfig::with_single_cert(certs, key).unwrap();
            QuinnTransport::new(1_024, server_config, client_config)
        }

        #[tokio::test]
        async fn test_transport_quinn() {
            let transport = create_quinn_transport();
            crate::transport::tests::test_transport_suite(&transport).await;
        }
    }
}

