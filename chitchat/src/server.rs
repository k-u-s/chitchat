use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rand::prelude::*;
use tokio::net::lookup_host;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::message::ChitchatMessage;
use crate::transport::{Socket, Transport};
use crate::{Chitchat, ChitchatConfig, NodeId};

/// Number of nodes picked for random gossip.
const GOSSIP_COUNT: usize = 3;

/// UDP Chitchat server handler.
///
/// It is necessary to hold (and not drop) the handler
/// for the server to keep running.
pub struct ChitchatHandle {
    node_id: NodeId,
    command_tx: UnboundedSender<Command>,
    chitchat: Arc<Mutex<Chitchat>>,
    join_handle: JoinHandle<Result<(), anyhow::Error>>,
}

const DNS_POLLING_DURATION: Duration = Duration::from_secs(60);

async fn dns_refresh_loop(
    seed_hosts_requiring_dns: HashSet<String>,
    seed_addrs_not_requiring_resolution: HashSet<SocketAddr>,
    seed_addrs_tx: watch::Sender<HashSet<SocketAddr>>,
) {
    let mut interval = time::interval(DNS_POLLING_DURATION);
    // We actually do not want to run the polling loop right away,
    // hence this tick.
    interval.tick().await;
    while seed_addrs_tx.receiver_count() > 0 {
        interval.tick().await;
        let mut seed_addrs = seed_addrs_not_requiring_resolution.clone();
        for seed_host in &seed_hosts_requiring_dns {
            resolve_seed_host(seed_host, &mut seed_addrs).await;
        }
        if seed_addrs_tx.send(seed_addrs).is_err() {
            return;
        }
    }
}

async fn resolve_seed_host(seed_host: &str, seed_addrs: &mut HashSet<SocketAddr>) {
    if let Ok(resolved_seed_addrs) = lookup_host(seed_host).await {
        for seed_addr in resolved_seed_addrs {
            if seed_addrs.contains(&seed_addr) {
                continue;
            }
            debug!(seed_host=seed_host, seed_addr=%seed_addr, "seed-addr-from_dns");
            seed_addrs.insert(seed_addr);
        }
    } else {
        warn!(seed_host=%seed_host, "Failed to lookup host");
    }
}

// The seed nodes address can be string representing a
// socket addr directly, or hostnames:port.
//
// The latter is especially important when relying on
// a headless service in k8s or when using DNS in general.
//
// In that case, we do not want to perform the resolution
// once and forall.
// We want to periodically retry DNS resolution,
// in order to avoid having a split cluster.
//
// The newcomers are supposed to chime in too,
// so there is no need to refresh it too often,
// especially if it is not empty.
async fn spawn_dns_refresh_loop(seeds: &[String]) -> watch::Receiver<HashSet<SocketAddr>> {
    let mut seed_addrs_not_requiring_resolution: HashSet<SocketAddr> = Default::default();
    let mut first_round_seed_resolution: HashSet<SocketAddr> = Default::default();
    let mut seed_requiring_dns: HashSet<String> = Default::default();
    for seed in seeds {
        if let Ok(seed_addr) = seed.parse() {
            seed_addrs_not_requiring_resolution.insert(seed_addr);
        } else {
            seed_requiring_dns.insert(seed.clone());
            // We run DNS resolution for the first iteration too.
            // It will be run in the DNS polling loop too, but running
            // it for the first iteration makes sure our first gossip
            // round will not be for nothing.
            resolve_seed_host(seed, &mut first_round_seed_resolution).await;
        }
    }

    let initial_seed_addrs: HashSet<SocketAddr> = seed_addrs_not_requiring_resolution
        .union(&first_round_seed_resolution)
        .cloned()
        .collect();

    info!(initial_seed_addrs=?initial_seed_addrs);

    let (seed_addrs_tx, seed_addrs_rx) = watch::channel(initial_seed_addrs);
    if !seed_requiring_dns.is_empty() {
        tokio::task::spawn(dns_refresh_loop(
            seed_requiring_dns,
            seed_addrs_not_requiring_resolution,
            seed_addrs_tx,
        ));
    }
    seed_addrs_rx
}

/// Launch a new server.
///
/// This will start the Chitchat server as a new Tokio background task.
pub async fn spawn_chitchat(
    config: ChitchatConfig,
    initial_key_values: Vec<(String, String)>,
    transport: &dyn Transport,
) -> anyhow::Result<ChitchatHandle> {
    let (command_tx, command_rx) = mpsc::unbounded_channel();

    let seed_addrs: watch::Receiver<HashSet<SocketAddr>> =
        spawn_dns_refresh_loop(&config.seed_nodes).await;

    let socket = transport.open(config.listen_addr).await?;

    let node_id = config.node_id.clone();

    let chitchat = Chitchat::with_node_id_and_seeds(config, seed_addrs, initial_key_values);
    let chitchat_arc = Arc::new(Mutex::new(chitchat));
    let chitchat_arc_clone = chitchat_arc.clone();

    let join_handle = tokio::spawn(async move {
        Server::new(command_rx, chitchat_arc_clone, socket)
            .await
            .run()
            .await
    });

    Ok(ChitchatHandle {
        node_id,
        command_tx,
        chitchat: chitchat_arc,
        join_handle,
    })
}

impl ChitchatHandle {
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn chitchat(&self) -> Arc<Mutex<Chitchat>> {
        self.chitchat.clone()
    }

    /// Call a function with mutable access to the [`Chitchat`].
    pub async fn with_chitchat<F, T>(&self, mut fun: F) -> T
    where F: FnMut(&mut Chitchat) -> T {
        let mut chitchat = self.chitchat.lock().await;
        fun(&mut chitchat)
    }

    /// Shut the server down.
    pub async fn shutdown(self) -> Result<(), anyhow::Error> {
        let _ = self.command_tx.send(Command::Shutdown);
        self.join_handle.await?
    }

    /// Perform a Chitchat "handshake" with another UDP server.
    pub fn gossip(&self, addr: SocketAddr) -> Result<(), anyhow::Error> {
        self.command_tx.send(Command::Gossip(addr))?;
        Ok(())
    }
}

/// UDP server for Chitchat communication.
struct Server {
    command_rx: UnboundedReceiver<Command>,
    chitchat: Arc<Mutex<Chitchat>>,
    transport: Box<dyn Socket>,
    rng: SmallRng,
}

impl Server {
    async fn new(
        command_rx: UnboundedReceiver<Command>,
        chitchat: Arc<Mutex<Chitchat>>,
        transport: Box<dyn Socket>,
    ) -> Self {
        let rng = SmallRng::from_rng(thread_rng()).expect("Failed to seed random generator");
        Self {
            chitchat,
            command_rx,
            transport,
            rng,
        }
    }

    /// Listen for new Chitchat messages.
    async fn run(&mut self) -> anyhow::Result<()> {
        let gossip_interval = self.chitchat.lock().await.config.gossip_interval;
        let mut gossip_interval = time::interval(gossip_interval);
        loop {
            tokio::select! {
                result = self.transport.recv() => match result {
                    Ok((from_addr, message)) => {
                        let _ = self.handle_message(from_addr, message).await;
                    }
                    Err(err) => return Err(err),
                },
                _ = gossip_interval.tick() => {
                    self.gossip_multiple().await
                },
                command = self.command_rx.recv() => match command {
                    Some(Command::Gossip(addr)) => {
                        let _ = self.gossip(addr).await;
                    },
                    Some(Command::Shutdown) | None => break,
                }
            }
        }
        Ok(())
    }

    /// Process a single UDP packet.
    async fn handle_message(
        &mut self,
        from_addr: SocketAddr,
        message: ChitchatMessage,
    ) -> anyhow::Result<()> {
        // Handle gossip from other servers.
        let response = self.chitchat.lock().await.process_message(message);
        // Send reply if necessary.
        if let Some(message) = response {
            self.transport.send(from_addr, message).await?;
        }
        Ok(())
    }

    /// Gossip to multiple randomly chosen nodes.
    async fn gossip_multiple(&mut self) {
        // Gossip with live nodes & probabilistically include a random dead node
        let mut chitchat_guard = self.chitchat.lock().await;
        let cluster_state = chitchat_guard.cluster_state();

        let peer_nodes = cluster_state
            .nodes()
            .filter(|node_id| *node_id != chitchat_guard.self_node_id())
            .map(|node_id| node_id.gossip_public_address)
            .collect::<HashSet<_>>();
        let live_nodes = chitchat_guard
            .live_nodes()
            .map(|node_id| node_id.gossip_public_address)
            .collect::<HashSet<_>>();
        let dead_nodes = chitchat_guard
            .dead_nodes()
            .map(|node_id| node_id.gossip_public_address)
            .collect::<HashSet<_>>();
        let seed_nodes: HashSet<SocketAddr> = chitchat_guard.seed_nodes();
        let (selected_nodes, random_dead_node_opt, random_seed_node_opt) = select_nodes_for_gossip(
            &mut self.rng,
            peer_nodes,
            live_nodes,
            dead_nodes,
            seed_nodes,
        );

        chitchat_guard.update_heartbeat();

        // Drop lock to prevent deadlock in [`UdpSocket::gossip`].
        drop(chitchat_guard);

        for node in selected_nodes {
            let result = self.gossip(node).await;
            if result.is_err() {
                error!(node = ?node, "Gossip error with a live node.");
            }
        }

        if let Some(random_dead_node) = random_dead_node_opt {
            let result = self.gossip(random_dead_node).await;
            if result.is_err() {
                error!(node = ?random_dead_node, "Gossip error with a dead node.")
            }
        }

        if let Some(random_seed_node) = random_seed_node_opt {
            let result = self.gossip(random_seed_node).await;
            if result.is_err() {
                error!(node = ?random_seed_node, "Gossip error with a seed node.")
            }
        }

        // Update nodes liveliness
        let mut chitchat_guard = self.chitchat.lock().await;
        chitchat_guard.update_nodes_liveliness();
    }

    /// Gossip to one other UDP server.
    async fn gossip(&mut self, addr: SocketAddr) -> anyhow::Result<()> {
        let syn = self.chitchat.lock().await.create_syn_message();
        self.transport.send(addr, syn).await?;
        Ok(())
    }
}

#[derive(Debug)]
enum Command {
    Gossip(SocketAddr),
    Shutdown,
}

fn select_nodes_for_gossip<R>(
    rng: &mut R,
    peer_nodes: HashSet<SocketAddr>,
    live_nodes: HashSet<SocketAddr>,
    dead_nodes: HashSet<SocketAddr>,
    seed_nodes: HashSet<SocketAddr>,
) -> (Vec<SocketAddr>, Option<SocketAddr>, Option<SocketAddr>)
where
    R: Rng + ?Sized,
{
    let live_nodes_count = live_nodes.len();
    let dead_nodes_count = dead_nodes.len();

    // Select `GOSSIP_COUNT` number of live nodes.
    // On startup, select from cluster nodes since we don't know any live node yet.
    let nodes = if live_nodes_count == 0 {
        peer_nodes
    } else {
        live_nodes
    }
    .iter()
    .cloned()
    .choose_multiple(rng, GOSSIP_COUNT);

    let mut has_gossiped_with_a_seed_node = false;
    for node_id in &nodes {
        if seed_nodes.contains(node_id) {
            has_gossiped_with_a_seed_node = true;
            break;
        }
    }

    // Select a dead node for potential gossip.
    let random_dead_node_opt: Option<SocketAddr> =
        select_dead_node_to_gossip_with(rng, &dead_nodes, live_nodes_count, dead_nodes_count);

    // Select a seed node for potential gossip.
    // It prevents network partition caused by the number of seeds.
    // See https://issues.apache.org/jira/browse/CASSANDRA-150
    let random_seed_node_opt: Option<SocketAddr> =
        if !has_gossiped_with_a_seed_node || live_nodes_count < seed_nodes.len() {
            select_seed_node_to_gossip_with(rng, &seed_nodes, live_nodes_count, dead_nodes_count)
        } else {
            None
        };

    (nodes, random_dead_node_opt, random_seed_node_opt)
}

/// Selects a dead node to gossip with, with some probability.
fn select_dead_node_to_gossip_with<R>(
    rng: &mut R,
    dead_nodes: &HashSet<SocketAddr>,
    live_nodes_count: usize,
    dead_nodes_count: usize,
) -> Option<SocketAddr>
where
    R: Rng + ?Sized,
{
    let selection_probability = dead_nodes_count as f64 / (live_nodes_count + 1) as f64;
    if selection_probability > rng.gen::<f64>() {
        return dead_nodes.iter().choose(rng).cloned();
    }
    None
}

/// Selects a seed node to gossip with, with some probability.
fn select_seed_node_to_gossip_with<R>(
    rng: &mut R,
    seed_nodes: &HashSet<SocketAddr>,
    live_nodes_count: usize,
    dead_nodes_count: usize,
) -> Option<SocketAddr>
where
    R: Rng + ?Sized,
{
    let selection_probability =
        seed_nodes.len() as f64 / (live_nodes_count + dead_nodes_count) as f64;
    if live_nodes_count == 0 || rng.gen::<f64>() <= selection_probability {
        return seed_nodes.iter().choose(rng).cloned();
    }
    None
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::time::Duration;

    use tokio_stream::{Stream, StreamExt};

    use super::*;
    use crate::message::ChitchatMessage;
    use crate::state::NodeState;
    use crate::transport::{ChannelTransport, Transport};
    use crate::HEARTBEAT_KEY;

    #[derive(Debug, Default)]
    struct RngForTest {
        value: u32,
    }

    impl RngCore for RngForTest {
        fn next_u32(&mut self) -> u32 {
            self.value += 1;
            self.value - 1
        }

        fn next_u64(&mut self) -> u64 {
            self.value += 1;
            (self.value - 1) as u64
        }

        fn fill_bytes(&mut self, _dest: &mut [u8]) {
            unimplemented!();
        }

        fn try_fill_bytes(&mut self, _dest: &mut [u8]) -> Result<(), rand::Error> {
            unimplemented!();
        }
    }

    fn to_hash_set<T: Eq + std::hash::Hash>(node_ids: Vec<T>) -> std::collections::HashSet<T> {
        node_ids.into_iter().collect()
    }

    async fn timeout<O>(future: impl Future<Output = O>) -> O {
        tokio::time::timeout(Duration::from_millis(100), future)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_syn() {
        let transport = ChannelTransport::default();
        let test_config = ChitchatConfig::for_test(1112);
        let test_addr = test_config.node_id.gossip_public_address;
        let peer_addr: SocketAddr = ([127u8, 0u8, 0u8, 1u8], 1111u16).into();
        let mut peer_transport = transport.open(peer_addr).await.unwrap();
        let server = spawn_chitchat(test_config, Vec::new(), &transport)
            .await
            .unwrap();
        server.gossip(peer_addr).unwrap();
        let (from, message) = timeout(peer_transport.recv()).await.unwrap();
        assert_eq!(from, test_addr);
        match message {
            ChitchatMessage::Syn { cluster_id, digest } => {
                assert_eq!(cluster_id, "default-cluster");
                assert_eq!(digest.node_max_version.len(), 1);
            }
            message => panic!("unexpected message: {:?}", message),
        }
    }

    #[cfg(test)]
    fn empty_seeds() -> watch::Receiver<HashSet<SocketAddr>> {
        watch::channel(Default::default()).1
    }

    #[tokio::test]
    async fn test_syn_ack() {
        let transport = ChannelTransport::default();

        let config2 = ChitchatConfig::for_test(2);
        let mut transport2 = transport
            .open(config2.node_id.gossip_public_address)
            .await
            .unwrap();

        let config1 = ChitchatConfig::for_test(1);
        let addr1 = config1.node_id.gossip_public_address;

        let mut chitchat = Chitchat::with_node_id_and_seeds(config2, empty_seeds(), Vec::new());
        let _handler = spawn_chitchat(config1, Vec::new(), &transport)
            .await
            .unwrap();

        let syn = chitchat.create_syn_message();
        transport2.send(addr1, syn).await.unwrap();

        let (from1, msg) = transport2.recv().await.unwrap();
        assert_eq!(from1, addr1);
        match msg {
            ChitchatMessage::SynAck { .. } => (),
            message => panic!("unexpected message: {:?}", message),
        }
    }

    #[tokio::test]
    async fn test_syn_bad_cluster() {
        let transport = ChannelTransport::default();
        let mut outsider_config = ChitchatConfig::for_test(2224);
        outsider_config.cluster_id = "another-cluster".to_string();
        let mut outsider_transport = transport
            .open(outsider_config.node_id.gossip_public_address)
            .await
            .unwrap();
        let mut outsider =
            Chitchat::with_node_id_and_seeds(outsider_config, empty_seeds(), Vec::new());

        let server_config = ChitchatConfig::for_test(2223);
        let server_addr = server_config.node_id.gossip_public_address;
        let _handler = spawn_chitchat(server_config, Vec::new(), &transport)
            .await
            .unwrap();

        let syn = outsider.create_syn_message();
        outsider_transport.send(server_addr, syn).await.unwrap();

        let (_from_addr, syn_ack) = timeout(outsider_transport.recv()).await.unwrap();
        match syn_ack {
            ChitchatMessage::BadCluster => (),
            message => panic!("unexpected message: {:?}", message),
        }
    }

    #[tokio::test]
    async fn test_seeding() {
        let transport = ChannelTransport::default();
        let seed_config = ChitchatConfig::for_test(5551);
        let seed_addr = seed_config.node_id.gossip_public_address;
        let mut seed_transport = transport.open(seed_addr).await.unwrap();

        let mut client_config = ChitchatConfig::for_test(5552);
        let client_addr = client_config.node_id.gossip_public_address;
        client_config.seed_nodes = vec![seed_addr.to_string()];
        let _handler = spawn_chitchat(client_config, Vec::new(), &transport)
            .await
            .unwrap();

        let (from, message) = timeout(seed_transport.recv()).await.unwrap();
        assert_eq!(from, client_addr);

        match message {
            ChitchatMessage::Syn { .. } => (),
            message => panic!("unexpected message: {:?}", message),
        }
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let transport = ChannelTransport::default();
        let test_config = ChitchatConfig::for_test(1);
        let test_addr = test_config.node_id.gossip_public_address;
        let mut test_chitchat =
            Chitchat::with_node_id_and_seeds(test_config, empty_seeds(), Vec::new());
        let mut test_transport = transport.open(test_addr).await.unwrap();

        let server_config = ChitchatConfig::for_test(2);
        let server_id = server_config.node_id.clone();
        let server_addr = server_config.node_id.gossip_public_address;
        let server_handle = spawn_chitchat(server_config, Vec::new(), &transport)
            .await
            .unwrap();

        // Add our test socket to the server's nodes.
        server_handle
            .with_chitchat(|server_chitchat| {
                server_chitchat.update_heartbeat();
                let syn = server_chitchat.create_syn_message();
                let syn_ack = test_chitchat.process_message(syn).unwrap();
                server_chitchat.process_message(syn_ack);
            })
            .await;

        // Wait for syn, with updated heartbeat
        let (_, syn_message) = timeout(test_transport.recv()).await.unwrap();

        // Reply.
        let syn_ack = test_chitchat.process_message(syn_message).unwrap();
        test_transport.send(server_addr, syn_ack).await.unwrap();

        // Wait for delta to ensure heartbeat key was incremented.
        let (_, chitchat_message) = timeout(test_transport.recv()).await.unwrap();
        let delta = if let ChitchatMessage::Ack { delta } = chitchat_message {
            delta
        } else {
            panic!("Expected ack");
        };

        let node_delta = &delta.node_deltas.get(&server_id).unwrap().key_values;
        let heartbeat = &node_delta.get(HEARTBEAT_KEY).unwrap().value;
        assert_eq!(heartbeat, "2");

        server_handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_member_change_event_is_broadcasted() {
        let transport = ChannelTransport::default();
        let node1_config = ChitchatConfig::for_test(6663);
        let node1_addr = node1_config.node_id.gossip_public_address;
        let node1 = spawn_chitchat(node1_config, Vec::new(), &transport)
            .await
            .unwrap();

        let mut node2_config = ChitchatConfig::for_test(6664);
        node2_config.seed_nodes = vec![node1_addr.to_string()];
        let node2_id = node2_config.node_id.clone();
        let node2 = spawn_chitchat(node2_config, Vec::new(), &transport)
            .await
            .unwrap();
        let mut ready_nodes_watcher = node1
            .chitchat()
            .lock()
            .await
            .ready_nodes_watcher()
            .skip_while(|live_nodes| live_nodes.is_empty());

        {
            let ready_nodes = next_ready_nodes(&mut ready_nodes_watcher).await;
            assert_eq!(ready_nodes.len(), 1);
            assert!(ready_nodes.contains(&node2_id));
        }

        node1.shutdown().await.unwrap();
        node2.shutdown().await.unwrap();
    }

    async fn next_ready_nodes<S: Unpin + Stream<Item = HashSet<NodeId>>>(
        watcher: &mut S,
    ) -> HashSet<NodeId> {
        tokio::time::timeout(Duration::from_secs(3), watcher.next())
            .await
            .expect("No Change within 3s")
            .expect("Channel was closed")
    }

    #[tokio::test]
    async fn test_is_ready_predicate() {
        const HEALTH_KEY: &str = "HEALTH";
        let is_ready_pred = |node_state: &NodeState| {
            node_state
                .get(HEALTH_KEY)
                .map(|health_val| health_val == "READY")
                .unwrap_or(false)
        };
        let transport = ChannelTransport::default();
        let mut node1_config = ChitchatConfig::for_test(6663);
        node1_config.set_is_ready_predicate(is_ready_pred);

        let node1_addr = node1_config.node_id.gossip_public_address;
        let node1 = spawn_chitchat(node1_config, vec![], &transport)
            .await
            .unwrap();

        let mut node2_config = ChitchatConfig::for_test(6664);
        node2_config.set_is_ready_predicate(is_ready_pred);

        node2_config.seed_nodes = vec![node1_addr.to_string()];
        let node2_id = node2_config.node_id.clone();
        let node2 = spawn_chitchat(
            node2_config,
            vec![(HEALTH_KEY.to_string(), "READY".to_string())],
            &transport,
        )
        .await
        .unwrap();
        let mut ready_nodes_watcher = node1
            .chitchat()
            .lock()
            .await
            .ready_nodes_watcher()
            .skip_while(|live_nodes| live_nodes.is_empty());
        {
            let ready_nodes = next_ready_nodes(&mut ready_nodes_watcher).await;
            assert_eq!(ready_nodes.len(), 1);
            assert!(ready_nodes.contains(&node2_id));
        }

        // node2 advertises itself as not ready.
        node2
            .chitchat()
            .lock()
            .await
            .self_node_state()
            .set(HEALTH_KEY, "NOT_READY");

        {
            let ready_nodes = next_ready_nodes(&mut ready_nodes_watcher).await;
            assert!(ready_nodes.is_empty());
        }

        // node2 goes back up.
        node2
            .chitchat()
            .lock()
            .await
            .self_node_state()
            .set(HEALTH_KEY, "READY");
        {
            let ready_nodes = next_ready_nodes(&mut ready_nodes_watcher).await;
            assert_eq!(ready_nodes.len(), 1);
            assert!(ready_nodes.contains(&node2_id));
        }

        node1.shutdown().await.unwrap();
        node2.shutdown().await.unwrap();
    }

    #[test]
    fn test_select_nodes_for_gossip() {
        let node1 = NodeId::for_test_localhost(10_001);
        let node2 = NodeId::for_test_localhost(10_002);
        let node3 = NodeId::for_test_localhost(10_003);
        let mut rng = RngForTest::default();
        let (nodes, dead_node, seed_node) = select_nodes_for_gossip(
            &mut rng,
            to_hash_set(vec![
                node1.gossip_public_address,
                node2.gossip_public_address,
                node3.gossip_public_address,
            ]),
            to_hash_set(vec![
                node1.gossip_public_address,
                node2.gossip_public_address,
            ]),
            to_hash_set(vec![node3.gossip_public_address]),
            to_hash_set(vec![node2.gossip_public_address]),
        );
        assert_eq!(nodes.len(), 2);
        assert_eq!(dead_node, Some(node3.gossip_public_address));
        assert_eq!(
            seed_node, None,
            "Should have already gossiped with a seed node."
        );
    }

    #[test]
    fn test_gossip_no_dead_node_no_seed_nodes() {
        let nodes: HashSet<SocketAddr> = (10_001..=10_005)
            .map(NodeId::for_test_localhost)
            .map(|node_id| node_id.gossip_public_address)
            .collect();
        let mut rng = RngForTest::default();
        let (nodes, dead_node, seed_node) = select_nodes_for_gossip(
            &mut rng,
            nodes.clone(),
            nodes,
            to_hash_set(vec![]),
            to_hash_set(vec![]),
        );
        assert_eq!(nodes.len(), 3);
        assert_eq!(dead_node, None);
        assert_eq!(seed_node, None);
    }

    #[test]
    fn test_gossip_dead_and_seed_node() {
        let nodes: Vec<SocketAddr> = (10_001..=10_005)
            .map(NodeId::for_test_localhost)
            .map(|node_id| node_id.gossip_public_address)
            .collect();
        let seeds: HashSet<SocketAddr> = nodes[3..5].iter().cloned().collect();
        let mut rng = RngForTest::default();
        let (gossip_nodes, gossip_dead_node, gossip_seed_node) = select_nodes_for_gossip(
            &mut rng,
            to_hash_set(nodes.clone()),
            to_hash_set(vec![nodes[0]]),
            nodes[1..].iter().cloned().collect(),
            seeds,
        );
        assert_eq!(gossip_nodes, &[nodes[0]]);
        assert!(gossip_dead_node.is_some());
        assert!(gossip_seed_node.is_some());
    }
}
