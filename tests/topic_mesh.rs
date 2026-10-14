use std::time::Duration;

use bitcoin::consensus::Decodable;
use tokio::time::timeout;
use tx_pigeon::topic::{TopicConfig, TopicEvent, spawn_topic_network};

const SAMPLE_TX_HEX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topic_mesh_delivers_transaction_between_two_nodes() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    println!("[test] topic_mesh_delivers_transaction_between_two_nodes: start");

    let mut node_a_config = TopicConfig::default();
    node_a_config.listen_addr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
    node_a_config.mdns = false;
    node_a_config.relay_server = false;
    node_a_config.flood_publish = true;

    let (node_a_handle, mut node_a_events) = spawn_topic_network(
        "node-a",
        false,
        node_a_config,
    )
    .await
    .expect("spawn node-a topic network");
    let node_a_peer_id = node_a_handle.local_peer_id().to_string();
    println!("[test] node-a peer id: {}", node_a_peer_id);

    let node_a_addr = wait_for_listen_address(&mut node_a_events)
        .await
        .expect("node-a should report a listen address");
    println!("[test] node-a listening on {}", node_a_addr);

    let mut node_b_config = TopicConfig::default();
    node_b_config.listen_addr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
    node_b_config.mdns = false;
    node_b_config.bootstrap_peers = vec![node_a_addr.parse().unwrap()];
    node_b_config.relay_server = false;
    node_b_config.flood_publish = true;

    let (node_b_handle, mut node_b_events) = spawn_topic_network(
        "node-b",
        false,
        node_b_config,
    )
    .await
    .expect("spawn node-b topic network");
    let node_b_peer_id = node_b_handle.local_peer_id().to_string();
    println!("[test] node-b peer id: {}", node_b_peer_id);

    let node_b_addr = wait_for_listen_address(&mut node_b_events)
        .await
        .expect("node-b should report a listen address");
    println!("[test] node-b listening on {}", node_b_addr);

    println!("[test] waiting for node-b to identify node-a");
    wait_for_identified_peer(&mut node_b_events, &node_a_peer_id)
        .await
        .expect("node-b should identify node-a");

    println!("[test] waiting for node-a to see node-b subscribe");
    wait_for_subscribed_peer(&mut node_a_events, &node_b_peer_id)
        .await
        .expect("node-a should see node-b subscribe");

    println!("[test] waiting for gossipsub heartbeat/mesh to settle");
    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("[test] publishing tx from node-a");
    node_a_handle
        .publish(SAMPLE_TX_HEX.to_string())
        .await
        .expect("node-a should queue tx for publish");

    println!("[test] waiting for node-b to receive the tx");
    let received_txid = wait_for_message_txid(&mut node_b_events)
        .await
        .expect("node-b should receive the published tx");

    let expected_txid = bitcoin::Transaction::consensus_decode(&mut std::io::Cursor::new(
        hex::decode(SAMPLE_TX_HEX).unwrap(),
    ))
    .unwrap()
    .compute_txid()
    .to_string();

    assert_eq!(
        received_txid, expected_txid,
        "node-b received txid should match published txid"
    );
    println!("[test] node-b received tx {} — ok", received_txid);
}

async fn wait_for_subscribed_peer(
    events: &mut tokio::sync::mpsc::Receiver<TopicEvent>,
    expected_peer_id: &str,
) -> Option<()> {
    loop {
        match timeout(Duration::from_secs(15), events.recv()).await {
            Ok(Some(TopicEvent::Subscribed { peer_id })) if peer_id == expected_peer_id => {
                println!("[test] expected peer subscribed: {}", peer_id);
                return Some(());
            }
            Ok(Some(TopicEvent::Subscribed { peer_id })) => {
                println!("[test] ignoring subscription from other peer: {}", peer_id);
            }
            Ok(Some(other)) => {
                println!("[test] ignoring event while waiting for subscribed: {:?}", other);
            }
            Ok(None) => return None,
            Err(_) => return None,
        }
    }
}

async fn wait_for_identified_peer(
    events: &mut tokio::sync::mpsc::Receiver<TopicEvent>,
    expected_peer_id: &str,
) -> Option<()> {
    loop {
        match timeout(Duration::from_secs(15), events.recv()).await {
            Ok(Some(TopicEvent::Identified { peer_id, .. })) if peer_id == expected_peer_id => {
                println!("[test] expected peer identified: {}", peer_id);
                return Some(());
            }
            Ok(Some(TopicEvent::Identified { peer_id, .. })) => {
                println!("[test] ignoring identify from other peer: {}", peer_id);
            }
            Ok(Some(other)) => {
                println!("[test] ignoring event while waiting for identified: {:?}", other);
            }
            Ok(None) => return None,
            Err(_) => return None,
        }
    }
}

async fn wait_for_listen_address(events: &mut tokio::sync::mpsc::Receiver<TopicEvent>) -> Option<String> {
    loop {
        match timeout(Duration::from_secs(5), events.recv()).await {
            Ok(Some(TopicEvent::Listening { address })) => return Some(address),
            Ok(Some(other)) => {
                println!("[test] ignoring event while waiting for listen address: {:?}", other);
            }
            Ok(None) => return None,
            Err(_) => return None,
        }
    }
}

async fn wait_for_message_txid(
    events: &mut tokio::sync::mpsc::Receiver<TopicEvent>,
) -> Option<String> {
    let deadline = Duration::from_secs(30);
    let start = tokio::time::Instant::now();

    while start.elapsed() < deadline {
        match timeout(Duration::from_secs(1), events.recv()).await {
            Ok(Some(TopicEvent::Message { txid })) => return Some(txid),
            Ok(Some(other)) => {
                println!("[test] ignoring event while waiting for message: {:?}", other);
            }
            Ok(None) => return None,
            Err(_) => continue,
        }
    }

    None
}
