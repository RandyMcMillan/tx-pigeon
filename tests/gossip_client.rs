use std::time::Duration;

use bitcoin::consensus::Decodable;
use tokio::time::timeout;
use tx_pigeon::topic::{TopicConfig, TopicEvent, spawn_gossip_client, spawn_topic_network};

const SAMPLE_TX_HEX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gossip_client_observes_published_transaction() {
    println!("[test] gossip_client_observes_published_transaction: start");

    let mut publisher_config = TopicConfig::default();
    publisher_config.listen_addr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
    publisher_config.mdns = false;
    publisher_config.relay_server = false;
    publisher_config.flood_publish = true;

    let (publisher_handle, mut publisher_events) = spawn_topic_network(
        "publisher",
        false,
        publisher_config,
    )
    .await
    .expect("spawn publisher topic network");
    let publisher_peer_id = publisher_handle.local_peer_id().to_string();
    println!("[test] publisher peer id: {}", publisher_peer_id);

    let publisher_addr = wait_for_listen_address(&mut publisher_events)
        .await
        .expect("publisher should report a listen address");
    println!("[test] publisher listening on {}", publisher_addr);

    let mut observer_config = TopicConfig::default();
    observer_config.listen_addr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
    observer_config.mdns = false;
    observer_config.bootstrap_peers = vec![publisher_addr.parse().unwrap()];
    observer_config.relay_server = false;
    observer_config.flood_publish = true;

    let mut observer_events = spawn_gossip_client(
        "observer",
        false,
        false,
        true,
        observer_config,
    )
    .await
    .expect("spawn observer gossip client");
    println!("[test] observer spawned");

    println!("[test] waiting for observer to identify publisher");
    wait_for_identified_peer(&mut observer_events, &publisher_peer_id)
        .await
        .expect("observer should identify publisher");

    println!("[test] waiting for publisher to see observer subscribe");
    let observer_peer_id = wait_for_subscribed_peer(&mut publisher_events)
        .await
        .expect("publisher should see observer subscribe");
    println!("[test] observer subscribed: {}", observer_peer_id);

    println!("[test] waiting for gossipsub heartbeat/mesh to settle");
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("[test] publishing tx from publisher");
    publisher_handle
        .publish(SAMPLE_TX_HEX.to_string())
        .await
        .expect("publisher should queue tx for publish");

    println!("[test] waiting for observer to receive the tx");
    let received_txid = wait_for_message_txid(&mut observer_events)
        .await
        .expect("observer should receive the published tx");

    let expected_txid = bitcoin::Transaction::consensus_decode(&mut std::io::Cursor::new(
        hex::decode(SAMPLE_TX_HEX).unwrap(),
    ))
    .unwrap()
    .compute_txid()
    .to_string();

    assert_eq!(
        received_txid, expected_txid,
        "observer received txid should match published txid"
    );
    println!("[test] observer received tx {} — ok", received_txid);
}

async fn wait_for_subscribed_peer(
    events: &mut tokio::sync::mpsc::Receiver<TopicEvent>,
) -> Option<String> {
    loop {
        match timeout(Duration::from_secs(15), events.recv()).await {
            Ok(Some(TopicEvent::Subscribed { peer_id })) => {
                println!("[test] peer subscribed: {}", peer_id);
                return Some(peer_id);
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
