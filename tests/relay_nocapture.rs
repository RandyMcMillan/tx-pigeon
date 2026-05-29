use std::time::Duration;

use bitcoin::consensus::Encodable;
use tx_pigeon::{
    blast_transaction_hex,
    fetch_transactions,
    mempool::fetch_recent_tx_hexes,
    topic::{run_gossip_client, spawn_topic_network, TopicRelayHandle},
};
use tokio::time::{interval, MissedTickBehavior, sleep};

const SEED_TX_HEX: &str = concat!(
    "01000000",
    "01",
    "0000000000000000000000000000000000000000000000000000000000000000",
    "ffffffff",
    "00",
    "ffffffff",
    "01",
    "0000000000000000",
    "00",
    "00000000"
);
// This test keeps the relay lifecycle visible under `--nocapture` by running
// several workers on the same cadence and logging each fetch/blast step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_nocapture_60_seconds() {
    let interval_secs = 5;
    let total_secs = 60;
    let ticks = total_secs / interval_secs;
    let limit = 5;
    let tor_only = std::env::var("TOR_ONLY").ok().as_deref() == Some("1");
    let relay = true;

    // Start a small topic-network cluster so the relay workers have something
    // persistent to talk to while the test is printing its lifecycle logs.
    println!(
        "[relay-swarm] starting 3 libp2p relay nodes and 3 bitcoin workers for {} seconds (tick every {} seconds, limit {})",
        total_secs, interval_secs, limit
    );

    let mut topic_nodes = Vec::new();
    for node_id in 1..=3 {
        println!("[topic-{node_id}] booting libp2p relay node (seed_tx={})", node_id == 1);
        let handle = spawn_topic_network(format!("topic-{node_id}"), tor_only)
            .await
            .expect("start topic relay network");
        topic_nodes.push(handle);
    }

    let gossip_client = tokio::spawn(async move {
        let _ = run_gossip_client("gossip-client", tor_only, true, true).await;
    });

    // Give mDNS a moment to discover the other relay nodes before the first
    // transaction publish kicks off.
    sleep(Duration::from_secs(3)).await;

    println!("[topic-1] seeding topic mesh with bootstrap tx");
    topic_nodes[0].publish(SEED_TX_HEX.to_string()).await.expect("seed topic mesh");

    let mut workers = Vec::new();
    for worker_id in 1..=3 {
        workers.push(tokio::spawn(relay_worker(
            worker_id,
            ticks,
            interval_secs,
            limit,
            tor_only,
            relay,
            topic_nodes[worker_id as usize - 1].clone(),
        )));
    }

    for worker in workers {
        let _ = worker.await;
    }

    gossip_client.abort();
    let _ = gossip_client.await;

    println!("[relay-swarm] finished visible relay swarm test");
}

// Each worker staggers startup slightly, then repeats the fetch/blast cycle so
// the log shows multiple overlapping relay lifecycles instead of one burst.
async fn relay_worker(
    worker_id: usize,
    ticks: u64,
    interval_secs: u64,
    limit: usize,
    tor_only: bool,
    relay: bool,
    topic: TopicRelayHandle,
) {
    let mut ticker = interval(Duration::from_secs(interval_secs));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    println!(
        "[relay-{worker_id}] worker online (ticks={}, interval={}s, limit={})",
        ticks, interval_secs, limit
    );

    sleep(Duration::from_secs((worker_id - 1) as u64)).await;

    for tick in 1..=ticks {
        ticker.tick().await;
        println!("[relay-{worker_id}] tick {}/{} - requesting mempool txs", tick, ticks);

        let mempool_result = fetch_recent_tx_hexes(limit).await;
        match mempool_result {
            Ok(txs) => {
                println!(
                    "[relay-{worker_id}] tick {}/{} - mempool sources returned {} recent txs",
                    tick,
                    ticks,
                    txs.len()
                );
                for (index, (txid, tx_hex)) in txs.iter().enumerate() {
                    println!(
                        "[relay-{worker_id}] tick {}/{} - mempool tx {}/{} {}",
                        tick,
                        ticks,
                        index + 1,
                        txs.len(),
                        txid
                    );
                    println!(
                        "[relay-{worker_id}] tick {}/{} - starting p2p transmission for {}",
                        tick, ticks, txid
                    );
                    if let Err(err) = topic.publish(tx_hex.to_string()).await {
                        println!(
                            "[relay-{worker_id}] tick {}/{} - topic publish error for {}: {}",
                            tick, ticks, txid, err
                        );
                    } else {
                        println!(
                            "[relay-{worker_id}] tick {}/{} - topic mesh received {}",
                            tick, ticks, txid
                        );
                    }
                    println!(
                        "[relay-{worker_id}] tick {}/{} - rebroadcasting/blasting {}",
                        tick, ticks, txid
                    );
                    match blast_transaction_hex(tx_hex, tor_only, relay).await {
                        Ok(peer_count) if peer_count > 0 => {
                            println!(
                                "[relay-{worker_id}] tick {}/{} - peer acknowledged receipt for {} via {} peers",
                                tick, ticks, txid, peer_count
                            );
                        }
                        Ok(_) => {
                            println!(
                                "[relay-{worker_id}] tick {}/{} - no peer acknowledged receipt for {}",
                                tick, ticks, txid
                            );
                        }
                        Err(err) => {
                            println!(
                                "[relay-{worker_id}] tick {}/{} - blast error for {}: {}",
                                tick, ticks, txid, err
                            );
                        }
                    }
                }
            }
            Err(err) => {
                println!(
                    "[relay-{worker_id}] tick {}/{} - mempool helper error: {}",
                    tick, ticks, err
                );
            }
        }

        println!("[relay-{worker_id}] tick {}/{} - requesting bitcoin nodes", tick, ticks);
        let node_result = fetch_transactions(limit, tor_only, relay).await;

        match node_result {
            Ok(txs) => {
                println!(
                    "[relay-{worker_id}] tick {}/{} - received {} node transactions",
                    tick,
                    ticks,
                    txs.len()
                );

                for (index, tx) in txs.iter().enumerate() {
                    let txid = tx.compute_txid();
                    // Re-encode the transaction so the blast path receives the
                    // exact wire-format hex the CLI uses.
                    let mut encoded = Vec::new();
                    tx.consensus_encode(&mut encoded)
                        .expect("encode tx for relay blast");
                    let tx_hex = hex::encode(encoded);
                    println!(
                        "[relay-{worker_id}] tick {}/{} - iterating tx {}/{} {}",
                        tick,
                        ticks,
                        index + 1,
                        txs.len(),
                        txid
                    );
                    println!(
                        "[relay-{worker_id}] tick {}/{} - sending tx {} to peers",
                        tick, ticks, txid
                    );
                    if let Err(err) = topic.publish(tx_hex.clone()).await {
                        println!(
                            "[relay-{worker_id}] tick {}/{} - topic publish error for {}: {}",
                            tick, ticks, txid, err
                        );
                    } else {
                        println!(
                            "[relay-{worker_id}] tick {}/{} - topic mesh received {}",
                            tick, ticks, txid
                        );
                    }
                    println!(
                        "[relay-{worker_id}] tick {}/{} - rebroadcasting/blasting {}",
                        tick, ticks, txid
                    );
                    match blast_transaction_hex(&tx_hex, tor_only, relay).await {
                        Ok(peer_count) if peer_count > 0 => {
                            println!(
                                "[relay-{worker_id}] tick {}/{} - peer acknowledged receipt for {} via {} peers",
                                tick, ticks, txid, peer_count
                            );
                        }
                        Ok(_) => {
                            println!(
                                "[relay-{worker_id}] tick {}/{} - no peer acknowledged receipt for {}",
                                tick, ticks, txid
                            );
                        }
                        Err(err) => {
                            println!(
                                "[relay-{worker_id}] tick {}/{} - blast error for {}: {}",
                                tick, ticks, txid, err
                            );
                        }
                    }
                }
            }
            Err(err) => {
                println!(
                    "[relay-{worker_id}] tick {}/{} - fetch error: {}",
                    tick, ticks, err
                );
            }
        }
    }

    println!("[relay-{worker_id}] worker complete");
}
