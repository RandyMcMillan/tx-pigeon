use std::time::Duration;

use bitcoin::consensus::Encodable;
use tx_pigeon::{blast_transaction_hex, fetch_transactions, mempool::fetch_recent_txids, topic::run_topic_network};
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
        let seed_tx = (node_id == 1).then_some(SEED_TX_HEX.to_string());
        topic_nodes.push(tokio::spawn(topic_node(node_id, tor_only, seed_tx)));
    }

    // Give mDNS a moment to discover the other relay nodes before the first
    // transaction publish kicks off.
    sleep(Duration::from_secs(3)).await;

    let mut workers = Vec::new();
    for worker_id in 1..=3 {
        workers.push(tokio::spawn(relay_worker(
            worker_id,
            ticks,
            interval_secs,
            limit,
            tor_only,
            relay,
        )));
    }

    for worker in workers {
        let _ = worker.await;
    }

    for node in topic_nodes {
        node.abort();
        let _ = node.await;
    }

    println!("[relay-swarm] finished visible relay swarm test");
}

// The topic network is intentionally fire-and-forget here; the relay workers
// only need a live peer set, not a return value.
async fn topic_node(worker_id: usize, tor_only: bool, seed_tx: Option<String>) {
    println!(
        "[topic-{worker_id}] booting libp2p relay node (seed_tx={})",
        seed_tx.is_some()
    );

    let result = run_topic_network(seed_tx, tor_only).await;
    println!("[topic-{worker_id}] libp2p relay node exited: {result:?}");
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
        println!("[relay-{worker_id}] tick {}/{} - requesting bitcoin nodes", tick, ticks);

        let node_fut = fetch_transactions(limit, tor_only, relay);
        let mempool_fut = fetch_recent_txids(limit);
        let (node_result, mempool_result) = tokio::join!(node_fut, mempool_fut);

        match mempool_result {
            Ok(txids) => {
                println!(
                    "[relay-{worker_id}] tick {}/{} - mempool sources returned {} recent txids",
                    tick,
                    ticks,
                    txids.len()
                );
                for (index, txid) in txids.iter().enumerate() {
                    println!(
                        "[relay-{worker_id}] tick {}/{} - mempool tx {}/{} {}",
                        tick,
                        ticks,
                        index + 1,
                        txids.len(),
                        txid
                    );
                }
            }
            Err(err) => {
                println!(
                    "[relay-{worker_id}] tick {}/{} - mempool helper error: {}",
                    tick, ticks, err
                );
            }
        }

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
                    if let Err(err) = blast_transaction_hex(&tx_hex, tor_only, relay).await {
                        println!(
                            "[relay-{worker_id}] tick {}/{} - blast error for {}: {}",
                            tick, ticks, txid, err
                        );
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
