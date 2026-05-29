use std::time::Duration;

use bitcoin::consensus::Encodable;
use tx_pigeon::{blast_transaction_hex, fetch_transactions};
use tokio::time::{interval, MissedTickBehavior, sleep};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_nocapture_60_seconds() {
    let interval_secs = 5;
    let total_secs = 60;
    let ticks = total_secs / interval_secs;
    let limit = 5;
    let tor_only = std::env::var("TOR_ONLY").ok().as_deref() == Some("1");
    let relay = true;

    println!(
        "[relay-swarm] starting 3 relay workers for {} seconds (tick every {} seconds, limit {})",
        total_secs, interval_secs, limit
    );

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

    println!("[relay-swarm] finished visible relay swarm test");
}

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
        println!("[relay-{worker_id}] tick {}/{} - connecting to peers", tick, ticks);

        match fetch_transactions(limit, tor_only, relay).await {
            Ok(txs) => {
                println!(
                    "[relay-{worker_id}] tick {}/{} - received {} transactions",
                    tick,
                    ticks,
                    txs.len()
                );

                for (index, tx) in txs.iter().enumerate() {
                    let txid = tx.compute_txid();
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
