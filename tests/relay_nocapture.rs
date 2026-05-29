use std::time::Duration;

use tx_pigeon::fetch_transactions;
use tokio::time::{interval, MissedTickBehavior};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_nocapture_60_seconds() {
    let interval_secs = 5;
    let total_secs = 60;
    let ticks = total_secs / interval_secs;
    let limit = 5;
    let tor_only = std::env::var("TOR_ONLY").ok().as_deref() == Some("1");

    println!(
        "[relay-nocapture] starting visible relay lifecycle test for {} seconds (tick every {} seconds, limit {})",
        total_secs, interval_secs, limit
    );

    let mut ticker = interval(Duration::from_secs(interval_secs));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    for tick in 1..=ticks {
        ticker.tick().await;
        println!("[relay-nocapture] tick {}/{} - requesting peer transactions", tick, ticks);

        match fetch_transactions(limit, tor_only, true).await {
            Ok(txs) => {
                println!(
                    "[relay-nocapture] tick {}/{} - received {} transactions",
                    tick,
                    ticks,
                    txs.len()
                );

                for (index, tx) in txs.iter().enumerate() {
                    let txid = tx.compute_txid();
                    println!(
                        "[relay-nocapture] tick {}/{} - iterating tx {}/{} {}",
                        tick,
                        ticks,
                        index + 1,
                        txs.len(),
                        txid
                    );
                }
            }
            Err(err) => {
                println!(
                    "[relay-nocapture] tick {}/{} - fetch error: {}",
                    tick,
                    ticks,
                    err
                );
            }
        }
    }

    println!("[relay-nocapture] finished visible relay lifecycle test");
}
