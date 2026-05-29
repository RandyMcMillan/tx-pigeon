use std::time::Duration;

use tokio::time::{interval, MissedTickBehavior};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_nocapture_60_seconds() {
    let interval_secs = 5;
    let total_secs = 60;
    let ticks = total_secs / interval_secs;

    println!(
        "[relay-nocapture] starting visible relay test for {} seconds (tick every {} seconds)",
        total_secs, interval_secs
    );

    let mut ticker = interval(Duration::from_secs(interval_secs));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    for tick in 1..=ticks {
        ticker.tick().await;
        println!(
            "[relay-nocapture] tick {}/{} at +{}s",
            tick,
            ticks,
            tick * interval_secs
        );
    }

    println!("[relay-nocapture] finished visible relay test");
}
