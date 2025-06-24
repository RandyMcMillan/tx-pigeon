use anyhow::Result;
use arti_client::{IsolationToken, StreamPrefs, TorClient, TorClientConfig};
use bitcoin::{Network, Transaction};
use tx_pigeon::{
    DNS_SEEDS, MAX_CONCURRENT_DELIVERIES, NetworkAddress, crawl_seed_node, deliver_poop_tx,
};

use crossterm::input::input;
use rand::seq::SliceRandom;

use rust_mempool::MempoolClient;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{net::lookup_host, sync::Semaphore, task::JoinSet, time::timeout};
use tracing::{error, info};

fn read_tx() -> String {
    let input = input();

    match input.read_line() {
        Ok(s) => s,
        Err(e) => format!("{}", e),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let tx_hex_string = read_tx();
    let tx = bitcoin::consensus::deserialize::<Transaction>(&hex::decode(&tx_hex_string)?)?;
    let txid = tx.compute_txid();

    let client = MempoolClient::new(Network::Bitcoin);

    match client.broadcast_transaction(&tx_hex_string.clone()).await {
        Ok(txid) => {
            info!("Transaction broadcast successfully! TXID: {}", txid);
        }
        Err(e) => {
            eprintln!("Failed to broadcast transaction: {:?}", e);
        }
    }

    let mut seed_addrs = Vec::new();
    let mut seed_tasks = JoinSet::new();

    for seed_host in DNS_SEEDS {
        info!("fetching addrs from {:?}", seed_host);

        let host = seed_host.to_owned();

        seed_tasks.spawn(async move {
            let lookup = lookup_host(format!("{}:8333", seed_host));

            match timeout(Duration::from_secs(2), lookup).await {
                Ok(Ok(addrs)) => {
                    let addrs: Vec<_> = addrs.collect();
                    Ok((host, addrs))
                }
                Ok(Err(e)) => Err(anyhow::Error::new(e)),
                Err(_) => {
                    error!("Timeout while looking up {}", seed_host);
                    Err(anyhow::anyhow!("Timeout"))
                }
            }
        });
    }

    while let Some(res) = seed_tasks.join_next().await {
        match res {
            Ok(Ok((host, addresses))) => {
                info!("{} returned {} IPs", host, addresses.len());
                seed_addrs.extend(addresses);
            }
            Ok(Err(crawl_error)) => {
                error!("dns seed node error: {crawl_error},");
            }
            Err(join_error) => {
                error!("join error during dns seed: {join_error}");
            }
        }
    }

    info!("found {} seed node addresses", seed_addrs.len());
    seed_addrs.shuffle(&mut rand::rng());

    info!("time to blast some nodes with pigeon poop! 🐦💩");

    info!("blasting tx {:?} to libre relay nodes...", txid);

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_DELIVERIES));

    let mut libre_peers = HashSet::<NetworkAddress>::new();
    let mut crawl_tasks = JoinSet::new();

    for addr in seed_addrs.clone() {
        crawl_tasks.spawn({
            let sem = semaphore.clone();
            async move {
                let _permit = sem.acquire_owned().await?;
                crawl_seed_node(&addr).await
            }
        });
    }

    while let Some(res) = crawl_tasks.join_next().await {
        match res {
            Ok(Ok(addresses)) => {
                libre_peers.extend(addresses);
            }
            Ok(Err(crawl_error)) => {
                error!("crawl seed node error: {}", crawl_error);
            }
            Err(join_error) => {
                error!("join error during crawl: {}", join_error);
            }
        }
    }

    info!(
        "found {} addresses advertising the libre relay service flag",
        libre_peers.len()
    );

    //connect to tor
    info!("Bootstrapping Tor client...");
    let config = TorClientConfig::builder().build()?;
    let tor_client = Arc::new(TorClient::create_bootstrapped(config).await?);

    let common_token = IsolationToken::no_isolation();
    let mut prefs = StreamPrefs::new();
    prefs.set_isolation(common_token);

    let mut poop_delivery_tasks = JoinSet::new();
    for peer_addr in libre_peers.clone() {
        let tx_clone = tx.clone();
        let permit = semaphore.clone().acquire_owned().await?;
        let tor_client = tor_client.clone();
        let peer_addr_cloned = peer_addr.clone();
        let prefs = prefs.clone();
        poop_delivery_tasks.spawn(async move {
            let _permit_guard = permit;
            match deliver_poop_tx(peer_addr_cloned.clone(), tx_clone, tor_client, prefs).await {
                Ok(true) => Ok(peer_addr_cloned.clone()),
                Ok(false) => Err((
                    peer_addr_cloned.clone(),
                    "No Tx confirmation, rejected, or skipped by peer.".to_string(),
                )),
                Err(e) => Err((peer_addr_cloned.clone(), e.to_string())),
            }
        });
    }

    let mut success_count = 0;

    while let Some(res) = poop_delivery_tasks.join_next().await {
        match res {
            Ok(Ok(_)) => {
                success_count += 1;
            }
            Ok(Err((_, _))) => (),
            Err(join_error) => {
                error!("join error during poop delivery: {}", join_error);
                if join_error.is_panic() {
                    error!("a poop delivery task panicked!");
                }
            }
        }
    }

    if success_count == 0 {
        error!(
            "No libre relay nodes accepted the transaction. TX {} may already be in a block or its invalid.",
            txid
        );
        return Ok(());
    }

    info!(
        "TX: {:?} blasted to {} libre relay nodes. GLHF",
        txid, success_count,
    );

    Ok(())
}
