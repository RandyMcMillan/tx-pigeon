//use anyhow::Result;
use arti_client::{IsolationToken, StreamPrefs, TorClient, TorClientConfig};
use bitcoin::{Network, Transaction};
use tx_pigeon::{
    Args, DNS_SEEDS, MAX_CONCURRENT_DELIVERIES, NetworkAddress, crawl_seed_node, deliver_poop_tx,
};

use clap::Parser;
use rand::seq::SliceRandom;

use rust_mempool::MempoolClient;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{net::lookup_host, sync::Semaphore, task::JoinSet, time::timeout};
use tracing::{error, info, trace, warn};

use anyhow::{Context, Result};
use reqwest::{Client, Error, Response};
use serde::{Deserialize, Serialize};

// Base URL for the Mempool.space API
const MEMPOOL_SPACE_API_BASE: &str = "https://mempool.space/api";

async fn fetch_txids() -> Vec<MempoolTransaction> {
    // Create an HTTP client
    let client = Client::new();

    // Construct the full URL
    let url = format!("{}/mempool/recent", MEMPOOL_SPACE_API_BASE);
    info!("\n{}", url);

    // Make the GET request and get the response
    let response = client.get(&url).send().await.expect("");

    // If not, reqwest::Response::error_for_status() will convert HTTP errors
    // into a reqwest::Error which can be propagated by `?`.
    let response = response.error_for_status().expect("");

    // Deserialize the JSON response into a Vec of MempoolTransaction structs
    let recent_txs: Vec<MempoolTransaction> = response
        .json()
        .await
        .context("Failed to parse JSON response from mempool.space")
        .expect("");

    // Print the number of transactions received
    info!(
        "\nSuccessfully fetched {} recent mempool transactions.",
        recent_txs.len()
    );

    ////let txs = Vec<String>;
    //// Print details of the first few transactions for demonstration
    for (i, tx) in recent_txs.iter().take(10).enumerate() {
        trace!("\n--- Transaction {} ---", i + 1);
        trace!("  TXID: {}", tx.txid);
        let _ = get_tx_hex(tx.txid.clone());
        trace!("  Fee: {} satoshis", tx.fee);
        //println!("  Size: {} bytes", tx.size);
        trace!("  VSize: {} vbytes", tx.vsize);
        trace!("  Value: {} satoshis", tx.value);
    }

    // You can also print the entire JSON structure if you want to inspect it
    // let raw_json: serde_json::Value = serde_json::from_str(&response.text().await?)?;
    // println!("\nRaw JSON response (first 1000 chars):\n{}", &serde_json::to_string_pretty(&raw_json)?[..1000]);

    recent_txs
}

pub async fn get_tx_hex(txid: String) -> Result<Response, Error> {
    //curl -sSL "https://mempool.space/api/tx/15e10745f15593a899cef391191bdd3d7c12412cc4696b7bcb669d0feadc8521/hex"
    // Create an HTTP client
    let client = Client::new();

    warn!("\ntxid={}", txid);
    // Construct the full URL
    let url = format!("{}/tx/{}/hex", MEMPOOL_SPACE_API_BASE, txid);
    warn!("Fetching data from:\n{}", url);

    // Make the GET request and get the response
    let response = client.get(&url).send().await.expect("");
    //println!("\nresponse:\n{:?}", response.text().await);

    // Check if the request was successful (HTTP status 200 OK)
    // If not, reqwest::Response::error_for_status() will convert HTTP errors
    // into a reqwest::Error which can be propagated by `?`.
    //let response = response.error_for_status()?;

    Ok(response)
}

async fn tx_obfuscation() -> Result<()> {
    let client = MempoolClient::new(Network::Bitcoin);
    let result = fetch_txids().await;
    for tx in result {
        warn!("\ntx.txid={}", tx.txid);
        let res = get_tx_hex(tx.txid).await?;
        //info!("\nres={:?}", res.text().await.expect("").clone());
        let txid = res.text().await.expect("").clone().to_string();
        //info!("\ntxid={:?}", txid.to_string());
        match client.broadcast_transaction(&txid).await {
            Ok(txid) => {
                warn!("broadcast success!\ntxid:{}", txid);
            }
            Err(e) => {
                eprintln!("Failed to broadcast transaction:\n{:?}", e);
            }
        }
    }

    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let args = Args::parse();
    let tx_hex_string = args.tx.clone();
    let tx = bitcoin::consensus::deserialize::<Transaction>(&hex::decode(tx_hex_string)?)?;
    let txid = tx.compute_txid();

    let _ = tx_obfuscation().await;

    let client = MempoolClient::new(Network::Bitcoin);
    match client.broadcast_transaction(&args.tx.clone()).await {
        Ok(txid) => {
            info!("broadcast success!\ntxid:{}", txid);
        }
        Err(e) => {
            eprintln!("Failed to broadcast transaction:\n{:?}", e);
        }
    }

    let _ = tx_obfuscation().await;

    let mut seed_addrs = Vec::new();
    let mut seed_tasks = JoinSet::new();

    for seed_host in DNS_SEEDS {
        info!("fetching addrs from:\n{:?}", seed_host);

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
                info!("\n{} returned {} IPs", host, addresses.len());
                seed_addrs.extend(addresses);
            }
            Ok(Err(crawl_error)) => {
                error!("\ndns seed node error: {crawl_error},");
            }
            Err(join_error) => {
                error!("\njoin error during dns seed: {join_error}");
            }
        }
    }

    info!("\nfound {} seed node addresses", seed_addrs.len());
    seed_addrs.shuffle(&mut rand::rng());

    info!("\ntime to blast some nodes with pigeon poop! 🐦💩");

    info!("\nblasting tx {:?} to libre relay nodes...", txid);

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

// Struct to represent a single transaction from the /mempool/recent endpoint
// We only need the fields we care about, based on the API response structure.
// Looking at the Mempool.space API docs, these are common fields for recent txs.
#[derive(Debug, Deserialize, Serialize)]
struct MempoolTransaction {
    txid: String,
    fee: u64,
    //    size: u64, // Virtual size in bytes
    vsize: u64, // Virtual size (for SegWit transactions)
    value: u64, // Total output value in satoshis
                // Add more fields if you need them, e.g., status, version, locktime, vin, vout
                // For a quick check of what fields are available, you can curl the endpoint
                // and inspect the JSON.
                // e.g., curl https://mempool.space/api/mempool/recent | head -n 20
}
