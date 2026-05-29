use anyhow::Result;
use reqwest::{Client, Proxy};
use serde::Deserialize;
use std::{collections::HashSet, time::Duration};
use tracing::{info, warn};

const MEMPOOL_RECENT_URL: &str = "https://mempool.space/api/mempool/recent";
const MEMPOOL_ONION_URL: &str =
    "http://mempoolhqx4isw62xs7abwphsq7ldayuidyx2v2oethdhhj6mlo2r6ad.onion/api/mempool/recent";

#[derive(Debug, Deserialize)]
struct MempoolRecentTx {
    txid: String,
}

pub async fn fetch_recent_txids(limit: usize) -> Result<Vec<String>> {
    let clearnet_client = Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let tor_client = build_tor_client().ok();

    let clearnet_fut = fetch_from_source(&clearnet_client, MEMPOOL_RECENT_URL, "mempool.space");
    let onion_fut = async {
        match tor_client.as_ref() {
            Some(client) => Some(fetch_from_source(client, MEMPOOL_ONION_URL, "mempool onion").await),
            None => None,
        }
    };

    let (clearnet_result, onion_result) = tokio::join!(clearnet_fut, onion_fut);

    let mut merged = Vec::new();
    let mut seen = HashSet::new();

    merge_recent("mempool.space", clearnet_result, limit, &mut seen, &mut merged);

    if let Some(onion_result) = onion_result {
        merge_recent("mempool onion", onion_result, limit, &mut seen, &mut merged);
    } else {
        warn!("mempool onion fetch skipped because no Tor SOCKS proxy was available");
    }

    Ok(merged)
}

async fn fetch_from_source(client: &Client, url: &str, label: &str) -> Result<Vec<String>> {
    let txids = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<MempoolRecentTx>>()
        .await?
        .into_iter()
        .map(|entry| entry.txid)
        .collect();

    info!(source = label, "fetched recent mempool txids");
    Ok(txids)
}

fn merge_recent(
    label: &str,
    result: Result<Vec<String>>,
    limit: usize,
    seen: &mut HashSet<String>,
    merged: &mut Vec<String>,
) {
    match result {
        Ok(txids) => {
            for txid in txids.into_iter().take(limit) {
                if merged.len() >= limit {
                    break;
                }
                if seen.insert(txid.clone()) {
                    merged.push(txid);
                }
            }
        }
        Err(err) => {
            warn!(source = label, error = %err, "failed to fetch recent mempool txids");
        }
    }
}

fn build_tor_client() -> Result<Client, reqwest::Error> {
    let proxy_url = std::env::var("TOR_SOCKS_PROXY")
        .ok()
        .unwrap_or_else(|| "socks5h://127.0.0.1:9050".to_string());

    Client::builder()
        .timeout(Duration::from_secs(20))
        .proxy(Proxy::all(&proxy_url)?)
        .build()
}
