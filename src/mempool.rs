use anyhow::Result;
use reqwest::{Client, Proxy};
use serde::Deserialize;
use std::{collections::HashSet, time::Duration};
use tracing::{debug, info, warn};

const MEMPOOL_RECENT_URL: &str = "https://mempool.space/api/mempool/recent";
const BITCOIN_GOB_SV_RECENT_URL: &str = "https://bitcoin.gob.sv/api/mempool/recent";
const MEMPOOL_TX_HEX_URL: &str = "https://mempool.space/api/tx/{txid}/hex";
const MEMPOOL_ONION_URL: &str =
    "http://mempoolhqx4isw62xs7abwphsq7ldayuidyx2v2oethdhhj6mlo2r6ad.onion/api/mempool/recent";
const MEMPOOL_ONION_TX_HEX_URL: &str =
    "http://mempoolhqx4isw62xs7abwphsq7ldayuidyx2v2oethdhhj6mlo2r6ad.onion/api/tx/{txid}/hex";

#[derive(Debug, Deserialize)]
struct MempoolRecentTx {
    txid: String,
}

pub async fn fetch_recent_txids(limit: usize) -> Result<Vec<String>> {
    let clearnet_client = Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let tor_client = build_tor_client().ok();

    info!(
        source = "mempool.space",
        url = MEMPOOL_RECENT_URL,
        limit,
        "requesting recent mempool txids"
    );
    info!(
        source = "bitcoin.gob.sv",
        url = BITCOIN_GOB_SV_RECENT_URL,
        limit,
        "requesting recent mempool txids"
    );
    let clearnet_fut = fetch_from_source(&clearnet_client, MEMPOOL_RECENT_URL, "mempool.space");
    let gob_sv_fut =
        fetch_from_source(&clearnet_client, BITCOIN_GOB_SV_RECENT_URL, "bitcoin.gob.sv");
    let onion_fut = async {
        match tor_client.as_ref() {
            Some(client) => {
                info!(
                    source = "mempool onion",
                    url = MEMPOOL_ONION_URL,
                    limit,
                    "requesting recent mempool txids"
                );
                Some(fetch_from_source(client, MEMPOOL_ONION_URL, "mempool onion").await)
            }
            None => None,
        }
    };

    let (clearnet_result, gob_sv_result, onion_result) =
        tokio::join!(clearnet_fut, gob_sv_fut, onion_fut);

    let mut merged = Vec::new();
    let mut seen = HashSet::new();

    merge_recent("mempool.space", clearnet_result, limit, &mut seen, &mut merged);
    merge_recent("bitcoin.gob.sv", gob_sv_result, limit, &mut seen, &mut merged);

    if let Some(onion_result) = onion_result {
        merge_recent("mempool onion", onion_result, limit, &mut seen, &mut merged);
    } else {
        debug!("mempool onion fetch skipped because no Tor SOCKS proxy was available");
    }

    info!(
        merged_count = merged.len(),
        limit,
        "combined recent mempool txids from available sources"
    );
    Ok(merged)
}

pub async fn fetch_recent_tx_hexes(limit: usize) -> Result<Vec<(String, String)>> {
    let txids = fetch_recent_txids(limit).await?;
    let clearnet_client = Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let tor_client = build_tor_client().ok();
    let mut txs = Vec::new();

    for txid in txids {
        match fetch_tx_hex(&clearnet_client, &txid, MEMPOOL_TX_HEX_URL, "mempool.space").await {
            Ok(tx_hex) => txs.push((txid, tx_hex)),
            Err(err) => {
                warn!(txid = %txid, error = %err, "clearnet tx hex fetch failed");
                if let Some(client) = tor_client.as_ref() {
                    match fetch_tx_hex(client, &txid, MEMPOOL_ONION_TX_HEX_URL, "mempool onion").await {
                        Ok(tx_hex) => txs.push((txid, tx_hex)),
                        Err(onion_err) => {
                            debug!(txid = %txid, error = %onion_err, "optional onion tx hex fetch failed");
                        }
                    }
                }
            }
        }
    }

    info!(count = txs.len(), "fetched recent transaction hexes");
    Ok(txs)
}

async fn fetch_from_source(client: &Client, url: &str, label: &str) -> Result<Vec<String>> {
    let txids: Vec<String> = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<MempoolRecentTx>>()
        .await?
        .into_iter()
        .map(|entry| entry.txid)
        .collect();

    info!(
        source = label,
        url,
        count = txids.len(),
        "fetched recent mempool txids"
    );
    Ok(txids)
}

async fn fetch_tx_hex(client: &Client, txid: &str, template: &str, label: &str) -> Result<String> {
    let url = template.replace("{txid}", txid);
    let tx_hex = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    info!(source = label, txid, url, "fetched transaction hex");
    Ok(tx_hex.trim().to_string())
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
            let before = merged.len();
            for txid in txids.into_iter().take(limit) {
                if merged.len() >= limit {
                    break;
                }
                if seen.insert(txid.clone()) {
                    merged.push(txid);
                }
            }
            info!(
                source = label,
                returned = merged.len().saturating_sub(before),
                total = merged.len(),
                limit,
                "merged recent mempool txids"
            );
        }
        Err(err) => {
            if label == "mempool onion" {
                debug!(
                    source = label,
                    error = %err,
                    "optional recent mempool txids source unavailable"
                );
            } else {
                warn!(source = label, error = %err, "failed to fetch recent mempool txids");
            }
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
