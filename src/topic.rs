use anyhow::{Context, Result};
use bitcoin::{Transaction, consensus::Decodable, io::Cursor};
use futures::StreamExt;
use libp2p::{
    Multiaddr, Swarm, SwarmBuilder, autonat, dcutr, gossipsub, mdns, relay,
    swarm::{NetworkBehaviour, SwarmEvent},
};
use sha3::{Digest, Sha3_256};
use std::collections::HashSet;
use tokio::sync::mpsc;
use tokio::signal;
use tokio::time::{Duration, sleep, interval};
use tracing::{debug, info, warn};

use crate::mempool::{fetch_recent_tx_hexes, fetch_recent_txids};
use crate::blast_transaction_hex;

const BITCOIN_PIGEON_TOPIC: &str = "bitcoin-pigeon";

#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
struct TopicBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
    relay_client: relay::client::Behaviour,
    autonat: autonat::Behaviour,
    dcutr: dcutr::Behaviour,
}

pub async fn run_topic_network(tx_hex: Option<String>, tor_only: bool) -> Result<()> {
    let (mut swarm, topic) = build_topic_swarm()?;
    let mut seen_txs = HashSet::new();
    if let Some(tx_hex) = tx_hex {
        publish_topic_tx("topic-cli", &mut swarm, &topic, tx_hex, tor_only, &mut seen_txs).await?;
    }

    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(TopicBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source,
                    message_id,
                    message,
                })) => {
                    info!(?propagation_source, %message_id, "received bitcoin-pigeon topic message");
                    handle_topic_tx("topic-cli", &message.data, tor_only, &mut seen_txs).await?;
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id, topic })) => {
                    info!(?peer_id, %topic, "peer subscribed to bitcoin-pigeon");
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::Gossipsub(gossipsub::Event::Unsubscribed { peer_id, topic })) => {
                    info!(?peer_id, %topic, "peer unsubscribed from bitcoin-pigeon");
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::Gossipsub(gossipsub::Event::GossipsubNotSupported { peer_id })) => {
                    warn!(?peer_id, "peer does not support gossipsub");
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::RelayClient(event)) => {
                    info!(?event, "relay client event");
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::Autonat(event)) => {
                    info!(?event, "autonat event");
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::Dcutr(event)) => {
                    info!(?event, "hole punch event");
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, addr) in list {
                        info!(?peer_id, ?addr, "mdns discovered peer");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        if let Err(err) = swarm.dial(addr.clone()) {
                            warn!(?peer_id, ?addr, "failed to dial discovered peer: {err}");
                        }
                    }
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, addr) in list {
                        info!(?peer_id, ?addr, "mdns expired peer");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    }
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    info!(?address, "listening for bitcoin-pigeon peers");
                }
                other => {
                    info!(?other, "swarm event");
                }
            },
            _ = signal::ctrl_c() => {
                info!("shutting down bitcoin-pigeon topic node");
                break;
            }
        }
    }

    Ok(())
}

pub async fn run_gossip_client(
    label: impl Into<String>,
    tor_only: bool,
    show_local: bool,
    show_remote: bool,
) -> Result<()> {
    let label = label.into();
    let (mut swarm, _topic) = build_topic_swarm()?;
    let mut seen_txs = HashSet::new();
    let mut mempool_tick = interval(Duration::from_secs(10));

    info!(
        %label,
        show_local,
        show_remote,
        "gossip client mode selection"
    );

    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(TopicBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source,
                    message_id,
                    message,
                })) if show_remote => {
                    let txid = decode_transaction_bytes(&message.data)
                        .map(|tx| tx.compute_txid().to_string())
                        .unwrap_or_else(|_| hex::encode(&message.data));
                    info!(%label, ?propagation_source, %message_id, %txid, "gossip client observed bitcoin-pigeon message");
                    if let Err(err) = observe_topic_tx(&label, &message.data, tor_only, &mut seen_txs).await {
                        warn!(error = %err, "failed to observe topic tx");
                    }
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id, topic })) if show_remote => {
                    info!(%label, ?peer_id, %topic, "gossip client subscribed");
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::RelayClient(event)) if show_remote => {
                    info!(%label, ?event, "gossip client relay event");
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::Autonat(event)) if show_remote => {
                    info!(%label, ?event, "gossip client autonat event");
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::Dcutr(event)) if show_remote => {
                    info!(%label, ?event, "gossip client hole punch event");
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::Mdns(mdns::Event::Discovered(list))) if show_remote => {
                    for (peer_id, addr) in list {
                        debug!(%label, ?peer_id, ?addr, "gossip client discovered peer");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        if let Err(err) = swarm.dial(addr.clone()) {
                            warn!(?peer_id, ?addr, "gossip client failed to dial discovered peer: {err}");
                        }
                    }
                }
                SwarmEvent::Behaviour(TopicBehaviourEvent::Mdns(mdns::Event::Expired(list))) if show_remote => {
                    for (peer_id, addr) in list {
                        info!(%label, ?peer_id, ?addr, "gossip client expired peer");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    }
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    info!(%label, ?address, "gossip client listening for bitcoin-pigeon peers");
                }
                other => {
                    if show_remote {
                        info!(%label, ?other, "gossip client swarm event");
                    }
                }
            },
            _ = mempool_tick.tick(), if show_local => {
                if let Err(err) = poll_recent_transactions(&label, tor_only, &mut seen_txs).await {
                    warn!(%label, error = %err, "gossip client failed to poll recent transactions");
                }
            },
            _ = signal::ctrl_c() => {
                info!(%label, "shutting down bitcoin-pigeon gossip client");
                break;
            }
        }
    }

    Ok(())
}

#[derive(Clone)]
pub struct TopicRelayHandle {
    publish_tx: mpsc::Sender<String>,
}

impl TopicRelayHandle {
    pub async fn publish(&self, tx_hex: String) -> Result<()> {
        self.publish_tx
            .send(tx_hex)
            .await
            .context("failed to queue topic publish")
    }
}

pub async fn spawn_topic_network(label: impl Into<String>, tor_only: bool) -> Result<TopicRelayHandle> {
    let label = label.into();
    let (mut swarm, topic) = build_topic_swarm()?;
    let (publish_tx, mut publish_rx) = mpsc::channel::<String>(64);
    let mut seen_txs = HashSet::new();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                event = swarm.select_next_some() => match event {
                    SwarmEvent::Behaviour(TopicBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                        propagation_source,
                        message_id,
                        message,
                    })) => {
                        let txid = decode_transaction_bytes(&message.data)
                            .map(|tx| tx.compute_txid().to_string())
                            .unwrap_or_else(|_| hex::encode(&message.data));
                        info!(%label, ?propagation_source, %message_id, %txid, "received bitcoin-pigeon topic message");
                        if let Err(err) = handle_topic_tx(&label, &message.data, tor_only, &mut seen_txs).await {
                            warn!(error = %err, "failed to handle topic tx");
                        }
                    }
                    SwarmEvent::Behaviour(TopicBehaviourEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id, topic })) => {
                        info!(?peer_id, %topic, "peer subscribed to bitcoin-pigeon");
                    }
                    SwarmEvent::Behaviour(TopicBehaviourEvent::Gossipsub(gossipsub::Event::Unsubscribed { peer_id, topic })) => {
                        info!(?peer_id, %topic, "peer unsubscribed from bitcoin-pigeon");
                    }
                    SwarmEvent::Behaviour(TopicBehaviourEvent::Gossipsub(gossipsub::Event::GossipsubNotSupported { peer_id })) => {
                        warn!(?peer_id, "peer does not support gossipsub");
                    }
                    SwarmEvent::Behaviour(TopicBehaviourEvent::RelayClient(event)) => {
                        info!(%label, ?event, "gossip client relay event");
                    }
                    SwarmEvent::Behaviour(TopicBehaviourEvent::Autonat(event)) => {
                        info!(%label, ?event, "gossip client autonat event");
                    }
                    SwarmEvent::Behaviour(TopicBehaviourEvent::Dcutr(event)) => {
                        info!(%label, ?event, "gossip client hole punch event");
                    }
                    SwarmEvent::Behaviour(TopicBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer_id, addr) in list {
                            info!(%label, ?peer_id, ?addr, "mdns discovered peer");
                            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                            if let Err(err) = swarm.dial(addr.clone()) {
                                warn!(?peer_id, ?addr, "failed to dial discovered peer: {err}");
                            }
                        }
                    }
                    SwarmEvent::Behaviour(TopicBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                        for (peer_id, addr) in list {
                            info!(%label, ?peer_id, ?addr, "mdns expired peer");
                            swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        }
                    }
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!(%label, ?address, "listening for bitcoin-pigeon peers");
                    }
                    other => {
                        info!(%label, ?other, "swarm event");
                    }
                },
                maybe_tx_hex = publish_rx.recv() => {
                    match maybe_tx_hex {
                        Some(tx_hex) => {
                            if let Err(err) = publish_topic_tx(&label, &mut swarm, &topic, tx_hex, tor_only, &mut seen_txs).await {
                                warn!(error = %err, "failed to publish queued topic tx");
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    });

    Ok(TopicRelayHandle { publish_tx })
}

fn build_topic_swarm() -> Result<(Swarm<TopicBehaviour>, gossipsub::IdentTopic)> {
    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            Default::default(),
            (libp2p::tls::Config::new, libp2p::noise::Config::new),
            libp2p::yamux::Config::default,
        )?
        .with_relay_client(
            (libp2p::tls::Config::new, libp2p::noise::Config::new),
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|keypair, relay_client| {
            let peer_id = keypair.public().to_peer_id();
            debug!("peer_id={}", peer_id);
            let topic_name = BITCOIN_PIGEON_TOPIC.to_owned();

            let mut config = gossipsub::ConfigBuilder::default();
            config.validation_mode(gossipsub::ValidationMode::Anonymous);
            config.message_id_fn(move |message: &gossipsub::Message| {
                tx_message_id(message, &topic_name)
            });
            let config = config
                .build()
                .map_err(|e| anyhow::anyhow!("failed to build gossipsub config: {e}"))?;

            let gossipsub = gossipsub::Behaviour::new(gossipsub::MessageAuthenticity::Anonymous, config)
                .map_err(|e| anyhow::anyhow!("failed to build gossipsub behaviour: {e}"))?;
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)
                .map_err(|e| anyhow::anyhow!("failed to build mdns behaviour: {e}"))?;
            let autonat = autonat::Behaviour::new(peer_id, autonat::Config::default());
            let dcutr = dcutr::Behaviour::new(peer_id);

            Ok(TopicBehaviour {
                gossipsub,
                mdns,
                relay_client,
                autonat,
                dcutr,
            })
        })?
        .build();

    let topic = gossipsub::IdentTopic::new(BITCOIN_PIGEON_TOPIC);
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&topic)
        .context("failed to subscribe to bitcoin-pigeon topic")?;

    swarm
        .listen_on("/ip4/0.0.0.0/tcp/0".parse::<Multiaddr>()?)
        .context("failed to start listening")?;

    Ok((swarm, topic))
}

async fn publish_topic_tx(
    label: &str,
    swarm: &mut Swarm<TopicBehaviour>,
    topic: &gossipsub::IdentTopic,
    tx_hex: String,
    tor_only: bool,
    seen_txs: &mut HashSet<bitcoin::Txid>,
) -> Result<()> {
    let tx = decode_transaction_hex(&tx_hex)?;
    let txid = tx.compute_txid();
    let raw_bytes = hex::decode(&tx_hex).context("failed to decode transaction hex")?;

    info!(%label, %txid, "publishing transaction to bitcoin-pigeon topic");
    log_deserialized_transaction(label, &tx);
    let published = publish_with_retry(swarm, topic, raw_bytes.clone(), txid).await?;
    if !published {
        warn!(
            %txid,
            "publishing tx on topic never found peers; continuing with local processing"
        );
    }

    handle_topic_tx(label, &raw_bytes, tor_only, seen_txs).await
}

async fn publish_with_retry(
    swarm: &mut Swarm<TopicBehaviour>,
    topic: &gossipsub::IdentTopic,
    raw_bytes: Vec<u8>,
    txid: bitcoin::Txid,
) -> Result<bool> {
    const MAX_ATTEMPTS: usize = 30;

    for attempt in 1..=MAX_ATTEMPTS {
        match swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic.clone(), raw_bytes.clone())
        {
            Ok(_) => {
                info!(%txid, attempt, "published bitcoin-pigeon topic transaction");
                return Ok(true);
            }
            Err(err) if err.to_string().contains("InsufficientPeers") => {
                warn!(
                    %txid,
                    attempt,
                    max_attempts = MAX_ATTEMPTS,
                    "topic swarm has no peers yet; retrying publish"
                );
                sleep(Duration::from_secs(1)).await;
            }
            Err(err) => {
                return Err(err).context("failed to publish tx on topic");
            }
        }
    }

    Ok(false)
}

async fn handle_topic_tx(
    label: &str,
    data: &[u8],
    tor_only: bool,
    seen_txs: &mut HashSet<bitcoin::Txid>,
) -> Result<()> {
    let tx = decode_transaction_bytes(data)?;
    let txid = tx.compute_txid();

    log_deserialized_transaction(label, &tx);
    if !seen_txs.insert(txid) {
        info!(%label, %txid, "already processed bitcoin-pigeon tx");
        return Ok(());
    }

    log_local_mempool(label).await?;

    let tx_hex = hex::encode(data);
    info!(%label, %txid, "rebroadcasting received tx from bitcoin-pigeon topic");
    info!(%label, %txid, "blasting transaction from bitcoin-pigeon topic");
    blast_transaction_hex(&tx_hex, tor_only, true).await?;
    Ok(())
}

async fn observe_topic_tx(
    label: &str,
    data: &[u8],
    tor_only: bool,
    seen_txs: &mut HashSet<bitcoin::Txid>,
) -> Result<()> {
    let tx = decode_transaction_bytes(data)?;
    let txid = tx.compute_txid();

    log_deserialized_transaction(label, &tx);
    if !seen_txs.insert(txid) {
        info!(%label, %txid, "already observed bitcoin-pigeon tx");
        return Ok(());
    }

    log_local_mempool(label).await?;
    info!(%label, %txid, "observed bitcoin-pigeon topic tx");
    if tor_only {
        info!(%label, %txid, "tor-only gossip watch active");
    }

    Ok(())
}

async fn poll_recent_transactions(
    label: &str,
    tor_only: bool,
    seen_txs: &mut HashSet<bitcoin::Txid>,
) -> Result<()> {
    const RECENT_LIMIT: usize = 5;

    let txs = fetch_recent_tx_hexes(RECENT_LIMIT).await?;
    info!(
        %label,
        count = txs.len(),
        "gossip client polling recent mempool transactions"
    );

    for (txid, tx_hex) in txs {
        let tx = decode_transaction_hex(&tx_hex)?;
        let decoded_txid = tx.compute_txid();

        if decoded_txid.to_string() != txid {
            warn!(
                %label,
                expected_txid = %txid,
                decoded_txid = %decoded_txid,
                "gossip client saw mismatched transaction ids while polling mempool"
            );
        }

        log_deserialized_transaction(label, &tx);
        if !seen_txs.insert(decoded_txid) {
            info!(%label, %decoded_txid, "already polled bitcoin-pigeon tx");
            continue;
        }

        log_local_mempool(label).await?;
        info!(%label, %decoded_txid, "observed recent mempool tx");
        if tor_only {
            info!(%label, %decoded_txid, "tor-only gossip watch active");
        }
    }

    Ok(())
}

fn log_deserialized_transaction(label: &str, tx: &Transaction) {
    let txid = tx.compute_txid();
    println!(
        "[{label}] deserialized tx {txid} version={:?} inputs={} outputs={} lock_time={:?}",
        tx.version,
        tx.input.len(),
        tx.output.len(),
        tx.lock_time
    );
    info!(
        %label,
        %txid,
        version = ?tx.version,
        input_count = tx.input.len(),
        output_count = tx.output.len(),
        lock_time = ?tx.lock_time,
        "gossip client deserialized tx"
    );

    for (index, input) in tx.input.iter().enumerate() {
        println!(
            "[{label}] tx {txid} input {}/{} prevout={:?} sequence={:?} script_sig_len={} witness_items={}",
            index + 1,
            tx.input.len(),
            input.previous_output,
            input.sequence,
            input.script_sig.len(),
            input.witness.len()
        );
        info!(
            %label,
            %txid,
            input_index = index,
            prevout = ?input.previous_output,
            sequence = ?input.sequence,
            script_sig_len = input.script_sig.len(),
            witness_items = input.witness.len(),
            "gossip client tx input"
        );
    }

    for (index, output) in tx.output.iter().enumerate() {
        println!(
            "[{label}] tx {txid} output {}/{} value={} sats script_pubkey={}",
            index + 1,
            tx.output.len(),
            output.value.to_sat(),
            output.script_pubkey
        );
        info!(
            %label,
            %txid,
            output_index = index,
            value = output.value.to_sat(),
            script_pubkey = %output.script_pubkey,
            "gossip client tx output"
        );
    }
}

async fn log_local_mempool(label: &str) -> Result<()> {
    const LOCAL_MEMPOOL_LIMIT: usize = 5;

    let txids = fetch_recent_txids(LOCAL_MEMPOOL_LIMIT).await?;
    info!(
        %label,
        count = txids.len(),
        "iterating local mempool after topic receipt"
    );

    for (index, txid) in txids.iter().enumerate() {
        info!(
            %label,
            index = index + 1,
            total = txids.len(),
            %txid,
            "local mempool tx"
        );
    }

    Ok(())
}

fn decode_transaction_hex(tx_hex: &str) -> Result<Transaction> {
    let bytes = hex::decode(tx_hex).context("failed to decode transaction hex")?;
    decode_transaction_bytes(&bytes)
}

fn decode_transaction_bytes(bytes: &[u8]) -> Result<Transaction> {
    let mut cursor = Cursor::new(bytes);
    let tx = Transaction::consensus_decode(&mut cursor).context("failed to decode transaction")?;
    Ok(tx)
}

fn tx_message_id(message: &gossipsub::Message, topic_name: &str) -> gossipsub::MessageId {
    let txid = decode_transaction_bytes(&message.data)
        .map(|tx| tx.compute_txid().to_string())
        .unwrap_or_else(|_| {
            let digest = Sha3_256::digest(&message.data);
            format!("{topic_name}:{}", hex::encode(digest))
        });

    gossipsub::MessageId::from(txid)
}
