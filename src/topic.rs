use anyhow::{Context, Result};
use bitcoin::{Transaction, consensus::Decodable, io::Cursor};
use futures::StreamExt;
use libp2p::{
    Multiaddr, Swarm, SwarmBuilder, gossipsub, mdns,
    swarm::{NetworkBehaviour, SwarmEvent},
};
use sha3::{Digest, Sha3_256};
use std::collections::HashSet;
use tokio::signal;
use tokio::time::{Duration, sleep};
use tracing::{debug, info, warn};

use crate::blast_transaction_hex;

const BITCOIN_PIGEON_TOPIC: &str = "bitcoin-pigeon";

#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
struct TopicBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

pub async fn run_topic_network(tx_hex: Option<String>, tor_only: bool) -> Result<()> {
    let mut seen_txs = HashSet::new();
    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            Default::default(),
            (libp2p::tls::Config::new, libp2p::noise::Config::new),
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|keypair| {
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

            Ok(TopicBehaviour { gossipsub, mdns })
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

    if let Some(tx_hex) = tx_hex {
        publish_topic_tx(&mut swarm, &topic, tx_hex, tor_only, &mut seen_txs).await?;
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
                    handle_topic_tx(&message.data, tor_only, &mut seen_txs).await?;
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

async fn publish_topic_tx(
    swarm: &mut Swarm<TopicBehaviour>,
    topic: &gossipsub::IdentTopic,
    tx_hex: String,
    tor_only: bool,
    seen_txs: &mut HashSet<bitcoin::Txid>,
) -> Result<()> {
    let tx = decode_transaction_hex(&tx_hex)?;
    let txid = tx.compute_txid();
    let raw_bytes = hex::decode(&tx_hex).context("failed to decode transaction hex")?;

    info!(%txid, "publishing transaction to bitcoin-pigeon topic");
    let published = publish_with_retry(swarm, topic, raw_bytes.clone(), txid).await?;
    if !published {
        warn!(
            %txid,
            "publishing tx on topic never found peers; continuing with local processing"
        );
    }

    handle_topic_tx(&raw_bytes, tor_only, seen_txs).await
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
    data: &[u8],
    tor_only: bool,
    seen_txs: &mut HashSet<bitcoin::Txid>,
) -> Result<()> {
    let tx = decode_transaction_bytes(data)?;
    let txid = tx.compute_txid();

    if !seen_txs.insert(txid) {
        info!(%txid, "already processed bitcoin-pigeon tx");
        return Ok(());
    }

    let tx_hex = hex::encode(data);
    info!(%txid, "blasting transaction from bitcoin-pigeon topic");
    blast_transaction_hex(&tx_hex, tor_only, true).await?;
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
