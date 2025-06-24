use anyhow::Result;
use arti_client::{StreamPrefs, TorClient};
use bitcoin::{
    Transaction,
    consensus::{Decodable, Encodable},
    io::Cursor,
    p2p::{
        Address, Magic, ServiceFlags,
        address::AddrV2,
        message::{NetworkMessage, RawNetworkMessage},
        message_blockdata::Inventory,
        message_network::VersionMessage,
    },
};

use clap::{Parser, arg, command};
use rand::seq::SliceRandom;
use sha3::{Digest, Sha3_256};
use tor_rtcompat::PreferredRuntime;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};

use data_encoding::BASE32_NOPAD;
use tracing::{error, info};

pub const DNS_SEEDS: &[&str] = &[
    "dnsseed.bluematt.me",
    "dnsseed.bitcoin.dashjr-list-of-p2p-nodes.us",
    "seed.bitcoinstats.com",
    "seed.btc.petertodd.net",
    "seed.bitcoin.sprovoost.nl",
    "dnsseed.emzy.de",
    "seed.bitcoin.wiz.biz",
    "seed.bitcoin.sipa.be",
    "seed.bitcoin.jonasschnelli.ch",
    "seed.mainnet.achownodes.xyz",
];

pub const NODE_LIBRE_RELAY: u64 = 1 << 29;
pub const NODE_NETWORK: u64 = 1 << 0;
pub const NODE_WITNESS: u64 = 1 << 3;
pub const MAX_CONCURRENT_DELIVERIES: usize = 100;
pub const CONNECTION_TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NetworkAddress {
    Ip(SocketAddr),
    Onion(String),
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct Args {
    /// Hex-encoded raw transaction you’d like to blast
    #[arg(long)]
    pub tx: String,
}

pub fn build_version_msg() -> VersionMessage {
    VersionMessage {
        version: 70016,
        services: ServiceFlags::from(NODE_NETWORK | NODE_WITNESS | NODE_LIBRE_RELAY),
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
        receiver: Address::new(
            &SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            ServiceFlags::from(NODE_NETWORK | NODE_LIBRE_RELAY),
        ),
        sender: Address::new(
            &SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            ServiceFlags::from(NODE_NETWORK | NODE_WITNESS | NODE_LIBRE_RELAY),
        ),
        nonce: rand::random::<u64>(),
        user_agent: "/Satoshi:27.0.0/".into(),
        start_height: 897157,
        relay: true,
    }
}

pub async fn deliver_poop_tx(
    addr: NetworkAddress,
    tx: Transaction,
    tor_client: Arc<TorClient<PreferredRuntime>>,
    prefs: StreamPrefs,
) -> Result<bool> {
    let txid = tx.compute_txid();

    let mut stream = match &addr {
        NetworkAddress::Ip(sa) => {
            let target = (sa.ip().to_string(), sa.port());

            timeout(
                CONNECTION_TIMEOUT,
                tor_client.connect_with_prefs(target, &prefs),
            )
            .await
            .map_err(|_| anyhow::anyhow!("timeout connecting to {}", sa))??
        }

        NetworkAddress::Onion(host) => timeout(
            CONNECTION_TIMEOUT,
            tor_client.connect_with_prefs((host.as_str(), 8333), &prefs),
        )
        .await
        .map_err(|_| anyhow::anyhow!("timeout connecting to {}", host))??,
    };

    if let Err(e) = send_msg(&mut stream, NetworkMessage::Version(build_version_msg())).await {
        return Err(e);
    }

    let (mut rd, mut wr) = stream.split();

    let peer_version_message = match timeout(Duration::from_secs(5), async {
        loop {
            match read_msg(&mut rd).await {
                Ok(raw_msg) => {
                    if let NetworkMessage::Version(version) = raw_msg.payload() {
                        break Ok::<VersionMessage, anyhow::Error>(version.clone());
                    }
                }
                Err(e) => {
                    break Err(e);
                }
            }
        }
    })
    .await
    {
        Ok(Ok(vm)) => vm,
        Ok(Err(e)) => {
            return Err(e);
        }
        Err(_) => {
            return Err(anyhow::anyhow!(
                "Timeout waiting for peer Version from {:?}",
                addr
            ));
        }
    };

    let libre_flag_check = ServiceFlags::from(NODE_LIBRE_RELAY);
    if !peer_version_message.services.has(libre_flag_check) {
        return Ok(false);
    }

    if let Err(e) = send_msg(&mut wr, NetworkMessage::Verack).await {
        return Err(e);
    }

    // Dont wait for verack, just send the tx

    if let Err(e) = send_msg(&mut wr, NetworkMessage::Tx(tx.clone())).await {
        return Err(e);
    }

    if let Err(e) = send_msg(
        &mut wr,
        NetworkMessage::GetData(vec![Inventory::Transaction(txid)]),
    )
    .await
    {
        return Err(e);
    }

    let mut tx_confirmed_by_peer = false;
    loop {
        match timeout(CONNECTION_TIMEOUT, read_msg(&mut rd)).await {
            Ok(Ok(m)) => match m.payload() {
                NetworkMessage::Tx(received_tx) => {
                    if received_tx.compute_txid() == txid {
                        info!(
                            "[CONFIRMED HIT]\n{:?}\n(UA: '{}') direct hit confirmed on libre node! poop deliverd",
                            addr, peer_version_message.user_agent
                        );
                        tx_confirmed_by_peer = true;
                        break;
                    }
                }
                NetworkMessage::NotFound(not_found_list) => {
                    if not_found_list.iter().any(|inv| {
                        if let Inventory::Transaction(hash) = inv {
                            *hash == txid
                        } else {
                            false
                        }
                    }) {
                        error!(
                            "[NotFound] {:?} (UA: '{}') Possible LIAR detected! This node might be lying and tricking the code!! (If tx {} is already in a block, this is expected)",
                            addr, peer_version_message.user_agent, txid
                        );
                        break;
                    }
                }
                NetworkMessage::Inv(inv_list) => {
                    if inv_list.iter().any(|inv| {
                        if let Inventory::Transaction(hash) = inv {
                            *hash == txid
                        } else {
                            false
                        }
                    }) {
                        info!(
                            "[CONFIRMED HIT]\nINV returned TX:\ndirect hit confirmed on libre node!\npoop deliverd to {:?}! (UA: '{}')",
                            addr, peer_version_message.user_agent
                        );
                        tx_confirmed_by_peer = true;
                        break;
                    }
                }

                _ => {}
            },
            Ok(Err(read_err)) => {
                error!(
                    "Read error from {:?} while awaiting Tx confirmation: {}. Possible LIAR detected!",
                    addr, read_err
                );
                break;
            }
            Err(_) => {
                error!(
                    "Timeout waiting for Tx, Inv, or Reject from {:?} (UA: '{}'). Possible LIAR detected!",
                    addr, peer_version_message.user_agent
                );
                break;
            }
        }
    }
    Ok(tx_confirmed_by_peer)
}

pub async fn crawl_seed_node(seed: &SocketAddr) -> Result<Vec<NetworkAddress>> {
    let mut found_peers = Vec::new();
    info!("\ncrawling seed {:?}", seed);
    let mut stream = match timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect((seed.ip().to_string(), seed.port())),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            return Err(e.into());
        }
        Err(_) => {
            return Err(anyhow::anyhow!("Timeout connecting to {}", seed));
        }
    };

    send_msg(&mut stream, NetworkMessage::Version(build_version_msg())).await?;

    let (mut rd, mut wr) = stream.split();

    info!("\nwaiting for addresses from\n{:?}", seed);
    loop {
        let msg = match timeout(CONNECTION_TIMEOUT, read_msg(&mut rd)).await {
            Ok(Ok(m)) => m,
            _ => break,
        };
        match msg.payload() {
            NetworkMessage::Version(_) => {
                send_msg(&mut wr, NetworkMessage::SendAddrV2).await?;
                send_msg(&mut wr, NetworkMessage::Verack).await?;
            }
            NetworkMessage::Verack => {
                send_msg(&mut wr, NetworkMessage::GetAddr).await?;
            }
            NetworkMessage::Addr(list) => {
                let flag = ServiceFlags::from(NODE_LIBRE_RELAY);
                found_peers.extend(list.iter().filter_map(|(_, a)| {
                    if !a.services.has(flag) {
                        return None;
                    }

                    if let Ok(addr) = a.socket_addr() {
                        Some(NetworkAddress::Ip(addr))
                    } else {
                        None
                    }
                }));
                break;
            }
            NetworkMessage::AddrV2(list) => {
                let flag = ServiceFlags::from(NODE_LIBRE_RELAY);
                found_peers.extend(list.iter().filter_map(|a| {
                    if !a.services.has(flag) {
                        return None;
                    }
                    match &a.addr {
                        AddrV2::Ipv4(ip) => {
                            Some(NetworkAddress::Ip(SocketAddr::new(IpAddr::V4(*ip), a.port)))
                        }
                        AddrV2::Ipv6(ip) => {
                            Some(NetworkAddress::Ip(SocketAddr::new(IpAddr::V6(*ip), a.port)))
                        }
                        AddrV2::TorV3(key) => {
                            let onion_addr = tor_v3_onion_from_pubkey(key);
                            Some(NetworkAddress::Onion(onion_addr))
                        }
                        _ => None,
                    }
                }));
                break;
            }
            _ => {}
        }
    }

    found_peers.shuffle(&mut rand::rng());

    Ok(found_peers)
}

pub async fn send_msg<S: AsyncWriteExt + Unpin>(stream: &mut S, msg: NetworkMessage) -> Result<()> {
    let mut buf = Vec::new();
    RawNetworkMessage::new(Magic::BITCOIN, msg).consensus_encode(&mut buf)?;

    stream.write_all(&buf).await?;

    stream.flush().await?;
    Ok(())
}

pub async fn read_msg<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<RawNetworkMessage> {
    let mut hdr = [0u8; 24];
    r.read_exact(&mut hdr).await?;
    let len = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as usize;
    let mut payload = vec![0; len];
    r.read_exact(&mut payload).await?;
    Ok(RawNetworkMessage::consensus_decode(&mut Cursor::new(
        [hdr.to_vec(), payload].concat(),
    ))?)
}

pub fn tor_v3_onion_from_pubkey(pubkey: &[u8; 32]) -> String {
    let mut hasher = Sha3_256::new();
    hasher.update(b".onion checksum");
    hasher.update(pubkey);
    hasher.update([0x03]);
    let checksum = &hasher.finalize()[..2];

    let mut addr_raw = Vec::with_capacity(35);
    addr_raw.extend_from_slice(pubkey);
    addr_raw.extend_from_slice(checksum);
    addr_raw.push(0x03);

    BASE32_NOPAD.encode(&addr_raw).to_lowercase() + ".onion"
}
