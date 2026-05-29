use anyhow::Result;
use arti_client::{IsolationToken, StreamPrefs, TorClient, TorClientConfig};
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
use data_encoding::BASE32_NOPAD;
use rand::seq::SliceRandom;
use sha3::{Digest, Sha3_256};
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    signal,
    net::lookup_host,
    sync::Semaphore,
    task::JoinSet,
    time::{MissedTickBehavior, interval, sleep, timeout},
};
use tracing::{debug, error, info};
use tor_rtcompat::PreferredRuntime;

pub mod topic;
pub mod mempool;
pub mod cli;

static TOR_ONLY: AtomicBool = AtomicBool::new(false);

pub fn set_tor_only(enabled: bool) {
    TOR_ONLY.store(enabled, Ordering::SeqCst);
}

fn tor_only_enabled() -> bool {
    TOR_ONLY.load(Ordering::SeqCst)
}

const DNS_SEEDS: &[&str] = &[
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

const NODE_LIBRE_RELAY: u64 = 1 << 29;
const NODE_NETWORK: u64 = 1 << 0;
const NODE_WITNESS: u64 = 1 << 3;
const MAX_CONCURRENT_DELIVERIES: usize = 100;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum NetworkAddress {
    Ip(SocketAddr),
    Onion(String),
}

pub async fn blast_transaction_hex(tx_hex: &str, tor_only: bool, relay: bool) -> Result<usize> {
    set_tor_only(tor_only);
    let tx = bitcoin::consensus::deserialize::<Transaction>(&hex::decode(tx_hex)?)?;
    blast_transaction(tx, tor_only, relay).await
}

pub async fn blast_transaction(tx: Transaction, _tor_only: bool, relay: bool) -> Result<usize> {
    let txid = tx.compute_txid();

    let libre_peers = discover_libre_peers().await?;

    info!("time to blast some nodes with pigeon poop! 🐦💩");
    info!("blasting tx {:?} to libre relay nodes...", txid);

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_DELIVERIES));

    let libre_peers = filter_peers_for_tor_only(libre_peers);

    info!("using {} peers after tor-only filtering", libre_peers.len());

    info!("Bootstrapping Tor client...");
    let config = TorClientConfig::builder().build()?;
    let tor_client = Arc::new(TorClient::create_bootstrapped(config).await?);

    let common_token = IsolationToken::no_isolation();
    let mut prefs = StreamPrefs::new();
    prefs.set_isolation(common_token);

    let mut poop_delivery_tasks = JoinSet::new();
    for peer_addr in libre_peers.clone() {
        if tor_only_enabled() && matches!(peer_addr, NetworkAddress::Ip(_)) {
            debug!("[TX {txid}] tor-only enabled; skipping clearnet peer {:?}", peer_addr);
            continue;
        }

        let tx_clone = tx.clone();
        let permit = semaphore.clone().acquire_owned().await?;
        let tor_client = tor_client.clone();
        let peer_addr_cloned = peer_addr.clone();
        let prefs = prefs.clone();
        debug!("\n[TX {txid}]\nscheduling delivery to {:?}", peer_addr_cloned);
        poop_delivery_tasks.spawn(async move {
            let _permit_guard = permit;
            match deliver_poop_tx(
                peer_addr_cloned.clone(),
                tx_clone,
                tor_client,
                prefs,
                relay,
            )
            .await
            {
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
            "No libre relay nodes accepted the transaction.\nTX {} may already be in a block or its invalid.",
            txid
        );
        return Ok(0);
    }

    info!(
        "TX: {:?}\nblasted to {} libre relay nodes. GLHF",
        txid, success_count,
    );

    Ok(success_count)
}

pub async fn fetch_transactions(limit: usize, tor_only: bool, relay: bool) -> Result<Vec<Transaction>> {
    set_tor_only(tor_only);
    let libre_peers = discover_libre_peers().await?;

    info!("time to fetch some nodes with pigeon poop! 🐦💩");
    info!("requesting transactions from libre relay peers...");

    let libre_peers = filter_peers_for_tor_only(libre_peers);
    info!("using {} peers after tor-only filtering", libre_peers.len());

    info!("Bootstrapping Tor client...");
    let config = TorClientConfig::builder().build()?;
    let tor_client = Arc::new(TorClient::create_bootstrapped(config).await?);

    let common_token = IsolationToken::no_isolation();
    let mut prefs = StreamPrefs::new();
    prefs.set_isolation(common_token);

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_DELIVERIES));
    let mut peer_tasks = JoinSet::new();

    for peer_addr in libre_peers {
        let permit = semaphore.clone().acquire_owned().await?;
        let tor_client = tor_client.clone();
        let prefs = prefs.clone();
        peer_tasks.spawn(async move {
            let _permit_guard = permit;
            fetch_peer_transactions(peer_addr, tor_client, prefs, relay, limit).await
        });
    }

    let mut fetched_txs = Vec::new();
    let mut seen_txids = HashSet::new();

    while let Some(res) = peer_tasks.join_next().await {
        match res {
            Ok(Ok(peer_txs)) => {
                for tx in peer_txs {
                    if fetched_txs.len() >= limit {
                        break;
                    }

                    let txid = tx.compute_txid();
                    if seen_txids.insert(txid) {
                        fetched_txs.push(tx);
                    }
                }
            }
            Ok(Err(fetch_error)) => {
                error!("peer fetch error: {fetch_error}");
            }
            Err(join_error) => {
                error!("join error during tx fetch: {}", join_error);
            }
        }
    }

    info!(count = fetched_txs.len(), "finished fetching transactions from peers");
    Ok(fetched_txs)
}

pub async fn relay_transactions(
    limit: usize,
    tor_only: bool,
    relay: bool,
    interval_secs: u64,
) -> Result<()> {
    // Mirror the test harness: a few staggered workers fetch and rebroadcast
    // independently instead of one serialized relay loop.
    let mut workers = Vec::new();
    for worker_id in 1..=3 {
        workers.push(tokio::spawn(relay_worker(
            worker_id,
            limit,
            tor_only,
            relay,
            interval_secs,
        )));
    }

    for worker in workers {
        let _ = worker.await;
    }

    Ok(())
}

async fn relay_worker(
    worker_id: usize,
    limit: usize,
    tor_only: bool,
    relay: bool,
    interval_secs: u64,
) {
    let mut seen_txids = HashSet::<bitcoin::Txid>::new();
    let mut ticker = interval(Duration::from_secs(interval_secs));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    info!(
        worker_id,
        interval_secs,
        "relay worker online"
    );

    // Stagger the worker start so all relays do not fetch at the same instant.
    sleep(Duration::from_secs((worker_id - 1) as u64)).await;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                info!(worker_id, interval_secs, "running p2p relay cycle");

                match fetch_transactions(limit, tor_only, relay).await {
                    Ok(txs) => {
                        info!(worker_id, count = txs.len(), "relay cycle fetched transactions");

                        for (index, tx) in txs.into_iter().enumerate() {
                            let txid = tx.compute_txid();
                            if !seen_txids.insert(txid) {
                                info!(worker_id, %txid, "already relayed this tx");
                                continue;
                            }

                            let tx_hex = match encode_transaction_hex(&tx) {
                                Ok(tx_hex) => tx_hex,
                                Err(err) => {
                                    error!(worker_id, %txid, error = %err, "failed to encode transaction for relay");
                                    continue;
                                }
                            };

                            info!(
                                worker_id,
                                tx_index = index + 1,
                                %txid,
                                "rebroadcasting fetched tx p2p"
                            );
                            if let Err(err) = blast_transaction_hex(&tx_hex, tor_only, relay).await {
                                error!(worker_id, %txid, error = %err, "relay blast failed");
                            }
                        }
                    }
                    Err(err) => {
                        error!(worker_id, error = %err, "relay cycle fetch failed");
                    }
                }
            }
            _ = signal::ctrl_c() => {
                info!("stopping p2p relay loop");
                break;
            }
        }
    }
}

fn build_version_msg(relay: bool) -> VersionMessage {
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
        user_agent: "/Satoshi:29.2.0/Knots:20251110/UASF-BIP110:0.1/".into(),
        start_height: 897157, //get current blockheight minus 20?
        relay,
    }
}

fn encode_transaction_hex(tx: &Transaction) -> Result<String> {
    let mut bytes = Vec::new();
    tx.consensus_encode(&mut bytes)?;
    Ok(hex::encode(bytes))
}

async fn discover_libre_peers() -> Result<HashSet<NetworkAddress>> {
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

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_DELIVERIES));
    let mut libre_peers = HashSet::<NetworkAddress>::new();
    let mut crawl_tasks = JoinSet::new();

    for addr in seed_addrs {
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

    Ok(libre_peers)
}

fn filter_peers_for_tor_only(peers: HashSet<NetworkAddress>) -> HashSet<NetworkAddress> {
    if tor_only_enabled() {
        info!("Tor-only mode enabled; filtering clearnet peers");
        peers
            .into_iter()
            .filter(|addr| matches!(addr, NetworkAddress::Onion(_)))
            .collect()
    } else {
        peers
    }
}

async fn fetch_peer_transactions(
    addr: NetworkAddress,
    tor_client: Arc<TorClient<PreferredRuntime>>,
    prefs: StreamPrefs,
    relay: bool,
    limit: usize,
) -> Result<Vec<Transaction>> {
    if tor_only_enabled() && matches!(addr, NetworkAddress::Ip(_)) {
        debug!("[FETCH] tor-only enabled; skipping {:?}", addr);
        return Ok(Vec::new());
    }

    debug!("[FETCH] connecting to {:?}", addr);
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
    debug!("[FETCH] connected to {:?}", addr);

    debug!("[FETCH] sending version to {:?}", addr);
    send_msg(&mut stream, NetworkMessage::Version(build_version_msg(relay))).await?;

    let (mut rd, mut wr) = stream.split();
    let peer_version_message = wait_for_version(&mut rd, &addr).await?;
    info!(
        "[FETCH] {:?} received version from peer (UA: '{}')",
        addr, peer_version_message.user_agent
    );

    debug!("[FETCH] sending verack to {:?}", addr);
    send_msg(&mut wr, NetworkMessage::Verack).await?;
    debug!("[FETCH] {:?} sent verack", addr);

    debug!("[FETCH] requesting mempool from {:?}", addr);
    send_msg(&mut wr, NetworkMessage::MemPool).await?;
    info!("[FETCH] {:?} requested mempool", addr);

    let mut announced = HashSet::<bitcoin::Txid>::new();
    let mut request_list = Vec::<bitcoin::Txid>::new();
    loop {
        match timeout(CONNECTION_TIMEOUT, read_msg(&mut rd)).await {
            Ok(Ok(m)) => match m.payload() {
                NetworkMessage::Inv(inv_list) => {
                    info!(
                        "[FETCH] {:?} (UA: '{}') advertised {} inventory entries; iterating",
                        addr,
                        peer_version_message.user_agent,
                        inv_list.len()
                    );
                    for inv in inv_list {
                        if let Inventory::Transaction(hash) = inv {
                            if announced.insert(*hash) {
                                info!(
                                    "[FETCH] {:?} queued tx {} for getdata",
                                    addr,
                                    hash
                                );
                                request_list.push(*hash);
                                if request_list.len() >= limit {
                                    break;
                                }
                            }
                        }
                    }
                    if request_list.len() >= limit {
                        break;
                    }
                }
                NetworkMessage::NotFound(_) => {}
                _ => {}
            },
            Ok(Err(read_err)) => {
                error!(
                    "Read error from {:?} while awaiting mempool invs: {} (UA: '{}')",
                    addr, read_err, peer_version_message.user_agent
                );
                break;
            }
            Err(_) => break,
        }
    }

    if request_list.is_empty() {
        info!(
            "[FETCH] {:?} (UA: '{}') returned no tx inventory",
            addr, peer_version_message.user_agent
        );
        return Ok(Vec::new());
    }

    debug!(
        "[FETCH] requesting {} txs from {:?}",
        request_list.len(),
        addr
    );
    info!(
        "[FETCH] {:?} (UA: '{}') sending getdata for {:?}",
        addr,
        peer_version_message.user_agent,
        request_list
    );
    send_msg(
        &mut wr,
        NetworkMessage::GetData(
            request_list
                .iter()
                .map(|txid| Inventory::Transaction(*txid))
                .collect(),
        ),
    )
    .await?;

    let mut fetched = Vec::<Transaction>::new();
    let mut fetched_ids = HashSet::<bitcoin::Txid>::new();
    let mut pending_txids = request_list.iter().copied().collect::<HashSet<_>>();
    while !pending_txids.is_empty() && fetched.len() < limit {
        match timeout(CONNECTION_TIMEOUT, read_msg(&mut rd)).await {
            Ok(Ok(m)) => match m.payload() {
                NetworkMessage::Tx(received_tx) => {
                    let received_txid = received_tx.compute_txid();
                    if pending_txids.remove(&received_txid) && fetched_ids.insert(received_txid) {
                        info!(
                            "[FETCH HIT] {:?} (UA: '{}') sent tx {} ({} of {})",
                            addr,
                            peer_version_message.user_agent,
                            received_txid,
                            fetched.len() + 1,
                            limit
                        );
                        fetched.push(received_tx.clone());
                    }
                }
                NetworkMessage::NotFound(not_found_list) => {
                    for inv in not_found_list {
                        if let Inventory::Transaction(hash) = inv {
                            pending_txids.remove(hash);
                        }
                    }
                }
                _ => {}
            },
            Ok(Err(read_err)) => {
                error!(
                    "Read error from {:?} while awaiting tx data: {} (UA: '{}')",
                    addr, read_err, peer_version_message.user_agent
                );
                break;
            }
            Err(_) => break,
        }
    }

    Ok(fetched)
}

async fn wait_for_version(
    rd: &mut (impl tokio::io::AsyncRead + Unpin),
    addr: &NetworkAddress,
) -> Result<VersionMessage> {
    let peer_version_message = match timeout(Duration::from_secs(5), async {
        loop {
            match read_msg(rd).await {
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

    Ok(peer_version_message)
}

async fn deliver_poop_tx(
    addr: NetworkAddress,
    tx: Transaction,
    tor_client: Arc<TorClient<PreferredRuntime>>,
    prefs: StreamPrefs,
    relay: bool,
) -> Result<bool> {
    let txid = tx.compute_txid();

    if tor_only_enabled() && matches!(addr, NetworkAddress::Ip(_)) {
        debug!("[TX {txid}] tor-only enabled; skipping {:?}", addr);
        return Ok(false);
    }

    debug!("[TX {txid}]\nconnecting to {:?}", addr);
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
    debug!("[TX {txid}]\nconnected to {:?}", addr);

    debug!("[TX {txid}]\nsending version to {:?}", addr);
    if let Err(e) = send_msg(&mut stream, NetworkMessage::Version(build_version_msg(relay))).await {
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
        debug!(
            "[TX {txid}] {:?}\ndoes not advertise NODE_LIBRE_RELAY,\nskipping",
            addr
        );
        return Ok(false);
    }

    debug!("[TX {txid}]\nsending verack to {:?}", addr);
    if let Err(e) = send_msg(&mut wr, NetworkMessage::Verack).await {
        return Err(e);
    }

    debug!("[TX {txid}]\nsending tx to {:?}", addr);
    if let Err(e) = send_msg(&mut wr, NetworkMessage::Tx(tx.clone())).await {
        return Err(e);
    }

    debug!("[TX {txid}]\nsending getdata to {:?}", addr);
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
                            "[CONFIRMED HIT] {:?} (UA: '{}') direct hit confirmed on libre node! poop deliverd",
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
                        info!(
                            "[CONFIRMED HIT] {:?} (UA: '{}') peer already knew tx {}; counting duplicate as receipt",
                            addr, peer_version_message.user_agent, txid
                        );
                        tx_confirmed_by_peer = true;
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
                            "[CONFIRMED HIT] INV returned TX:\ndirect hit confirmed on libre node!\npoop deliverd to\n{:?}! (UA: '{}')",
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

async fn crawl_seed_node(seed: &SocketAddr) -> Result<Vec<NetworkAddress>> {
    let mut found_peers = Vec::new();
    info!("crawling seed {:?}", seed);
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

    send_msg(&mut stream, NetworkMessage::Version(build_version_msg(true))).await?;

    let (mut rd, mut wr) = stream.split();

    info!("waiting for addresses from {:?}...", seed);
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

async fn send_msg<S: AsyncWriteExt + Unpin>(stream: &mut S, msg: NetworkMessage) -> Result<()> {
    let mut buf = Vec::new();
    RawNetworkMessage::new(Magic::BITCOIN, msg).consensus_encode(&mut buf)?;

    stream.write_all(&buf).await?;

    stream.flush().await?;
    Ok(())
}

async fn read_msg<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<RawNetworkMessage> {
    let mut hdr = [0u8; 24];
    r.read_exact(&mut hdr).await?;
    let len = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as usize;
    let mut payload = vec![0; len];
    r.read_exact(&mut payload).await?;
    Ok(RawNetworkMessage::consensus_decode(&mut Cursor::new(
        [hdr.to_vec(), payload].concat(),
    ))?)
}

fn tor_v3_onion_from_pubkey(pubkey: &[u8; 32]) -> String {
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
