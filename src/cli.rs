use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Hex-encoded raw transaction you'd like to blast
    #[arg(long)]
    tx: Option<String>,

    /// Blast only to onion peers
    #[arg(long)]
    tor_only: bool,

    /// Override the libp2p gossipsub protocol prefix, e.g. /gnostr
    #[arg(long)]
    protocol: Option<String>,

    /// Override the libp2p gossipsub protocol version, e.g. 1.0.0 or 1.1.0
    #[arg(long = "protocol-version")]
    protocol_version: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the bitcoin-pigeon topic network
    Topic {
        /// Optional transaction hex to publish on startup
        #[arg(long)]
        tx: Option<String>,
    },

    /// Fetch transactions from other libre relay peers
    Fetch {
        /// Maximum number of transactions to collect
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },

    /// Fetch and rebroadcast transactions from peers every 15 seconds
    Relay {
        /// Maximum number of transactions to collect per cycle
        #[arg(long, default_value_t = 20)]
        limit: usize,

        /// Seconds between relay cycles
        #[arg(long, default_value_t = 15)]
        interval_secs: u64,
    },

    /// Watch bitcoin-pigeon gossip activity without rebroadcasting it
    Gossip {
        /// Human-readable label for the watcher
        #[arg(long, default_value = "gossip-client")]
        label: String,

        /// Include local mempool polling and local tx summaries
        #[arg(long)]
        local: bool,

        /// Include remote gossipsub/hole-punch tx summaries
        #[arg(long)]
        remote: bool,
    },
}

pub async fn run() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let args = Args::parse();
    match args.command {
        Some(Command::Topic { tx }) => {
            crate::topic::run_topic_network(tx, args.tor_only, args.protocol, args.protocol_version).await?;
        }
        Some(Command::Fetch { limit }) => {
            let txs = crate::fetch_transactions(limit, args.tor_only, true).await?;
            tracing::info!(count = txs.len(), "fetched transactions from peers");
        }
        Some(Command::Relay {
            limit,
            interval_secs,
        }) => {
            crate::relay_transactions(limit, args.tor_only, true, interval_secs).await?;
        }
        Some(Command::Gossip {
            label,
            local: local_flag,
            remote: remote_flag,
        }) => {
            // Default to both views when neither flag is set so the watcher
            // shows the full network picture out of the box.
            let local = local_flag || (!local_flag && !remote_flag);
            let remote = remote_flag || (!local_flag && !remote_flag);
            crate::topic::run_gossip_client(
                label,
                args.tor_only,
                local,
                remote,
                args.protocol,
                args.protocol_version,
            ).await?;
        }
        None => {
            let tx = args.tx.context("missing --tx")?;
            crate::blast_transaction_hex(&tx, args.tor_only, true).await?;
        }
    }

    Ok(())
}
