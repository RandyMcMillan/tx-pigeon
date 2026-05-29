use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Hex-encoded raw transaction you’d like to blast
    #[arg(long)]
    tx: Option<String>,

    /// Blast only to onion peers
    #[arg(long)]
    tor_only: bool,

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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let args = Args::parse();
    match args.command {
        Some(Command::Topic { tx }) => {
            tx_pigeon::topic::run_topic_network(tx, args.tor_only).await?;
        }
        Some(Command::Fetch { limit }) => {
            let txs = tx_pigeon::fetch_transactions(limit, args.tor_only, true).await?;
            tracing::info!(count = txs.len(), "fetched transactions from peers");
        }
        Some(Command::Relay {
            limit,
            interval_secs,
        }) => {
            tx_pigeon::relay_transactions(limit, args.tor_only, true, interval_secs).await?;
        }
        None => {
            let tx = args.tx.context("missing --tx")?;
            tx_pigeon::blast_transaction_hex(&tx, args.tor_only, true).await?;
        }
    }

    Ok(())
}
