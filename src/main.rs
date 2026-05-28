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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let args = Args::parse();
    match args.command {
        Some(Command::Topic { tx }) => {
            tx_pigeon::topic::run_topic_network(tx, args.tor_only).await?;
        }
        None => {
            let tx = args.tx.context("missing --tx")?;
            tx_pigeon::blast_transaction_hex(&tx, args.tor_only).await?;
        }
    }

    Ok(())
}
