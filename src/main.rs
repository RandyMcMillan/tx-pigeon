use anyhow::Result;
use clap::{Parser, arg, command};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Hex-encoded raw transaction you’d like to blast
    #[arg(long)]
    tx: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let args = Args::parse();
    tx_pigeon::blast_transaction_hex(&args.tx).await?;

    Ok(())
}
