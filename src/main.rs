#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tx_pigeon::cli::run().await
}
