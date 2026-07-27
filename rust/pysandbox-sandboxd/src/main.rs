#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt().with_target(false).try_init();
    tracing::info!(
        protocol_version = pysandbox_protocol::PROTOCOL_VERSION,
        "sandbox daemon started"
    );
    Ok(())
}
