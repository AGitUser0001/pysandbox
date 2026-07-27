use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_target(false).try_init();
    pysandbox_sandboxd::run(std::env::args_os().skip(1)).await
}
