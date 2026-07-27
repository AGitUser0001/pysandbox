use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::{Result, bail};

pub mod component_worker;
pub mod server;

pub async fn run(arguments: impl IntoIterator<Item = OsString>) -> Result<()> {
    let mut arguments = arguments.into_iter();
    let socket_name = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| anyhow::anyhow!(usage()))?;
    let component_path = arguments
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!(usage()))?;
    let python_root = arguments
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!(usage()))?;
    let max_ipc_frame_bytes = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| anyhow::anyhow!(usage()))?
        .parse()?;
    if arguments.next().is_some() {
        bail!(usage());
    }

    server::serve(
        &socket_name,
        &component_path,
        &python_root,
        max_ipc_frame_bytes,
    )
    .await
}

fn usage() -> &'static str {
    "usage: pysandbox-sandboxd <socket-name> <component> <python-root> <max-ipc-frame-bytes>"
}
