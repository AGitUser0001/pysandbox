use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};

pub mod component_worker;
mod cpu_share;
mod remote_vfs;
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
    let worker_queue_capacity = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| anyhow::anyhow!(usage()))?
        .parse()?;
    let compilation_cache = arguments
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!(usage()))?;
    let compilation_cache =
        (compilation_cache != PathBuf::from("none")).then_some(compilation_cache);
    let cache_vfs = arguments
        .next()
        .map(|value| {
            value
                .into_string()
                .map_err(|_| anyhow::anyhow!(usage()))?
                .parse()
                .map_err(anyhow::Error::from)
        })
        .transpose()?
        .unwrap_or(false);
    let cache_vfs_negative = arguments
        .next()
        .map(|value| {
            value
                .into_string()
                .map_err(|_| anyhow::anyhow!(usage()))?
                .parse()
                .map_err(anyhow::Error::from)
        })
        .transpose()?
        .unwrap_or(false);
    let cpu_share_enabled = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| anyhow::anyhow!(usage()))?
        .parse()?;
    let cpu_share_limit_percent = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| anyhow::anyhow!(usage()))?;
    let cpu_share_limit_percent = if cpu_share_limit_percent == "none" {
        None
    } else {
        Some(cpu_share_limit_percent.parse()?)
    };
    let cpu_share_sample_interval = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| anyhow::anyhow!(usage()))?
        .parse()
        .map(Duration::from_millis)?;
    let cpu_share_activity_timeout = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| anyhow::anyhow!(usage()))?
        .parse()
        .map(Duration::from_millis)?;
    if arguments.next().is_some() {
        bail!(usage());
    }

    server::serve(
        &socket_name,
        &component_path,
        &python_root,
        compilation_cache.as_deref(),
        max_ipc_frame_bytes,
        worker_queue_capacity,
        cache_vfs,
        cache_vfs_negative,
        cpu_share_enabled,
        cpu_share_limit_percent,
        cpu_share_sample_interval,
        cpu_share_activity_timeout,
    )
    .await
}

fn usage() -> &'static str {
    "usage: pysandbox-sandboxd <socket-name> <component> <python-root> \
     <max-ipc-frame-bytes> <worker-queue-capacity> <compilation-cache> \
     <cache-vfs> <cache-vfs-negative> \
     <cpu-share-enabled> <cpu-share-limit-percent> <cpu-share-sample-interval-ms> \
     <cpu-share-activity-timeout-ms>"
}
