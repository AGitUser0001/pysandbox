use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use async_trait::async_trait;
use eryx_vfs::{DirEntry, Metadata, VfsError, VfsResult, VfsStorage};
use pysandbox_protocol::{
    Frame, FrameKind, VfsErrorCode, VfsRequest, VfsResponse, VfsValue, encode_payload,
};
use tokio::sync::{Mutex, mpsc, oneshot};

pub(crate) type PendingVfsRequests = Arc<Mutex<HashMap<u64, oneshot::Sender<VfsResponse>>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CachePolicy {
    None,
    Invalidated { negative: bool },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum CacheKey {
    Stat(String),
    Read(String),
    List(String),
}

#[derive(Clone)]
pub(crate) struct RemoteVfs {
    outgoing: mpsc::Sender<Frame>,
    next_request_id: Arc<AtomicU64>,
    pending: PendingVfsRequests,
    policy: CachePolicy,
    generation: Arc<AtomicU64>,
    cache: Arc<Mutex<HashMap<CacheKey, VfsResponse>>>,
}

impl RemoteVfs {
    pub(crate) fn new(
        outgoing: mpsc::Sender<Frame>,
        next_request_id: Arc<AtomicU64>,
        pending: PendingVfsRequests,
        policy: CachePolicy,
    ) -> Self {
        Self {
            outgoing,
            next_request_id,
            pending,
            policy,
            generation: Arc::new(AtomicU64::new(0)),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) async fn accept_response(&self, request_id: u64, response: VfsResponse) {
        let Some(waiter) = self.pending.lock().await.remove(&request_id) else {
            return;
        };
        let _ = waiter.send(response);
    }

    pub(crate) async fn invalidate(&self, path: Option<&str>) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        let mut cache = self.cache.lock().await;
        let Some(path) = path else {
            cache.clear();
            return;
        };
        let path = normalize_path(path);
        let parent = parent_path(&path);
        cache.retain(|key, _| match key {
            CacheKey::Stat(cached) | CacheKey::Read(cached) => {
                !is_same_or_descendant(cached, &path)
            }
            CacheKey::List(cached) => !is_same_or_descendant(cached, &path) && cached != &parent,
        });
    }

    async fn request(&self, key: CacheKey, request: VfsRequest) -> VfsResult<VfsValue> {
        if matches!(self.policy, CachePolicy::Invalidated { .. })
            && let Some(response) = self.cache.lock().await.get(&key).cloned()
        {
            return response_value(response);
        }

        let generation = self.generation.load(Ordering::Acquire);
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let payload = encode_payload(&request).map_err(|error| VfsError::Io(error.to_string()))?;
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(request_id, sender);
        if self
            .outgoing
            .send(Frame::new(FrameKind::VfsRequest, 0, request_id, payload))
            .await
            .is_err()
        {
            self.pending.lock().await.remove(&request_id);
            return Err(VfsError::Io(
                "sandbox supervisor connection is closed".into(),
            ));
        }
        let response = receiver
            .await
            .map_err(|_| VfsError::Io("VFS response channel was closed".into()))?;
        if response_is_cacheable(self.policy, &response)
            && generation == self.generation.load(Ordering::Acquire)
        {
            self.cache.lock().await.insert(key, response.clone());
        }
        response_value(response)
    }
}

fn response_is_cacheable(policy: CachePolicy, response: &VfsResponse) -> bool {
    match (&response.value, &response.error) {
        (Some(_), None) => matches!(policy, CachePolicy::Invalidated { .. }),
        (None, Some(error)) => {
            matches!(policy, CachePolicy::Invalidated { negative: true })
                && !matches!(error.code, VfsErrorCode::Io)
        }
        _ => false,
    }
}

fn is_same_or_descendant(candidate: &str, path: &str) -> bool {
    candidate == path
        || path == "/"
        || candidate
            .strip_prefix(path)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

#[async_trait]
impl VfsStorage for RemoteVfs {
    async fn read(&self, path: &str) -> VfsResult<Vec<u8>> {
        let path = normalize_path(path);
        match self
            .request(CacheKey::Read(path.clone()), VfsRequest::Read { path })
            .await?
        {
            VfsValue::Bytes(bytes) => Ok(bytes),
            _ => Err(VfsError::Io(
                "VFS read returned the wrong value type".into(),
            )),
        }
    }

    async fn read_at(&self, path: &str, offset: u64, len: u64) -> VfsResult<Vec<u8>> {
        let bytes = self.read(path).await?;
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        let length = usize::try_from(len).unwrap_or(usize::MAX);
        Ok(bytes[start..bytes.len().min(start.saturating_add(length))].to_vec())
    }

    async fn write(&self, path: &str, _data: &[u8]) -> VfsResult<()> {
        Err(readonly(path))
    }

    async fn write_at(&self, path: &str, _offset: u64, _data: &[u8]) -> VfsResult<()> {
        Err(readonly(path))
    }

    async fn set_size(&self, path: &str, _size: u64) -> VfsResult<()> {
        Err(readonly(path))
    }

    async fn delete(&self, path: &str) -> VfsResult<()> {
        Err(readonly(path))
    }

    async fn exists(&self, path: &str) -> VfsResult<bool> {
        match self.stat(path).await {
            Ok(_) => Ok(true),
            Err(VfsError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn list(&self, path: &str) -> VfsResult<Vec<DirEntry>> {
        let path = normalize_path(path);
        match self
            .request(CacheKey::List(path.clone()), VfsRequest::List { path })
            .await?
        {
            VfsValue::Entries(entries) => Ok(entries
                .into_iter()
                .map(|entry| DirEntry {
                    name: entry.name,
                    metadata: metadata(entry.metadata),
                })
                .collect()),
            _ => Err(VfsError::Io(
                "VFS list returned the wrong value type".into(),
            )),
        }
    }

    async fn stat(&self, path: &str) -> VfsResult<Metadata> {
        let path = normalize_path(path);
        match self
            .request(CacheKey::Stat(path.clone()), VfsRequest::Stat { path })
            .await?
        {
            VfsValue::Metadata(value) => Ok(metadata(value)),
            _ => Err(VfsError::Io(
                "VFS stat returned the wrong value type".into(),
            )),
        }
    }

    async fn mkdir(&self, path: &str) -> VfsResult<()> {
        Err(readonly(path))
    }

    async fn rmdir(&self, path: &str) -> VfsResult<()> {
        Err(readonly(path))
    }

    async fn rename(&self, from: &str, _to: &str) -> VfsResult<()> {
        Err(readonly(from))
    }

    fn mkdir_sync(&self, path: &str) -> VfsResult<()> {
        if normalize_path(path) == "/" {
            Ok(())
        } else {
            Err(readonly(path))
        }
    }
}

fn metadata(value: pysandbox_protocol::VfsMetadata) -> Metadata {
    let now = SystemTime::UNIX_EPOCH;
    Metadata {
        is_dir: value.kind == pysandbox_protocol::VfsNodeKind::Directory,
        size: value.size,
        created: now,
        modified: now,
        accessed: now,
    }
}

fn protocol_error(code: VfsErrorCode, message: String) -> VfsError {
    match code {
        VfsErrorCode::NotFound => VfsError::NotFound(message),
        VfsErrorCode::NotDirectory => VfsError::NotDirectory(message),
        VfsErrorCode::IsDirectory => VfsError::NotFile(message),
        VfsErrorCode::PermissionDenied => VfsError::PermissionDenied(message),
        VfsErrorCode::Invalid => VfsError::InvalidPath(message),
        VfsErrorCode::Io => VfsError::Io(message),
    }
}

fn response_value(response: VfsResponse) -> VfsResult<VfsValue> {
    match (response.value, response.error) {
        (Some(value), None) => Ok(value),
        (None, Some(error)) => Err(protocol_error(error.code, error.message)),
        _ => Err(VfsError::Io("malformed VFS response".into())),
    }
}

fn readonly(path: &str) -> VfsError {
    VfsError::PermissionDenied(format!("read-only virtual filesystem: {path}"))
}

fn normalize_path(path: &str) -> String {
    let mut components = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                components.pop();
            }
            component => components.push(component),
        }
    }
    format!("/{}", components.join("/"))
}

fn parent_path(path: &str) -> String {
    path.rsplit_once('/').map_or_else(
        || "/".into(),
        |(parent, _)| {
            if parent.is_empty() {
                "/".into()
            } else {
                parent.into()
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use pysandbox_protocol::{VfsError as ProtocolVfsError, VfsMetadata, VfsNodeKind};

    use super::*;

    fn error(code: VfsErrorCode) -> VfsResponse {
        VfsResponse {
            value: None,
            error: Some(ProtocolVfsError {
                code,
                message: "test".into(),
            }),
        }
    }

    #[test]
    fn cache_policy_never_caches_io_or_malformed_responses() {
        let successes = VfsResponse {
            value: Some(VfsValue::Metadata(VfsMetadata {
                kind: VfsNodeKind::File,
                size: 1,
            })),
            error: None,
        };
        let malformed = VfsResponse {
            value: None,
            error: None,
        };
        let positive_only = CachePolicy::Invalidated { negative: false };
        let with_negative = CachePolicy::Invalidated { negative: true };

        assert!(response_is_cacheable(positive_only, &successes));
        assert!(!response_is_cacheable(
            positive_only,
            &error(VfsErrorCode::NotFound)
        ));
        assert!(response_is_cacheable(
            with_negative,
            &error(VfsErrorCode::NotFound)
        ));
        assert!(!response_is_cacheable(
            with_negative,
            &error(VfsErrorCode::Io)
        ));
        assert!(!response_is_cacheable(with_negative, &malformed));
        assert!(!response_is_cacheable(CachePolicy::None, &successes));
    }
}
