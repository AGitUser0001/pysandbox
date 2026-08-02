use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use async_trait::async_trait;
use eryx_vfs::{DirEntry, Metadata, VfsError, VfsResult, VfsStorage};
use pysandbox_protocol::{
    Frame, FrameKind, VfsErrorCode, VfsRequest, VfsResponse, VfsStatResult, VfsValue,
    encode_payload,
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

#[derive(Clone, Copy)]
enum CacheSlot {
    Stat,
    Read,
    List,
}

impl CacheKey {
    fn parts(&self) -> (CacheSlot, &str) {
        match self {
            Self::Stat(path) => (CacheSlot::Stat, path),
            Self::Read(path) => (CacheSlot::Read, path),
            Self::List(path) => (CacheSlot::List, path),
        }
    }
}

#[derive(Default)]
struct CacheNode {
    stat: Option<VfsResponse>,
    read: Option<VfsResponse>,
    list: Option<VfsResponse>,
    children: HashMap<String, CacheNode>,
}

impl CacheNode {
    fn get(&self, slot: CacheSlot) -> Option<&VfsResponse> {
        match slot {
            CacheSlot::Stat => self.stat.as_ref(),
            CacheSlot::Read => self.read.as_ref(),
            CacheSlot::List => self.list.as_ref(),
        }
    }

    fn set(&mut self, slot: CacheSlot, response: VfsResponse) {
        *match slot {
            CacheSlot::Stat => &mut self.stat,
            CacheSlot::Read => &mut self.read,
            CacheSlot::List => &mut self.list,
        } = Some(response);
    }

    fn remove(&mut self, slot: CacheSlot) {
        *match slot {
            CacheSlot::Stat => &mut self.stat,
            CacheSlot::Read => &mut self.read,
            CacheSlot::List => &mut self.list,
        } = None;
    }

    fn is_empty(&self) -> bool {
        self.stat.is_none()
            && self.read.is_none()
            && self.list.is_none()
            && self.children.is_empty()
    }
}

#[derive(Default)]
struct VfsCache {
    root: CacheNode,
}

impl VfsCache {
    fn clear(&mut self) {
        self.root = CacheNode::default();
    }

    fn get(&self, key: &CacheKey) -> Option<&VfsResponse> {
        let (slot, path) = key.parts();
        self.node(path)?.get(slot)
    }

    fn insert(&mut self, key: CacheKey, response: VfsResponse) {
        let (slot, path) = key.parts();
        self.node_mut_or_create(path).set(slot, response);
    }

    fn invalidate(&mut self, path: &str) {
        if path == "/" {
            self.clear();
            return;
        }
        self.take_subtree(path);
        self.remove(&CacheKey::List(parent_path(path)));
    }

    fn rename(&mut self, from: &str, to: &str) {
        let subtree = self.take_subtree(from);
        self.take_subtree(to);
        if let Some(subtree) = subtree {
            self.insert_subtree(to, subtree);
        }
        self.remove(&CacheKey::List(parent_path(from)));
        self.remove(&CacheKey::List(parent_path(to)));
    }

    fn node(&self, path: &str) -> Option<&CacheNode> {
        let mut node = &self.root;
        for component in path_components(path) {
            node = node.children.get(component)?;
        }
        Some(node)
    }

    fn node_mut_or_create(&mut self, path: &str) -> &mut CacheNode {
        let mut node = &mut self.root;
        for component in path_components(path) {
            node = node.children.entry(component.to_owned()).or_default();
        }
        node
    }

    fn remove(&mut self, key: &CacheKey) {
        let (slot, path) = key.parts();
        let components = path_components(path).collect::<Vec<_>>();
        Self::remove_from_node(&mut self.root, &components, slot);
    }

    fn remove_from_node(node: &mut CacheNode, components: &[&str], slot: CacheSlot) -> bool {
        if let Some((component, remaining)) = components.split_first() {
            if let Some(child) = node.children.get_mut(*component)
                && Self::remove_from_node(child, remaining, slot)
            {
                node.children.remove(*component);
            }
        } else {
            node.remove(slot);
        }
        node.is_empty()
    }

    fn take_subtree(&mut self, path: &str) -> Option<CacheNode> {
        if path == "/" {
            return Some(std::mem::take(&mut self.root));
        }
        let mut components = path_components(path).collect::<Vec<_>>();
        let name = components.pop()?;
        let mut parent = &mut self.root;
        for component in components {
            parent = parent.children.get_mut(component)?;
        }
        parent.children.remove(name)
    }

    fn insert_subtree(&mut self, path: &str, subtree: CacheNode) {
        if path == "/" {
            self.root = subtree;
            return;
        }
        let mut components = path_components(path).collect::<Vec<_>>();
        let name = components.pop().expect("non-root path has a component");
        let mut parent = &mut self.root;
        for component in components {
            parent = parent.children.entry(component.to_owned()).or_default();
        }
        parent.children.insert(name.to_owned(), subtree);
    }
}

#[derive(Clone)]
pub(crate) struct RemoteVfs {
    worker_id: u64,
    outgoing: mpsc::Sender<Frame>,
    next_request_id: Arc<AtomicU64>,
    pending: PendingVfsRequests,
    policy: CachePolicy,
    generation: Arc<AtomicU64>,
    cache: Arc<Mutex<VfsCache>>,
}

impl RemoteVfs {
    pub(crate) fn new(
        outgoing: mpsc::Sender<Frame>,
        next_request_id: Arc<AtomicU64>,
        pending: PendingVfsRequests,
        policy: CachePolicy,
    ) -> Self {
        Self {
            worker_id: 0,
            outgoing,
            next_request_id,
            pending,
            policy,
            generation: Arc::new(AtomicU64::new(0)),
            cache: Arc::new(Mutex::new(VfsCache::default())),
        }
    }

    pub(crate) fn for_worker(&self, worker_id: u64) -> Self {
        let mut vfs = self.clone();
        vfs.worker_id = worker_id;
        vfs
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
        cache.invalidate(&path);
    }

    async fn rename_cached_path(&self, from: &str, to: &str) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.cache.lock().await.rename(from, to);
    }

    async fn cached_stat(&self, path: &str) -> Option<VfsStatResult> {
        if !matches!(self.policy, CachePolicy::Invalidated { .. }) {
            return None;
        }
        self.cache
            .lock()
            .await
            .get(&CacheKey::Stat(path.to_owned()))
            .map(stat_result)
    }

    async fn request(
        &self,
        path: &str,
        key: Option<CacheKey>,
        request: VfsRequest,
    ) -> VfsResult<VfsValue> {
        self.request_with_rename(path, key, request, None).await
    }

    async fn request_with_rename(
        &self,
        path: &str,
        key: Option<CacheKey>,
        request: VfsRequest,
        rename_to: Option<&str>,
    ) -> VfsResult<VfsValue> {
        if let Some(key) = &key
            && matches!(self.policy, CachePolicy::Invalidated { .. })
            && let Some(response) = self.cache.lock().await.get(key).cloned()
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
            .send(Frame::new(
                FrameKind::VfsRequest,
                self.worker_id,
                request_id,
                payload,
            ))
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
        if response.invalidate {
            if let Some(to) = rename_to {
                self.rename_cached_path(path, to).await;
            } else {
                self.invalidate(Some(path)).await;
            }
        } else if generation == self.generation.load(Ordering::Acquire) {
            let mut cache = self.cache.lock().await;
            if let Some(stat) = &response.stat {
                let stat_response = response_from_stat(stat.clone());
                if response_is_cacheable(self.policy, &stat_response) {
                    cache.insert(CacheKey::Stat(path.to_owned()), stat_response);
                }
            }
            if matches!(self.policy, CachePolicy::Invalidated { .. })
                && let Some(VfsValue::Entries(entries)) = &response.value
            {
                for entry in entries {
                    let child = if path == "/" {
                        format!("/{}", entry.name)
                    } else {
                        format!("{}/{}", path.trim_end_matches('/'), entry.name)
                    };
                    cache.insert(
                        CacheKey::Stat(child),
                        VfsResponse {
                            value: Some(VfsValue::Metadata(entry.metadata.clone())),
                            error: None,
                            stat: None,
                            invalidate: false,
                        },
                    );
                }
            }
            if let Some(key) = key
                && response_is_cacheable(self.policy, &response)
            {
                cache.insert(key, response.clone());
            }
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

fn path_components(path: &str) -> impl Iterator<Item = &str> {
    path.split('/').filter(|component| !component.is_empty())
}

#[async_trait]
impl VfsStorage for RemoteVfs {
    async fn read(&self, path: &str) -> VfsResult<Vec<u8>> {
        let path = normalize_path(path);
        let stat = self.cached_stat(&path).await;
        match self
            .request(
                &path,
                Some(CacheKey::Read(path.clone())),
                VfsRequest::Read {
                    path: path.clone(),
                    stat,
                },
            )
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

    async fn write(&self, path: &str, data: &[u8]) -> VfsResult<()> {
        self.write_request(path, data, None).await
    }

    async fn write_at(&self, path: &str, offset: u64, data: &[u8]) -> VfsResult<()> {
        self.write_request(path, data, Some(offset)).await
    }

    async fn append(&self, path: &str, data: &[u8]) -> VfsResult<()> {
        let path = normalize_path(path);
        let stat = self.cached_stat(&path).await;
        match self
            .request(
                &path,
                None,
                VfsRequest::Append {
                    path: path.clone(),
                    data: data.to_vec(),
                    stat,
                },
            )
            .await?
        {
            VfsValue::Unit => Ok(()),
            _ => Err(VfsError::Io(
                "VFS append returned the wrong value type".into(),
            )),
        }
    }

    async fn set_size(&self, path: &str, size: u64) -> VfsResult<()> {
        let path = normalize_path(path);
        let stat = self.cached_stat(&path).await;
        match self
            .request(
                &path,
                None,
                VfsRequest::Truncate {
                    path: path.clone(),
                    size,
                    stat,
                },
            )
            .await?
        {
            VfsValue::Unit => Ok(()),
            _ => Err(VfsError::Io(
                "VFS truncate returned the wrong value type".into(),
            )),
        }
    }

    async fn delete(&self, path: &str) -> VfsResult<()> {
        self.delete_request(path, false).await
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
        let stat = self.cached_stat(&path).await;
        match self
            .request(
                &path,
                Some(CacheKey::List(path.clone())),
                VfsRequest::List {
                    path: path.clone(),
                    stat,
                },
            )
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
            .request(
                &path,
                Some(CacheKey::Stat(path.clone())),
                VfsRequest::Stat { path: path.clone() },
            )
            .await?
        {
            VfsValue::Metadata(value) => Ok(metadata(value)),
            _ => Err(VfsError::Io(
                "VFS stat returned the wrong value type".into(),
            )),
        }
    }

    async fn mkdir(&self, path: &str) -> VfsResult<()> {
        let path = normalize_path(path);
        let stat = self.cached_stat(&path).await;
        let value = self
            .request(
                &path,
                None,
                VfsRequest::Mkdir {
                    path: path.clone(),
                    stat,
                },
            )
            .await?;
        match value {
            VfsValue::Unit => Ok(()),
            _ => Err(VfsError::Io(
                "VFS mkdir returned the wrong value type".into(),
            )),
        }
    }

    async fn rmdir(&self, path: &str) -> VfsResult<()> {
        self.delete_request(path, true).await
    }

    async fn rename(&self, from: &str, to: &str) -> VfsResult<()> {
        let from = normalize_path(from);
        let to = normalize_path(to);
        let stat = self.cached_stat(&from).await;
        let to_stat = self.cached_stat(&to).await;
        let value = self
            .request_with_rename(
                &from,
                None,
                VfsRequest::Rename {
                    from: from.clone(),
                    to: to.clone(),
                    stat,
                    to_stat,
                },
                Some(&to),
            )
            .await?;
        match value {
            VfsValue::Unit => Ok(()),
            _ => Err(VfsError::Io(
                "VFS rename returned the wrong value type".into(),
            )),
        }
    }

    fn mkdir_sync(&self, path: &str) -> VfsResult<()> {
        if normalize_path(path) == "/" {
            Ok(())
        } else {
            Err(VfsError::PermissionDenied(format!(
                "synchronous directory creation is unavailable: {path}"
            )))
        }
    }
}

impl RemoteVfs {
    async fn write_request(&self, path: &str, data: &[u8], offset: Option<u64>) -> VfsResult<()> {
        let path = normalize_path(path);
        let stat = self.cached_stat(&path).await;
        let value = self
            .request(
                &path,
                None,
                VfsRequest::Write {
                    path: path.clone(),
                    data: data.to_vec(),
                    offset,
                    stat,
                },
            )
            .await?;
        match value {
            VfsValue::Unit => Ok(()),
            _ => Err(VfsError::Io(
                "VFS write returned the wrong value type".into(),
            )),
        }
    }

    async fn delete_request(&self, path: &str, directory: bool) -> VfsResult<()> {
        let path = normalize_path(path);
        let stat = self.cached_stat(&path).await;
        let value = self
            .request(
                &path,
                None,
                VfsRequest::Delete {
                    path: path.clone(),
                    directory,
                    stat,
                },
            )
            .await?;
        match value {
            VfsValue::Unit => Ok(()),
            _ => Err(VfsError::Io(
                "VFS delete returned the wrong value type".into(),
            )),
        }
    }
}

fn metadata(value: pysandbox_protocol::VfsMetadata) -> Metadata {
    let now = SystemTime::UNIX_EPOCH;
    Metadata {
        is_dir: value.kind == pysandbox_protocol::VfsNodeKind::Directory,
        size: value.size,
        readable: value.read,
        writable: value.write,
        created: now,
        modified: now,
        accessed: now,
    }
}

fn protocol_error(code: VfsErrorCode, message: String) -> VfsError {
    match code {
        VfsErrorCode::NotFound => VfsError::NotFound(message),
        VfsErrorCode::AlreadyExists => VfsError::AlreadyExists(message),
        VfsErrorCode::NotDirectory => VfsError::NotDirectory(message),
        VfsErrorCode::IsDirectory => VfsError::NotFile(message),
        VfsErrorCode::DirectoryNotEmpty => VfsError::DirectoryNotEmpty(message),
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

fn stat_result(response: &VfsResponse) -> VfsStatResult {
    match (&response.value, &response.error) {
        (Some(VfsValue::Metadata(value)), None) => VfsStatResult {
            value: Some(value.clone()),
            error: None,
        },
        (None, Some(error)) => VfsStatResult {
            value: None,
            error: Some(error.clone()),
        },
        _ => VfsStatResult {
            value: None,
            error: Some(pysandbox_protocol::VfsError {
                code: VfsErrorCode::Io,
                message: "malformed cached VFS stat".into(),
            }),
        },
    }
}

fn response_from_stat(stat: VfsStatResult) -> VfsResponse {
    VfsResponse {
        value: stat.value.map(VfsValue::Metadata),
        error: stat.error,
        stat: None,
        invalidate: false,
    }
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
            stat: None,
            invalidate: false,
        }
    }

    fn metadata_response(size: u64) -> VfsResponse {
        VfsResponse {
            value: Some(VfsValue::Metadata(VfsMetadata {
                kind: VfsNodeKind::File,
                size,
                read: true,
                write: false,
            })),
            error: None,
            stat: None,
            invalidate: false,
        }
    }

    fn cached_size(cache: &VfsCache, key: &CacheKey) -> Option<u64> {
        match &cache.get(key)?.value {
            Some(VfsValue::Metadata(metadata)) => Some(metadata.size),
            _ => None,
        }
    }

    #[test]
    fn cache_policy_never_caches_io_or_malformed_responses() {
        let successes = VfsResponse {
            value: Some(VfsValue::Metadata(VfsMetadata {
                kind: VfsNodeKind::File,
                size: 1,
                read: true,
                write: false,
            })),
            error: None,
            stat: None,
            invalidate: false,
        };
        let malformed = VfsResponse {
            value: None,
            error: None,
            stat: None,
            invalidate: false,
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

    #[test]
    fn trie_invalidation_removes_a_subtree_and_its_parent_listing() {
        let mut cache = VfsCache::default();
        cache.insert(CacheKey::List("/123".into()), metadata_response(1));
        cache.insert(CacheKey::Stat("/123/folder".into()), metadata_response(2));
        cache.insert(
            CacheKey::Read("/123/folder/file.py".into()),
            metadata_response(3),
        );
        cache.insert(
            CacheKey::Stat("/123/sibling.py".into()),
            metadata_response(4),
        );
        cache.insert(CacheKey::List("/".into()), metadata_response(5));

        cache.invalidate("/123/folder");

        assert!(cache.get(&CacheKey::List("/123".into())).is_none());
        assert!(cache.get(&CacheKey::Stat("/123/folder".into())).is_none());
        assert!(
            cache
                .get(&CacheKey::Read("/123/folder/file.py".into()))
                .is_none()
        );
        assert_eq!(
            cached_size(&cache, &CacheKey::Stat("/123/sibling.py".into())),
            Some(4)
        );
        assert_eq!(cached_size(&cache, &CacheKey::List("/".into())), Some(5));
    }

    #[test]
    fn trie_rename_moves_the_cached_subtree() {
        let mut cache = VfsCache::default();
        cache.insert(CacheKey::List("/source".into()), metadata_response(1));
        cache.insert(CacheKey::List("/target".into()), metadata_response(2));
        cache.insert(
            CacheKey::Stat("/source/folder".into()),
            metadata_response(3),
        );
        cache.insert(
            CacheKey::Read("/source/folder/file.py".into()),
            metadata_response(4),
        );
        cache.insert(
            CacheKey::Stat("/target/renamed/old.py".into()),
            metadata_response(5),
        );
        cache.insert(CacheKey::Stat("/unrelated.py".into()), metadata_response(6));

        cache.rename("/source/folder", "/target/renamed");

        assert!(cache.get(&CacheKey::List("/source".into())).is_none());
        assert!(cache.get(&CacheKey::List("/target".into())).is_none());
        assert!(
            cache
                .get(&CacheKey::Stat("/source/folder".into()))
                .is_none()
        );
        assert_eq!(
            cached_size(&cache, &CacheKey::Stat("/target/renamed".into())),
            Some(3)
        );
        assert_eq!(
            cached_size(&cache, &CacheKey::Read("/target/renamed/file.py".into())),
            Some(4)
        );
        assert!(
            cache
                .get(&CacheKey::Stat("/target/renamed/old.py".into()))
                .is_none()
        );
        assert_eq!(
            cached_size(&cache, &CacheKey::Stat("/unrelated.py".into())),
            Some(6)
        );
    }
}
