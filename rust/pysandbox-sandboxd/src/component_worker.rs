use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use bytes::Bytes;
use eryx_vfs::{HybridVfsCtx, HybridVfsState, HybridVfsView, RealDir, add_hybrid_vfs_to_linker};
use pysandbox_protocol::{Frame, FrameKind, FuelOperation, RpcCall, encode_payload};
use tokio::io::AsyncWrite;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use wasmtime::component::{Accessor, Component, HasSelf, Linker, ResourceTable};
use wasmtime::{
    AsContextMut, Config, Engine, ResourceLimiter, Store, StoreContextMut, StoreLimits,
    StoreLimitsBuilder, UpdateDeadline,
};
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};
use wasmtime_wasi::p2::{OutputStream, Pollable, StreamError};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::cpu_share::{CpuShare, CpuShareConfig, CpuShareWorker};
use crate::remote_vfs::RemoteVfs;

const EPOCH_INTERVAL: Duration = Duration::from_millis(10);
const DEFAULT_FUEL_YIELD_INTERVAL: u64 = 10_000_000;

wasmtime::component::bindgen!({
    path: "../../component/wit",
    world: "python",
    imports: {
        default: async,
    },
    exports: {
        default: async | store,
    },
    require_store_data_send: true,
});

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputSource {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputEvent {
    pub source: OutputSource,
    pub data: Bytes,
}

#[derive(Clone, Debug)]
pub struct Output {
    state: Arc<Mutex<OutputState>>,
}

#[derive(Debug, Default)]
struct OutputState {
    events: Vec<OutputEvent>,
    max_bytes: usize,
    size: usize,
    limit_exceeded: bool,
    sender: Option<mpsc::UnboundedSender<OutputEvent>>,
}

impl Default for Output {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(OutputState {
                events: Vec::new(),
                max_bytes: usize::MAX,
                size: 0,
                limit_exceeded: false,
                sender: None,
            })),
        }
    }
}

impl Output {
    pub fn events(&self) -> Vec<OutputEvent> {
        self.state
            .lock()
            .expect("output lock poisoned")
            .events
            .clone()
    }

    fn begin(&self, max_bytes: usize, sender: mpsc::UnboundedSender<OutputEvent>) {
        let mut state = self.state.lock().expect("output lock poisoned");
        state.events.clear();
        state.max_bytes = max_bytes;
        state.size = 0;
        state.limit_exceeded = false;
        state.sender = Some(sender);
    }

    fn finish(&self) {
        self.state.lock().expect("output lock poisoned").sender = None;
    }

    fn set_limit(&self, max_bytes: usize) -> Result<()> {
        let mut state = self.state.lock().expect("output lock poisoned");
        if state.size > max_bytes {
            bail!(
                "guest has already produced {} bytes, above the new output limit of {max_bytes}",
                state.size
            );
        }
        state.max_bytes = max_bytes;
        Ok(())
    }

    fn writable_bytes(&self) -> Result<usize> {
        let mut state = self.state.lock().expect("output lock poisoned");
        let remaining = state.max_bytes.saturating_sub(state.size);
        if remaining == 0 {
            state.limit_exceeded = true;
            bail!("guest output exceeded {} bytes", state.max_bytes);
        }
        Ok(remaining)
    }

    fn write(&self, source: OutputSource, data: &[u8]) -> Result<()> {
        let mut state = self.state.lock().expect("output lock poisoned");
        let remaining = state.max_bytes.saturating_sub(state.size);
        if data.len() > remaining {
            state.limit_exceeded = true;
            bail!("guest output exceeded {} bytes", state.max_bytes);
        }

        state.size += data.len();
        if !data.is_empty() {
            let event = OutputEvent {
                source,
                data: Bytes::copy_from_slice(data),
            };
            state.events.push(event.clone());
            if let Some(sender) = &state.sender {
                let _ = sender.send(event);
            }
        }
        Ok(())
    }

    pub fn limit_error(&self) -> Option<String> {
        let state = self.state.lock().expect("output lock poisoned");
        state
            .limit_exceeded
            .then(|| format!("guest output exceeded {} bytes", state.max_bytes))
    }
}

#[derive(Clone)]
struct CapturedOutputStream {
    output: Output,
    source: OutputSource,
}

impl IsTerminal for CapturedOutputStream {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for CapturedOutputStream {
    fn async_stream(&self) -> Box<dyn AsyncWrite + Send + Sync> {
        Box::new(self.clone())
    }

    fn p2_stream(&self) -> Box<dyn OutputStream> {
        Box::new(self.clone())
    }
}

impl AsyncWrite for CapturedOutputStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let stream = self.get_mut();
        match stream.output.write(stream.source, data) {
            Ok(()) => Poll::Ready(Ok(data.len())),
            Err(error) => Poll::Ready(Err(io::Error::other(error))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[wasmtime_wasi::async_trait]
impl OutputStream for CapturedOutputStream {
    fn write(&mut self, data: Bytes) -> std::result::Result<(), StreamError> {
        self.output
            .write(self.source, &data)
            .map_err(|error| StreamError::Trap(wasmtime::Error::msg(error.to_string())))
    }

    fn flush(&mut self) -> std::result::Result<(), StreamError> {
        Ok(())
    }

    fn check_write(&mut self) -> std::result::Result<usize, StreamError> {
        self.output
            .writable_bytes()
            .map_err(|error| StreamError::Trap(wasmtime::Error::msg(error.to_string())))
    }
}

#[wasmtime_wasi::async_trait]
impl Pollable for CapturedOutputStream {
    async fn ready(&mut self) {}
}

#[derive(Clone, Debug)]
pub struct ExecutionLimits {
    pub max_memory_bytes: usize,
    pub max_output_bytes: usize,
    pub max_guest_rpc_bytes: usize,
    pub cpu_share_weight: u64,
    pub fuel: u64,
    pub timeout: Option<Duration>,
}

pub(crate) enum ControlMessage {
    ApplyUpdates,
    Close,
}

#[derive(Debug)]
pub(crate) struct WorkerCallMessage {
    request_id: u64,
    path: Vec<String>,
    arguments: Vec<u8>,
    fuel: Option<FuelOperation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkerCallEnqueueError {
    Full,
    Closed,
}

#[derive(Debug)]
enum PendingStoreUpdate {
    SetFuel {
        fuel: u64,
        applied: oneshot::Sender<std::result::Result<(), String>>,
    },
    AddFuel {
        amount: u64,
        cap: Option<u64>,
        applied: oneshot::Sender<std::result::Result<(), String>>,
    },
    SetMemoryLimit {
        max_memory_bytes: usize,
        applied: oneshot::Sender<std::result::Result<(), String>>,
    },
    SetGuestRpcLimit {
        max_guest_rpc_bytes: usize,
        applied: oneshot::Sender<std::result::Result<(), String>>,
    },
    SetCpuShareWeight {
        weight: u64,
        applied: oneshot::Sender<std::result::Result<(), String>>,
    },
}

#[derive(Debug, Default)]
struct ExecutionControlState {
    active_execution: AtomicU64,
    cancelled: AtomicBool,
    closed: AtomicBool,
    pending_cancellations: Mutex<HashSet<u64>>,
    timeout_deadline: Mutex<Option<Instant>>,
    pending: Mutex<Vec<PendingStoreUpdate>>,
}

#[derive(Clone)]
pub struct WorkerControl {
    sender: mpsc::UnboundedSender<ControlMessage>,
    worker_calls: mpsc::Sender<WorkerCallMessage>,
    state: Arc<ExecutionControlState>,
    output: Output,
}

impl WorkerControl {
    pub(crate) fn new(
        worker_queue_capacity: usize,
    ) -> (
        Self,
        mpsc::UnboundedReceiver<ControlMessage>,
        mpsc::Receiver<WorkerCallMessage>,
    ) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (worker_calls, worker_call_receiver) = mpsc::channel(worker_queue_capacity);
        (
            Self {
                sender,
                worker_calls,
                state: Arc::new(ExecutionControlState::default()),
                output: Output::default(),
            },
            receiver,
            worker_call_receiver,
        )
    }

    pub fn cancel(&self, execution_id: u64) -> Result<()> {
        if self.state.active_execution.load(Ordering::Acquire) == execution_id {
            self.state.cancelled.store(true, Ordering::Release);
        } else {
            self.state
                .pending_cancellations
                .lock()
                .expect("worker cancellation lock poisoned")
                .insert(execution_id);
        }
        Ok(())
    }

    pub async fn set_fuel(&self, execution_id: u64, fuel: u64) -> Result<()> {
        let (applied, receiver) = oneshot::channel();
        self.queue(execution_id, PendingStoreUpdate::SetFuel { fuel, applied })?;
        receive_update_result(receiver).await
    }

    pub async fn add_fuel(&self, execution_id: u64, amount: u64, cap: Option<u64>) -> Result<()> {
        let (applied, receiver) = oneshot::channel();
        self.queue(
            execution_id,
            PendingStoreUpdate::AddFuel {
                amount,
                cap,
                applied,
            },
        )?;
        receive_update_result(receiver).await
    }

    pub async fn set_limits(
        &self,
        execution_id: u64,
        max_memory_bytes: Option<usize>,
        max_output_bytes: Option<usize>,
        max_guest_rpc_bytes: Option<usize>,
        cpu_share_weight: Option<u64>,
        timeout: Option<Duration>,
    ) -> Result<()> {
        self.ensure_active(execution_id)?;
        if cpu_share_weight == Some(0) {
            anyhow::bail!("CPU share weight must be positive");
        }
        if let Some(max_output_bytes) = max_output_bytes {
            self.output.set_limit(max_output_bytes)?;
        }
        let mut updates = Vec::new();
        if let Some(max_memory_bytes) = max_memory_bytes {
            let (applied, receiver) = oneshot::channel();
            self.state
                .pending
                .lock()
                .expect("worker control lock poisoned")
                .push(PendingStoreUpdate::SetMemoryLimit {
                    max_memory_bytes,
                    applied,
                });
            updates.push(receiver);
        }
        if let Some(max_guest_rpc_bytes) = max_guest_rpc_bytes {
            let (applied, receiver) = oneshot::channel();
            self.state
                .pending
                .lock()
                .expect("worker control lock poisoned")
                .push(PendingStoreUpdate::SetGuestRpcLimit {
                    max_guest_rpc_bytes,
                    applied,
                });
            updates.push(receiver);
        }
        if let Some(weight) = cpu_share_weight {
            let (applied, receiver) = oneshot::channel();
            self.state
                .pending
                .lock()
                .expect("worker control lock poisoned")
                .push(PendingStoreUpdate::SetCpuShareWeight { weight, applied });
            updates.push(receiver);
        }
        if let Some(timeout) = timeout {
            *self
                .state
                .timeout_deadline
                .lock()
                .expect("worker deadline lock poisoned") = Some(Instant::now() + timeout);
        }
        if !updates.is_empty() {
            self.send(ControlMessage::ApplyUpdates)?;
        }
        for receiver in updates {
            receive_update_result(receiver).await?;
        }
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        self.state.closed.store(true, Ordering::Release);
        self.send(ControlMessage::Close)
    }

    pub fn was_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    pub fn timed_out(&self) -> bool {
        self.state
            .timeout_deadline
            .lock()
            .expect("worker deadline lock poisoned")
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    pub(crate) fn worker_call(
        &self,
        request_id: u64,
        path: Vec<String>,
        arguments: Vec<u8>,
        fuel: Option<FuelOperation>,
    ) -> std::result::Result<(), WorkerCallEnqueueError> {
        self.worker_calls
            .try_send(WorkerCallMessage {
                request_id,
                path,
                arguments,
                fuel,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => WorkerCallEnqueueError::Full,
                mpsc::error::TrySendError::Closed(_) => WorkerCallEnqueueError::Closed,
            })
    }

    fn send(&self, message: ControlMessage) -> Result<()> {
        self.sender
            .send(message)
            .map_err(|_| anyhow!("worker control queue is closed"))
    }

    fn begin(&self, execution_id: u64, timeout: Option<Duration>) {
        let cancelled = self
            .state
            .pending_cancellations
            .lock()
            .expect("worker cancellation lock poisoned")
            .remove(&execution_id);
        self.state.cancelled.store(cancelled, Ordering::Release);
        reject_pending_store_updates(
            &self.state,
            "execution ended before applying its control update",
        );
        *self
            .state
            .timeout_deadline
            .lock()
            .expect("worker deadline lock poisoned") =
            timeout.map(|duration| Instant::now() + duration);
        self.state
            .active_execution
            .store(execution_id, Ordering::Release);
    }

    fn finish(&self, execution_id: u64) {
        self.state
            .pending_cancellations
            .lock()
            .expect("worker cancellation lock poisoned")
            .remove(&execution_id);
        let _ = self.state.active_execution.compare_exchange(
            execution_id,
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        reject_pending_store_updates(
            &self.state,
            "execution ended before applying its control update",
        );
    }

    fn ensure_active(&self, execution_id: u64) -> Result<()> {
        if self.state.active_execution.load(Ordering::Acquire) != execution_id {
            bail!("execution {execution_id} is not active");
        }
        Ok(())
    }

    fn queue(&self, execution_id: u64, update: PendingStoreUpdate) -> Result<()> {
        self.ensure_active(execution_id)?;
        self.state
            .pending
            .lock()
            .expect("worker control lock poisoned")
            .push(update);
        self.send(ControlMessage::ApplyUpdates)
    }
}

fn reject_pending_store_updates(control: &ExecutionControlState, error: &str) {
    let updates = std::mem::take(
        &mut *control
            .pending
            .lock()
            .expect("worker control lock poisoned"),
    );
    for update in updates {
        let applied = match update {
            PendingStoreUpdate::SetFuel { applied, .. }
            | PendingStoreUpdate::AddFuel { applied, .. }
            | PendingStoreUpdate::SetMemoryLimit { applied, .. }
            | PendingStoreUpdate::SetGuestRpcLimit { applied, .. }
            | PendingStoreUpdate::SetCpuShareWeight { applied, .. } => applied,
        };
        let _ = applied.send(Err(error.into()));
    }
}

async fn receive_update_result(
    receiver: oneshot::Receiver<std::result::Result<(), String>>,
) -> Result<()> {
    receiver
        .await
        .map_err(|_| anyhow!("worker stopped before applying its control update"))?
        .map_err(anyhow::Error::msg)
}

struct ComponentState {
    program: String,
    table: ResourceTable,
    wasi: WasiCtx,
    vfs: HybridVfsCtx<RemoteVfs>,
    limits: ExecutionStoreLimits,
    control_queue: Arc<AsyncMutex<mpsc::UnboundedReceiver<ControlMessage>>>,
    worker_call_queue: Arc<AsyncMutex<mpsc::Receiver<WorkerCallMessage>>>,
    execution_control: Arc<ExecutionControlState>,
    rpc: RpcBridge,
    rpc_methods: HashSet<String>,
    max_guest_rpc_bytes: usize,
    cpu_share: Arc<CpuShareWorker>,
}

struct ExecutionStoreLimits {
    inner: StoreLimits,
    max_memory_bytes: usize,
    memory_limit_exceeded: bool,
}

impl ExecutionStoreLimits {
    fn new(max_memory_bytes: usize) -> Self {
        Self {
            inner: StoreLimitsBuilder::new()
                .memory_size(max_memory_bytes)
                .trap_on_grow_failure(true)
                .build(),
            max_memory_bytes,
            memory_limit_exceeded: false,
        }
    }

    fn limit_error(&self) -> Option<String> {
        self.memory_limit_exceeded
            .then(|| format!("guest memory exceeded {} bytes", self.max_memory_bytes))
    }
}

impl ResourceLimiter for ExecutionStoreLimits {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> std::result::Result<bool, wasmtime::Error> {
        if desired > self.max_memory_bytes {
            self.memory_limit_exceeded = true;
        }
        self.inner.memory_growing(current, desired, maximum)
    }

    fn memory_grow_failed(
        &mut self,
        error: wasmtime::Error,
    ) -> std::result::Result<(), wasmtime::Error> {
        self.inner.memory_grow_failed(error)
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> std::result::Result<bool, wasmtime::Error> {
        self.inner.table_growing(current, desired, maximum)
    }

    fn table_grow_failed(
        &mut self,
        error: wasmtime::Error,
    ) -> std::result::Result<(), wasmtime::Error> {
        self.inner.table_grow_failed(error)
    }

    fn instances(&self) -> usize {
        self.inner.instances()
    }

    fn tables(&self) -> usize {
        self.inner.tables()
    }

    fn memories(&self) -> usize {
        self.inner.memories()
    }
}

pub(crate) type PendingGuestCalls = Arc<
    AsyncMutex<
        std::collections::HashMap<u64, oneshot::Sender<std::result::Result<Vec<u8>, String>>>,
    >,
>;

#[derive(Clone)]
pub(crate) struct RpcBridge {
    worker_id: u64,
    outgoing: mpsc::Sender<Frame>,
    next_request_id: Arc<AtomicU64>,
    pending: PendingGuestCalls,
}

impl RpcBridge {
    pub(crate) fn new(
        worker_id: u64,
        outgoing: mpsc::Sender<Frame>,
        next_request_id: Arc<AtomicU64>,
        pending: PendingGuestCalls,
    ) -> Self {
        Self {
            worker_id,
            outgoing,
            next_request_id,
            pending,
        }
    }

    async fn call(&self, method: String, arguments: Vec<u8>) -> Result<Vec<u8>, String> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let payload =
            encode_payload(&RpcCall { method, arguments }).map_err(|error| error.to_string())?;
        let (response, receiver) = oneshot::channel();
        self.pending.lock().await.insert(request_id, response);
        if self
            .outgoing
            .send(Frame::new(
                FrameKind::GuestCall,
                self.worker_id,
                request_id,
                payload,
            ))
            .await
            .is_err()
        {
            self.pending.lock().await.remove(&request_id);
            return Err("sandbox supervisor connection is closed".into());
        }
        receiver
            .await
            .map_err(|_| "guest RPC response channel was closed".into())
            .and_then(|result| result)
    }

    async fn worker_response(&self, request_id: u64, value: Vec<u8>, error: Option<String>) {
        let payload = match encode_payload(&pysandbox_protocol::RpcResult { value, error }) {
            Ok(payload) => payload,
            Err(_) => return,
        };
        let _ = self
            .outgoing
            .send(Frame::new(
                FrameKind::WorkerResponse,
                self.worker_id,
                request_id,
                payload,
            ))
            .await;
    }
}

impl WasiView for ComponentState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl HybridVfsView for ComponentState {
    type Storage = RemoteVfs;

    fn hybrid_vfs(&mut self) -> HybridVfsState<'_, Self::Storage> {
        HybridVfsState::new(&mut self.vfs, &mut self.table)
    }
}

impl pysandbox::python::host::Host for ComponentState {
    async fn program(&mut self) -> String {
        self.program.clone()
    }
}

impl pysandbox::python::host::HostWithStore<ComponentState> for HasSelf<ComponentState> {
    async fn call(
        accessor: &Accessor<ComponentState, Self>,
        method: String,
        arguments: Vec<u8>,
    ) -> std::result::Result<Vec<u8>, String> {
        let rpc = accessor.with(|mut access| {
            let state = access.get();
            if arguments.len() > state.max_guest_rpc_bytes {
                return Err(format!(
                    "guest RPC payload exceeded {} bytes",
                    state.max_guest_rpc_bytes
                ));
            }
            if !state.rpc_methods.contains(&method) {
                return Err(format!("RPC method is not available: {method}"));
            }
            Ok(state.rpc.clone())
        })?;
        rpc.call(method, arguments).await
    }

    async fn spin_next(
        accessor: &Accessor<ComponentState, Self>,
    ) -> Option<pysandbox::python::host::SpinEvent> {
        let (control_queue, worker_call_queue, execution_control) = accessor.with(|mut access| {
            let state = access.get();
            (
                state.control_queue.clone(),
                state.worker_call_queue.clone(),
                state.execution_control.clone(),
            )
        });
        let mut control_queue = control_queue.lock().await;
        let mut worker_call_queue = worker_call_queue.lock().await;
        tokio::select! {
          biased;
          message = control_queue.recv() => match message? {
            ControlMessage::ApplyUpdates => {
                accessor.with(|mut access| {
                    apply_pending_store_updates(access.as_context_mut(), &execution_control);
                });
                Some(pysandbox::python::host::SpinEvent { call: None })
            }
            ControlMessage::Close => None,
          },
          message = worker_call_queue.recv() => {
            let WorkerCallMessage {
                request_id,
                path,
                arguments,
                fuel,
            } = message?;
            let fuel_result =
                accessor.with(|mut access| apply_call_fuel(access.as_context_mut(), fuel));
            if let Err(error) = fuel_result {
                let rpc = accessor.with(|mut access| access.get().rpc.clone());
                rpc.worker_response(request_id, Vec::new(), Some(error))
                    .await;
                return Some(pysandbox::python::host::SpinEvent { call: None });
            }
            Some(pysandbox::python::host::SpinEvent {
                call: Some(pysandbox::python::host::WorkerCall {
                    request_id,
                    path,
                    arguments,
                }),
            })
          },
        }
    }

    async fn worker_response(
        accessor: &Accessor<ComponentState, Self>,
        request_id: u64,
        value: Vec<u8>,
        error: Option<String>,
    ) {
        let (rpc, maximum) = accessor.with(|mut access| {
            let state = access.get();
            (state.rpc.clone(), state.max_guest_rpc_bytes)
        });
        if value.len() > maximum {
            rpc.worker_response(
                request_id,
                Vec::new(),
                Some(format!("guest RPC payload exceeded {maximum} bytes")),
            )
            .await;
        } else {
            rpc.worker_response(request_id, value, error).await;
        }
    }
}

fn apply_call_fuel(
    mut store: StoreContextMut<'_, ComponentState>,
    operation: Option<FuelOperation>,
) -> std::result::Result<(), String> {
    let result = match operation {
        None => Ok(()),
        Some(FuelOperation::Set { fuel }) => {
            store.set_fuel(fuel).map_err(|error| error.to_string())
        }
        Some(FuelOperation::Add { amount, cap }) => store
            .get_fuel()
            .map(|fuel| fuel.saturating_add(amount))
            .and_then(|fuel| store.set_fuel(cap.map_or(fuel, |cap| fuel.min(cap))))
            .map_err(|error| error.to_string()),
    };
    if result.is_ok()
        && operation.is_some()
        && let Ok(fuel) = store.get_fuel()
    {
        store.data().cpu_share.reset_fuel(fuel);
    }
    result
}

pub struct ComponentWorker {
    store: Store<ComponentState>,
    guest: Python,
    output: Output,
    control: WorkerControl,
    cpu_share: Arc<CpuShareWorker>,
}

#[derive(Clone)]
pub struct ComponentRuntime {
    engine: Engine,
    component: Component,
    linker: Arc<Linker<ComponentState>>,
    vfs: RemoteVfs,
    hybrid_filesystem: bool,
    cpu_share: Arc<CpuShare>,
}

impl ComponentRuntime {
    pub(crate) fn load(
        component_path: &Path,
        vfs: RemoteVfs,
        cpu_share_enabled: bool,
        cpu_share_limit_percent: Option<f64>,
        cpu_share_sample_interval: Duration,
        cpu_share_activity_timeout: Duration,
    ) -> Result<Self> {
        let mut cpu_share_config = CpuShareConfig::new(DEFAULT_FUEL_YIELD_INTERVAL);
        cpu_share_config.enabled = cpu_share_enabled;
        cpu_share_config.limit_percent = cpu_share_limit_percent;
        cpu_share_config.sample_interval = cpu_share_sample_interval;
        cpu_share_config.activity_timeout = cpu_share_activity_timeout;
        Self::load_with_filesystem(component_path, vfs, true, cpu_share_config)
    }

    fn load_with_filesystem(
        component_path: &Path,
        vfs: RemoteVfs,
        hybrid_filesystem: bool,
        cpu_share_config: CpuShareConfig,
    ) -> Result<Self> {
        let mut config = Config::new();
        config.wasm_component_model_async(true);
        config.consume_fuel(true);
        config.epoch_interruption(true);

        let engine = Engine::new(&config)?;
        let epoch_engine = engine.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(EPOCH_INTERVAL);
            loop {
                interval.tick().await;
                epoch_engine.increment_epoch();
            }
        });
        let component = Component::from_file(&engine, component_path)?;
        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        if hybrid_filesystem {
            linker.allow_shadowing(true);
            add_hybrid_vfs_to_linker(&mut linker)?;
            linker.allow_shadowing(false);
        }
        wasmtime_wasi::p3::add_to_linker(&mut linker)?;
        pysandbox::python::host::add_to_linker::<_, HasSelf<_>>(&mut linker, |state| state)?;

        Ok(Self {
            engine,
            component,
            linker: Arc::new(linker),
            vfs,
            hybrid_filesystem,
            cpu_share: CpuShare::start(cpu_share_config),
        })
    }
}

impl ComponentWorker {
    pub(crate) async fn load(
        runtime: &ComponentRuntime,
        python_root: &Path,
        worker_id: u64,
        rpc: RpcBridge,
        control: WorkerControl,
        control_receiver: mpsc::UnboundedReceiver<ControlMessage>,
        worker_call_receiver: mpsc::Receiver<WorkerCallMessage>,
        package_paths: &[String],
    ) -> Result<Self> {
        Self::load_with_python_permissions(
            runtime,
            python_root,
            worker_id,
            rpc,
            control,
            control_receiver,
            worker_call_receiver,
            package_paths,
            DirPerms::READ,
            FilePerms::READ,
        )
        .await
    }

    async fn load_with_python_permissions(
        runtime: &ComponentRuntime,
        python_root: &Path,
        worker_id: u64,
        rpc: RpcBridge,
        control: WorkerControl,
        control_receiver: mpsc::UnboundedReceiver<ControlMessage>,
        worker_call_receiver: mpsc::Receiver<WorkerCallMessage>,
        package_paths: &[String],
        python_dir_perms: DirPerms,
        python_file_perms: FilePerms,
    ) -> Result<Self> {
        let engine = runtime.engine.clone();
        let output = control.output.clone();
        let cpu_share = Arc::new(runtime.cpu_share.worker(worker_id));
        let mut wasi = WasiCtxBuilder::new();
        wasi.stdout(CapturedOutputStream {
            output: output.clone(),
            source: OutputSource::Stdout,
        })
        .stderr(CapturedOutputStream {
            output: output.clone(),
            source: OutputSource::Stderr,
        });
        if !runtime.hybrid_filesystem {
            wasi.preopened_dir(python_root, "/python", python_dir_perms, python_file_perms)?;
        }
        let wasi = wasi.build();
        let vfs = python_vfs(
            runtime,
            python_root,
            worker_id,
            package_paths,
            python_dir_perms,
            python_file_perms,
        )?;
        let mut store = Store::new(
            &engine,
            ComponentState {
                program: String::new(),
                table: ResourceTable::new(),
                wasi,
                vfs,
                limits: ExecutionStoreLimits::new(usize::MAX),
                control_queue: Arc::new(AsyncMutex::new(control_receiver)),
                worker_call_queue: Arc::new(AsyncMutex::new(worker_call_receiver)),
                execution_control: control.state.clone(),
                rpc,
                rpc_methods: HashSet::new(),
                max_guest_rpc_bytes: 10 * 1024 * 1024,
                cpu_share: cpu_share.clone(),
            },
        );
        store.limiter(|state| &mut state.limits);
        let control_state = control.state.clone();
        let epoch_cpu_share = cpu_share.clone();
        store.epoch_deadline_callback(move |mut store| {
            if control_state.cancelled.load(Ordering::Acquire) {
                return Ok(UpdateDeadline::Interrupt);
            }
            if control_state.closed.load(Ordering::Acquire) {
                return Ok(UpdateDeadline::Interrupt);
            }
            if control_state
                .timeout_deadline
                .lock()
                .expect("worker deadline lock poisoned")
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return Ok(UpdateDeadline::Interrupt);
            }

            let updated = apply_pending_store_updates(store.as_context_mut(), &control_state);
            if let Ok(fuel) = store.get_fuel() {
                epoch_cpu_share.observe_fuel(fuel);
            }
            Ok(if updated {
                UpdateDeadline::Continue(1)
            } else {
                UpdateDeadline::Yield(1)
            })
        });
        store.set_epoch_deadline(1);
        store.set_fuel(u64::MAX)?;
        let guest =
            Python::instantiate_async(&mut store, &runtime.component, &runtime.linker).await?;
        let initialize = store
            .run_concurrent(async |store| guest.call_initialize(store).await)
            .await??;
        initialize.map_err(anyhow::Error::msg)?;

        Ok(Self {
            store,
            guest,
            output,
            control,
            cpu_share,
        })
    }

    pub fn output(&self) -> Output {
        self.output.clone()
    }

    pub fn control(&self) -> WorkerControl {
        self.control.clone()
    }

    pub fn memory_limit_error(&self) -> Option<String> {
        self.store.data().limits.limit_error()
    }

    pub async fn run(
        &mut self,
        execution_id: u64,
        program: String,
        limits: ExecutionLimits,
        rpc_methods: Vec<String>,
        output_sender: mpsc::UnboundedSender<OutputEvent>,
    ) -> Result<std::result::Result<(), String>> {
        self.control.begin(execution_id, limits.timeout);
        self.store.data_mut().program = program;
        self.store.data_mut().rpc_methods = rpc_methods.into_iter().collect();
        if let Err(error) = self.apply_limits(&limits, output_sender) {
            self.output.finish();
            self.control.finish(execution_id);
            return Err(error);
        }
        let guest = &self.guest;
        let cpu_share = self.cpu_share.clone();
        let result = self
            .store
            .run_concurrent(async move |store| cpu_share.run(guest.call_run(store)).await)
            .await;

        self.cpu_share.finish();
        self.output.finish();
        self.control.finish(execution_id);

        Ok(result??)
    }

    fn apply_limits(
        &mut self,
        limits: &ExecutionLimits,
        output_sender: mpsc::UnboundedSender<OutputEvent>,
    ) -> Result<()> {
        self.output.begin(limits.max_output_bytes, output_sender);
        self.store.data_mut().max_guest_rpc_bytes = limits.max_guest_rpc_bytes;
        self.store.data_mut().limits = ExecutionStoreLimits::new(limits.max_memory_bytes);
        self.store.set_fuel(limits.fuel)?;
        self.cpu_share.begin(limits.fuel, limits.cpu_share_weight);
        self.store
            .fuel_async_yield_interval(Some(DEFAULT_FUEL_YIELD_INTERVAL))?;
        self.store.set_epoch_deadline(1);
        Ok(())
    }
}

pub async fn compile_python_root(component_path: &Path, python_root: &Path) -> Result<()> {
    let mut files = Vec::new();
    collect_python_files(python_root, python_root, &mut files)?;
    let mut program = String::from(
        "import sys\nsys.dont_write_bytecode = True\nimport py_compile\nfor path in (\n",
    );
    for file in files {
        program.push_str(&format!("  {file:?},\n"));
    }
    program.push_str("):\n  py_compile.compile(path, doraise=True)\n");

    let (result, output) = run_build_program(component_path, python_root, program).await?;
    result.map_err(|error| {
        let output = output
            .into_iter()
            .map(|event| String::from_utf8_lossy(&event.data).into_owned())
            .collect::<String>();
        anyhow!("failed to compile Python standard library: {error}\n{output}")
    })
}

async fn run_build_program(
    component_path: &Path,
    python_root: &Path,
    program: String,
) -> Result<(std::result::Result<(), String>, Vec<OutputEvent>)> {
    let (outgoing, _outgoing_receiver) = mpsc::channel(1);
    let pending_guest_calls = Arc::new(AsyncMutex::new(std::collections::HashMap::new()));
    let pending_vfs_requests = Arc::new(AsyncMutex::new(std::collections::HashMap::new()));
    let next_request_id = Arc::new(AtomicU64::new(1));
    let vfs = RemoteVfs::new(
        outgoing.clone(),
        next_request_id.clone(),
        pending_vfs_requests,
        crate::remote_vfs::CachePolicy::None,
    );
    let mut cpu_share_config = CpuShareConfig::new(DEFAULT_FUEL_YIELD_INTERVAL);
    cpu_share_config.enabled = false;
    let runtime =
        ComponentRuntime::load_with_filesystem(component_path, vfs, false, cpu_share_config)?;
    let (control, control_receiver, worker_call_receiver) = WorkerControl::new(1);
    let rpc = RpcBridge::new(0, outgoing, next_request_id, pending_guest_calls);
    let mut worker = ComponentWorker::load_with_python_permissions(
        &runtime,
        python_root,
        0,
        rpc,
        control,
        control_receiver,
        worker_call_receiver,
        &[],
        DirPerms::all(),
        FilePerms::all(),
    )
    .await?;
    let (output_sender, _output_receiver) = mpsc::unbounded_channel();
    let result = worker
        .run(
            0,
            program,
            ExecutionLimits {
                max_memory_bytes: 512 * 1024 * 1024,
                max_output_bytes: 1024 * 1024,
                max_guest_rpc_bytes: 1,
                cpu_share_weight: 1,
                fuel: u64::MAX,
                timeout: Some(Duration::from_secs(120)),
            },
            Vec::new(),
            output_sender,
        )
        .await?;
    Ok((result, worker.output().events()))
}

fn collect_python_files(root: &Path, directory: &Path, files: &mut Vec<String>) -> Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_python_files(root, &path, files)?;
        } else if path.extension().is_some_and(|extension| extension == "py") {
            files.push(format!(
                "/python/{}",
                path.strip_prefix(root)?
                    .to_str()
                    .ok_or_else(|| anyhow!("Python source path is not UTF-8"))?
                    .replace('\\', "/")
            ));
        }
    }
    files.sort();
    Ok(())
}

fn python_vfs(
    runtime: &ComponentRuntime,
    python_root: &Path,
    worker_id: u64,
    package_paths: &[String],
    dir_perms: DirPerms,
    file_perms: FilePerms,
) -> Result<HybridVfsCtx<RemoteVfs>> {
    let mut vfs = HybridVfsCtx::new(runtime.vfs.for_worker(worker_id));
    vfs.allow_blocking_current_thread(true);
    vfs.add_vfs_preopen("/", DirPerms::READ, FilePerms::READ);
    let mut python = RealDir::open_ambient(python_root, dir_perms, file_perms)?;
    python.allow_blocking = true;
    for package_path in package_paths {
        let path = Path::new(package_path);
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("package path is not UTF-8: {package_path}"))?;
        let guest_path = format!("lib/python3.14/site-packages/{name}");
        if path.is_dir() {
            python.overlay_directory(&guest_path, path)?;
        } else if path.is_file() {
            python.overlay_file(&guest_path, path)?;
        } else {
            return Err(anyhow!("package path does not exist: {package_path}"));
        }
    }
    vfs.add_real_preopen("/python", python);
    Ok(vfs)
}

fn apply_pending_store_updates(
    mut store: StoreContextMut<'_, ComponentState>,
    control: &ExecutionControlState,
) -> bool {
    let updates = std::mem::take(
        &mut *control
            .pending
            .lock()
            .expect("worker control lock poisoned"),
    );
    let updated = !updates.is_empty();
    for update in updates {
        match update {
            PendingStoreUpdate::SetFuel { fuel, applied } => {
                let result = store.set_fuel(fuel).map_err(|error| error.to_string());
                if result.is_ok() {
                    store.data().cpu_share.reset_fuel(fuel);
                }
                let _ = applied.send(result);
            }
            PendingStoreUpdate::AddFuel {
                amount,
                cap,
                applied,
            } => {
                let result = store
                    .get_fuel()
                    .map(|fuel| fuel.saturating_add(amount))
                    .and_then(|fuel| store.set_fuel(cap.map_or(fuel, |cap| fuel.min(cap))))
                    .map_err(|error| error.to_string());
                if result.is_ok()
                    && let Ok(fuel) = store.get_fuel()
                {
                    store.data().cpu_share.reset_fuel(fuel);
                }
                let _ = applied.send(result);
            }
            PendingStoreUpdate::SetMemoryLimit {
                max_memory_bytes,
                applied,
            } => {
                store.data_mut().limits = ExecutionStoreLimits::new(max_memory_bytes);
                let _ = applied.send(Ok(()));
            }
            PendingStoreUpdate::SetGuestRpcLimit {
                max_guest_rpc_bytes,
                applied,
            } => {
                store.data_mut().max_guest_rpc_bytes = max_guest_rpc_bytes;
                let _ = applied.send(Ok(()));
            }
            PendingStoreUpdate::SetCpuShareWeight { weight, applied } => {
                store.data().cpu_share.set_weight(weight);
                let _ = applied.send(Ok(()));
            }
        }
    }
    updated
}

#[cfg(test)]
mod tests {
    use super::{Output, OutputSource};
    use tokio::sync::mpsc;

    #[test]
    fn output_preserves_order_and_enforces_one_combined_limit() {
        let output = Output::default();
        let (sender, _receiver) = mpsc::unbounded_channel();
        output.begin(3, sender);
        output.write(OutputSource::Stdout, b"a").unwrap();
        output.write(OutputSource::Stderr, b"b").unwrap();
        output.write(OutputSource::Stdout, b"c").unwrap();

        assert_eq!(
            output.events(),
            vec![
                super::OutputEvent {
                    source: OutputSource::Stdout,
                    data: b"a".as_slice().into(),
                },
                super::OutputEvent {
                    source: OutputSource::Stderr,
                    data: b"b".as_slice().into(),
                },
                super::OutputEvent {
                    source: OutputSource::Stdout,
                    data: b"c".as_slice().into(),
                },
            ]
        );
        assert!(output.writable_bytes().is_err());
        assert!(output.write(OutputSource::Stderr, b"d").is_err());
        assert_eq!(
            output.limit_error().as_deref(),
            Some("guest output exceeded 3 bytes")
        );
    }
}
