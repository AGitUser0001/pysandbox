use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use interprocess::local_socket::{
    GenericFilePath, GenericNamespaced, ToFsName, ToNsName,
    tokio::{Stream, prelude::*},
};
use pyo3::exceptions::{
    PyAttributeError, PyFileExistsError, PyIndexError, PyIsADirectoryError, PyNotADirectoryError,
    PyNotImplementedError, PyPermissionError, PyRuntimeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};
use pysandbox_protocol::{
    CancelRequest, ControlResult, DEFAULT_MAX_FRAME_BYTES, ExecuteRequest, ExecuteResult,
    ExecutionControl, ExecutionLimits, Frame, FrameKind, FuelOperation, InvalidateVfs,
    OutputPayload, OutputSource, RpcCall, RpcResult, TerminationReason, VfsDirectoryEntry,
    VfsError, VfsErrorCode, VfsMetadata, VfsNodeKind, VfsRequest, VfsResponse, VfsStatResult,
    VfsValue, WorkerRpcCall, decode_payload, encode_payload, read_frame, write_frame,
};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

const START_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(10);

pyo3::create_exception!(_core, WorkerStoppedError, PyRuntimeError);

#[pyclass(frozen, get_all)]
struct RpcContext {
    worker_id: u64,
    request_id: u64,
}

#[pyfunction]
fn protocol_version() -> u16 {
    pysandbox_protocol::PROTOCOL_VERSION
}

#[pyfunction]
fn sleep<'py>(py: Python<'py>, milliseconds: u64) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        tokio::time::sleep(Duration::from_millis(milliseconds)).await;
        Python::attach(|py| Ok(py.None()))
    })
}

#[pyfunction]
fn run_sandboxd(
    py: Python<'_>,
    socket_name: String,
    component_path: PathBuf,
    python_root: PathBuf,
    max_ipc_frame_bytes: usize,
    worker_queue_capacity: usize,
    compilation_cache: Option<PathBuf>,
    cache_vfs: bool,
    cache_vfs_negative: bool,
    cpu_share_enabled: bool,
    cpu_share_limit_percent: Option<f64>,
    cpu_share_sample_interval_ms: u64,
    cpu_share_activity_timeout_ms: u64,
) -> PyResult<()> {
    py.detach(move || {
        let runtime = tokio::runtime::Runtime::new().map_err(runtime_error)?;
        runtime
            .block_on(pysandbox_sandboxd::server::serve(
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
                Duration::from_millis(cpu_share_sample_interval_ms),
                Duration::from_millis(cpu_share_activity_timeout_ms),
            ))
            .map_err(runtime_error)
    })
}

#[pyclass]
struct SandboxProcess {
    requests: mpsc::Sender<ConnectionRequest>,
    child: Arc<Mutex<Option<Child>>>,
    next_request_id: Arc<AtomicU64>,
    closed: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
    rpc_handlers: RpcHandlers,
    vfs_handler: VfsHandler,
}

type RpcHandlers = Arc<StdMutex<HashMap<String, RpcHandler>>>;
type VfsHandler = Arc<StdMutex<Option<RpcHandler>>>;

struct RpcHandler {
    callable: Py<PyAny>,
    locals: pyo3_async_runtimes::TaskLocals,
}

#[pymethods]
impl SandboxProcess {
    #[getter]
    fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn expose(&self, py: Python<'_>, method: String, handler: Py<PyAny>) -> PyResult<()> {
        if !handler.bind(py).is_callable() {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "RPC handler must be callable",
            ));
        }
        let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
        self.rpc_handlers
            .lock()
            .expect("RPC handler lock poisoned")
            .insert(
                method,
                RpcHandler {
                    callable: handler,
                    locals,
                },
            );
        Ok(())
    }

    fn set_vfs(&self, py: Python<'_>, handler: Py<PyAny>) -> PyResult<()> {
        let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
        *self.vfs_handler.lock().expect("VFS handler lock poisoned") = Some(RpcHandler {
            callable: handler,
            locals,
        });
        Ok(())
    }

    fn invalidate_vfs<'py>(
        &self,
        py: Python<'py>,
        path: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let requests = self.requests.clone();
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let payload = encode_payload(&InvalidateVfs { path }).map_err(runtime_error)?;
            let response = request(
                &requests,
                Frame::new(FrameKind::InvalidateVfs, 0, request_id, payload),
                Some(FrameKind::ControlResult),
            )
            .await?
            .ok_or_else(|| PyRuntimeError::new_err("VFS invalidation returned no response"))?;
            let result: ControlResult =
                decode_payload(&response.frame.payload).map_err(runtime_error)?;
            if let Some(error) = result.error {
                return Err(PyRuntimeError::new_err(error));
            }
            Python::attach(|py| Ok(py.None()))
        })
    }

    fn health<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let requests = self.requests.clone();
        let closed = self.closed.clone();
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if closed.load(Ordering::Acquire) {
                return Err(PyRuntimeError::new_err("sandbox process is closed"));
            }

            let _ = request(
                &requests,
                Frame::new(FrameKind::HealthCheck, 0, request_id, Vec::new()),
                Some(FrameKind::HealthStatus),
            )
            .await?;

            Python::attach(|py| Ok(py.None()))
        })
    }

    #[pyo3(signature = (
        program,
        *,
        worker_id = 0,
        rpc_methods = Vec::new(),
        package_paths = Vec::new(),
        max_memory_bytes = 128 * 1024 * 1024,
        max_output_bytes = 256 * 1024,
        max_guest_rpc_bytes = 10 * 1024 * 1024,
        guest_dispatch_request_concurrency = 16,
        guest_dispatch_request_queue_capacity = 64,
        cpu_share_weight = 1,
        fuel = u64::MAX,
        timeout = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn run(
        &self,
        py: Python<'_>,
        program: String,
        worker_id: u64,
        rpc_methods: Vec<String>,
        package_paths: Vec<PathBuf>,
        max_memory_bytes: u64,
        max_output_bytes: u64,
        max_guest_rpc_bytes: u64,
        guest_dispatch_request_concurrency: u64,
        guest_dispatch_request_queue_capacity: u64,
        cpu_share_weight: u64,
        fuel: u64,
        timeout: Option<f64>,
    ) -> PyResult<Py<NativeExecution>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PyRuntimeError::new_err("sandbox process is closed"));
        }
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let output = Arc::new(StdMutex::new(Vec::new()));
        let native_output = Py::new(
            py,
            NativeOutput {
                output: output.clone(),
            },
        )?;
        let (result_sender, result) = watch::channel(None);
        let (ready_sender, ready) = watch::channel(false);
        let requests = self.requests.clone();
        let timeout_ms = timeout_milliseconds(timeout)?;
        validate_guest_dispatch_limits(
            guest_dispatch_request_concurrency,
            guest_dispatch_request_queue_capacity,
        )?;
        validate_cpu_share_weight(cpu_share_weight)?;
        let payload = encode_payload(&ExecuteRequest {
            program,
            rpc_methods,
            package_paths: package_paths
                .into_iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect(),
            limits: ExecutionLimits {
                max_memory_bytes,
                max_output_bytes,
                max_guest_rpc_bytes,
                guest_dispatch_request_concurrency,
                guest_dispatch_request_queue_capacity,
                cpu_share_weight,
                fuel,
                timeout_ms,
            },
        })
        .map_err(runtime_error)?;
        let task_output = output.clone();
        pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
            let outcome = execute_request(
                &requests,
                worker_id,
                request_id,
                payload,
                task_output,
                ready_sender,
            )
            .await
            .map_err(|error| error.to_string());
            let _ = result_sender.send(Some(outcome));
        });

        Py::new(
            py,
            NativeExecution {
                worker_id,
                execution_id: request_id,
                requests: self.requests.clone(),
                next_request_id: self.next_request_id.clone(),
                native_output,
                ready,
                result,
                shutdown: self.shutdown.subscribe(),
            },
        )
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let requests = self.requests.clone();
        let child = self.child.clone();
        let closed = self.closed.clone();
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let was_closed = closed.swap(true, Ordering::AcqRel);
            if !was_closed {
                request(
                    &requests,
                    Frame::new(FrameKind::Shutdown, 0, request_id, Vec::new()),
                    None,
                )
                .await?;
            }

            let Some(mut child) = child.lock().await.take() else {
                return Python::attach(|py| Ok(py.None()));
            };
            let status = child.wait().await.map_err(runtime_error)?;
            if !status.success() && !was_closed {
                return Err(PyRuntimeError::new_err(format!(
                    "sandbox process exited with {status}"
                )));
            }

            Python::attach(|py| Ok(py.None()))
        })
    }

    fn terminate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let child = self.child.clone();
        let closed = self.closed.clone();
        let shutdown = self.shutdown.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            closed.store(true, Ordering::Release);
            shutdown.send_replace(true);
            let Some(mut child) = child.lock().await.take() else {
                return Python::attach(|py| Ok(py.None()));
            };
            child.start_kill().map_err(runtime_error)?;
            child.wait().await.map_err(runtime_error)?;
            Python::attach(|py| Ok(py.None()))
        })
    }

    fn close_worker<'py>(&self, py: Python<'py>, worker_id: u64) -> PyResult<Bound<'py, PyAny>> {
        let requests = self.requests.clone();
        let closed = self.closed.clone();
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if closed.load(Ordering::Acquire) {
                return Err(PyRuntimeError::new_err("sandbox process is closed"));
            }
            let response = request(
                &requests,
                Frame::new(FrameKind::CloseWorker, worker_id, request_id, Vec::new()),
                Some(FrameKind::ControlResult),
            )
            .await?
            .ok_or_else(|| PyRuntimeError::new_err("worker close returned no response"))?;
            let result: ControlResult =
                decode_payload(&response.frame.payload).map_err(runtime_error)?;
            if let Some(error) = result.error {
                return Err(PyRuntimeError::new_err(error));
            }
            Python::attach(|py| Ok(py.None()))
        })
    }
}

#[pyfunction]
#[pyo3(signature = (
    executable,
    socket_name,
    component_path,
    python_root,
    *,
    executable_arguments = Vec::new(),
    max_ipc_frame_bytes = DEFAULT_MAX_FRAME_BYTES,
    worker_queue_capacity = 256,
    compilation_cache = None,
    host_dispatch_concurrency = 64,
    host_dispatch_queue_capacity = 256,
    cache_vfs = false,
    cache_vfs_negative = false,
    cpu_share_enabled = false,
    cpu_share_limit_percent = None,
    cpu_share_sample_interval_ms = 100,
    cpu_share_activity_timeout_ms = 300,
))]
fn start_sandbox<'py>(
    py: Python<'py>,
    executable: PathBuf,
    socket_name: String,
    component_path: PathBuf,
    python_root: PathBuf,
    executable_arguments: Vec<String>,
    max_ipc_frame_bytes: usize,
    worker_queue_capacity: usize,
    compilation_cache: Option<PathBuf>,
    host_dispatch_concurrency: usize,
    host_dispatch_queue_capacity: usize,
    cache_vfs: bool,
    cache_vfs_negative: bool,
    cpu_share_enabled: bool,
    cpu_share_limit_percent: Option<f64>,
    cpu_share_sample_interval_ms: u64,
    cpu_share_activity_timeout_ms: u64,
) -> PyResult<Bound<'py, PyAny>> {
    if worker_queue_capacity == 0 {
        return Err(PyValueError::new_err(
            "worker_queue_capacity must be positive",
        ));
    }
    if host_dispatch_concurrency == 0 {
        return Err(PyValueError::new_err(
            "host_dispatch_concurrency must be positive",
        ));
    }
    if host_dispatch_queue_capacity == 0 {
        return Err(PyValueError::new_err(
            "host_dispatch_queue_capacity must be positive",
        ));
    }
    if cpu_share_sample_interval_ms == 0 {
        return Err(PyValueError::new_err(
            "cpu_share_sample_interval_ms must be positive",
        ));
    }
    if cpu_share_activity_timeout_ms == 0 {
        return Err(PyValueError::new_err(
            "cpu_share_activity_timeout_ms must be positive",
        ));
    }
    if cpu_share_limit_percent.is_some_and(|percent| !percent.is_finite() || percent <= 0.0) {
        return Err(PyValueError::new_err(
            "cpu_share_limit_percent must be positive and finite",
        ));
    }
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let mut child = Command::new(executable)
            .args(executable_arguments)
            .arg(&socket_name)
            .arg(component_path)
            .arg(python_root)
            .arg(max_ipc_frame_bytes.to_string())
            .arg(worker_queue_capacity.to_string())
            .arg(compilation_cache.map_or_else(|| "none".into(), |path| path.into_os_string()))
            .arg(cache_vfs.to_string())
            .arg(cache_vfs_negative.to_string())
            .arg(cpu_share_enabled.to_string())
            .arg(
                cpu_share_limit_percent
                    .map_or_else(|| "none".to_owned(), |percent| percent.to_string()),
            )
            .arg(cpu_share_sample_interval_ms.to_string())
            .arg(cpu_share_activity_timeout_ms.to_string())
            .kill_on_drop(true)
            .spawn()
            .map_err(runtime_error)?;
        let connection = connect_to_sandbox(&mut child, &socket_name).await?;
        let (host_dispatch, host_dispatch_receiver) = mpsc::channel(host_dispatch_queue_capacity);
        let closed = Arc::new(AtomicBool::new(false));
        let (requests, shutdown) = spawn_connection_actor(
            connection,
            host_dispatch,
            max_ipc_frame_bytes,
            closed.clone(),
        );
        let rpc_handlers = Arc::new(StdMutex::new(HashMap::new()));
        let vfs_handler = Arc::new(StdMutex::new(None));
        spawn_host_dispatcher(
            host_dispatch_receiver,
            requests.clone(),
            rpc_handlers.clone(),
            vfs_handler.clone(),
            Arc::new(Semaphore::new(host_dispatch_concurrency)),
        );

        Python::attach(|py| {
            Py::new(
                py,
                SandboxProcess {
                    requests,
                    child: Arc::new(Mutex::new(Some(child))),
                    next_request_id: Arc::new(AtomicU64::new(1)),
                    closed,
                    shutdown,
                    rpc_handlers,
                    vfs_handler,
                },
            )
        })
    })
}

struct ConnectionRequest {
    frame: Frame,
    expected_kind: Option<FrameKind>,
    response: oneshot::Sender<Result<Option<ConnectionResponse>, RequestError>>,
    output: Option<SharedOutput>,
    ready: Option<watch::Sender<bool>>,
}

struct PendingRequest {
    worker_id: u64,
    request_kind: FrameKind,
    expected_kind: FrameKind,
    response: oneshot::Sender<Result<Option<ConnectionResponse>, RequestError>>,
    output: SharedOutput,
    ready: Option<watch::Sender<bool>>,
}

enum RequestError {
    Runtime(String),
    WorkerStopped(String),
}

impl RequestError {
    fn into_pyerr(self) -> PyErr {
        match self {
            Self::Runtime(error) => PyRuntimeError::new_err(error),
            Self::WorkerStopped(reason) => {
                WorkerStoppedError::new_err(format!("worker execution has stopped ({reason})"))
            }
        }
    }
}

struct ConnectionResponse {
    frame: Frame,
}

type SharedOutput = Arc<StdMutex<Vec<OutputPayload>>>;

enum HostDispatchRequest {
    GuestCall(Frame),
    Vfs(Frame),
}

struct AdmittedHostDispatchRequest {
    request: HostDispatchRequest,
    _guest_dispatch_permit: OwnedSemaphorePermit,
}

fn spawn_connection_actor(
    connection: Stream,
    host_dispatch: mpsc::Sender<AdmittedHostDispatchRequest>,
    max_ipc_frame_bytes: usize,
    closed: Arc<AtomicBool>,
) -> (mpsc::Sender<ConnectionRequest>, watch::Sender<bool>) {
    let (requests, mut request_receiver) = mpsc::channel::<ConnectionRequest>(64);
    let (incoming, mut incoming_receiver) = mpsc::channel(64);
    let (shutdown, mut shutdown_receiver) = watch::channel(false);
    let actor_shutdown = shutdown.clone();
    let connection_requests = requests.clone();
    let (mut reader, mut writer) = connection.split();

    tokio::spawn(async move {
        loop {
            let frame = read_frame(&mut reader, max_ipc_frame_bytes)
                .await
                .map_err(|error| error.to_string());
            let failed = frame.is_err();
            if incoming.send(frame).await.is_err() || failed {
                return;
            }
        }
    });

    tokio::spawn(async move {
        let mut pending = HashMap::<u64, PendingRequest>::new();
        let mut guest_dispatchers = HashMap::<u64, mpsc::Sender<HostDispatchRequest>>::new();
        let mut guest_dispatch_limits = HashMap::<u64, (usize, usize)>::new();
        async {
            loop {
                tokio::select! {
                    changed = shutdown_receiver.changed() => {
                        if changed.is_err() || *shutdown_receiver.borrow() {
                            fail_pending_requests(
                                &mut pending,
                                "sandbox process was terminated".into(),
                            );
                            return;
                        }
                    }
                    request = request_receiver.recv() => {
                        let Some(request) = request else {
                            return;
                        };

                        let ConnectionRequest {
                            frame,
                            expected_kind,
                            response,
                            output,
                            ready,
                        } = request;
                        if frame.kind == FrameKind::Execute
                            && let Ok(request) =
                                decode_payload::<ExecuteRequest>(&frame.payload)
                            && let (Ok(concurrency), Ok(queue_capacity)) = (
                                usize::try_from(
                                    request
                                        .limits
                                        .guest_dispatch_request_concurrency,
                                ),
                                usize::try_from(
                                    request
                                        .limits
                                        .guest_dispatch_request_queue_capacity,
                                ),
                            )
                        {
                            guest_dispatch_limits.insert(
                                frame.worker_id,
                                (concurrency, queue_capacity),
                            );
                        }
                        let mut notification_response = None;
                        match expected_kind {
                            Some(expected_kind) => {
                                pending.insert(
                                    frame.request_id,
                                    PendingRequest {
                                        worker_id: frame.worker_id,
                                        request_kind: frame.kind,
                                        expected_kind,
                                        response,
                                        output: output.unwrap_or_default(),
                                        ready,
                                    },
                                );
                            }
                            None => notification_response = Some(response),
                        }

                        if let Err(error) = write_frame(&mut writer, &frame).await {
                            if let Some(pending_request) = pending.remove(&frame.request_id) {
                                let _ = pending_request
                                    .response
                                    .send(Err(RequestError::Runtime(error.to_string())));
                            } else if let Some(response) = notification_response.take() {
                                let _ =
                                    response.send(Err(RequestError::Runtime(error.to_string())));
                            }
                            fail_pending_requests(&mut pending, error.to_string());
                            return;
                        }

                        if let Some(response) = notification_response {
                            let _ = response.send(Ok(None));
                        }
                    }
                    incoming = incoming_receiver.recv() => {
                        let Some(incoming) = incoming else {
                            fail_pending_requests(
                                &mut pending,
                                "sandbox connection reader stopped".into(),
                            );
                            return;
                        };
                        let frame = match incoming {
                            Ok(frame) => frame,
                            Err(error) => {
                                fail_pending_requests(&mut pending, error);
                                return;
                            }
                        };

                        if frame.kind == FrameKind::Output {
                            let Some(request) = pending.get_mut(&frame.request_id) else {
                                continue;
                            };
                            match decode_payload::<OutputPayload>(&frame.payload) {
                                Ok(output) => {
                                    let mut output_events =
                                        request.output.lock().expect("output lock poisoned");
                                    if let Some(previous) = output_events.last_mut()
                                        && previous.source == output.source
                                    {
                                        previous.data.extend(output.data);
                                    } else {
                                        output_events.push(output);
                                    }
                                }
                                Err(error) => {
                                    let request = pending
                                        .remove(&frame.request_id)
                                        .expect("pending request was just found");
                                    let _ = request
                                        .response
                                        .send(Err(RequestError::Runtime(error.to_string())));
                                }
                            }
                            continue;
                        }

                        if frame.kind == FrameKind::ExecuteStarted {
                            if let Some(request) = pending.get_mut(&frame.request_id)
                                && request.request_kind == FrameKind::Execute
                                && let Some(ready) = request.ready.take()
                            {
                                ready.send_replace(true);
                            }
                            continue;
                        }

                        if matches!(frame.kind, FrameKind::GuestCall | FrameKind::VfsRequest) {
                            let worker_id = frame.worker_id;
                            let request = if frame.kind == FrameKind::GuestCall {
                                HostDispatchRequest::GuestCall(frame)
                            } else {
                                HostDispatchRequest::Vfs(frame)
                            };
                            let Some(&(concurrency, queue_capacity)) =
                                guest_dispatch_limits.get(&worker_id)
                            else {
                                let response = host_dispatch_error_response(
                                    request,
                                    "guest dispatch limits are unavailable",
                                );
                                if let Err(error) =
                                    write_frame(&mut writer, &response).await
                                {
                                    fail_pending_requests(
                                        &mut pending,
                                        error.to_string(),
                                    );
                                    return;
                                }
                                continue;
                            };
                            let guest_dispatch = guest_dispatchers
                                .entry(worker_id)
                                .or_insert_with(|| {
                                    spawn_guest_dispatcher(
                                        host_dispatch.clone(),
                                        connection_requests.clone(),
                                        concurrency,
                                        queue_capacity,
                                    )
                                });
                            if let Err(error) = guest_dispatch.try_send(request) {
                                let (request, message) = match error {
                                    mpsc::error::TrySendError::Full(request) => {
                                        (
                                            request,
                                            "guest dispatch request queue is full",
                                        )
                                    }
                                    mpsc::error::TrySendError::Closed(request) => {
                                        (request, "guest dispatcher stopped")
                                    }
                                };
                                let response = host_dispatch_error_response(request, message);
                                if let Err(error) = write_frame(&mut writer, &response).await {
                                    fail_pending_requests(&mut pending, error.to_string());
                                    return;
                                }
                            }
                            continue;
                        }

                        if frame.kind == FrameKind::ExecuteResult {
                            guest_dispatchers.remove(&frame.worker_id);
                            guest_dispatch_limits.remove(&frame.worker_id);
                            let reason = decode_payload::<ExecuteResult>(&frame.payload)
                                .map(|result| termination_reason_name(result.reason))
                                .unwrap_or("unknown");
                            fail_pending_worker_calls(
                                &mut pending,
                                frame.worker_id,
                                reason.into(),
                            );
                        } else if frame.kind == FrameKind::ControlResult
                            && pending.get(&frame.request_id).is_some_and(|request| {
                                request.request_kind == FrameKind::CloseWorker
                            })
                            && decode_payload::<ControlResult>(&frame.payload)
                                .is_ok_and(|result| result.error.is_none())
                        {
                            guest_dispatchers.remove(&frame.worker_id);
                            guest_dispatch_limits.remove(&frame.worker_id);
                            fail_pending_worker_calls(
                                &mut pending,
                                frame.worker_id,
                                "closed".into(),
                            );
                        }

                        let Some(request) = pending.remove(&frame.request_id) else {
                            continue;
                        };
                        if frame.kind == FrameKind::Error {
                            let error = String::from_utf8_lossy(&frame.payload).into_owned();
                            let _ = request.response.send(Err(RequestError::Runtime(error)));
                        } else if frame.kind != request.expected_kind {
                            let _ = request.response.send(Err(RequestError::Runtime(format!(
                                "expected {:?}, received {:?}",
                                request.expected_kind,
                                frame.kind,
                            ))));
                        } else {
                            let _ = request.response.send(Ok(Some(ConnectionResponse {
                                frame,
                            })));
                        }
                    }
                }
            }
        }
        .await;
        closed.store(true, Ordering::Release);
        actor_shutdown.send_replace(true);
    });

    (requests, shutdown)
}

fn spawn_host_dispatcher(
    mut host_dispatch_requests: mpsc::Receiver<AdmittedHostDispatchRequest>,
    requests: mpsc::Sender<ConnectionRequest>,
    handlers: RpcHandlers,
    handler: VfsHandler,
    host_dispatch: Arc<Semaphore>,
) {
    pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
        loop {
            let Ok(permit) = host_dispatch.clone().acquire_owned().await else {
                return;
            };
            let Some(dispatch_request) = host_dispatch_requests.recv().await else {
                return;
            };
            let requests = requests.clone();
            let handlers = handlers.clone();
            let handler = handler.clone();
            pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
                let _permit = permit;
                let _guest_dispatch_permit = dispatch_request._guest_dispatch_permit;
                match dispatch_request.request {
                    HostDispatchRequest::GuestCall(frame) => {
                        dispatch_guest_call(frame, &requests, &handlers).await;
                    }
                    HostDispatchRequest::Vfs(frame) => {
                        dispatch_vfs_request(frame, &requests, &handler).await;
                    }
                }
            });
        }
    });
}

fn spawn_guest_dispatcher(
    host_dispatch: mpsc::Sender<AdmittedHostDispatchRequest>,
    requests: mpsc::Sender<ConnectionRequest>,
    concurrency: usize,
    queue_capacity: usize,
) -> mpsc::Sender<HostDispatchRequest> {
    let (sender, mut receiver) = mpsc::channel(queue_capacity);
    let semaphore = Arc::new(Semaphore::new(concurrency));
    pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
        loop {
            let Ok(permit) = semaphore.clone().acquire_owned().await else {
                return;
            };
            let Some(request_to_dispatch) = receiver.recv().await else {
                return;
            };
            let admitted = AdmittedHostDispatchRequest {
                request: request_to_dispatch,
                _guest_dispatch_permit: permit,
            };
            if let Err(error) = host_dispatch.try_send(admitted) {
                let (admitted, message) = match error {
                    mpsc::error::TrySendError::Full(admitted) => {
                        (admitted, "host dispatch queue is full")
                    }
                    mpsc::error::TrySendError::Closed(admitted) => {
                        (admitted, "host dispatcher stopped")
                    }
                };
                let response = host_dispatch_error_response(admitted.request, message);
                let _ = request(&requests, response, None).await;
            }
        }
    });
    sender
}

async fn dispatch_guest_call(
    frame: Frame,
    requests: &mpsc::Sender<ConnectionRequest>,
    handlers: &RpcHandlers,
) {
    let result = match decode_payload::<RpcCall>(&frame.payload) {
        Ok(call) => invoke_rpc_handler(handlers, frame.worker_id, frame.request_id, call).await,
        Err(error) => Err(error.to_string()),
    };
    let response = match result {
        Ok(value) => RpcResult { value, error: None },
        Err(error) => RpcResult {
            value: Vec::new(),
            error: Some(error),
        },
    };
    let Ok(payload) = encode_payload(&response) else {
        return;
    };
    let _ = request(
        requests,
        Frame::new(
            FrameKind::GuestResponse,
            frame.worker_id,
            frame.request_id,
            payload,
        ),
        None,
    )
    .await;
}

async fn dispatch_vfs_request(
    frame: Frame,
    requests: &mpsc::Sender<ConnectionRequest>,
    handler: &VfsHandler,
) {
    let response = match decode_payload::<VfsRequest>(&frame.payload) {
        Ok(call) => invoke_vfs_handler(handler, call).await,
        Err(error) => VfsResponse {
            value: None,
            error: Some(VfsError {
                code: VfsErrorCode::Invalid,
                message: error.to_string(),
            }),
            stat: None,
            invalidate: false,
        },
    };
    let Ok(payload) = encode_payload(&response) else {
        return;
    };
    let _ = request(
        requests,
        Frame::new(
            FrameKind::VfsResponse,
            frame.worker_id,
            frame.request_id,
            payload,
        ),
        None,
    )
    .await;
}

fn host_dispatch_error_response(request: HostDispatchRequest, message: &str) -> Frame {
    match request {
        HostDispatchRequest::GuestCall(frame) => Frame::new(
            FrameKind::GuestResponse,
            frame.worker_id,
            frame.request_id,
            encode_payload(&RpcResult {
                value: Vec::new(),
                error: Some(message.into()),
            })
            .expect("RPC overload response must encode"),
        ),
        HostDispatchRequest::Vfs(frame) => Frame::new(
            FrameKind::VfsResponse,
            frame.worker_id,
            frame.request_id,
            encode_payload(&VfsResponse {
                value: None,
                error: Some(VfsError {
                    code: VfsErrorCode::Io,
                    message: message.into(),
                }),
                stat: None,
                invalidate: false,
            })
            .expect("VFS overload response must encode"),
        ),
    }
}

async fn invoke_vfs_handler(handler: &VfsHandler, request: VfsRequest) -> VfsResponse {
    match request {
        VfsRequest::Stat { path } => match call_vfs_path_method(handler, "stat", &path).await {
            Ok(value) => match Python::attach(|py| extract_vfs_metadata(value.bind(py))) {
                Ok(value) => vfs_success(VfsValue::Metadata(value), None, false),
                Err(error) => vfs_failure(vfs_python_error(error), None),
            },
            Err(error) => vfs_failure(error, None),
        },
        VfsRequest::Read { path, stat } => {
            let stat = resolve_vfs_stat(handler, &path, stat).await;
            let metadata = match stat_value(&stat) {
                Ok(metadata) => metadata,
                Err(error) => return vfs_failure(error, Some(stat)),
            };
            if !metadata.read {
                return vfs_failure(permission_denied(&path, "read"), Some(stat));
            }
            if metadata.kind == VfsNodeKind::Directory {
                return vfs_failure(is_directory(&path), Some(stat));
            }
            match call_vfs_path_method(handler, "read", &path).await {
                Ok(value) => match Python::attach(|py| value.bind(py).extract::<Vec<u8>>()) {
                    Ok(value) => vfs_success(VfsValue::Bytes(value), Some(stat), false),
                    Err(error) => vfs_failure(vfs_python_error(error), Some(stat)),
                },
                Err(error) => vfs_failure(error, Some(stat)),
            }
        }
        VfsRequest::Write {
            path,
            data,
            offset,
            stat,
        } => {
            let stat = resolve_vfs_stat(handler, &path, stat).await;
            if let Ok(metadata) = stat_value(&stat) {
                if !metadata.write {
                    return vfs_failure(permission_denied(&path, "write"), Some(stat));
                }
                if metadata.kind == VfsNodeKind::Directory {
                    return vfs_failure(is_directory(&path), Some(stat));
                }
            } else if matches!(
                &stat.error,
                Some(VfsError {
                    code: VfsErrorCode::NotFound,
                    ..
                })
            ) {
                let parent = vfs_parent_path(&path);
                let parent_stat = resolve_vfs_stat(handler, &parent, None).await;
                let parent_metadata = match stat_value(&parent_stat) {
                    Ok(metadata) => metadata,
                    Err(error) => return vfs_failure(error, Some(stat)),
                };
                if parent_metadata.kind != VfsNodeKind::Directory {
                    return vfs_failure(not_directory(&parent), Some(stat));
                }
                if !parent_metadata.write {
                    return vfs_failure(permission_denied(&parent, "write"), Some(stat));
                }
            } else {
                let error = stat.error.clone().expect("invalid VFS stat result");
                return vfs_failure(error, Some(stat));
            }
            match call_vfs_write_method(handler, &path, &data, offset).await {
                Ok(()) => vfs_success(VfsValue::Unit, None, true),
                Err(error) => vfs_failure(error, Some(stat)),
            }
        }
        VfsRequest::Append { path, data, stat } => {
            let stat = resolve_vfs_stat(handler, &path, stat).await;
            let metadata = match stat_value(&stat) {
                Ok(metadata) => metadata,
                Err(error) => return vfs_failure(error, Some(stat)),
            };
            if !metadata.write {
                return vfs_failure(permission_denied(&path, "write"), Some(stat));
            }
            if metadata.kind == VfsNodeKind::Directory {
                return vfs_failure(is_directory(&path), Some(stat));
            }
            match call_vfs_append_method(handler, &path, &data, metadata.size).await {
                Ok(()) => vfs_success(VfsValue::Unit, None, true),
                Err(error) => vfs_failure(error, Some(stat)),
            }
        }
        VfsRequest::Truncate { path, size, stat } => {
            let stat = resolve_vfs_stat(handler, &path, stat).await;
            let metadata = match stat_value(&stat) {
                Ok(metadata) => metadata,
                Err(error) => return vfs_failure(error, Some(stat)),
            };
            if metadata.kind == VfsNodeKind::Directory {
                return vfs_failure(is_directory(&path), Some(stat));
            }
            if !metadata.write {
                return vfs_failure(permission_denied(&path, "write"), Some(stat));
            }
            let result = match call_vfs_truncate_method(handler, &path, size).await {
                Ok(true) => Ok(()),
                Ok(false) if !metadata.read => {
                    Err(permission_denied(&path, "read for truncate fallback"))
                }
                Ok(false) => truncate_vfs_file(handler, &path, size).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => vfs_success(VfsValue::Unit, None, true),
                Err(error) => vfs_failure(error, Some(stat)),
            }
        }
        VfsRequest::Delete {
            path,
            directory,
            stat,
        } => {
            let stat = resolve_vfs_stat(handler, &path, stat).await;
            let metadata = match stat_value(&stat) {
                Ok(metadata) => metadata,
                Err(error) => return vfs_failure(error, Some(stat)),
            };
            if directory != (metadata.kind == VfsNodeKind::Directory) {
                let error = if directory {
                    not_directory(&path)
                } else {
                    is_directory(&path)
                };
                return vfs_failure(error, Some(stat));
            }
            if let Err(error) = require_writable_parent(handler, &path).await {
                return vfs_failure(error, Some(stat));
            }
            match call_vfs_path_mutation(handler, "delete", &path).await {
                Ok(()) => vfs_success(VfsValue::Unit, None, true),
                Err(error) => vfs_failure(error, Some(stat)),
            }
        }
        VfsRequest::Mkdir { path, stat } => {
            let stat = resolve_vfs_stat(handler, &path, stat).await;
            if stat_value(&stat).is_ok() {
                return vfs_failure(already_exists(&path), Some(stat));
            }
            if !matches!(stat.error.as_ref(), Some(error) if error.code == VfsErrorCode::NotFound) {
                let error = stat.error.clone().unwrap_or_else(|| malformed_vfs_stat());
                return vfs_failure(error, Some(stat));
            }
            if let Err(error) = require_writable_parent(handler, &path).await {
                return vfs_failure(error, Some(stat));
            }
            match call_vfs_path_mutation(handler, "mkdir", &path).await {
                Ok(()) => vfs_success(VfsValue::Unit, None, true),
                Err(error) => vfs_failure(error, Some(stat)),
            }
        }
        VfsRequest::Rename {
            from,
            to,
            stat,
            to_stat,
        } => invoke_vfs_rename(handler, from, to, stat, to_stat).await,
        VfsRequest::List { path, stat } => {
            let stat = resolve_vfs_stat(handler, &path, stat).await;
            let metadata = match stat_value(&stat) {
                Ok(metadata) => metadata,
                Err(error) => return vfs_failure(error, Some(stat)),
            };
            if !metadata.read {
                return vfs_failure(permission_denied(&path, "read"), Some(stat));
            }
            if metadata.kind != VfsNodeKind::Directory {
                return vfs_failure(not_directory(&path), Some(stat));
            }
            match call_vfs_path_method(handler, "list", &path).await {
                Ok(value) => {
                    let entries = Python::attach(|py| {
                        value
                            .bind(py)
                            .try_iter()?
                            .map(|entry| {
                                let entry = entry?;
                                Ok(VfsDirectoryEntry {
                                    name: entry.getattr("name")?.extract()?,
                                    metadata: extract_vfs_metadata(&entry)?,
                                })
                            })
                            .collect::<PyResult<Vec<_>>>()
                    });
                    match entries {
                        Ok(entries) => vfs_success(VfsValue::Entries(entries), Some(stat), false),
                        Err(error) => vfs_failure(vfs_python_error(error), Some(stat)),
                    }
                }
                Err(error) => vfs_failure(error, Some(stat)),
            }
        }
    }
}

async fn resolve_vfs_stat(
    handler: &VfsHandler,
    path: &str,
    cached: Option<VfsStatResult>,
) -> VfsStatResult {
    if let Some(cached) = cached {
        return cached;
    }
    match call_vfs_path_method(handler, "stat", path).await {
        Ok(value) => match Python::attach(|py| extract_vfs_metadata(value.bind(py))) {
            Ok(value) => VfsStatResult {
                value: Some(value),
                error: None,
            },
            Err(error) => VfsStatResult {
                value: None,
                error: Some(vfs_python_error(error)),
            },
        },
        Err(error) => VfsStatResult {
            value: None,
            error: Some(error),
        },
    }
}

fn stat_value(stat: &VfsStatResult) -> Result<&VfsMetadata, VfsError> {
    match (&stat.value, &stat.error) {
        (Some(value), None) => Ok(value),
        (None, Some(error)) => Err(error.clone()),
        _ => Err(malformed_vfs_stat()),
    }
}

fn malformed_vfs_stat() -> VfsError {
    VfsError {
        code: VfsErrorCode::Io,
        message: "malformed VFS stat result".into(),
    }
}

async fn require_writable_parent(handler: &VfsHandler, path: &str) -> Result<(), VfsError> {
    let parent = vfs_parent_path(path);
    let stat = resolve_vfs_stat(handler, &parent, None).await;
    let metadata = stat_value(&stat)?;
    if metadata.kind != VfsNodeKind::Directory {
        return Err(not_directory(&parent));
    }
    if !metadata.write {
        return Err(permission_denied(&parent, "write"));
    }
    Ok(())
}

async fn call_vfs_path_method(
    handler: &VfsHandler,
    method: &str,
    path: &str,
) -> Result<Py<PyAny>, VfsError> {
    let (handler, locals) = Python::attach(|py| {
        let handler = handler.lock().expect("VFS handler lock poisoned");
        let handler = handler.as_ref().ok_or_else(|| VfsError {
            code: VfsErrorCode::NotFound,
            message: format!("virtual path not found: {path}"),
        })?;
        Ok::<_, VfsError>((handler.callable.clone_ref(py), handler.locals.clone()))
    })?;

    let invocation = Python::attach(|py| -> PyResult<(Py<PyAny>, bool)> {
        let result = handler.bind(py).call_method1(method, (path,))?;
        let is_awaitable = py
            .import("inspect")?
            .call_method1("isawaitable", (&result,))?
            .is_truthy()?;
        Ok((result.unbind(), is_awaitable))
    })
    .map_err(vfs_python_error)?;

    let value = if invocation.1 {
        let future = Python::attach(|py| {
            pyo3_async_runtimes::into_future_with_locals(&locals, invocation.0.bind(py).clone())
        })
        .map_err(vfs_python_error)?;
        future.await.map_err(vfs_python_error)?
    } else {
        invocation.0
    };

    Ok(value)
}

async fn call_vfs_write_method(
    handler: &VfsHandler,
    path: &str,
    data: &[u8],
    offset: Option<u64>,
) -> Result<(), VfsError> {
    let (handler, locals) = Python::attach(|py| {
        let handler = handler.lock().expect("VFS handler lock poisoned");
        let handler = handler.as_ref().ok_or_else(|| VfsError {
            code: VfsErrorCode::NotFound,
            message: format!("virtual path not found: {path}"),
        })?;
        Ok::<_, VfsError>((handler.callable.clone_ref(py), handler.locals.clone()))
    })?;
    let invocation = Python::attach(|py| -> PyResult<(Py<PyAny>, bool)> {
        let result = handler
            .bind(py)
            .call_method1("write", (path, data, offset))?;
        let is_awaitable = py
            .import("inspect")?
            .call_method1("isawaitable", (&result,))?
            .is_truthy()?;
        Ok((result.unbind(), is_awaitable))
    })
    .map_err(vfs_python_error)?;
    if invocation.1 {
        let future = Python::attach(|py| {
            pyo3_async_runtimes::into_future_with_locals(&locals, invocation.0.bind(py).clone())
        })
        .map_err(vfs_python_error)?;
        future.await.map_err(vfs_python_error)?;
    }
    Ok(())
}

async fn call_vfs_append_method(
    handler: &VfsHandler,
    path: &str,
    data: &[u8],
    offset: u64,
) -> Result<(), VfsError> {
    let (callable, locals) = vfs_callable(handler, path)?;
    let invocation = Python::attach(|py| {
        optional_invocation(py, callable.bind(py).call_method1("append", (path, data)))
    })?;
    if let Some(invocation) = invocation
        && await_optional_vfs_invocation(locals, invocation)
            .await?
            .is_some()
    {
        return Ok(());
    }
    call_vfs_write_method(handler, path, data, Some(offset)).await
}

async fn call_vfs_path_mutation(
    handler: &VfsHandler,
    method: &str,
    path: &str,
) -> Result<(), VfsError> {
    call_vfs_path_method(handler, method, path)
        .await
        .map(|_| ())
}

async fn call_vfs_truncate_method(
    handler: &VfsHandler,
    path: &str,
    size: u64,
) -> Result<bool, VfsError> {
    let (callable, locals) = vfs_callable(handler, path)?;
    let invocation = Python::attach(|py| {
        optional_invocation(py, callable.bind(py).call_method1("truncate", (path, size)))
    })?;
    match invocation {
        Some(invocation) => await_optional_vfs_invocation(locals, invocation)
            .await
            .map(|value| value.is_some()),
        None => Ok(false),
    }
}

async fn call_vfs_rename_method(
    handler: &VfsHandler,
    from: &str,
    to: &str,
) -> Result<bool, VfsError> {
    let (callable, locals) = vfs_callable(handler, from)?;
    let invocation = Python::attach(|py| {
        optional_invocation(py, callable.bind(py).call_method1("rename", (from, to)))
    })?;
    match invocation {
        Some(invocation) => await_optional_vfs_invocation(locals, invocation)
            .await
            .map(|value| value.is_some()),
        None => Ok(false),
    }
}

fn vfs_callable(
    handler: &VfsHandler,
    path: &str,
) -> Result<(Py<PyAny>, pyo3_async_runtimes::TaskLocals), VfsError> {
    Python::attach(|py| {
        let handler = handler.lock().expect("VFS handler lock poisoned");
        let handler = handler.as_ref().ok_or_else(|| VfsError {
            code: VfsErrorCode::NotFound,
            message: format!("virtual path not found: {path}"),
        })?;
        Ok((handler.callable.clone_ref(py), handler.locals.clone()))
    })
}

fn invocation_result(py: Python<'_>, result: Bound<'_, PyAny>) -> PyResult<(Py<PyAny>, bool)> {
    let is_awaitable = py
        .import("inspect")?
        .call_method1("isawaitable", (&result,))?
        .is_truthy()?;
    Ok((result.unbind(), is_awaitable))
}

fn optional_invocation(
    py: Python<'_>,
    result: PyResult<Bound<'_, PyAny>>,
) -> Result<Option<(Py<PyAny>, bool)>, VfsError> {
    match result {
        Ok(result) => invocation_result(py, result)
            .map(Some)
            .map_err(vfs_python_error),
        Err(error) if vfs_method_is_unimplemented(py, &error) => Ok(None),
        Err(error) => Err(vfs_python_error(error)),
    }
}

async fn await_optional_vfs_invocation(
    locals: pyo3_async_runtimes::TaskLocals,
    invocation: (Py<PyAny>, bool),
) -> Result<Option<Py<PyAny>>, VfsError> {
    if !invocation.1 {
        return Ok(Some(invocation.0));
    }
    let future = Python::attach(|py| {
        pyo3_async_runtimes::into_future_with_locals(&locals, invocation.0.bind(py).clone())
    })
    .map_err(vfs_python_error)?;
    match future.await {
        Ok(value) => Ok(Some(value)),
        Err(error) if Python::attach(|py| vfs_method_is_unimplemented(py, &error)) => Ok(None),
        Err(error) => Err(vfs_python_error(error)),
    }
}

async fn read_vfs_bytes(handler: &VfsHandler, path: &str) -> Result<Vec<u8>, VfsError> {
    let value = call_vfs_path_method(handler, "read", path).await?;
    Python::attach(|py| value.bind(py).extract::<Vec<u8>>()).map_err(vfs_python_error)
}

async fn truncate_vfs_file(handler: &VfsHandler, path: &str, size: u64) -> Result<(), VfsError> {
    let size = usize::try_from(size).map_err(|_| VfsError {
        code: VfsErrorCode::Invalid,
        message: format!("truncate size is too large: {size}"),
    })?;
    let mut data = read_vfs_bytes(handler, path).await?;
    data.resize(size, 0);
    call_vfs_write_method(handler, path, &data, None).await
}

async fn invoke_vfs_rename(
    handler: &VfsHandler,
    from: String,
    to: String,
    stat: Option<VfsStatResult>,
    to_stat: Option<VfsStatResult>,
) -> VfsResponse {
    let stat = resolve_vfs_stat(handler, &from, stat).await;
    let metadata = match stat_value(&stat) {
        Ok(metadata) => metadata.clone(),
        Err(error) => return vfs_failure(error, Some(stat)),
    };
    if from == to {
        return vfs_success(VfsValue::Unit, None, false);
    }
    if let Err(error) = require_writable_parent(handler, &from).await {
        return vfs_failure(error, Some(stat));
    }
    if let Err(error) = require_writable_parent(handler, &to).await {
        return vfs_failure(error, Some(stat));
    }

    let native_rename = call_vfs_rename_method(handler, &from, &to).await;
    let result = if matches!(native_rename, Ok(true)) {
        Ok(())
    } else if let Err(error) = native_rename {
        Err(error)
    } else if metadata.kind == VfsNodeKind::Directory {
        Err(VfsError {
            code: VfsErrorCode::PermissionDenied,
            message: format!("directory rename requires a VFS rename method: {from}"),
        })
    } else if !metadata.read {
        Err(permission_denied(&from, "read for rename fallback"))
    } else {
        let to_stat = resolve_vfs_stat(handler, &to, to_stat).await;
        let destination_allowed = match stat_value(&to_stat) {
            Ok(destination) if destination.kind == VfsNodeKind::Directory => Err(is_directory(&to)),
            Ok(destination) if !destination.write => Err(permission_denied(&to, "write")),
            Ok(_) => Ok(()),
            Err(error) if error.code == VfsErrorCode::NotFound => Ok(()),
            Err(error) => Err(error),
        };
        match destination_allowed {
            Err(error) => Err(error),
            Ok(()) => match read_vfs_bytes(handler, &from).await {
                Ok(data) => match call_vfs_write_method(handler, &to, &data, None).await {
                    Ok(()) => call_vfs_path_mutation(handler, "delete", &from).await,
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            },
        }
    };

    match result {
        Ok(()) => vfs_success(VfsValue::Unit, None, true),
        Err(error) => vfs_failure(error, Some(stat)),
    }
}

fn vfs_success(value: VfsValue, stat: Option<VfsStatResult>, invalidate: bool) -> VfsResponse {
    VfsResponse {
        value: Some(value),
        error: None,
        stat,
        invalidate,
    }
}

fn vfs_failure(error: VfsError, stat: Option<VfsStatResult>) -> VfsResponse {
    VfsResponse {
        value: None,
        error: Some(error),
        stat,
        invalidate: false,
    }
}

fn permission_denied(path: &str, operation: &str) -> VfsError {
    VfsError {
        code: VfsErrorCode::PermissionDenied,
        message: format!("{operation} permission denied: {path}"),
    }
}

fn is_directory(path: &str) -> VfsError {
    VfsError {
        code: VfsErrorCode::IsDirectory,
        message: format!("path is a directory: {path}"),
    }
}

fn already_exists(path: &str) -> VfsError {
    VfsError {
        code: VfsErrorCode::AlreadyExists,
        message: format!("path already exists: {path}"),
    }
}

fn not_directory(path: &str) -> VfsError {
    VfsError {
        code: VfsErrorCode::NotDirectory,
        message: format!("path is not a directory: {path}"),
    }
}

fn vfs_parent_path(path: &str) -> String {
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

fn extract_vfs_metadata(value: &Bound<'_, PyAny>) -> PyResult<VfsMetadata> {
    let kind = value.getattr("kind")?.extract::<String>()?;
    let kind = match kind.as_str() {
        "file" => VfsNodeKind::File,
        "directory" => VfsNodeKind::Directory,
        _ => {
            return Err(PyValueError::new_err(
                "VFS node kind must be 'file' or 'directory'",
            ));
        }
    };
    Ok(VfsMetadata {
        kind,
        size: value.getattr("size")?.extract()?,
        read: value.getattr("read")?.extract()?,
        write: value.getattr("write")?.extract()?,
    })
}

fn vfs_python_error(error: PyErr) -> VfsError {
    Python::attach(|py| {
        let code = if error.is_instance_of::<pyo3::exceptions::PyFileNotFoundError>(py) {
            VfsErrorCode::NotFound
        } else if error.is_instance_of::<PyFileExistsError>(py) {
            VfsErrorCode::AlreadyExists
        } else if error.is_instance_of::<PyNotADirectoryError>(py) {
            VfsErrorCode::NotDirectory
        } else if error.is_instance_of::<PyIsADirectoryError>(py) {
            VfsErrorCode::IsDirectory
        } else if error.is_instance_of::<PyPermissionError>(py) {
            VfsErrorCode::PermissionDenied
        } else if error.is_instance_of::<PyNotImplementedError>(py) {
            VfsErrorCode::PermissionDenied
        } else if python_error_has_errno(py, &error, "ENOTEMPTY") {
            VfsErrorCode::DirectoryNotEmpty
        } else if error.is_instance_of::<pyo3::exceptions::PyValueError>(py) {
            VfsErrorCode::Invalid
        } else {
            VfsErrorCode::Io
        };
        VfsError {
            code,
            message: error.to_string(),
        }
    })
}

fn vfs_method_is_unimplemented(py: Python<'_>, error: &PyErr) -> bool {
    error.is_instance_of::<PyNotImplementedError>(py)
        || error.is_instance_of::<PyAttributeError>(py)
}

fn python_error_has_errno(py: Python<'_>, error: &PyErr, name: &str) -> bool {
    let actual = error
        .value(py)
        .getattr("errno")
        .and_then(|value| value.extract::<i32>());
    let expected = py
        .import("errno")
        .and_then(|module| module.getattr(name))
        .and_then(|value| value.extract::<i32>());
    matches!((actual, expected), (Ok(actual), Ok(expected)) if actual == expected)
}

async fn invoke_rpc_handler(
    handlers: &RpcHandlers,
    worker_id: u64,
    request_id: u64,
    call: RpcCall,
) -> Result<Vec<u8>, String> {
    let (handler, locals) = Python::attach(|py| {
        let handlers = handlers.lock().expect("RPC handler lock poisoned");
        let handler = handlers
            .get(&call.method)
            .ok_or_else(|| format!("unknown RPC method: {}", call.method))?;
        Ok::<_, String>((handler.callable.clone_ref(py), handler.locals.clone()))
    })?;

    let invocation = Python::attach(|py| -> PyResult<(Py<PyAny>, bool)> {
        let cbor2 = py.import("cbor2")?;
        let decoded = cbor2.call_method1("loads", (PyBytes::new(py, &call.arguments),))?;
        let args = decoded.get_item(0)?;
        let kwargs = decoded.get_item(1)?;
        let args = py.get_type::<PyTuple>().call1((args,))?;
        let context = Py::new(
            py,
            RpcContext {
                worker_id,
                request_id,
            },
        )?;
        let guest_args = args.cast::<PyTuple>()?;
        let mut positional = Vec::with_capacity(guest_args.len() + 1);
        positional.push(context.into_bound(py).into_any());
        positional.extend(guest_args.iter());
        let args = PyTuple::new(py, positional)?;
        let result = handler
            .bind(py)
            .call(&args, Some(kwargs.cast::<PyDict>()?))?;
        let is_awaitable = py
            .import("inspect")?
            .call_method1("isawaitable", (&result,))?
            .is_truthy()?;
        Ok((result.unbind(), is_awaitable))
    })
    .map_err(|error| error.to_string())?;

    let value = if invocation.1 {
        let future = Python::attach(|py| {
            pyo3_async_runtimes::into_future_with_locals(&locals, invocation.0.bind(py).clone())
        })
        .map_err(|error| error.to_string())?;
        future.await.map_err(|error| error.to_string())?
    } else {
        invocation.0
    };

    Python::attach(|py| {
        let options = PyDict::new(py);
        options
            .set_item("value_sharing", true)
            .map_err(|error| error.to_string())?;
        py.import("cbor2")
            .and_then(|cbor2| cbor2.call_method("dumps", (value.bind(py),), Some(&options)))
            .and_then(|encoded| encoded.extract::<Vec<u8>>())
            .map_err(|error| error.to_string())
    })
}

async fn request(
    requests: &mpsc::Sender<ConnectionRequest>,
    frame: Frame,
    expected_kind: Option<FrameKind>,
) -> PyResult<Option<ConnectionResponse>> {
    request_with_output(requests, frame, expected_kind, None).await
}

async fn request_with_output(
    requests: &mpsc::Sender<ConnectionRequest>,
    frame: Frame,
    expected_kind: Option<FrameKind>,
    output: Option<SharedOutput>,
) -> PyResult<Option<ConnectionResponse>> {
    request_with_output_and_ready(requests, frame, expected_kind, output, None).await
}

async fn request_with_output_and_ready(
    requests: &mpsc::Sender<ConnectionRequest>,
    frame: Frame,
    expected_kind: Option<FrameKind>,
    output: Option<SharedOutput>,
    ready: Option<watch::Sender<bool>>,
) -> PyResult<Option<ConnectionResponse>> {
    let (response, receiver) = oneshot::channel();
    requests
        .send(ConnectionRequest {
            frame,
            expected_kind,
            response,
            output,
            ready,
        })
        .await
        .map_err(|_| PyRuntimeError::new_err("sandbox connection is closed"))?;
    let response = receiver
        .await
        .map_err(|_| PyRuntimeError::new_err("sandbox connection stopped"))?
        .map_err(RequestError::into_pyerr)?;
    Ok(response)
}

fn fail_pending_requests(pending: &mut HashMap<u64, PendingRequest>, error: String) {
    for (_, request) in pending.drain() {
        let _ = request
            .response
            .send(Err(RequestError::Runtime(error.clone())));
    }
}

fn fail_pending_worker_calls(
    pending: &mut HashMap<u64, PendingRequest>,
    worker_id: u64,
    reason: String,
) {
    let request_ids = pending
        .iter()
        .filter_map(|(request_id, request)| {
            (request.worker_id == worker_id && request.expected_kind == FrameKind::WorkerResponse)
                .then_some(*request_id)
        })
        .collect::<Vec<_>>();
    for request_id in request_ids {
        let request = pending
            .remove(&request_id)
            .expect("pending worker request was just found");
        let _ = request
            .response
            .send(Err(RequestError::WorkerStopped(reason.clone())));
    }
}

async fn execute_request(
    requests: &mpsc::Sender<ConnectionRequest>,
    worker_id: u64,
    request_id: u64,
    payload: Vec<u8>,
    output: SharedOutput,
    ready: watch::Sender<bool>,
) -> PyResult<pysandbox_protocol::ExecuteResult> {
    let response = request_with_output_and_ready(
        requests,
        Frame::new(FrameKind::Execute, worker_id, request_id, payload),
        Some(FrameKind::ExecuteResult),
        Some(output),
        Some(ready),
    )
    .await?
    .ok_or_else(|| PyRuntimeError::new_err("execution returned no response"))?;
    decode_payload(&response.frame.payload).map_err(runtime_error)
}

fn termination_reason_name(reason: TerminationReason) -> &'static str {
    match reason {
        TerminationReason::Completed => "completed",
        TerminationReason::Exited => "exited",
        TerminationReason::GuestError => "guest_error",
        TerminationReason::Timeout => "timeout",
        TerminationReason::Cancelled => "cancelled",
        TerminationReason::FuelExhausted => "fuel_exhausted",
        TerminationReason::OutputLimit => "output_limit",
        TerminationReason::MemoryLimit => "memory_limit",
        TerminationReason::RuntimeError => "runtime_error",
        TerminationReason::InfrastructureError => "infrastructure_error",
    }
}

#[pyclass(name = "Execution")]
struct NativeExecution {
    worker_id: u64,
    execution_id: u64,
    requests: mpsc::Sender<ConnectionRequest>,
    next_request_id: Arc<AtomicU64>,
    native_output: Py<NativeOutput>,
    ready: watch::Receiver<bool>,
    result: watch::Receiver<Option<Result<pysandbox_protocol::ExecuteResult, String>>>,
    shutdown: watch::Receiver<bool>,
}

#[pymethods]
impl NativeExecution {
    #[getter]
    fn execution_id(&self) -> u64 {
        self.execution_id
    }

    #[getter]
    fn output(&self, py: Python<'_>) -> Py<NativeOutput> {
        self.native_output.clone_ref(py)
    }

    fn result<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut result = self.result.clone();
        let mut shutdown = self.shutdown.clone();
        let output = self.native_output.clone_ref(py);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            loop {
                if let Some(outcome) = result.borrow().clone() {
                    let result = outcome.map_err(PyRuntimeError::new_err)?;
                    return Python::attach(|py| {
                        Py::new(
                            py,
                            NativeExecutionResult {
                                error: result.error,
                                reason: termination_reason_name(result.reason).into(),
                                exit_code: result.exit_code,
                                output: output.clone_ref(py),
                            },
                        )
                    });
                }
                if *shutdown.borrow() {
                    return Err(PyRuntimeError::new_err(
                        "sandbox process stopped during execution",
                    ));
                }
                tokio::select! {
                    changed = result.changed() => {
                        changed.map_err(|_| {
                            PyRuntimeError::new_err("execution task stopped")
                        })?;
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Err(PyRuntimeError::new_err(
                                "sandbox process stopped during execution",
                            ));
                        }
                    }
                }
            }
        })
    }

    fn cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.control(
            py,
            FrameKind::Cancel,
            encode_payload(&CancelRequest {
                execution_id: self.execution_id,
            })
            .map_err(runtime_error)?,
        )
    }

    #[pyo3(signature = (path, fuel, /, *args, **kwargs))]
    fn call<'py>(
        &self,
        py: Python<'py>,
        path: Vec<String>,
        fuel: Option<(String, u64, Option<u64>)>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let empty_kwargs = PyDict::new(py);
        let kwargs = kwargs.unwrap_or(&empty_kwargs);
        let cbor_options = PyDict::new(py);
        cbor_options.set_item("value_sharing", true)?;
        let arguments = py
            .import("cbor2")?
            .call_method("dumps", ((args, kwargs),), Some(&cbor_options))?
            .extract::<Vec<u8>>()?;
        let fuel = fuel
            .map(|(operation, value, cap)| match operation.as_str() {
                "set" => Ok(FuelOperation::Set { fuel: value }),
                "add" => Ok(FuelOperation::Add { amount: value, cap }),
                _ => Err(PyValueError::new_err(format!(
                    "unknown fuel operation: {operation}"
                ))),
            })
            .transpose()?;
        let payload = encode_payload(&WorkerRpcCall {
            path,
            fuel,
            arguments,
        })
        .map_err(runtime_error)?;
        let requests = self.requests.clone();
        let worker_id = self.worker_id;
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let ready = self.ready.clone();
        let result = self.result.clone();
        let shutdown = self.shutdown.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            wait_until_execution_ready(ready, result, shutdown).await?;
            let response = request(
                &requests,
                Frame::new(FrameKind::WorkerCall, worker_id, request_id, payload),
                Some(FrameKind::WorkerResponse),
            )
            .await?
            .ok_or_else(|| PyRuntimeError::new_err("worker RPC returned no response"))?;
            let result: RpcResult =
                decode_payload(&response.frame.payload).map_err(runtime_error)?;
            if let Some(error) = result.error {
                return Err(PyRuntimeError::new_err(error));
            }
            Python::attach(|py| {
                py.import("cbor2")?
                    .call_method1("loads", (PyBytes::new(py, &result.value),))
                    .map(Bound::unbind)
            })
        })
    }

    fn set_fuel<'py>(&self, py: Python<'py>, fuel: u64) -> PyResult<Bound<'py, PyAny>> {
        self.update(
            py,
            ExecutionControl::SetFuel {
                execution_id: self.execution_id,
                fuel,
            },
        )
    }

    #[pyo3(signature = (amount, *, cap = None))]
    fn add_fuel<'py>(
        &self,
        py: Python<'py>,
        amount: u64,
        cap: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.update(
            py,
            ExecutionControl::AddFuel {
                execution_id: self.execution_id,
                amount,
                cap,
            },
        )
    }

    #[pyo3(signature = (
        *,
        max_memory_bytes = None,
        max_output_bytes = None,
        max_guest_rpc_bytes = None,
        cpu_share_weight = None,
        timeout = None,
    ))]
    fn set_limits<'py>(
        &self,
        py: Python<'py>,
        max_memory_bytes: Option<u64>,
        max_output_bytes: Option<u64>,
        max_guest_rpc_bytes: Option<u64>,
        cpu_share_weight: Option<u64>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if let Some(weight) = cpu_share_weight {
            validate_cpu_share_weight(weight)?;
        }
        self.update(
            py,
            ExecutionControl::SetLimits {
                execution_id: self.execution_id,
                max_memory_bytes,
                max_output_bytes,
                max_guest_rpc_bytes,
                cpu_share_weight,
                timeout_ms: timeout_milliseconds(timeout)?,
            },
        )
    }
}

impl NativeExecution {
    fn update<'py>(
        &self,
        py: Python<'py>,
        update: ExecutionControl,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.control(
            py,
            FrameKind::UpdateLimits,
            encode_payload(&update).map_err(runtime_error)?,
        )
    }

    fn control<'py>(
        &self,
        py: Python<'py>,
        kind: FrameKind,
        payload: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let requests = self.requests.clone();
        let worker_id = self.worker_id;
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let ready = self.ready.clone();
        let result = self.result.clone();
        let shutdown = self.shutdown.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            wait_until_execution_ready(ready, result, shutdown).await?;
            let response = request(
                &requests,
                Frame::new(kind, worker_id, request_id, payload),
                Some(FrameKind::ControlResult),
            )
            .await?
            .ok_or_else(|| PyRuntimeError::new_err("control request returned no response"))?;
            let result: ControlResult =
                decode_payload(&response.frame.payload).map_err(runtime_error)?;
            if let Some(error) = result.error {
                return Err(PyRuntimeError::new_err(error));
            }
            Python::attach(|py| Ok(py.None()))
        })
    }
}

async fn wait_until_execution_ready(
    mut ready: watch::Receiver<bool>,
    mut result: watch::Receiver<Option<Result<pysandbox_protocol::ExecuteResult, String>>>,
    mut shutdown: watch::Receiver<bool>,
) -> PyResult<()> {
    loop {
        if *ready.borrow() {
            return Ok(());
        }
        if result.borrow().is_some() {
            return Err(PyRuntimeError::new_err(
                "execution stopped before becoming ready",
            ));
        }
        if *shutdown.borrow() {
            return Err(PyRuntimeError::new_err(
                "sandbox process stopped before execution became ready",
            ));
        }
        tokio::select! {
            changed = ready.changed() => {
                if changed.is_err() {
                    return Err(PyRuntimeError::new_err(
                        "execution stopped before becoming ready",
                    ));
                }
            }
            changed = result.changed() => {
                if changed.is_err() || result.borrow().is_some() {
                    return Err(PyRuntimeError::new_err(
                        "execution stopped before becoming ready",
                    ));
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Err(PyRuntimeError::new_err(
                        "sandbox process stopped before execution became ready",
                    ));
                }
            }
        }
    }
}

#[derive(Eq, Hash, PartialEq)]
#[pyclass(name = "OutputEvent", frozen, module = "pysandbox._core")]
struct NativeOutputEvent {
    source: &'static str,
    data: Vec<u8>,
}

#[pyclass(name = "Output", frozen)]
struct NativeOutput {
    output: SharedOutput,
}

#[pymethods]
impl NativeOutput {
    #[new]
    fn new() -> Self {
        Self {
            output: SharedOutput::default(),
        }
    }

    fn len(&self) -> usize {
        self.output.lock().expect("output lock poisoned").len()
    }

    fn get_item(&self, py: Python<'_>, index: usize) -> PyResult<Py<NativeOutputEvent>> {
        let event = self
            .output
            .lock()
            .expect("output lock poisoned")
            .get(index)
            .cloned()
            .ok_or_else(|| PyIndexError::new_err("output index out of range"))?;
        output_event(py, event)
    }

    fn get_slice(
        &self,
        py: Python<'_>,
        start: isize,
        stop: isize,
        step: isize,
    ) -> PyResult<Vec<Py<NativeOutputEvent>>> {
        if step == 0 {
            return Err(PyValueError::new_err("slice step cannot be zero"));
        }
        let mut events = Vec::new();
        {
            let output = self.output.lock().expect("output lock poisoned");
            let mut index = start;
            while if step > 0 { index < stop } else { index > stop } {
                let event = usize::try_from(index)
                    .ok()
                    .and_then(|index| output.get(index))
                    .cloned()
                    .ok_or_else(|| PyIndexError::new_err("output index out of range"))?;
                events.push(event);
                index = index
                    .checked_add(step)
                    .ok_or_else(|| PyIndexError::new_err("output slice index overflow"))?;
            }
        }
        events
            .into_iter()
            .map(|event| output_event(py, event))
            .collect()
    }
}

#[pymethods]
impl NativeOutputEvent {
    #[new]
    fn new(source: &str, data: Vec<u8>) -> PyResult<Self> {
        let source = match source {
            "stdout" => "stdout",
            "stderr" => "stderr",
            _ => {
                return Err(PyValueError::new_err(
                    "output source must be 'stdout' or 'stderr'",
                ));
            }
        };
        Ok(Self { source, data })
    }

    #[getter]
    fn source(&self) -> &'static str {
        self.source
    }

    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.data)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let data = PyBytes::new(py, &self.data).repr()?.extract::<String>()?;
        Ok(format!(
            "OutputEvent(source='{}', data={data})",
            self.source
        ))
    }

    fn __eq__(&self, other: &Self) -> bool {
        self == other
    }

    fn __hash__(&self) -> isize {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        let hash = hasher.finish() as isize;
        if hash == -1 { -2 } else { hash }
    }
}

#[pyclass(name = "ExecutionResult", frozen)]
struct NativeExecutionResult {
    error: Option<String>,
    reason: String,
    exit_code: Option<i32>,
    output: Py<NativeOutput>,
}

#[pymethods]
impl NativeExecutionResult {
    #[getter]
    fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    #[getter]
    fn reason(&self) -> &str {
        &self.reason
    }

    #[getter]
    fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }

    #[getter]
    fn output(&self, py: Python<'_>) -> Py<NativeOutput> {
        self.output.clone_ref(py)
    }
}

fn output_event(py: Python<'_>, event: OutputPayload) -> PyResult<Py<NativeOutputEvent>> {
    Py::new(
        py,
        NativeOutputEvent {
            source: match event.source {
                OutputSource::Stdout => "stdout",
                OutputSource::Stderr => "stderr",
            },
            data: event.data,
        },
    )
}

async fn connect_to_sandbox(child: &mut Child, socket_name: &str) -> PyResult<Stream> {
    let deadline = tokio::time::Instant::now() + START_TIMEOUT;
    loop {
        let name = if GenericNamespaced::is_supported() {
            socket_name
                .to_ns_name::<GenericNamespaced>()
                .map_err(runtime_error)?
        } else {
            socket_name
                .to_fs_name::<GenericFilePath>()
                .map_err(runtime_error)?
        };

        match Stream::connect(name).await {
            Ok(connection) => return Ok(connection),
            Err(error) => {
                if let Some(status) = child.try_wait().map_err(runtime_error)? {
                    return Err(PyRuntimeError::new_err(format!(
                        "sandbox process exited before accepting connections: {status}"
                    )));
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(PyRuntimeError::new_err(format!(
                        "timed out connecting to sandbox process: {error}"
                    )));
                }
                tokio::time::sleep(CONNECT_RETRY_INTERVAL).await;
            }
        }
    }
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn timeout_milliseconds(timeout: Option<f64>) -> PyResult<Option<u64>> {
    let Some(timeout) = timeout else {
        return Ok(None);
    };
    if !timeout.is_finite() || timeout <= 0.0 {
        return Err(PyValueError::new_err(
            "timeout must be a positive finite number of seconds",
        ));
    }
    let milliseconds = (timeout * 1_000.0).ceil();
    if milliseconds > u64::MAX as f64 {
        return Err(PyValueError::new_err("timeout is too large"));
    }
    Ok(Some(milliseconds.max(1.0) as u64))
}

fn validate_guest_dispatch_limits(concurrency: u64, queue_capacity: u64) -> PyResult<()> {
    if concurrency == 0 {
        return Err(PyValueError::new_err(
            "guest_dispatch_request_concurrency must be positive",
        ));
    }
    if queue_capacity == 0 {
        return Err(PyValueError::new_err(
            "guest_dispatch_request_queue_capacity must be positive",
        ));
    }
    Ok(())
}

fn validate_cpu_share_weight(weight: u64) -> PyResult<()> {
    if weight == 0 {
        return Err(PyValueError::new_err("cpu_share_weight must be positive"));
    }
    Ok(())
}

#[pymodule]
fn _core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add(
        "WorkerStoppedError",
        module.py().get_type::<WorkerStoppedError>(),
    )?;
    module.add_class::<NativeExecution>()?;
    module.add_class::<NativeExecutionResult>()?;
    module.add_class::<NativeOutput>()?;
    module.add_class::<NativeOutputEvent>()?;
    module.add_class::<RpcContext>()?;
    module.add_class::<SandboxProcess>()?;
    module.add_function(wrap_pyfunction!(protocol_version, module)?)?;
    module.add_function(wrap_pyfunction!(run_sandboxd, module)?)?;
    module.add_function(wrap_pyfunction!(sleep, module)?)?;
    module.add_function(wrap_pyfunction!(start_sandbox, module)?)?;
    Ok(())
}
