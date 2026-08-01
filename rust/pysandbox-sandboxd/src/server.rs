use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::AtomicU64};
use std::time::Duration;

use interprocess::local_socket::{
    GenericFilePath, GenericNamespaced, ListenerOptions, ToFsName, ToNsName,
    tokio::{Stream, prelude::*},
};
use pysandbox_protocol::{
    CancelRequest, ControlResult, ExecuteRequest, ExecuteResult, ExecutionControl, Frame,
    FrameKind, InvalidateVfs, OutputPayload, RpcResult, TerminationReason, VfsResponse,
    WorkerRpcCall, decode_payload, encode_payload, read_frame, write_frame,
};
use tokio::sync::{Mutex, mpsc};

use crate::component_worker::{
    ComponentRuntime, ComponentWorker, ExecutionLimits, OutputEvent, OutputSource,
    PendingGuestCalls, RpcBridge, WorkerCallEnqueueError,
};
use crate::remote_vfs::{CachePolicy, PendingVfsRequests, RemoteVfs};

pub async fn serve(
    socket_name: &str,
    component_path: &Path,
    python_root: &Path,
    max_ipc_frame_bytes: usize,
    worker_queue_capacity: usize,
    cache_vfs: bool,
    cache_vfs_negative: bool,
    cpu_share_enabled: bool,
    cpu_share_limit_percent: Option<f64>,
    cpu_share_sample_interval: Duration,
    cpu_share_activity_timeout: Duration,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        worker_queue_capacity > 0,
        "worker queue capacity must be positive"
    );
    anyhow::ensure!(
        cpu_share_sample_interval > Duration::ZERO,
        "CPU share sample interval must be positive"
    );
    anyhow::ensure!(
        cpu_share_activity_timeout > Duration::ZERO,
        "CPU share activity timeout must be positive"
    );
    anyhow::ensure!(
        cpu_share_limit_percent.is_none_or(|percent| percent.is_finite() && percent > 0.0),
        "CPU share limit percent must be positive and finite"
    );
    let name = if GenericNamespaced::is_supported() {
        socket_name.to_ns_name::<GenericNamespaced>()?
    } else {
        socket_name.to_fs_name::<GenericFilePath>()?
    };
    let listener = ListenerOptions::new()
        .name(name)
        .try_overwrite(true)
        .create_tokio()?;
    let connection = listener.accept().await?;
    serve_connection(
        connection,
        component_path.to_owned(),
        python_root.to_owned(),
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

async fn serve_connection(
    connection: Stream,
    component_path: PathBuf,
    python_root: PathBuf,
    max_ipc_frame_bytes: usize,
    worker_queue_capacity: usize,
    cache_vfs: bool,
    cache_vfs_negative: bool,
    cpu_share_enabled: bool,
    cpu_share_limit_percent: Option<f64>,
    cpu_share_sample_interval: Duration,
    cpu_share_activity_timeout: Duration,
) -> anyhow::Result<()> {
    let (mut reader, mut writer) = connection.split();
    let (outgoing, mut outgoing_receiver) = mpsc::channel::<Frame>(256);
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = outgoing_receiver.recv().await {
            write_frame(&mut writer, &frame).await?;
        }
        Ok::<(), pysandbox_protocol::ProtocolError>(())
    });
    let mut workers = HashMap::<u64, WorkerHandle>::new();
    let pending_guest_calls: PendingGuestCalls = Arc::new(Mutex::new(HashMap::new()));
    let next_guest_call_id = Arc::new(AtomicU64::new(1 << 63));
    let pending_vfs_requests: PendingVfsRequests = Arc::new(Mutex::new(HashMap::new()));
    let vfs = RemoteVfs::new(
        outgoing.clone(),
        next_guest_call_id.clone(),
        pending_vfs_requests,
        if cache_vfs {
            CachePolicy::Invalidated {
                negative: cache_vfs_negative,
            }
        } else {
            CachePolicy::None
        },
    );
    let runtime = ComponentRuntime::load(
        &component_path,
        vfs.clone(),
        cpu_share_enabled,
        cpu_share_limit_percent,
        cpu_share_sample_interval,
        cpu_share_activity_timeout,
    )?;

    loop {
        let frame = read_frame(&mut reader, max_ipc_frame_bytes).await?;
        match frame.kind {
            FrameKind::HealthCheck => {
                outgoing
                    .send(Frame::new(
                        FrameKind::HealthStatus,
                        frame.worker_id,
                        frame.request_id,
                        Vec::new(),
                    ))
                    .await?;
            }
            FrameKind::Execute => {
                let request = match decode_payload::<ExecuteRequest>(&frame.payload) {
                    Ok(request) => request,
                    Err(error) => {
                        send_error(
                            &outgoing,
                            frame.worker_id,
                            frame.request_id,
                            error.to_string(),
                        )
                        .await?;
                        continue;
                    }
                };
                let worker = workers.entry(frame.worker_id).or_insert_with(|| {
                    spawn_worker(
                        frame.worker_id,
                        runtime.clone(),
                        python_root.clone(),
                        outgoing.clone(),
                        next_guest_call_id.clone(),
                        pending_guest_calls.clone(),
                        worker_queue_capacity,
                        request.package_paths.clone(),
                    )
                });
                let command = WorkerCommand::Execute {
                    request_id: frame.request_id,
                    request,
                };
                match worker.commands.try_send(command) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        send_execute_result(
                            &outgoing,
                            frame.worker_id,
                            frame.request_id,
                            Some("worker command queue is full".into()),
                            TerminationReason::InfrastructureError,
                        )
                        .await?;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        send_execute_result(
                            &outgoing,
                            frame.worker_id,
                            frame.request_id,
                            Some("worker actor stopped".into()),
                            TerminationReason::InfrastructureError,
                        )
                        .await?;
                        workers.remove(&frame.worker_id);
                    }
                }
            }
            FrameKind::Cancel => {
                let result = decode_payload::<CancelRequest>(&frame.payload)
                    .map_err(anyhow::Error::from)
                    .and_then(|request| {
                        workers
                            .get(&frame.worker_id)
                            .ok_or_else(|| anyhow::anyhow!("worker does not exist"))?
                            .control
                            .cancel(request.execution_id)
                    });
                send_control_result(
                    &outgoing,
                    frame.worker_id,
                    frame.request_id,
                    result.err().map(|error| error.to_string()),
                )
                .await?;
            }
            FrameKind::UpdateLimits => {
                let result = match decode_payload::<ExecutionControl>(&frame.payload) {
                    Ok(update) => apply_control(workers.get(&frame.worker_id), update).await,
                    Err(error) => Err(error.into()),
                };
                send_control_result(
                    &outgoing,
                    frame.worker_id,
                    frame.request_id,
                    result.err().map(|error| error.to_string()),
                )
                .await?;
            }
            FrameKind::CloseWorker => {
                let result = workers
                    .remove(&frame.worker_id)
                    .ok_or_else(|| anyhow::anyhow!("worker does not exist"))
                    .and_then(|worker| worker.control.close());
                send_control_result(
                    &outgoing,
                    frame.worker_id,
                    frame.request_id,
                    result.err().map(|error| error.to_string()),
                )
                .await?;
            }
            FrameKind::GuestResponse => {
                let Some(waiter) = pending_guest_calls.lock().await.remove(&frame.request_id)
                else {
                    continue;
                };
                let result = match decode_payload::<RpcResult>(&frame.payload) {
                    Ok(response) => match response.error {
                        Some(error) => Err(error),
                        None => Ok(response.value),
                    },
                    Err(error) => Err(error.to_string()),
                };
                let _ = waiter.send(result);
            }
            FrameKind::VfsResponse => match decode_payload::<VfsResponse>(&frame.payload) {
                Ok(response) => vfs.accept_response(frame.request_id, response).await,
                Err(error) => {
                    vfs.accept_response(
                        frame.request_id,
                        VfsResponse {
                            value: None,
                            error: Some(pysandbox_protocol::VfsError {
                                code: pysandbox_protocol::VfsErrorCode::Io,
                                message: error.to_string(),
                            }),
                        },
                    )
                    .await;
                }
            },
            FrameKind::InvalidateVfs => {
                let result = decode_payload::<InvalidateVfs>(&frame.payload);
                match result {
                    Ok(request) => {
                        vfs.invalidate(request.path.as_deref()).await;
                        send_control_result(&outgoing, frame.worker_id, frame.request_id, None)
                            .await?;
                    }
                    Err(error) => {
                        send_control_result(
                            &outgoing,
                            frame.worker_id,
                            frame.request_id,
                            Some(error.to_string()),
                        )
                        .await?;
                    }
                }
            }
            FrameKind::WorkerCall => {
                let error = match decode_payload::<WorkerRpcCall>(&frame.payload) {
                    Err(error) => Some(error.to_string()),
                    Ok(call) => match workers.get(&frame.worker_id) {
                        None => Some("worker does not exist".into()),
                        Some(worker) => worker
                            .control
                            .worker_call(frame.request_id, call.path, call.arguments, call.fuel)
                            .err()
                            .map(|error| match error {
                                WorkerCallEnqueueError::Full => "worker call queue is full".into(),
                                WorkerCallEnqueueError::Closed => "worker actor stopped".into(),
                            }),
                    },
                };
                if let Some(error) = error {
                    send_worker_response(
                        &outgoing,
                        frame.worker_id,
                        frame.request_id,
                        Vec::new(),
                        Some(error),
                    )
                    .await?;
                }
            }
            FrameKind::Shutdown => {
                for worker in workers.values() {
                    let _ = worker.control.close();
                }
                workers.clear();
                drop(runtime);
                drop(vfs);
                drop(outgoing);
                writer_task.await??;
                return Ok(());
            }
            kind => {
                send_error(
                    &outgoing,
                    frame.worker_id,
                    frame.request_id,
                    format!("unsupported message kind: {kind:?}"),
                )
                .await?;
            }
        }
    }
}

enum WorkerCommand {
    Execute {
        request_id: u64,
        request: ExecuteRequest,
    },
}

struct WorkerHandle {
    commands: mpsc::Sender<WorkerCommand>,
    control: crate::component_worker::WorkerControl,
}

fn spawn_worker(
    worker_id: u64,
    runtime: ComponentRuntime,
    python_root: PathBuf,
    outgoing: mpsc::Sender<Frame>,
    next_guest_call_id: Arc<AtomicU64>,
    pending_guest_calls: PendingGuestCalls,
    worker_queue_capacity: usize,
    package_paths: Vec<String>,
) -> WorkerHandle {
    let (commands, mut command_receiver) = mpsc::channel(worker_queue_capacity);
    let (control, control_receiver, worker_call_receiver) =
        crate::component_worker::WorkerControl::new(worker_queue_capacity);
    let actor_control = control.clone();
    let rpc = RpcBridge::new(
        worker_id,
        outgoing.clone(),
        next_guest_call_id,
        pending_guest_calls,
    );
    tokio::spawn(async move {
        let mut worker = match ComponentWorker::load(
            &runtime,
            &python_root,
            worker_id,
            rpc,
            actor_control,
            control_receiver,
            worker_call_receiver,
            &package_paths,
        )
        .await
        {
            Ok(worker) => worker,
            Err(error) => {
                while let Some(WorkerCommand::Execute { request_id, .. }) =
                    command_receiver.recv().await
                {
                    let _ = send_execute_result(
                        &outgoing,
                        worker_id,
                        request_id,
                        Some(error.to_string()),
                        TerminationReason::InfrastructureError,
                    )
                    .await;
                }
                return;
            }
        };

        while let Some(command) = command_receiver.recv().await {
            match command {
                WorkerCommand::Execute {
                    request_id,
                    request,
                } => {
                    execute(&mut worker, &outgoing, worker_id, request_id, request).await;
                }
            }
        }
    });
    WorkerHandle { commands, control }
}

async fn execute(
    worker: &mut ComponentWorker,
    outgoing: &mpsc::Sender<Frame>,
    worker_id: u64,
    request_id: u64,
    request: ExecuteRequest,
) {
    let limits = match execution_limits(request.limits) {
        Ok(limits) => limits,
        Err(error) => {
            let _ = send_execute_result(
                outgoing,
                worker_id,
                request_id,
                Some(error.to_string()),
                TerminationReason::InfrastructureError,
            )
            .await;
            return;
        }
    };
    let (output_sender, mut output_receiver) = mpsc::unbounded_channel();
    let output = outgoing.clone();
    let output_task = tokio::spawn(async move {
        while let Some(event) = output_receiver.recv().await {
            let payload = encode_payload(&output_payload(event))?;
            if output
                .send(Frame::new(
                    FrameKind::Output,
                    worker_id,
                    request_id,
                    payload,
                ))
                .await
                .is_err()
            {
                return Ok(());
            }
        }
        Ok::<(), pysandbox_protocol::ProtocolError>(())
    });

    let result = worker
        .run(
            request_id,
            request.program,
            limits,
            request.rpc_methods,
            output_sender,
            async {
                outgoing
                    .send(Frame::new(
                        FrameKind::ExecuteStarted,
                        worker_id,
                        request_id,
                        Vec::new(),
                    ))
                    .await
                    .map_err(|_| anyhow::anyhow!("sandbox connection is closed"))
            },
        )
        .await;
    let (error, reason) = match result {
        Ok(Ok(())) => (None, TerminationReason::Completed),
        Ok(Err(error)) => (Some(error), TerminationReason::GuestError),
        Err(error) => {
            let output_limit_error = worker.output().limit_error();
            let memory_limit_error = worker.memory_limit_error();
            let message = output_limit_error
                .clone()
                .or_else(|| memory_limit_error.clone())
                .unwrap_or_else(|| error.to_string());
            let reason = if output_limit_error.is_some() {
                TerminationReason::OutputLimit
            } else if memory_limit_error.is_some() {
                TerminationReason::MemoryLimit
            } else if worker.control().was_cancelled() {
                TerminationReason::Cancelled
            } else if worker.control().timed_out() {
                TerminationReason::Timeout
            } else if error.downcast_ref::<wasmtime::Trap>() == Some(&wasmtime::Trap::OutOfFuel) {
                TerminationReason::FuelExhausted
            } else {
                TerminationReason::RuntimeError
            };
            (Some(message), reason)
        }
    };
    let _ = output_task.await;
    let _ = send_execute_result(outgoing, worker_id, request_id, error, reason).await;
}

async fn apply_control(
    worker: Option<&WorkerHandle>,
    update: ExecutionControl,
) -> anyhow::Result<()> {
    let control = &worker
        .ok_or_else(|| anyhow::anyhow!("worker does not exist"))?
        .control;
    match update {
        ExecutionControl::SetFuel { execution_id, fuel } => {
            control.set_fuel(execution_id, fuel).await
        }
        ExecutionControl::AddFuel {
            execution_id,
            amount,
            cap,
        } => control.add_fuel(execution_id, amount, cap).await,
        ExecutionControl::SetLimits {
            execution_id,
            max_memory_bytes,
            max_output_bytes,
            max_guest_rpc_bytes,
            cpu_share_weight,
            timeout_ms,
        } => {
            control
                .set_limits(
                    execution_id,
                    max_memory_bytes.map(usize::try_from).transpose()?,
                    max_output_bytes.map(usize::try_from).transpose()?,
                    max_guest_rpc_bytes.map(usize::try_from).transpose()?,
                    cpu_share_weight,
                    timeout_ms.map(Duration::from_millis),
                )
                .await
        }
    }
}

async fn send_control_result(
    outgoing: &mpsc::Sender<Frame>,
    worker_id: u64,
    request_id: u64,
    error: Option<String>,
) -> anyhow::Result<()> {
    outgoing
        .send(Frame::new(
            FrameKind::ControlResult,
            worker_id,
            request_id,
            encode_payload(&ControlResult { error })?,
        ))
        .await?;
    Ok(())
}

fn execution_limits(
    limits: pysandbox_protocol::ExecutionLimits,
) -> anyhow::Result<ExecutionLimits> {
    anyhow::ensure!(
        limits.cpu_share_weight > 0,
        "CPU share weight must be positive"
    );
    Ok(ExecutionLimits {
        max_memory_bytes: usize::try_from(limits.max_memory_bytes)?,
        max_output_bytes: usize::try_from(limits.max_output_bytes)?,
        max_guest_rpc_bytes: usize::try_from(limits.max_guest_rpc_bytes)?,
        cpu_share_weight: limits.cpu_share_weight,
        fuel: limits.fuel,
        timeout: limits.timeout_ms.map(Duration::from_millis),
    })
}

fn output_payload(event: OutputEvent) -> OutputPayload {
    OutputPayload {
        source: match event.source {
            OutputSource::Stdout => pysandbox_protocol::OutputSource::Stdout,
            OutputSource::Stderr => pysandbox_protocol::OutputSource::Stderr,
        },
        data: event.data.to_vec(),
    }
}

async fn send_execute_result(
    outgoing: &mpsc::Sender<Frame>,
    worker_id: u64,
    request_id: u64,
    error: Option<String>,
    reason: TerminationReason,
) -> anyhow::Result<()> {
    outgoing
        .send(Frame::new(
            FrameKind::ExecuteResult,
            worker_id,
            request_id,
            encode_payload(&ExecuteResult { error, reason })?,
        ))
        .await?;
    Ok(())
}

async fn send_worker_response(
    outgoing: &mpsc::Sender<Frame>,
    worker_id: u64,
    request_id: u64,
    value: Vec<u8>,
    error: Option<String>,
) -> anyhow::Result<()> {
    outgoing
        .send(Frame::new(
            FrameKind::WorkerResponse,
            worker_id,
            request_id,
            encode_payload(&RpcResult { value, error })?,
        ))
        .await?;
    Ok(())
}

async fn send_error(
    outgoing: &mpsc::Sender<Frame>,
    worker_id: u64,
    request_id: u64,
    error: String,
) -> anyhow::Result<()> {
    outgoing
        .send(Frame::new(
            FrameKind::Error,
            worker_id,
            request_id,
            error.into_bytes(),
        ))
        .await?;
    Ok(())
}
