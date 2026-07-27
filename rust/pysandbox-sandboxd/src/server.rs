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
    PendingGuestCalls, RpcBridge,
};
use crate::remote_vfs::{CachePolicy, PendingVfsRequests, RemoteVfs};

pub async fn serve(
    socket_name: &str,
    component_path: &Path,
    python_root: &Path,
    max_ipc_frame_bytes: usize,
    cache_vfs: bool,
) -> anyhow::Result<()> {
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
        cache_vfs,
    )
    .await
}

async fn serve_connection(
    connection: Stream,
    component_path: PathBuf,
    python_root: PathBuf,
    max_ipc_frame_bytes: usize,
    cache_vfs: bool,
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
            CachePolicy::Invalidated
        } else {
            CachePolicy::None
        },
    );
    let runtime = ComponentRuntime::load(&component_path, vfs.clone())?;

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
                    )
                });
                if worker
                    .commands
                    .send(WorkerCommand::Execute {
                        request_id: frame.request_id,
                        request,
                    })
                    .await
                    .is_err()
                {
                    send_error(
                        &outgoing,
                        frame.worker_id,
                        frame.request_id,
                        "worker actor stopped".into(),
                    )
                    .await?;
                    workers.remove(&frame.worker_id);
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
                let result = decode_payload::<WorkerRpcCall>(&frame.payload)
                    .map_err(anyhow::Error::from)
                    .and_then(|call| {
                        workers
                            .get(&frame.worker_id)
                            .ok_or_else(|| anyhow::anyhow!("worker does not exist"))?
                            .control
                            .worker_call(frame.request_id, call.path, call.arguments, call.fuel)
                    });
                if let Err(error) = result {
                    send_error(
                        &outgoing,
                        frame.worker_id,
                        frame.request_id,
                        error.to_string(),
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
) -> WorkerHandle {
    let (commands, mut command_receiver) = mpsc::channel(16);
    let (control, control_receiver) = crate::component_worker::WorkerControl::new();
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
            rpc,
            actor_control,
            control_receiver,
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
        .run(request_id, request.program, limits, output_sender)
        .await;
    let (error, reason) = match result {
        Ok(Ok(())) => (None, TerminationReason::Completed),
        Ok(Err(error)) => (Some(error), TerminationReason::GuestError),
        Err(error) => {
            let message = error.to_string();
            let reason = if worker.control().was_cancelled() {
                TerminationReason::Cancelled
            } else if worker.control().timed_out() {
                TerminationReason::Timeout
            } else if error.downcast_ref::<wasmtime::Trap>() == Some(&wasmtime::Trap::OutOfFuel) {
                TerminationReason::FuelExhausted
            } else if message.contains("guest output exceeded") {
                TerminationReason::OutputLimit
            } else if message.contains("memory") && message.contains("limit") {
                TerminationReason::MemoryLimit
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
            timeout_ms,
        } => {
            control
                .set_limits(
                    execution_id,
                    max_memory_bytes.map(usize::try_from).transpose()?,
                    max_output_bytes.map(usize::try_from).transpose()?,
                    max_guest_rpc_bytes.map(usize::try_from).transpose()?,
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
    Ok(ExecutionLimits {
        max_memory_bytes: usize::try_from(limits.max_memory_bytes)?,
        max_output_bytes: usize::try_from(limits.max_output_bytes)?,
        max_guest_rpc_bytes: usize::try_from(limits.max_guest_rpc_bytes)?,
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
