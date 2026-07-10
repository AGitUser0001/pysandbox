import abc
import asyncio
import multiprocessing as mp
import multiprocessing.spawn as mp_spawn
import os
import re
import threading
import time
import traceback
from collections import UserList
from collections.abc import Callable, Sequence
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from multiprocessing.context import SpawnContext, SpawnProcess
from multiprocessing.connection import Connection
from pathlib import Path
from types import TracebackType
from typing import cast

import wasmtime


__all__ = [
    "Output",
    "OutputEvent",
    "Runtime",
    "RuntimeError",
    "RuntimeExecutionError",
    "RuntimeLimits",
    "RuntimeLimitState",
    "RuntimeMount",
    "RuntimeOutputLimitError",
    "RuntimeParameters",
    "RuntimeResult",
    "RuntimeSetupError",
    "WasmtimeEnvironment",
    "Worker",
]


GetPreparationData = Callable[[str], dict[str, object]]

_suppress_main_module_fixup = ContextVar(
    "suppress_main_module_fixup",
    default=False,
)
_original_get_preparation_data = cast(
    GetPreparationData,
    mp_spawn.get_preparation_data,
)


def get_preparation_data(name: str) -> dict[str, object]:
    data = _original_get_preparation_data(name)

    if _suppress_main_module_fixup.get():
        data.pop("init_main_from_name", None)
        data.pop("init_main_from_path", None)

    return data


mp_spawn.get_preparation_data = get_preparation_data


class RuntimeError(Exception):
    """Base error for runtime setup and execution failures."""


class RuntimeSetupError(RuntimeError):
    """Raised when a runtime cannot be prepared."""


class RuntimeWorkerTraceback(RuntimeError):
    def __init__(self, traceback: str) -> None:
        super().__init__()
        self.traceback = traceback

    def __str__(self) -> str:
        return self.traceback


class RuntimeExecutionError(RuntimeError):
    """Raised when a runtime fails while executing guest code."""

    def __init__(
        self,
        message: str,
        *,
        error_type: str | None = None,
        traceback: str | None = None,
    ) -> None:
        super().__init__(message)
        self.type = error_type
        self.traceback = traceback
        if traceback is not None:
            self.__cause__ = RuntimeWorkerTraceback(traceback)


class RuntimeOutputLimitError(RuntimeError):
    """Raised when stdout/stderr exceed the configured output limit."""


@dataclass(frozen=True)
class RuntimeMount:
    host: Path
    guest: str
    readonly: bool = True
    file_readonly: bool | None = None


@dataclass(frozen=True)
class RuntimeLimits:
    max_memory_size: int = 128 * 1024 * 1024
    max_wasm_stack: int = 512 * 1024
    max_output_bytes: int = 256 * 1024
    max_rpc_message_bytes: int = 256 * 1024
    max_rpc_file_bytes: int = 2 * 1024 * 1024
    wasmtime_cache: bool = True
    fuel: int | None = None
    replenish_fuel_interval: float | None = None


@dataclass
class RuntimeParameters:
    wasm_path: Path
    limits: RuntimeLimits = field(default_factory=RuntimeLimits)
    argv: tuple[str, ...] = ()
    env: dict[str, str] = field(default_factory=dict)
    stdin: bytes = b""
    mounts: list[RuntimeMount] = field(default_factory=list)


RuntimeStartCallback = Callable[[SpawnProcess, object | None], None]
RuntimeWorkerStartCallback = Callable[[SpawnProcess, object | None, Connection], None]


@dataclass
class RuntimeLimitState:
    current: RuntimeLimits


@dataclass(frozen=True, slots=True)
class OutputEvent:
    source: str
    data: bytes


class Output(UserList[OutputEvent]):
    @property
    def stdout(self) -> bytes:
        return b"".join(
            event.data for event in self.data if event.source == "stdout"
        )

    @property
    def stderr(self) -> bytes:
        return b"".join(
            event.data for event in self.data if event.source == "stderr"
        )

    @property
    def text(self) -> str:
        return self.bytes.decode(
            "utf-8",
            errors="replace",
        )

    @property
    def bytes(self) -> bytes:
        return b"".join(event.data for event in self.data)

    def formatted(
        self,
        *,
        stdout: tuple[bytes | None, bytes | None] = (None, None),
        stderr: tuple[bytes | None, bytes | None] = (None, None),
    ) -> bytes:
        data = bytearray()
        current_source: str | None = None
        affixes = {
            "stdout": stdout,
            "stderr": stderr,
        }

        for event in self.data:
            if event.source != current_source:
                if current_source is not None:
                    previous_after = affixes[current_source][1]
                    if previous_after is not None:
                        data.extend(previous_after)

                current_before = affixes[event.source][0]
                if current_before is not None:
                    data.extend(current_before)

                current_source = event.source

            data.extend(event.data)

        if current_source is not None:
            final_after = affixes[current_source][1]
            if final_after is not None:
                data.extend(final_after)

        return bytes(data)
    
    def formatted_text(
        self,
        *,
        stdout: tuple[bytes | None, bytes | None] = (None, None),
        stderr: tuple[bytes | None, bytes | None] = (None, None),
    ) -> str:
        return self.formatted(stdout=stdout, stderr=stderr).decode("utf-8", errors="replace")


OUTPUT_SEPARATOR = b"\0"


@dataclass
class OutputWriter:
    limits: RuntimeLimitState
    connection: Connection
    control_connection: Connection
    size: int = 0

    def write_stdout(self, data: bytes) -> int:
        return self.write("stdout", data)

    def write_stderr(self, data: bytes) -> int:
        return self.write("stderr", data)

    def write(self, source: str, data: bytes) -> int:
        drain_limit_updates(self.limits, self.control_connection)
        self.size += len(data)
        max_bytes = self.limits.current.max_output_bytes
        if self.size > max_bytes:
            raise RuntimeOutputLimitError(
                f"output exceeded {max_bytes} bytes"
            )

        if source not in {"stdout", "stderr"}:
            raise ValueError(f"unknown output source: {source}")

        self.connection.send_bytes(encode_output_event(OutputEvent(source, data)))

        return len(data)


@dataclass
class EpochTimer:
    engine: wasmtime.Engine
    timeout: float
    cancelled: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None

    def start(self) -> None:
        self.thread = threading.Thread(
            target=self.run,
            name="wasmtime-epoch-timer",
            daemon=True,
        )
        self.thread.start()

    def cancel(self) -> None:
        self.cancelled.set()
        if self.thread is not None:
            self.thread.join(timeout=1)

    def run(self) -> None:
        if not self.cancelled.wait(self.timeout):
            self.engine.increment_epoch()


@dataclass
class WasmtimeEnvironment:
    config: wasmtime.Config
    engine: wasmtime.Engine
    store: wasmtime.Store
    linker: wasmtime.Linker
    wasi_config: wasmtime.WasiConfig
    module: wasmtime.Module
    instance: wasmtime.Instance
    output: OutputWriter
    limits: RuntimeLimitState
    control_connection: Connection
    stdin_writer: threading.Thread
    epoch_timer: EpochTimer | None


@dataclass(frozen=True)
class WorkerFailure:
    error_type: str
    message: str
    traceback: str


class MainModuleFixupSuppressed:
    def __init__(self) -> None:
        self.token: Token[bool] | None = None

    def __enter__(self) -> None:
        self.token = _suppress_main_module_fixup.set(True)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self.token is not None:
            _suppress_main_module_fixup.reset(self.token)


@dataclass
class RuntimeResult:
    output: Output = field(default_factory=Output)
    elapsed: float = 0
    exit_code: int | None = None

    @property
    def stdout(self) -> bytes:
        return self.output.stdout

    @property
    def stderr(self) -> bytes:
        return self.output.stderr

    @property
    def text(self) -> str:
        return self.output.text

    def formatted_text(
        self,
        *,
        stdout: tuple[bytes | None, bytes | None] = (None, None),
        stderr: tuple[bytes | None, bytes | None] = (None, None),
    ) -> str:
        return self.output.formatted_text(stdout=stdout, stderr=stderr)


class Runtime(abc.ABC):
    """Base class for a Wasmtime-backed guest runtime."""

    def __init__(self) -> None:
        self._last_fuel_replenish: float | None = None

    @property
    @abc.abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def generate_runtime_parameters(
        self,
        program: str | bytes,
        limits: RuntimeLimits,
    ) -> RuntimeParameters:
        raise NotImplementedError

    def execute(
        self,
        program: str | bytes,
        *,
        limits: RuntimeLimits | None = None,
        timeout: float | None = None,
        after_start: RuntimeStartCallback | None = None,
        worker_after_start: RuntimeWorkerStartCallback | None = None,
        result: RuntimeResult | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> RuntimeResult:
        if result is None:
            result = RuntimeResult()

        execution_limits = limits or RuntimeLimits()
        parameters = self.generate_runtime_parameters(program, execution_limits)

        def capture_start(
            process: SpawnProcess,
            execution: object | None,
            control_connection: Connection,
        ) -> None:
            if after_start is not None:
                after_start(process, execution)
            if worker_after_start is not None:
                worker_after_start(process, execution, control_connection)

        (
            parent_connection,
            parent_output_connection,
            parent_control_connection,
            process,
            execution,
        ) = self.start_worker_process(
            parameters,
            timeout=timeout,
            after_start=capture_start,
            loop=loop,
        )
        try:
            try:
                response = self.recv_worker_response(
                    parent_connection,
                    parent_output_connection,
                    parameters,
                    process,
                    result,
                    timeout=timeout,
                )
            finally:
                close_connection(parent_connection)
                close_connection(parent_output_connection)
                close_connection(parent_control_connection)

            process.join(timeout=1)
            if process.is_alive():
                process.kill()
                process.join()

            if isinstance(response, WorkerFailure):
                raise RuntimeExecutionError(
                    response.message,
                    error_type=response.error_type,
                    traceback=response.traceback,
                )

            result.elapsed = response.elapsed
            result.exit_code = response.exit_code
            return result
        finally:
            self.after_worker_finish(parameters, execution=execution)

    def run(
        self,
        program: str | bytes,
        *,
        limits: RuntimeLimits | None = None,
        timeout: float | None = None,
        after_start: RuntimeStartCallback | None = None,
    ) -> "Worker":
        loop = asyncio.get_running_loop()
        execution_future: asyncio.Future[object | None] = loop.create_future()
        control_future: asyncio.Future[Connection] = loop.create_future()
        result = RuntimeResult()
        process_box: list[SpawnProcess] = []
        cancel_requested = threading.Event()

        def set_execution_result(value: object | None) -> None:
            if not execution_future.done():
                execution_future.set_result(value)

        def set_execution_exception(exc: BaseException) -> None:
            if not execution_future.done():
                execution_future.set_exception(exc)
                execution_future.exception()

        def set_control_result(value: Connection) -> None:
            if not control_future.done():
                control_future.set_result(value)

        def set_control_exception(exc: BaseException) -> None:
            if not control_future.done():
                control_future.set_exception(exc)
                control_future.exception()

        def capture_start(
            process: SpawnProcess,
            execution: object | None,
            control_connection: Connection,
        ) -> None:
            process_box.append(process)

            if cancel_requested.is_set():
                self.terminate_process(process)

            loop.call_soon_threadsafe(set_control_result, control_connection)
            loop.call_soon_threadsafe(set_execution_result, execution)

        def execute_in_thread() -> RuntimeResult:
            try:
                return self.execute(
                    program,
                    limits=limits,
                    timeout=timeout,
                    after_start=after_start,
                    worker_after_start=capture_start,
                    result=result,
                    loop=loop,
                )
            except BaseException as exc:
                loop.call_soon_threadsafe(set_execution_exception, exc)
                loop.call_soon_threadsafe(set_control_exception, exc)
                raise

        async def wait_result() -> RuntimeResult:
            try:
                return await asyncio.to_thread(execute_in_thread)
            except asyncio.CancelledError:
                cancel_requested.set()
                if process_box:
                    self.terminate_process(process_box[0])
                raise

        task = asyncio.create_task(wait_result())
        return Worker(
            runtime=self,
            task=task,
            execution_future=execution_future,
            result=result,
            control_future=control_future,
        )

    def start_worker_process(
        self,
        parameters: RuntimeParameters,
        *,
        timeout: float | None = None,
        after_start: RuntimeWorkerStartCallback | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> tuple[Connection, Connection, Connection, SpawnProcess, object | None]:
        parent_connection, child_connection = self.create_worker_pipe()
        parent_output_connection, child_output_connection = self.create_worker_pipe()
        child_control_connection, parent_control_connection = self.create_worker_pipe()
        process = self.create_worker_process(
            parameters,
            child_connection,
            child_output_connection,
            child_control_connection,
            timeout=timeout,
        )
        execution = self.before_worker_start(parameters, loop=loop)
        try:
            with self.suppress_main_module_fixup():
                process.start()
            close_connection(child_connection)
            close_connection(child_output_connection)
            close_connection(child_control_connection)
            if after_start is not None:
                after_start(process, execution, parent_control_connection)
            return (
                parent_connection,
                parent_output_connection,
                parent_control_connection,
                process,
                execution,
            )
        except BaseException:
            close_connection(parent_connection)
            close_connection(parent_output_connection)
            close_connection(parent_control_connection)
            close_connection(child_connection)
            close_connection(child_output_connection)
            close_connection(child_control_connection)
            self.terminate_process(process)
            self.after_worker_finish(parameters, execution=execution)
            raise

    def terminate_process(self, process: SpawnProcess) -> None:
        if process.exitcode is not None:
            return

        process.terminate()
        process.join(timeout=1)
        if process.is_alive():
            process.kill()
            process.join()

    def create_worker_pipe(self) -> tuple[Connection, Connection]:
        return self.multiprocessing_context().Pipe(duplex=False)

    def create_worker_process(
        self,
        parameters: RuntimeParameters,
        connection: Connection,
        output_connection: Connection,
        control_connection: Connection,
        *,
        timeout: float | None,
    ) -> SpawnProcess:
        return self.multiprocessing_context().Process(
            target=runtime_worker_entrypoint,
            args=(
                self,
                parameters,
                connection,
                output_connection,
                control_connection,
                timeout,
            ),
            name=f"{self.name}-runtime-worker",
        )

    def recv_worker_response(
        self,
        connection: Connection,
        output_connection: Connection,
        parameters: RuntimeParameters,
        process: SpawnProcess,
        result: RuntimeResult,
        *,
        timeout: float | None,
    ) -> RuntimeResult | WorkerFailure:
        def drain_output() -> None:
            while output_connection.poll():
                try:
                    packet = output_connection.recv_bytes()
                except EOFError:
                    return

                event = decode_output_event(packet)
                result.output.append(event)

        if timeout is None:
            while True:
                self.check_worker_process(parameters, process)
                drain_output()
                if connection.poll(0.1):
                    response = connection.recv()
                    drain_output()
                    return response

                if process.exitcode is not None:
                    drain_output()
                    raise RuntimeExecutionError(
                        f"runtime worker exited without a response (exit code {process.exitcode})"
                    )

        deadline = time.monotonic() + timeout + 1
        while time.monotonic() < deadline:
            self.check_worker_process(parameters, process)
            drain_output()
            if connection.poll(0.1):
                response = connection.recv()
                drain_output()
                return response

            if process.exitcode is not None:
                drain_output()
                raise RuntimeExecutionError(
                    f"runtime worker exited without a response (exit code {process.exitcode})"
                )

        process.terminate()
        process.join(timeout=1)
        if process.is_alive():
            process.kill()
            process.join()

        raise RuntimeExecutionError(f"runtime worker timed out after {timeout}s")

    def multiprocessing_context(self) -> SpawnContext:
        return mp.get_context("spawn")

    def suppress_main_module_fixup(self) -> MainModuleFixupSuppressed:
        return MainModuleFixupSuppressed()

    def before_worker_start(
        self,
        parameters: RuntimeParameters,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> object | None:
        return None

    def after_worker_finish(
        self,
        parameters: RuntimeParameters,
        *,
        execution: object | None = None,
    ) -> None:
        return

    def check_worker_process(
        self,
        parameters: RuntimeParameters,
        process: SpawnProcess,
    ) -> None:
        return

    def execute_in_worker(
        self,
        parameters: RuntimeParameters,
        output_connection: Connection,
        control_connection: Connection,
        *,
        timeout: float | None = None,
    ) -> RuntimeResult:
        started_at = time.monotonic()
        environment: WasmtimeEnvironment | None = None
        result: RuntimeResult | None = None
        error: BaseException | None = None

        try:
            environment = self.setup_wasmtime(
                parameters,
                output_connection,
                control_connection,
                timeout=timeout,
            )
            self.before_execution(environment, parameters)
            result = self.run_entrypoint(environment)
            return RuntimeResult(
                elapsed=time.monotonic() - started_at,
                exit_code=result.exit_code,
            )
        except BaseException as exc:
            error = exc
            raise
        finally:
            if environment is not None:
                self.after_execution(environment, parameters, result=result, error=error)
                if environment.epoch_timer is not None:
                    environment.epoch_timer.cancel()
                environment.stdin_writer.join(timeout=1)

    def setup_wasmtime(
        self,
        parameters: RuntimeParameters,
        output_connection: Connection,
        control_connection: Connection,
        *,
        timeout: float | None = None,
    ) -> WasmtimeEnvironment:
        limits = RuntimeLimitState(parameters.limits)
        config = wasmtime.Config()
        self.configure_wasmtime(config, limits=limits.current, timeout=timeout)

        engine = wasmtime.Engine(config)
        store = wasmtime.Store(engine)
        epoch_timer = self.configure_store(
            store,
            limits=limits.current,
            engine=engine,
            timeout=timeout,
        )

        linker = wasmtime.Linker(engine)
        output = OutputWriter(
            limits=limits,
            connection=output_connection,
            control_connection=control_connection,
        )
        stdin_writer = self.configure_worker_stdin(parameters.stdin)
        wasi_config = wasmtime.WasiConfig()

        self.configure_wasi(
            wasi_config,
            parameters,
            output=output,
        )
        store.set_wasi(wasi_config)
        self.configure_linker(
            linker,
            store,
            limits=limits,
            control_connection=control_connection,
        )

        module = wasmtime.Module.from_file(engine, str(parameters.wasm_path))
        instance = linker.instantiate(store, module)

        return WasmtimeEnvironment(
            config=config,
            engine=engine,
            store=store,
            linker=linker,
            wasi_config=wasi_config,
            module=module,
            instance=instance,
            output=output,
            limits=limits,
            control_connection=control_connection,
            stdin_writer=stdin_writer,
            epoch_timer=epoch_timer,
        )

    def configure_wasmtime(
        self,
        config: wasmtime.Config,
        *,
        limits: RuntimeLimits,
        timeout: float | None,
    ) -> None:
        self.validate_limits(limits)

        if limits.wasmtime_cache:
            config.cache = True

        config.max_wasm_stack = limits.max_wasm_stack

        if self.fuel_enabled(limits):
            config.consume_fuel = True

        if timeout is not None:
            config.epoch_interruption = True

    @staticmethod
    def validate_limits(limits: RuntimeLimits) -> None:
        if limits.max_rpc_message_bytes <= 0:
            raise ValueError("max_rpc_message_bytes must be positive")

        if limits.max_rpc_file_bytes <= 0:
            raise ValueError("max_rpc_file_bytes must be positive")

        if limits.replenish_fuel_interval is not None:
            if limits.fuel is None:
                raise ValueError("replenish_fuel_interval requires fuel")

            if limits.replenish_fuel_interval <= 0:
                raise ValueError("replenish_fuel_interval must be positive")

    def configure_store(
        self,
        store: wasmtime.Store,
        *,
        limits: RuntimeLimits,
        engine: wasmtime.Engine,
        timeout: float | None,
    ) -> EpochTimer | None:
        store.set_limits(
            memory_size=limits.max_memory_size,
            instances=1,
            tables=1,
            memories=1,
        )

        if limits.fuel is not None:
            store.set_fuel(limits.fuel)
            self._last_fuel_replenish = time.monotonic()

        if timeout is not None:
            if timeout <= 0:
                raise ValueError("timeout must be positive")

            store.set_epoch_deadline(1)
            timer = EpochTimer(engine=engine, timeout=timeout)
            timer.start()
            return timer

        return None

    def fuel_enabled(self, limits: RuntimeLimits) -> bool:
        return limits.fuel is not None

    def configure_linker(
        self,
        linker: wasmtime.Linker,
        store: wasmtime.Store,
        *,
        limits: RuntimeLimitState,
        control_connection: Connection,
    ) -> None:
        linker.allow_shadowing = limits.current.replenish_fuel_interval is not None
        linker.define_wasi()

        if limits.current.replenish_fuel_interval is not None:
            linker.define_func(
                "wasi_snapshot_preview1",
                "sched_yield",
                wasmtime.FuncType([], [wasmtime.ValType.i32()]),
                lambda: self.sched_yield(store, limits, control_connection),
            )

    def sched_yield(
        self,
        store: wasmtime.Store,
        limits: RuntimeLimitState,
        control_connection: Connection,
    ) -> int:
        drain_limit_updates(limits, control_connection)
        current = limits.current
        if (
            self._last_fuel_replenish is None
            or current.fuel is None
            or current.replenish_fuel_interval is None
        ):
            return 0
        delta = time.monotonic() - self._last_fuel_replenish
        if delta < current.replenish_fuel_interval:
            return 0
        store.set_fuel(current.fuel)
        self._last_fuel_replenish = time.monotonic()
        return 0

    def configure_wasi(
        self,
        wasi_config: wasmtime.WasiConfig,
        parameters: RuntimeParameters,
        *,
        output: OutputWriter,
    ) -> None:
        wasi_config.inherit_stdin()
        wasi_config.stdout_custom = output.write_stdout
        wasi_config.stderr_custom = output.write_stderr
        wasi_config.argv = list(parameters.argv)

        if parameters.env:
            wasi_config.env = list(parameters.env.items())

        self.configure_mounts(wasi_config, parameters.mounts)

    def configure_worker_stdin(self, stdin: bytes) -> threading.Thread:
        read_fd, write_fd = os.pipe()
        os.dup2(read_fd, 0)
        os.close(read_fd)

        writer = threading.Thread(
            target=write_worker_stdin,
            args=(write_fd, stdin),
            name=f"{self.name}-stdin-writer",
            daemon=True,
        )
        writer.start()
        return writer

    def configure_mounts(
        self,
        wasi_config: wasmtime.WasiConfig,
        mounts: Sequence[RuntimeMount],
    ) -> None:
        for mount in mounts:
            file_readonly = (
                mount.readonly
                if mount.file_readonly is None
                else mount.file_readonly
            )
            wasi_config.preopen_dir(
                str(mount.host),
                mount.guest,
                dir_perms=dir_permissions_for_readonly(mount.readonly),
                file_perms=file_permissions_for_readonly(file_readonly),
            )

    def before_execution(
        self,
        environment: WasmtimeEnvironment,
        parameters: RuntimeParameters,
    ) -> None:
        return

    def after_execution(
        self,
        environment: WasmtimeEnvironment,
        parameters: RuntimeParameters,
        *,
        result: RuntimeResult | None,
        error: BaseException | None,
    ) -> None:
        return

    def run_entrypoint(self, environment: WasmtimeEnvironment) -> RuntimeResult:
        start = environment.instance.exports(environment.store).get("_start")
        if not isinstance(start, wasmtime.Func):
            raise RuntimeSetupError("Wasm module has no _start export")

        exit_code = 0
        try:
            start(environment.store)
        except wasmtime.ExitTrap as exc:
            exit_code = exit_code_from_exit_trap(exc)

        return RuntimeResult(
            elapsed=0,
            exit_code=exit_code,
        )


@dataclass
class Worker:
    runtime: Runtime
    task: asyncio.Task[RuntimeResult]
    execution_future: asyncio.Future[object | None]
    result: RuntimeResult
    control_future: asyncio.Future[Connection] | None = None

    def update_limits(self, limits: RuntimeLimits) -> None:
        control_connection = self.control_connection()
        if control_connection is None:
            raise RuntimeExecutionError("worker control connection is not ready")

        control_connection.send(limits)
        if self.execution_future.done():
            try:
                execution = self.execution_future.result()
            except BaseException:
                return

            update_limits = getattr(execution, "update_limits", None)
            if callable(update_limits):
                update_limits(limits)

    def control_connection(self) -> Connection | None:
        if self.control_future is None or not self.control_future.done():
            return None

        try:
            return self.control_future.result()
        except BaseException:
            return None

    def terminate(self) -> None:
        self.task.cancel()

    async def close(self) -> None:
        self.terminate()
        try:
            await self.task
        except (asyncio.CancelledError, EOFError, RuntimeError):
            return
        finally:
            control_connection = self.control_connection()
            if control_connection is not None:
                close_connection(control_connection)


def encode_output_event(event: OutputEvent) -> bytes:
    source = event.source.encode("utf-8")
    if OUTPUT_SEPARATOR in source:
        raise RuntimeOutputLimitError("output source label contains null byte")

    return source + OUTPUT_SEPARATOR + event.data


def decode_output_event(packet: bytes) -> OutputEvent:
    try:
        source, data = packet.split(OUTPUT_SEPARATOR, 1)
    except ValueError:
        raise RuntimeOutputLimitError("output frame has no source separator")

    return OutputEvent(
        source=source.decode("utf-8"),
        data=data,
    )


def close_connection(connection: Connection) -> None:
    try:
        connection.close()
    except OSError:
        return


def drain_limit_updates(
    limits: RuntimeLimitState,
    control_connection: Connection,
) -> None:
    while control_connection.poll():
        value = control_connection.recv()
        if not isinstance(value, RuntimeLimits):
            continue

        Runtime.validate_limits(value)
        limits.current = value


def exit_code_from_exit_trap(exc: wasmtime.ExitTrap) -> int:
    match = re.search(r"Exited with i32 exit status (\d+)", str(exc))
    if match is None:
        raise exc

    return int(match.group(1))


def dir_permissions_for_readonly(readonly: bool) -> wasmtime.DirPerms:
    if readonly:
        return wasmtime.DirPerms.READ_ONLY

    return wasmtime.DirPerms.READ_WRITE


def file_permissions_for_readonly(readonly: bool) -> wasmtime.FilePerms:
    if readonly:
        return wasmtime.FilePerms.READ_ONLY

    return wasmtime.FilePerms.READ_WRITE


def runtime_worker_entrypoint(
    runtime: Runtime,
    parameters: RuntimeParameters,
    connection: Connection,
    output_connection: Connection,
    control_connection: Connection,
    timeout: float | None,
) -> None:
    try:
        connection.send(
            runtime.execute_in_worker(
                parameters,
                output_connection,
                control_connection,
                timeout=timeout,
            )
        )
    except BaseException as exc:
        connection.send(
            WorkerFailure(
                error_type=type(exc).__name__,
                message=str(exc),
                traceback=traceback.format_exc(),
            )
        )
    finally:
        close_connection(connection)
        close_connection(output_connection)
        close_connection(control_connection)


def write_worker_stdin(write_fd: int, stdin: bytes) -> None:
    try:
        view = memoryview(stdin)
        written = 0
        while written < len(view):
            written += os.write(write_fd, view[written:])
    except BrokenPipeError:
        return
    finally:
        os.close(write_fd)
