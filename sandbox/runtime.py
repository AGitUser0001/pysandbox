import abc
import asyncio
import multiprocessing as mp
import multiprocessing.spawn as mp_spawn
import os
import re
import threading
import time
import traceback
from collections.abc import Callable, Mapping, Sequence
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from multiprocessing.context import SpawnContext, SpawnProcess
from multiprocessing.connection import Connection
from pathlib import Path
from types import TracebackType
from typing import cast

import wasmtime


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


class RuntimeExecutionError(RuntimeError):
    """Raised when a runtime fails while executing guest code."""


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
    wasmtime_cache: bool = True
    fuel: int | None = None
    replenish_fuel_interval: float | None = None
    epoch_deadline_ticks: int | None = None


@dataclass
class RuntimeParameters:
    wasm_path: Path
    argv: tuple[str, ...] = ()
    env: dict[str, str] = field(default_factory=dict)
    stdin: bytes = b""
    mounts: list[RuntimeMount] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class OutputEvent:
    source: str
    data: bytes


@dataclass(slots=True)
class OutputChunk:
    source: str
    data: bytes | bytearray


@dataclass
class OutputBuffer:
    max_bytes: int
    events: list[OutputEvent | OutputChunk] = field(default_factory=list)
    size: int = 0

    def write_stdout(self, data: bytes) -> int:
        return self.write("stdout", data)

    def write_stderr(self, data: bytes) -> int:
        return self.write("stderr", data)

    def write(self, source: str, data: bytes) -> int:
        self.size += len(data)
        if self.size > self.max_bytes:
            raise RuntimeOutputLimitError(
                f"output exceeded {self.max_bytes} bytes"
            )

        if source not in {"stdout", "stderr"}:
            raise ValueError(f"unknown output source: {source}")

        if self.events and self.events[-1].source == source:
            previous = self.events[-1]
            if isinstance(previous.data, bytearray):
                previous.data.extend(data)
            else:
                self.events[-1] = OutputChunk(
                    source=previous.source,
                    data=bytearray(previous.data + data),
                )
        else:
            if self.events and isinstance(self.events[-1], OutputChunk):
                previous = self.events[-1]
                self.events[-1] = OutputEvent(
                    source=previous.source,
                    data=bytes(previous.data),
                )

            self.events.append(OutputChunk(source=source, data=bytearray(data)))

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
    output: OutputBuffer
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


@dataclass(frozen=True)
class RuntimeResult:
    output: tuple[OutputEvent, ...]
    elapsed: float
    exit_code: int | None = None

    @property
    def stdout(self) -> bytes:
        return b"".join(
            event.data for event in self.output if event.source == "stdout"
        )

    @property
    def stderr(self) -> bytes:
        return b"".join(
            event.data for event in self.output if event.source == "stderr"
        )

    @property
    def text(self) -> str:
        return b"".join(event.data for event in self.output).decode(
            "utf-8",
            errors="replace",
        )

    def formatted_text(
        self,
        *,
        stdout: tuple[bytes | None, bytes | None] = (None, None),
        stderr: tuple[bytes | None, bytes | None] = (None, None),
    ) -> str:
        data = bytearray()
        current_source: str | None = None

        for event in self.output:
            if event.source != current_source:
                if current_source is not None:
                    previous_after = self.output_affixes(
                        current_source,
                        stdout=stdout,
                        stderr=stderr,
                    )[1]
                    if previous_after is not None:
                        data.extend(previous_after)

                current_before = self.output_affixes(
                    event.source,
                    stdout=stdout,
                    stderr=stderr,
                )[0]
                if current_before is not None:
                    data.extend(current_before)

                current_source = event.source

            data.extend(event.data)

        if current_source is not None:
            final_after = self.output_affixes(
                current_source,
                stdout=stdout,
                stderr=stderr,
            )[1]
            if final_after is not None:
                data.extend(final_after)

        return bytes(data).decode("utf-8", errors="replace")

    @staticmethod
    def output_affixes(
        source: str,
        *,
        stdout: tuple[bytes | None, bytes | None],
        stderr: tuple[bytes | None, bytes | None],
    ) -> tuple[bytes | None, bytes | None]:
        if source == "stdout":
            return stdout

        if source == "stderr":
            return stderr

        raise ValueError(f"unknown output source: {source}")


class Runtime(abc.ABC):
    """Base class for a Wasmtime-backed guest runtime."""

    def __init__(self, *, limits: RuntimeLimits | None = None) -> None:
        self.limits = limits or RuntimeLimits()
        self._last_fuel_replenish: float | None = None

    @property
    @abc.abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def generate_runtime_parameters(self, program: str | bytes) -> RuntimeParameters:
        raise NotImplementedError

    def execute(self, program: str | bytes, *, timeout: float | None = None) -> RuntimeResult:
        parameters = self.generate_runtime_parameters(program)
        parent_connection, child_connection = self.create_worker_pipe()

        try:
            process = self.create_worker_process(
                parameters,
                child_connection,
                timeout=timeout,
            )
            self.before_worker_start(parameters)
            with self.suppress_main_module_fixup():
                process.start()
            child_connection.close()

            try:
                response = self.recv_worker_response(
                    parent_connection,
                    parameters,
                    process,
                    timeout=timeout,
                )
            finally:
                parent_connection.close()

            process.join(timeout=1)
            if process.is_alive():
                process.kill()
                process.join()

            if isinstance(response, WorkerFailure):
                raise RuntimeExecutionError(
                    f"{response.error_type}: {response.message}\n{response.traceback}"
                )

            return response
        finally:
            self.after_worker_finish(parameters)

    async def execute_async(
        self,
        program: str | bytes,
        *,
        timeout: float | None = None,
    ) -> RuntimeResult:
        return await asyncio.to_thread(self.execute, program, timeout=timeout)

    def create_worker_pipe(self) -> tuple[Connection, Connection]:
        return self.multiprocessing_context().Pipe(duplex=False)

    def create_worker_process(
        self,
        parameters: RuntimeParameters,
        connection: Connection,
        *,
        timeout: float | None,
    ) -> SpawnProcess:
        return self.multiprocessing_context().Process(
            target=runtime_worker_entrypoint,
            args=(self, parameters, connection, timeout),
            name=f"{self.name}-runtime-worker",
        )

    def recv_worker_response(
        self,
        connection: Connection,
        parameters: RuntimeParameters,
        process: SpawnProcess,
        *,
        timeout: float | None,
    ) -> RuntimeResult | WorkerFailure:
        if timeout is None:
            while True:
                self.check_worker_process(parameters, process)
                if connection.poll(0.1):
                    return connection.recv()

                if process.exitcode is not None:
                    raise RuntimeExecutionError(
                        f"runtime worker exited without a response (exit code {process.exitcode})"
                    )

        deadline = time.monotonic() + timeout + 1
        while time.monotonic() < deadline:
            self.check_worker_process(parameters, process)
            if connection.poll(0.1):
                return connection.recv()

            if process.exitcode is not None:
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

    def before_worker_start(self, parameters: RuntimeParameters) -> None:
        return

    def after_worker_finish(self, parameters: RuntimeParameters) -> None:
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
                timeout=timeout,
            )
            self.before_execution(environment, parameters)
            result = self.run_entrypoint(environment)
            return RuntimeResult(
                output=result.output,
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
        *,
        timeout: float | None = None,
    ) -> WasmtimeEnvironment:
        config = wasmtime.Config()
        self.configure_wasmtime(config, timeout=timeout)

        engine = wasmtime.Engine(config)
        store = wasmtime.Store(engine)
        epoch_timer = self.configure_store(
            store,
            engine=engine,
            timeout=timeout,
        )

        linker = wasmtime.Linker(engine)
        output = OutputBuffer(max_bytes=self.limits.max_output_bytes)
        stdin_writer = self.configure_worker_stdin(parameters.stdin)
        wasi_config = wasmtime.WasiConfig()

        self.configure_wasi(
            wasi_config,
            parameters,
            output=output,
        )
        store.set_wasi(wasi_config)
        self.configure_linker(linker, store)

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
            stdin_writer=stdin_writer,
            epoch_timer=epoch_timer,
        )

    def configure_wasmtime(
        self,
        config: wasmtime.Config,
        *,
        timeout: float | None,
    ) -> None:
        self.validate_limits()

        if self.limits.wasmtime_cache:
            config.cache = True

        config.max_wasm_stack = self.limits.max_wasm_stack

        if self.fuel_enabled():
            config.consume_fuel = True

        if self.limits.epoch_deadline_ticks is not None or timeout is not None:
            config.epoch_interruption = True

    def validate_limits(self) -> None:
        if self.limits.replenish_fuel_interval is not None:
            if self.limits.fuel is None:
                raise ValueError("replenish_fuel_interval requires fuel")

            if self.limits.replenish_fuel_interval <= 0:
                raise ValueError("replenish_fuel_interval must be positive")

    def configure_store(
        self,
        store: wasmtime.Store,
        *,
        engine: wasmtime.Engine,
        timeout: float | None,
    ) -> EpochTimer | None:
        store.set_limits(
            memory_size=self.limits.max_memory_size,
            instances=1,
            tables=1,
            memories=1,
        )

        if self.limits.fuel is not None:
            store.set_fuel(self.limits.fuel)
            self._last_fuel_replenish = time.monotonic()

        if timeout is not None:
            if timeout <= 0:
                raise ValueError("timeout must be positive")

            store.set_epoch_deadline(self.limits.epoch_deadline_ticks or 1)
            timer = EpochTimer(engine=engine, timeout=timeout)
            timer.start()
            return timer

        if self.limits.epoch_deadline_ticks is not None:
            store.set_epoch_deadline(self.limits.epoch_deadline_ticks)

        return None

    def fuel_enabled(self) -> bool:
        return (
            self.limits.fuel is not None
        )

    def configure_linker(
        self,
        linker: wasmtime.Linker,
        store: wasmtime.Store,
    ) -> None:
        linker.allow_shadowing = self.limits.replenish_fuel_interval is not None
        linker.define_wasi()

        if self.limits.replenish_fuel_interval is not None:
            linker.define_func(
                "wasi_snapshot_preview1",
                "sched_yield",
                wasmtime.FuncType([], [wasmtime.ValType.i32()]),
                lambda: self.sched_yield(store),
            )

    def sched_yield(self, store: wasmtime.Store) -> int:
        if self._last_fuel_replenish is None or self.limits.fuel is None or self.limits.replenish_fuel_interval is None:
            return 0
        delta = time.monotonic() - self._last_fuel_replenish
        if delta < self.limits.replenish_fuel_interval:
            return 0
        store.set_fuel(self.limits.fuel)
        self._last_fuel_replenish = time.monotonic()
        return 0

    def configure_wasi(
        self,
        wasi_config: wasmtime.WasiConfig,
        parameters: RuntimeParameters,
        *,
        output: OutputBuffer,
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
            output=freeze_output_events(environment.output.events),
            elapsed=0,
            exit_code=exit_code,
        )


def freeze_output_events(
    events: Sequence[OutputEvent | OutputChunk],
) -> tuple[OutputEvent, ...]:
    return tuple(
        OutputEvent(source=event.source, data=bytes(event.data))
        for event in events
    )


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
    timeout: float | None,
) -> None:
    try:
        connection.send(runtime.execute_in_worker(parameters, timeout=timeout))
    except BaseException as exc:
        connection.send(
            WorkerFailure(
                error_type=type(exc).__name__,
                message=str(exc),
                traceback=traceback.format_exc(),
            )
        )
    finally:
        connection.close()


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
