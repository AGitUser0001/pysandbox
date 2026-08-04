import asyncio
import builtins
import itertools
import keyword
import math
import sys
import tempfile
from collections.abc import Callable, Collection, Iterable, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from enum import StrEnum
from importlib.resources import files
from pathlib import Path
from typing import Self, overload

from platformdirs import user_cache_path

from . import _core
from .packages import (
  DEFAULT_PACKAGE_CACHE,
  DEFAULT_PACKAGE_INDEX,
  PackageCache,
  PackageEnvironment,
  PackageIndex,
  PackageManager,
)
from .rpc import RpcHost
from .vfs import VirtualFileSystem

__all__ = [
  "AddFuel",
  "CpuShareConfig",
  "Output",
  "OutputEvent",
  "PythonRuntime",
  "RuntimeLimits",
  "RuntimeResult",
  "RuntimeSetupError",
  "SetFuel",
  "TerminationReason",
  "Worker",
  "WorkerCallOptions",
  "WorkerStoppedError",
]


class RuntimeSetupError(Exception):
  """Raised when the packaged sandbox runtime cannot be started."""


WorkerStoppedError = _core.WorkerStoppedError


@dataclass(frozen=True, slots=True)
class CpuShareConfig:
  enabled: bool = False
  limit_percent: float | None = None
  sample_interval: float = 0.1
  activity_timeout: float = 0.3

  def __post_init__(self) -> None:
    if self.limit_percent is not None and (
      not math.isfinite(self.limit_percent) or self.limit_percent <= 0
    ):
      raise ValueError("cpu_share.limit_percent must be positive and finite")
    for name, value in (
      ("sample_interval", self.sample_interval),
      ("activity_timeout", self.activity_timeout),
    ):
      if not math.isfinite(value) or value <= 0:
        raise ValueError(f"cpu_share.{name} must be positive and finite")


class TerminationReason(StrEnum):
  COMPLETED = "completed"
  EXITED = "exited"
  GUEST_ERROR = "guest_error"
  TIMEOUT = "timeout"
  CANCELLED = "cancelled"
  FUEL_EXHAUSTED = "fuel_exhausted"
  OUTPUT_LIMIT = "output_limit"
  MEMORY_LIMIT = "memory_limit"
  RUNTIME_ERROR = "runtime_error"
  INFRASTRUCTURE_ERROR = "infrastructure_error"


@dataclass(frozen=True, slots=True)
class RuntimeLimits:
  max_memory_bytes: int = 128 * 1024 * 1024
  max_output_bytes: int = 256 * 1024
  max_guest_rpc_bytes: int = 10 * 1024 * 1024
  guest_dispatch_request_concurrency: int = 16
  guest_dispatch_request_queue_capacity: int = 64
  cpu_share_weight: int = 1
  fuel: int = 2**64 - 1
  timeout: float | None = None

  def __post_init__(self) -> None:
    if self.guest_dispatch_request_concurrency <= 0:
      raise ValueError("guest_dispatch_request_concurrency must be positive")
    if self.guest_dispatch_request_queue_capacity <= 0:
      raise ValueError(
        "guest_dispatch_request_queue_capacity must be positive",
      )
    if self.cpu_share_weight <= 0:
      raise ValueError("cpu_share_weight must be positive")


@dataclass(frozen=True, slots=True)
class SetFuel:
  fuel: int


@dataclass(frozen=True, slots=True)
class AddFuel:
  amount: int
  cap: int | None = None


type FuelOperation = SetFuel | AddFuel


@dataclass(frozen=True, slots=True)
class WorkerCallOptions:
  fuel: FuelOperation | None = None
  timeout: float | None = None


OutputEvent = _core.OutputEvent


class Output(Sequence[OutputEvent]):
  __slots__ = ("_data", "_native")

  def __init__(
    self,
    source: _core.Output | Iterable[OutputEvent] | None = None,
  ) -> None:
    if source is None or isinstance(source, _core.Output):
      self._native = source or _core.Output()
      self._data = None
    else:
      self._native = None
      self._data = list(source)

  def __len__(self) -> int:
    return self._native.len() if self._native is not None else len(self._data or ())

  @overload
  def __getitem__(self, index: int) -> OutputEvent: ...

  @overload
  def __getitem__(self, index: slice) -> "Output": ...

  def __getitem__(self, index: int | slice) -> "OutputEvent | Output":
    if isinstance(index, slice):
      if self._native is not None:
        start, stop, step = index.indices(len(self))
        return Output(self._native.get_slice(start, stop, step))
      assert self._data is not None
      return Output(self._data[index])
    if self._native is not None:
      if index < 0:
        index += len(self)
      if index < 0:
        raise IndexError("output index out of range")
      return self._native.get_item(index)
    assert self._data is not None
    return self._data[index]

  def __repr__(self) -> str:
    return repr(list(self))

  def __eq__(self, other: object) -> bool:
    if isinstance(other, Output):
      return list(self) == list(other)
    return list(self) == other

  @property
  def stdout(self) -> bytes:
    return b"".join(event.data for event in self if event.source == "stdout")

  @property
  def stderr(self) -> bytes:
    return b"".join(event.data for event in self if event.source == "stderr")

  @property
  def bytes(self) -> bytes:
    return b"".join(event.data for event in self)

  @property
  def text(self) -> str:
    return self.bytes.decode("utf-8", errors="replace")

  def formatted(
    self,
    *,
    stdout: tuple[builtins.bytes | None, builtins.bytes | None] = (None, None),
    stderr: tuple[builtins.bytes | None, builtins.bytes | None] = (None, None),
  ) -> builtins.bytes:
    affixes = {"stdout": stdout, "stderr": stderr}
    data = bytearray()
    current_source: str | None = None

    for event in self:
      if event.source != current_source:
        if current_source is not None:
          after = affixes[current_source][1]
          if after is not None:
            data.extend(after)
        before = affixes[event.source][0]
        if before is not None:
          data.extend(before)
        current_source = event.source
      data.extend(event.data)

    if current_source is not None:
      after = affixes[current_source][1]
      if after is not None:
        data.extend(after)
    return bytes(data)

  def formatted_text(
    self,
    *,
    stdout: tuple[builtins.bytes | None, builtins.bytes | None] = (None, None),
    stderr: tuple[builtins.bytes | None, builtins.bytes | None] = (None, None),
  ) -> str:
    return self.formatted(stdout=stdout, stderr=stderr).decode(
      "utf-8",
      errors="replace",
    )


@dataclass(slots=True)
class RuntimeResult:
  output: Output = field(default_factory=Output)
  error: str | None = None
  reason: TerminationReason | None = None
  exit_code: int | None = None


class PythonRuntime:
  def __init__(
    self,
    *,
    max_ipc_frame_bytes: int = 50 * 1024 * 1024,
    worker_queue_capacity: int = 256,
    host_dispatch_concurrency: int = 64,
    host_dispatch_queue_capacity: int = 256,
    cpu_share: CpuShareConfig | None = None,
    vfs: VirtualFileSystem | None = None,
    cache_vfs: bool = False,
    cache_vfs_negative: bool = False,
    package_cache: PackageCache | None = DEFAULT_PACKAGE_CACHE,
    package_index: PackageIndex = DEFAULT_PACKAGE_INDEX,
    compilation_cache: bool | Path = True,
  ) -> None:
    if max_ipc_frame_bytes <= 0:
      raise ValueError("max_ipc_frame_bytes must be positive")
    if worker_queue_capacity <= 0:
      raise ValueError("worker_queue_capacity must be positive")
    if host_dispatch_concurrency <= 0:
      raise ValueError("host_dispatch_concurrency must be positive")
    if host_dispatch_queue_capacity <= 0:
      raise ValueError("host_dispatch_queue_capacity must be positive")
    self.max_ipc_frame_bytes = max_ipc_frame_bytes
    self.worker_queue_capacity = worker_queue_capacity
    self.host_dispatch_concurrency = host_dispatch_concurrency
    self.host_dispatch_queue_capacity = host_dispatch_queue_capacity
    self.cpu_share = cpu_share or CpuShareConfig()
    self.vfs = vfs
    self.cache_vfs = cache_vfs
    self.cache_vfs_negative = cache_vfs_negative
    self.compilation_cache = compilation_cache
    self.packages = PackageManager(cache=package_cache, index=package_index)
    self.rpc = RpcHost(self._register_handler)
    self._sandbox: _core.SandboxProcess | None = None
    self._start_lock: asyncio.Lock | None = None
    self._socket_directory: tempfile.TemporaryDirectory[str] | None = None
    self._worker_ids = itertools.count(1)

  @property
  def is_open(self) -> bool:
    return self._sandbox is not None and not self._sandbox.closed

  async def execute(
    self,
    program: str,
    *,
    limits: RuntimeLimits | None = None,
    rpc_methods: Collection[str] | None = None,
    spin: bool = False,
    spin_concurrent: bool = True,
    packages: PackageEnvironment | None = None,
  ) -> RuntimeResult:
    worker = self._run(
      program,
      limits=limits,
      rpc_methods=rpc_methods,
      spin=spin,
      spin_concurrent=spin_concurrent,
      packages=packages,
    )
    return await worker.task

  def run(
    self,
    program: str,
    *,
    limits: RuntimeLimits | None = None,
    rpc_methods: Collection[str] | None = None,
    spin: bool = True,
    spin_concurrent: bool = True,
    packages: PackageEnvironment | None = None,
  ) -> "Worker":
    return self._run(
      program,
      limits=limits,
      rpc_methods=rpc_methods,
      spin=spin,
      spin_concurrent=spin_concurrent,
      packages=packages,
    )

  async def close(self) -> None:
    sandbox, self._sandbox = self._sandbox, None
    try:
      if sandbox is not None:
        await sandbox.close()
    finally:
      self._cleanup_socket_directory()

  async def reopen(self) -> None:
    await self.close()
    await self._get_sandbox()

  async def invalidate_vfs(self, path: str | None = None) -> None:
    if not self.cache_vfs:
      return
    await (await self._get_sandbox()).invalidate_vfs(path)

  async def __aenter__(self) -> Self:
    await self._get_sandbox()
    return self

  async def __aexit__(
    self,
    exc_type: type[BaseException] | None,
    exc: BaseException | None,
    traceback: object,
  ) -> None:
    await self.close()

  def _run(
    self,
    program: str,
    *,
    limits: RuntimeLimits | None,
    rpc_methods: Collection[str] | None,
    spin: bool,
    spin_concurrent: bool,
    packages: PackageEnvironment | None,
  ) -> "Worker":
    asyncio.get_running_loop()
    selected_rpc_methods = self._select_rpc_methods(rpc_methods)
    return Worker(
      runtime=self,
      worker_id=next(self._worker_ids),
      program=self._prepare_program(
        program,
        rpc_methods=selected_rpc_methods,
        spin=spin,
        spin_concurrent=spin_concurrent,
      ),
      packages=packages,
      limits=limits or RuntimeLimits(),
      rpc_methods=selected_rpc_methods,
    )

  async def _get_sandbox(self) -> _core.SandboxProcess:
    if self._sandbox is not None and not self._sandbox.closed:
      return self._sandbox
    if self._start_lock is None:
      self._start_lock = asyncio.Lock()

    async with self._start_lock:
      if self._sandbox is not None and not self._sandbox.closed:
        return self._sandbox
      stale, self._sandbox = self._sandbox, None
      if stale is not None:
        with suppress(Exception):
          await stale.close()
        self._cleanup_socket_directory()
      component, python_root = component_paths()
      self._socket_directory = tempfile.TemporaryDirectory(
        prefix="pysandbox-",
      )
      socket_name = socket_path(self._socket_directory.name)
      try:
        sandbox = await _core.start_sandbox(
          sys.executable,
          socket_name,
          component,
          python_root,
          executable_arguments=["-m", "pysandbox._sandboxd"],
          max_ipc_frame_bytes=self.max_ipc_frame_bytes,
          worker_queue_capacity=self.worker_queue_capacity,
          compilation_cache=self._compilation_cache_path(),
          host_dispatch_concurrency=self.host_dispatch_concurrency,
          host_dispatch_queue_capacity=self.host_dispatch_queue_capacity,
          cache_vfs=self.cache_vfs,
          cache_vfs_negative=self.cache_vfs_negative,
          cpu_share_enabled=self.cpu_share.enabled,
          cpu_share_limit_percent=self.cpu_share.limit_percent,
          cpu_share_sample_interval_ms=max(
            1,
            round(self.cpu_share.sample_interval * 1_000),
          ),
          cpu_share_activity_timeout_ms=max(
            1,
            round(self.cpu_share.activity_timeout * 1_000),
          ),
        )
      except BaseException as error:
        self._socket_directory.cleanup()
        self._socket_directory = None
        raise RuntimeSetupError(str(error)) from error

      for method, handler in self.rpc.handlers():
        sandbox.expose(method, handler)
      if self.vfs is not None:
        sandbox.set_vfs(self.vfs)
      self._sandbox = sandbox
      return sandbox

  def _compilation_cache_path(self) -> Path | None:
    if self.compilation_cache is True:
      return user_cache_path("pysandbox") / "wasmtime"
    if self.compilation_cache is False:
      return None
    return self.compilation_cache.resolve()

  def _cleanup_socket_directory(self) -> None:
    if self._socket_directory is not None:
      self._socket_directory.cleanup()
      self._socket_directory = None

  def _register_handler(
    self,
    method: str,
    handler: Callable[..., object],
  ) -> None:
    if self._sandbox is not None:
      self._sandbox.expose(method, handler)

  def _prepare_program(
    self,
    program: str,
    *,
    rpc_methods: tuple[str, ...],
    spin: bool,
    spin_concurrent: bool,
  ) -> str:
    proxies = [
      (
        f"async def {method}(*args, **kwargs):\n"
        f"  return await call({method!r}, *args, **kwargs)\n"
      )
      for method in rpc_methods
      if method.isidentifier() and not keyword.iskeyword(method)
    ]
    source = "".join(proxies) + program
    if spin:
      source += f"\nawait spin(concurrent={spin_concurrent!r})\n"
    return source

  def _select_rpc_methods(
    self,
    rpc_methods: Collection[str] | None,
  ) -> tuple[str, ...]:
    available = self.rpc.methods
    if rpc_methods is None:
      return available
    selected = tuple(dict.fromkeys(rpc_methods))
    unknown = set(selected).difference(available)
    if unknown:
      names = ", ".join(sorted(unknown))
      raise ValueError(f"unknown RPC methods: {names}")
    return selected


class Worker:
  def __init__(
    self,
    *,
    runtime: PythonRuntime,
    worker_id: int,
    program: str,
    limits: RuntimeLimits,
    rpc_methods: tuple[str, ...],
    packages: PackageEnvironment | None,
  ) -> None:
    self.runtime = runtime
    self.worker_id = worker_id
    self.result = RuntimeResult()
    self._program = program
    self._limits = limits
    self._rpc_methods = rpc_methods
    self._packages = packages
    self._execution_ready: asyncio.Future[None] = (
      asyncio.get_running_loop().create_future()
    )
    self._execution: _core.Execution | None = None
    self.task = asyncio.create_task(
      self._execute(),
      name=f"pysandbox-worker-{worker_id}",
    )
    self.task.add_done_callback(self._task_done)

  def _task_done(self, _task: asyncio.Task[RuntimeResult]) -> None:
    if not self._execution_ready.done():
      self._execution_ready.set_result(None)

  async def _wait_for_execution(self) -> _core.Execution:
    await self._execution_ready
    if self._execution is not None:
      return self._execution
    await self.task
    raise WorkerStoppedError("worker execution stopped before starting")

  async def call(
    self,
    path: tuple[str, ...],
    options: WorkerCallOptions | None,
    /,
    *args: object,
    **kwargs: object,
  ) -> object:
    if not path:
      raise ValueError("worker call path must not be empty")
    return await self._call(path, options, *args, **kwargs)

  async def _call(
    self,
    path: tuple[str, ...],
    options: WorkerCallOptions | None,
    /,
    *args: object,
    **kwargs: object,
  ) -> object:
    if options is not None and options.timeout is not None and options.timeout <= 0:
      raise ValueError("worker call timeout must be positive")
    if self.task.done():
      reason = self.result.reason
      raise WorkerStoppedError(
        "worker execution has stopped"
        + (f" ({reason.value})" if reason is not None else "")
      )
    execution = await self._wait_for_execution()
    fuel = native_fuel_operation(options.fuel if options is not None else None)
    call = execution.call(path, fuel, *args, **kwargs)
    if options is None or options.timeout is None:
      return await call
    async with asyncio.timeout(options.timeout):
      return await call

  async def call_function(
    self,
    source: str,
    fuel: SetFuel | AddFuel | None,
    /,
    **kwargs: object,
  ) -> object:
    options = WorkerCallOptions(fuel=fuel) if fuel is not None else None
    return await self._call((), options, source, **kwargs)

  async def set_fuel(self, fuel: int) -> None:
    await (await self._wait_for_execution()).set_fuel(fuel)

  async def add_fuel(self, amount: int, *, cap: int | None = None) -> None:
    await (await self._wait_for_execution()).add_fuel(amount, cap=cap)

  async def set_limits(
    self,
    *,
    max_memory_bytes: int | None = None,
    max_output_bytes: int | None = None,
    max_guest_rpc_bytes: int | None = None,
    cpu_share_weight: int | None = None,
    timeout: float | None = None,
  ) -> None:
    if cpu_share_weight is not None and cpu_share_weight <= 0:
      raise ValueError("cpu_share_weight must be positive")
    await (await self._wait_for_execution()).set_limits(
      max_memory_bytes=max_memory_bytes,
      max_output_bytes=max_output_bytes,
      max_guest_rpc_bytes=max_guest_rpc_bytes,
      cpu_share_weight=cpu_share_weight,
      timeout=timeout,
    )

  async def cancel(self) -> None:
    if self.task.done():
      return
    if self._execution is None:
      self.task.cancel()
      return
    await self._execution.cancel()

  async def close(self) -> None:
    with suppress(Exception):
      await self.cancel()
    try:
      await asyncio.shield(self.task)
    except asyncio.CancelledError:
      if self.task.cancelled():
        return
      raise
    except Exception:
      return

  async def _execute(self) -> RuntimeResult:
    sandbox = await self.runtime._get_sandbox()
    execution = sandbox.run(
      self._program,
      worker_id=self.worker_id,
      rpc_methods=list(self._rpc_methods),
      package_paths=(
        [str(path) for path in self._packages.paths]
        if self._packages is not None
        else []
      ),
      max_memory_bytes=self._limits.max_memory_bytes,
      max_output_bytes=self._limits.max_output_bytes,
      max_guest_rpc_bytes=self._limits.max_guest_rpc_bytes,
      guest_dispatch_request_concurrency=(
        self._limits.guest_dispatch_request_concurrency
      ),
      guest_dispatch_request_queue_capacity=(
        self._limits.guest_dispatch_request_queue_capacity
      ),
      cpu_share_weight=self._limits.cpu_share_weight,
      fuel=self._limits.fuel,
      timeout=self._limits.timeout,
    )
    self._execution = execution
    self.result.output._native = execution.output
    self._execution_ready.set_result(None)
    try:
      native_result = await execution.result()
      self.result.error = native_result.error
      self.result.reason = TerminationReason(native_result.reason)
      self.result.exit_code = native_result.exit_code
      return self.result
    finally:
      with suppress(Exception):
        await sandbox.close_worker(self.worker_id)


def native_fuel_operation(
  operation: FuelOperation | None,
) -> tuple[str, int, int | None] | None:
  if isinstance(operation, SetFuel):
    return ("set", operation.fuel, None)
  if isinstance(operation, AddFuel):
    return ("add", operation.amount, operation.cap)
  return None


def component_paths() -> tuple[Path, Path]:
  packaged = files("pysandbox").joinpath("_runtime")
  component = Path(str(packaged.joinpath("pysandbox.wasm")))
  runtime = Path(str(packaged.joinpath("runtime")))
  if component.is_file() and runtime.is_dir():
    return component, runtime

  project_component = Path(__file__).parents[1] / "component"
  component = project_component / "pysandbox.wasm"
  runtime = project_component / "runtime"
  if component.is_file() and runtime.is_dir():
    return component, runtime
  raise RuntimeSetupError("packaged Python component is missing")


def socket_path(directory: str) -> str:
  if sys.platform in {"linux", "win32"}:
    return f"pysandbox-{Path(directory).name}"
  return str(Path(directory) / "sandbox.sock")
