import asyncio
import builtins
import itertools
import keyword
import sys
import tempfile
from collections import UserList
from collections.abc import Callable, Collection
from contextlib import suppress
from dataclasses import dataclass, field
from enum import StrEnum
from importlib.resources import files
from pathlib import Path
from typing import Self

from . import _core
from .packages import PackageCache, PackageEnvironment, PackageManager
from .rpc import RpcHost
from .vfs import VirtualFileSystem

__all__ = [
  "AddFuel",
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


class TerminationReason(StrEnum):
  COMPLETED = "completed"
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
  fuel: int = 2**64 - 1
  timeout: float | None = None

  def __post_init__(self) -> None:
    if self.guest_dispatch_request_concurrency <= 0:
      raise ValueError("guest_dispatch_request_concurrency must be positive")
    if self.guest_dispatch_request_queue_capacity <= 0:
      raise ValueError(
        "guest_dispatch_request_queue_capacity must be positive",
      )


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


@dataclass(frozen=True, slots=True)
class OutputEvent:
  source: str
  data: bytes


class Output(UserList[OutputEvent]):
  @property
  def stdout(self) -> bytes:
    return b"".join(event.data for event in self.data if event.source == "stdout")

  @property
  def stderr(self) -> bytes:
    return b"".join(event.data for event in self.data if event.source == "stderr")

  @property
  def bytes(self) -> bytes:
    return b"".join(event.data for event in self.data)

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

    for event in self.data:
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
  reason: TerminationReason = TerminationReason.COMPLETED

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


class PythonRuntime:
  def __init__(
    self,
    *,
    max_ipc_frame_bytes: int = 50 * 1024 * 1024,
    worker_queue_capacity: int = 256,
    host_dispatch_concurrency: int = 64,
    host_dispatch_queue_capacity: int = 256,
    vfs: VirtualFileSystem | None = None,
    cache_vfs: bool = False,
    cache_vfs_negative: bool = False,
    package_cache: PackageCache | None = None,
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
    self.vfs = vfs
    self.cache_vfs = cache_vfs
    self.cache_vfs_negative = cache_vfs_negative
    self.packages = PackageManager(cache=package_cache)
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
          host_dispatch_concurrency=self.host_dispatch_concurrency,
          host_dispatch_queue_capacity=self.host_dispatch_queue_capacity,
          cache_vfs=self.cache_vfs,
          cache_vfs_negative=self.cache_vfs_negative,
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
    self._execution: asyncio.Future[_core.Execution] = (
      asyncio.get_running_loop().create_future()
    )
    self.task = asyncio.create_task(
      self._execute(),
      name=f"pysandbox-worker-{worker_id}",
    )

  @property
  def output(self) -> Output:
    if not self._execution.done() or self._execution.cancelled():
      return self.result.output
    with suppress(BaseException):
      return output_from_native(self._execution.result().output)
    return self.result.output

  async def call(
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
      raise WorkerStoppedError(
        f"worker execution has stopped ({self.result.reason.value})"
      )
    execution = await self._execution
    fuel = native_fuel_operation(options.fuel if options is not None else None)
    call = execution.call(path, fuel, *args, **kwargs)
    if options is None or options.timeout is None:
      return await call
    async with asyncio.timeout(options.timeout):
      return await call

  async def set_fuel(self, fuel: int) -> None:
    await (await self._execution).set_fuel(fuel)

  async def add_fuel(self, amount: int, *, cap: int | None = None) -> None:
    await (await self._execution).add_fuel(amount, cap=cap)

  async def set_limits(
    self,
    *,
    max_memory_bytes: int | None = None,
    max_output_bytes: int | None = None,
    max_guest_rpc_bytes: int | None = None,
    timeout: float | None = None,
  ) -> None:
    await (await self._execution).set_limits(
      max_memory_bytes=max_memory_bytes,
      max_output_bytes=max_output_bytes,
      max_guest_rpc_bytes=max_guest_rpc_bytes,
      timeout=timeout,
    )

  async def cancel(self) -> None:
    if self._execution.done() and not self._execution.cancelled():
      await self._execution.result().cancel()

  async def close(self) -> None:
    if not self._execution.done():
      self.task.cancel()
    else:
      sandbox = await self.runtime._get_sandbox()
      with suppress(Exception):
        await sandbox.close_worker(self.worker_id)
    with suppress(asyncio.CancelledError, Exception):
      await self.task

  async def _execute(self) -> RuntimeResult:
    try:
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
        fuel=self._limits.fuel,
        timeout=self._limits.timeout,
      )
      self._execution.set_result(execution)
      try:
        native_result = await execution.result()
        self.result.output = output_from_native(native_result.output)
        self.result.error = native_result.error
        self.result.reason = TerminationReason(native_result.reason)
        return self.result
      finally:
        with suppress(Exception):
          await sandbox.close_worker(self.worker_id)
    except BaseException as error:
      if not self._execution.done():
        if isinstance(error, asyncio.CancelledError):
          self._execution.cancel()
        else:
          self._execution.set_exception(error)
      raise


def output_from_native(events: list[_core.OutputEvent]) -> Output:
  return Output(OutputEvent(source=event.source, data=event.data) for event in events)


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
