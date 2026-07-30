from collections.abc import Awaitable, Callable
from os import PathLike

class WorkerStoppedError(RuntimeError): ...

class RpcContext:
  @property
  def worker_id(self) -> int: ...
  @property
  def request_id(self) -> int: ...

def protocol_version() -> int: ...
def sleep(milliseconds: int) -> Awaitable[None]: ...
def run_sandboxd(
  socket_name: str,
  component_path: str | PathLike[str],
  python_root: str | PathLike[str],
  max_ipc_frame_bytes: int,
  worker_queue_capacity: int,
  cache_vfs: bool,
  cache_vfs_negative: bool,
) -> None: ...

class SandboxProcess:
  @property
  def closed(self) -> bool: ...
  def expose(self, method: str, handler: Callable[..., object]) -> None: ...
  def set_vfs(self, handler: object) -> None: ...
  def invalidate_vfs(self, path: str | None = None) -> Awaitable[None]: ...
  def health(self) -> Awaitable[None]: ...
  def run(
    self,
    program: str,
    *,
    worker_id: int = 0,
    rpc_methods: list[str] = [],
    max_memory_bytes: int = 128 * 1024 * 1024,
    max_output_bytes: int = 256 * 1024,
    max_guest_rpc_bytes: int = 10 * 1024 * 1024,
    guest_dispatch_request_concurrency: int = 16,
    guest_dispatch_request_queue_capacity: int = 64,
    fuel: int = 2**64 - 1,
    timeout: float | None = None,
  ) -> Execution: ...
  def execute(
    self,
    program: str,
    *,
    worker_id: int = 0,
    rpc_methods: list[str] = [],
    max_memory_bytes: int = 128 * 1024 * 1024,
    max_output_bytes: int = 256 * 1024,
    max_guest_rpc_bytes: int = 10 * 1024 * 1024,
    guest_dispatch_request_concurrency: int = 16,
    guest_dispatch_request_queue_capacity: int = 64,
    fuel: int = 2**64 - 1,
    timeout: float | None = None,
  ) -> Awaitable[ExecutionResult]: ...
  def close_worker(self, worker_id: int) -> Awaitable[None]: ...
  def terminate(self) -> Awaitable[None]: ...
  def close(self) -> Awaitable[None]: ...

class Execution:
  @property
  def execution_id(self) -> int: ...
  @property
  def output(self) -> list[OutputEvent]: ...
  def result(self) -> Awaitable[ExecutionResult]: ...
  def cancel(self) -> Awaitable[None]: ...
  def call(
    self,
    path: tuple[str, ...],
    fuel: tuple[str, int, int | None] | None,
    /,
    *args: object,
    **kwargs: object,
  ) -> Awaitable[object]: ...
  def set_fuel(self, fuel: int) -> Awaitable[None]: ...
  def add_fuel(self, amount: int, *, cap: int | None = None) -> Awaitable[None]: ...
  def set_limits(
    self,
    *,
    max_memory_bytes: int | None = None,
    max_output_bytes: int | None = None,
    max_guest_rpc_bytes: int | None = None,
    timeout: float | None = None,
  ) -> Awaitable[None]: ...

class OutputEvent:
  @property
  def source(self) -> str: ...
  @property
  def data(self) -> bytes: ...

class ExecutionResult:
  @property
  def error(self) -> str | None: ...
  @property
  def reason(self) -> str: ...
  @property
  def output(self) -> list[OutputEvent]: ...
  @property
  def stdout(self) -> bytes: ...
  @property
  def stderr(self) -> bytes: ...

def start_sandbox(
  executable: str | PathLike[str],
  socket_name: str,
  component_path: str | PathLike[str],
  python_root: str | PathLike[str],
  *,
  executable_arguments: list[str] = [],
  max_ipc_frame_bytes: int = 50 * 1024 * 1024,
  worker_queue_capacity: int = 256,
  host_dispatch_concurrency: int = 64,
  host_dispatch_queue_capacity: int = 256,
  cache_vfs: bool = False,
  cache_vfs_negative: bool = False,
) -> Awaitable[SandboxProcess]: ...
