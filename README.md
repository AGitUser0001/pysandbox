# pysandbox

[![CI](https://github.com/AGitUser0001/pysandbox/actions/workflows/CI.yml/badge.svg)](https://github.com/AGitUser0001/pysandbox/actions/workflows/CI.yml)
[![CodeQL](https://github.com/AGitUser0001/pysandbox/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/AGitUser0001/pysandbox/actions/workflows/github-code-scanning/codeql)
[![PyPI](https://img.shields.io/pypi/v/pysandbox-wasi)](https://pypi.org/project/pysandbox-wasi/)
[![License](https://img.shields.io/pypi/l/pysandbox-wasi)](LICENSE.md)
[![Python](https://img.shields.io/pypi/pyversions/pysandbox-wasi)](https://pypi.org/project/pysandbox-wasi/)
[![Release](https://img.shields.io/github/v/release/AGitUser0001/pysandbox)](https://github.com/AGitUser0001/pysandbox/releases)

An asynchronous Wasmtime-backed Python sandbox with two-way RPC.

## Installation

Install a prebuilt wheel from PyPI:

```sh
pip install pysandbox-wasi
```

For source builds, including builds that reuse the platform-independent
component-runtime artifact, see
[Building](https://github.com/AGitUser0001/pysandbox/blob/main/CONTRIBUTING.md#building).

## Demo

```sh
uv run demo.py
```

## One-Shot Execution

```python
import asyncio

from pysandbox import PythonRuntime, RpcContext, RuntimeLimits


async def main() -> None:
  runtime = PythonRuntime()

  @runtime.rpc.expose
  def add(context: RpcContext, /, a: int, b: int) -> int:
    return a + b

  try:
    result = await runtime.execute(
      'print("2 + 5 =", await add(2, 5), flush=True)',
      limits=RuntimeLimits(timeout=30),
    )
    print(result.text, end="")
  finally:
    await runtime.close()


asyncio.run(main())
```

Exposed host handlers may be synchronous or asynchronous. Guest proxies are
asynchronous, so guest code calls them with `await`. Each handler receives an
`RpcContext` containing the worker and request IDs as its positional-only first
argument. Pass `rpc_methods={"method"}` to `execute()` or `run()` to restrict
which exposed methods that guest may call. Methods that are valid Python
identifiers receive convenience proxy functions in the guest namespace.
Non-identifier names are deliberately hidden from that namespace and must be
invoked explicitly with `await call("method/name", ...)`.

## Persistent Workers

`run()` starts a persistent guest and returns immediately:

```python
worker = runtime.run(
  """
value = 40

def increment(amount):
    global value
    value += amount
    return value
"""
)

print(await worker.call(("increment",), None, 2))
await worker.close()
```

The worker preserves its Python globals between calls. `worker.task` resolves
when the guest exits, and `worker.output` exposes output collected so far.
Calls made after the worker stops, or still pending when it closes, raise
`WorkerStoppedError`.
Worker calls are dispatched concurrently by default. Pass
`spin_concurrent=False` to process them sequentially:

```python
worker = runtime.run(program, spin_concurrent=False)
```

Both `execute()` and `run()` accept `spin`. It defaults to `False` for
one-shot execution and `True` for persistent workers.

The positional options slot controls the call without reserving guest keyword
arguments:

```python
from pysandbox import AddFuel, WorkerCallOptions

options = WorkerCallOptions(
  fuel=AddFuel(500_000, cap=2_000_000),
  timeout=10,
)
print(await worker.call(("increment",), options, 2))
```

`WorkerCallOptions.timeout` limits how long the caller waits. It does not
cancel the guest operation, which may continue executing and producing side
effects.

## Limits

Limits belong to an execution:

```python
limits = RuntimeLimits(
  max_memory_bytes=128 * 1024 * 1024,
  max_output_bytes=256 * 1024,
  max_guest_rpc_bytes=10 * 1024 * 1024,
  fuel=2**64 - 1,
  timeout=30,
)
```

Persistent workers can update limits while running:

```python
await worker.set_fuel(1_000_000)
await worker.add_fuel(500_000, cap=2_000_000)
await worker.set_limits(max_output_bytes=512 * 1024, timeout=60)
```

## CPU Sharing

Active workers share the sandbox subprocess's measured CPU capacity. Configure
the process-wide sampler when creating the runtime:

```python
from pysandbox import CpuShareConfig, PythonRuntime

runtime = PythonRuntime(
  cpu_share=CpuShareConfig(
    enabled=True,
    limit_percent=100.0,
    sample_interval=0.1,
    activity_timeout=0.3,
  ),
)
```

Each execution has an integer weight, defaulting to one:

```python
worker = runtime.run(program, limits=RuntimeLimits(cpu_share_weight=2))
await worker.set_limits(cpu_share_weight=4)
```

`limit_percent=100` caps the whole sandbox subprocess at approximately one
logical core; `200` permits two. Use `None` for weighted fairness without a
total CPU ceiling.

Weights are progressive rather than reserved. A worker consumes its first
weight unit before spilling into its second, so unused units remain available
to other active workers. CPU sharing is disabled by default. Fuel limits and
epoch interruption remain active when it is disabled.

## Output

stdout and stderr are retained as interlaced output events:

```python
print(result.stdout)
print(result.stderr)
print(result.text)
```

`formatted_text()` can mark transitions between streams:

```python
print(
  result.formatted_text(stderr=(b"\x1b[31m", b"\x1b[0m")),
  end="",
)
```

## Virtual Filesystem

`/python` is the packaged, immutable Python runtime. Other guest paths can be
served by a host VFS:

```python
from pysandbox import (
  PythonRuntime,
  VfsDirectoryEntry,
  VfsMetadata,
  VirtualFileSystem,
)


class Vfs(VirtualFileSystem):
  async def stat(self, path: str) -> VfsMetadata: ...

  async def read(self, path: str) -> bytes: ...

  async def list(self, path: str) -> list[VfsDirectoryEntry]: ...

  async def write(
    self,
    path: str,
    data: bytes,
    offset: int | None,
  ) -> None: ...


runtime = PythonRuntime(vfs=Vfs(), cache_vfs=True)
```

`VirtualFileSystem` requires `stat`, `read`, and `list`. Writable filesystems
may implement `write`, `delete`, `mkdir`, `append`, `truncate`, and `rename`.
Unsupported optional operations raise `NotImplementedError`. Native fallbacks
provide append from an offset write, truncate from a complete read and rewrite,
and non-atomic file rename from read, write, and delete. Directory rename
requires an explicit implementation.

`VfsMetadata` and `VfsDirectoryEntry` expose independent `read` and `write`
permissions. The guest receives normal filesystem errors for denied,
unsupported, missing, conflicting, and non-empty-directory operations.

Handlers may be synchronous or asynchronous. With caching enabled, results
are shared across workers until the host calls
`await runtime.invalidate_vfs(path)`. Passing no path clears the whole cache.
Successful responses are cached by default. Set `cache_vfs_negative=True` to
also cache non-I/O errors such as missing paths; overload, transport, and
malformed-response errors are never cached. Successful mutations invalidate
the affected cached paths.

`worker_queue_capacity` bounds pending executions and user-level calls for
each worker. When either queue is full, new work fails immediately without
blocking traffic for other workers:

```python
runtime = PythonRuntime(worker_queue_capacity=256)
```

`host_dispatch_concurrency` limits the combined number of guest RPC and VFS
callbacks actively dispatched into the host Python application:

```python
runtime = PythonRuntime(host_dispatch_concurrency=64)
```

`host_dispatch_queue_capacity` separately bounds pending host operations.
When that queue is full, guest RPC and VFS operations receive an immediate
overload error while connection routing remains responsive.

`guest_dispatch_request_concurrency` and
`guest_dispatch_request_queue_capacity` apply the same controls independently
to each worker. RPC and VFS requests share the worker's limits, preventing one
guest from consuming the sandbox-wide host dispatch budget. Configure them on
the execution's `RuntimeLimits`.

## Packages

Resolve pure-Python wheels into immutable, reusable package layers, then attach
the resulting environment to an execution:

```python
from pysandbox import Package

environment = await runtime.packages.resolve(
  Package("requests==2.32.5", build=False),
  cache="by_version",
)
result = await runtime.execute(
  "import requests; print(requests.__version__)",
  packages=environment,
)
```

Dependencies are included by default. Set `include_dependencies=False` to
resolve only the requested distribution. `build=False` rejects source
distributions; enabling builds runs their build backend on the trusted host.
Only compatible pure-Python wheels are accepted.

`runtime.packages.cache` exposes `add()`, `resolve()`, `remove()`, and
`packages()`. The default uses the platform cache directory returned by
`platformdirs.user_cache_path("pysandbox")`. Pass
`package_cache=PackageCache(path)` to `PythonRuntime` to choose another
location. `cache="never"` creates temporary layers owned by the returned
environment; call `environment.close()` when it is no longer in use.

## Internals

A PyO3 extension supervises a shared Rust sandbox subprocess without blocking
the application's asyncio loop. The subprocess shares its Wasmtime engine and
compiled component while giving every worker an isolated Store, component
instance, CPython state, memory, capabilities, limits, and output stream.

See [DOCS.md](https://github.com/AGitUser0001/pysandbox/blob/main/DOCS.md) for
the complete architecture and
[CONTRIBUTING.md](https://github.com/AGitUser0001/pysandbox/blob/main/CONTRIBUTING.md)
for development, testing, and build instructions.
