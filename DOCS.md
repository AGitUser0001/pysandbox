# Internals

## Architecture

```text
Python asyncio application
        |
        | PyO3 awaitables
        v
pysandbox._core (Rust supervisor in the application process)
        |
        | versioned CBOR frames over one local socket
        v
pysandbox._sandboxd (shared Rust subprocess)
        |
        | one shared Engine, Linker, and compiled Component
        v
worker actors
        |
        | one Store, component instance, CPython state, and WASI context each
        v
componentized Python guest
```

The application process owns the Python facade and PyO3 supervisor. Untrusted
Python runs only in the sandbox subprocess. A `PythonRuntime` lazily starts one
subprocess and shares its expensive Wasmtime infrastructure across isolated
workers.

## Python Facade

`pysandbox.runtime.PythonRuntime` owns runtime configuration, exposed RPC
handlers, the optional VFS, package manager, local-socket directory, and native
`SandboxProcess`. `execute()` creates a worker and awaits its task. `run()`
returns the worker immediately for persistent use.

Each `Worker` has a stable worker ID, a live `RuntimeResult`, and a future for
the native execution handle. Its task owns execution lifetime. Cancelling or
closing the worker closes the corresponding daemon worker; calls made after it
stops fail with `WorkerStoppedError`.

If the shared subprocess exits, pending native requests fail and the facade
discards the closed process. A later operation starts a fresh subprocess.
Existing workers are not recoverable because their Stores and Python state died
with the old process.

## Supervisor and Subprocess

`pysandbox._core` is a PyO3 extension. It uses `pyo3-async-runtimes` to expose
Tokio futures as Python awaitables without blocking the application's asyncio
loop. It launches the packaged `_sandboxd` module with the current Python
executable, connects to its local socket, and runs one connection actor.

The daemon ignores interactive terminal interruption signals so Ctrl+C is
handled by the parent application. Normal runtime shutdown sends a protocol
`Shutdown` frame and waits for the child. Process termination remains available
if graceful shutdown cannot complete.

Local sockets map to Unix-domain sockets on Unix and named local sockets on
Windows through `interprocess`.

## IPC Protocol

`pysandbox-protocol` defines the shared wire types. A frame contains:

- protocol version
- frame kind
- worker ID
- request ID
- CBOR payload bytes

Frames are encoded as CBOR values directly on the byte stream through the Rust
`cbor2` async API. CBOR provides its own value framing, so there is no separate
application length prefix. Reads enforce the sandbox-wide maximum frame size
before allocating an unbounded message.

Frame kinds cover execution, output, bidirectional RPC, VFS operations, limit
updates, cancellation, worker closure, health checks, and subprocess shutdown.
Request IDs route concurrent responses without serializing unrelated workers.

## Component Runtime

The guest in `component/src/main.py` is built by `componentize-py` against
`component/wit/pysandbox.wit`. Its exported async `run` function obtains one
program from the host, compiles it with `PyCF_ALLOW_TOP_LEVEL_AWAIT`, and
evaluates it in a persistent `types.ModuleType("__main__")`. The module is also
installed in `sys.modules`, so importing `__main__` observes the same namespace.

The bootstrap provides two guest globals:

- `call()`, for guest-to-host RPC
- `spin()`, for queued host-to-worker calls

The Python facade prepends convenience proxy functions only for exposed RPC
method names that are valid Python identifiers. Other exposed names remain
callable explicitly through `call()`.

The component disables bytecode writes. `/python` contains the immutable
packaged CPython standard library and pure-Python cbor2. The build runs that
same componentized interpreter over the copied tree to generate compatible
bytecode before packaging it.

## Wasmtime Ownership

The daemon creates one `ComponentRuntime` containing the Wasmtime Engine,
Linker, and compiled Component. Each worker actor creates an independent:

- Store and component instance
- CPython interpreter state and `__main__` module
- linear memory and resource table
- WASI context and filesystem view
- execution limits, fuel, epoch deadline, and CPU-share state
- worker-call and control queues

The Store is actor-owned and never accessed concurrently. Sharing compiled
runtime infrastructure reduces startup cost without sharing guest state.

## Execution and Output

An `Execute` frame creates or addresses a worker actor and queues one execution.
The actor configures the Store limits, supplies the program through the WIT host
interface, and calls the component's async `run` export.

WASI stdout and stderr use custom Wasmtime streams. Writes become labelled
`Output` frames immediately and are appended to the execution's in-memory
`Output` sequence in arrival order. The configured output limit applies to the
combined byte count. A limit violation terminates the execution with
`TerminationReason.OUTPUT_LIMIT`; captured output remains available.

The final result separates guest-visible diagnostics from infrastructure
failures and carries a structured termination reason:

- completed
- guest error
- timeout
- cancelled
- fuel exhausted
- output limit
- memory limit
- runtime error
- infrastructure error

## RPC

Guest-to-host RPC serializes `(args, kwargs)` with CBOR value sharing and calls
the async WIT `host.call` import. The daemon forwards a `GuestCall` frame to the
supervisor. The supervisor selects the exposed Python handler, constructs an
`RpcContext(worker_id, request_id)`, and schedules either its synchronous result
or awaitable on the application's event loop. A `GuestResponse` resumes the
waiting component call.

Each execution receives an explicit RPC allowlist. Unknown or unselected
methods are rejected even if another execution on the shared runtime may call
them.

Host-to-worker RPC travels in the opposite direction as a `WorkerCall`. The
worker actor queues the call until guest `spin()` receives it. `spin` can process
calls sequentially or spawn guest asyncio tasks concurrently. Results return as
`WorkerResponse` frames. Caller-side timeouts stop waiting but deliberately do
not cancel a guest operation that may already have side effects.

## Concurrency and Backpressure

The native supervisor dispatches guest RPC and VFS callbacks concurrently under
one sandbox-wide semaphore. A bounded pending queue rejects overload without
blocking the central connection actor, so responses and control traffic for
other workers continue flowing.

The daemon also bounds execution and worker-call queues per worker. Per-worker
guest dispatch concurrency and queue capacity prevent one guest from consuming
the complete host-dispatch budget. Cancellation, close, and limit controls use
a separate control path so they remain available when user-work queues are full.

## Limits, Timeouts, and CPU Sharing

Limits are supplied per execution and may be updated on a persistent worker.
They cover memory, combined output, outbound guest RPC size, guest dispatch,
fuel, timeout, and CPU-share weight.

Fuel is Wasmtime's deterministic execution budget. `set_fuel()` replaces the
remaining amount; `add_fuel()` optionally caps the result. Fuel operations can
also accompany a worker call so long-lived workers can receive budget when work
arrives.

Epoch interruption provides asynchronous yield and interruption boundaries.
Timeout state uses a monotonic deadline and interrupts both ordinary guest
execution and guest waits in async host operations. Cancellation follows the
same worker control path and resolves the execution with a structured reason.

Optional CPU sharing samples subprocess CPU use and accounts active workers by
fuel consumption. Active workers receive progressive weighted shares. A worker
must consume one weight unit before spilling into another, leaving unused
capacity available to other workers. Delays occur at Wasmtime poll boundaries,
and an optional process-wide percentage cap limits approximate logical-core use.

## Filesystem and Packages

The WASI filesystem is hybrid:

- `/python` is a physical read-only packaged runtime.
- Resolved package layers overlay directories below the real Python
  `site-packages` path.
- Other guest paths are served by the optional host VFS.

`VirtualFileSystem` requires `stat`, `read`, and `list`; handlers may be
synchronous or asynchronous. Metadata and directory entries carry independent
read and write permissions. Optional `write`, `append`, `truncate`, `rename`,
`mkdir`, and `delete` handlers expose the corresponding guest operations.
Unsupported optional methods raise `NotImplementedError`.

The native layer supplies fallbacks where the implemented primitives permit:
append becomes an offset write, truncate becomes a complete read and rewrite,
and file rename becomes a non-atomic copy and delete. Directory rename requires
an explicit handler. Host exceptions are translated into the corresponding
guest filesystem errors.

Successful results may be cached across workers in a component-indexed tree.
Exact lookup walks one child map per path component. Directory invalidation
detaches the cached subtree directly, and directory rename moves that subtree
to its new parent without scanning or rewriting every descendant key.
Generation checks prevent an in-flight stale response from repopulating an
invalidated node. Errors are not cached by default; optional negative caching
covers stable non-I/O failures but never overload or malformed responses.
Successful mutations invalidate affected cached paths and parent listings.

`pysandbox.packages` resolves requirements in Python with `resolvelib`. Wheels
are validated and installed into immutable reusable layers. Source distributions
or directory sources may be built only when explicitly enabled; build backends
execute as trusted host code. Package environments pass only their layer paths
to Rust, where the final directory name becomes the overlay name. Native wheels
are rejected because guest extensions must target the componentized WASI Python
runtime. Optional package limits bound dependency count, artifact bytes, and
extracted file count for direct and transitive packages. They do not sandbox an
explicitly enabled source build.

## Build and Distribution

`pysandbox-runtime-build` copies the vendored CPython standard library, excludes
its test tree, adds pure-Python cbor2, builds the component, and compiles the
runtime tree with the componentized interpreter. Maturin's native build script
either generates those inputs or copies them from `PYSANDBOX_PREBUILT_RUNTIME`.

CI builds the platform-independent component runtime once and reuses it for
every wheel. Linux and Windows targets are cross-built on a Linux ARM runner;
both macOS targets are built on a macOS ARM runner. The resulting wheels are
then installed and tested on their native x86_64/ARM64 platforms with CPython
3.12 and free-threaded 3.14. Native wheels remain specific to every supported
CPython ABI, including free-threaded ABIs.

Publishing is an explicitly dispatched workflow operation. Its selected commit
and requested tag are validated before the matrix runs. After checks and native
wheel tests pass, the protected `release` environment uses OIDC Trusted
Publishing to upload attested artifacts to PyPI, then creates the matching Git
tag and GitHub Release and attaches the wheels and reusable component runtime.

## Trust Boundaries

The application, exposed RPC handlers, VFS implementation, package resolver,
and enabled source-package build backends are trusted. Guest Python is
untrusted and is confined to its Store, configured capabilities, resource
limits, host-authorized filesystem view, and explicit RPC allowlist.

The shared daemon improves efficiency but is still a single infrastructure
failure domain: a daemon failure stops every worker it owns. Store isolation is
the boundary between guests; subprocess isolation is the boundary between guest
runtime infrastructure and the application process.
