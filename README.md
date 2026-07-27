# pysandbox

An asynchronous Wasmtime-backed Python sandbox with two-way RPC.

## Demo

```sh
uv run demo.py
```

## One-Shot Execution

```python
import asyncio

from pysandbox import PythonRuntime, RuntimeLimits


async def main() -> None:
    runtime = PythonRuntime()

    @runtime.rpc.expose
    def add(a: int, b: int) -> int:
        return a + b

    try:
        result = await runtime.execute(
            'print("2 + 5 =", await add(2, 5), flush=True)',
            limits=RuntimeLimits(timeout=30),
        )
        result.raise_for_error()
        print(result.text, end="")
    finally:
        await runtime.close()


asyncio.run(main())
```

Exposed host handlers may be synchronous or asynchronous. Guest proxies are
asynchronous, so guest code calls them with `await`.

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

## Internals

- A PyO3 extension supervises a Rust sandbox subprocess without blocking the
  application's asyncio loop.
- The subprocess shares a Wasmtime engine and compiled component. Each worker
  owns an isolated Store and component instance.
- Guest Python uses a componentized CPython runtime and supports top-level
  `await`.
- A persistent framed local socket carries lifecycle commands, output, limit
  updates, cancellation, and bidirectional RPC.
- The packaged component and runtime modules are built with the wheel.
- Closing a one-shot execution or worker destroys its Store. Closing the
  runtime shuts down the shared sandbox subprocess.
