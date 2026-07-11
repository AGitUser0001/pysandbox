# pysandbox

A small Wasmtime-backed Python sandbox. It runs WASI CPython in an isolated worker process, captures stdout/stderr, and exposes a synchronous two-way RPC bridge.

## Interactive Demo

```sh
uv run demo.py
```

## Basic Use

```python
from pysandbox import PythonRuntime, RuntimeLimits

runtime = PythonRuntime()

@runtime.rpc.expose
def add(a: int, b: int) -> int:
    return int(a) + int(b)

result = runtime.execute(
    "print('2 + 5 =', add(2, 5))",
    limits=RuntimeLimits(
        fuel=10_000_000_000,
        replenish_fuel_interval=30,
    ),
    timeout=30,
)

print(result.exit_code)
print(result.text)
```

## RPC

Expose host functions with `runtime.rpc.expose`:

```python
@runtime.rpc.expose("sum")
def add(a: int, b: int) -> int:
    return a + b
```

Guest code can call configured methods directly because `PythonRuntime` prepends `from api import *`:

```python
print(sum(2, 5))
```

Long-lived workers can receive host calls through the same channel.

## Output

stdout and stderr are captured as interlaced output events:

```python
print(result.stdout)
print(result.stderr)
print(result.text)
```

For terminal output, `formatted_text()` can add stream-specific markers:

```python
print(
    result.formatted_text(
        stderr=(b"\x1b[31m", b"\x1b[0m"),
    ),
    end="",
)
```

## Internals

- Runtime assets are checked against their GitHub releases on setup; each extracted asset stores its selected release in `.release`.
- The guest `api`, `messaging`, and `cbor2` packages are mounted directly at guest `site-packages`; `cbor2` is stored beside the runtime under `python-wasi/cbor2`.
- After an asset update, the WASI interpreter compiles the runtime `lib` tree and the `cbor2` tree once.
- Runtime files are mounted readonly for normal execution.
- Each execution runs in a spawned `multiprocessing` worker.
- Wasmtime loads `python.wasm`; WASI preopens the runtime at `/`.
- stdin is fed from memory through the worker stdin pipe.
- stdout and stderr are captured through Wasmtime custom stream callbacks.
- RPC messages are CBOR packets framed by `messaging.Messenger`.
- The guest receives the `api`, `messaging`, and `cbor2` packages in `/lib/.../site-packages`.
- The RPC transport uses two rotating request files and two rotating response files under `/__pysandbox_rpc__`.
- Request files are writable by the guest; response files and runtime files are readonly during normal execution.
