import sys
from collections.abc import Mapping
from typing import Any

from sandbox.python import PythonRuntime
from sandbox.runtime import RuntimeLimits


def add(params: Mapping[str, Any]) -> int:
    return int(params["a"]) + int(params["b"])


def main() -> None:
    source = sys.stdin.read()
    if not source.strip():
        source = "\n".join(
            [
                "print('hello from WASI Python')",
                "print('2 + 5 =', add(a=2, b=5))",
            ]
        )

    runtime = PythonRuntime(limits=RuntimeLimits(
        fuel=10_000_000_000,
        replenish_fuel_interval=30
    ))
    runtime.rpc.expose("add", add)

    result = runtime.execute(source, timeout=30)

    print(f"exit code: {result.exit_code}")
    print(
        result.formatted_text(
            stdout=(b"[stdout] ", None),
            stderr=(b"[stderr] ", None),
        ),
        end="",
    )


if __name__ == "__main__":
    main()
