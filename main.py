import sys

from . import PythonRuntime, RuntimeLimits

runtime = PythonRuntime(
    limits=RuntimeLimits(
        fuel=10_000_000_000,
        replenish_fuel_interval=30
    )
)

@runtime.rpc.expose
def add(a: int, b: int) -> int:
    return int(a) + int(b)

def main() -> None:
    source = sys.stdin.read()
    if not source.strip():
        source = "\n".join(
            [
                "print('hello from WASI Python')",
                "print('2 + 5 =', add(a=2, b=5))",
            ]
        )

    result = runtime.execute(source, timeout=30)

    print(f"exit code: {result.exit_code}")
    print(
        result.formatted_text(
            stderr=(b"\x1b[31m", b"\x1b[0m"),
        ),
        end="",
    )


if __name__ == "__main__":
    main()
