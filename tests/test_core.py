import asyncio
import os
import sys
import tempfile
from pathlib import Path

import pytest

from pysandbox import _core
from pysandbox.rpc import RpcContext
from pysandbox.runtime import TerminationReason, component_paths


class TestCore:
  def test_protocol_version(self) -> None:
    assert _core.protocol_version() == 5

  async def test_tokio_awaitable(self) -> None:
    assert await _core.sleep(0) is None

  async def test_sandbox_process_health_and_shutdown(self) -> None:
    component, python_root = component_paths()

    with tempfile.TemporaryDirectory() as directory:
      if sys.platform in {"linux", "win32"}:
        socket_name = f"pysandbox-test-{os.getpid()}"
      else:
        socket_name = str(Path(directory) / "sandbox.sock")

      sandbox = await _core.start_sandbox(
        sys.executable,
        socket_name,
        component,
        python_root,
        executable_arguments=["-m", "pysandbox._sandboxd"],
        worker_queue_capacity=1,
      )
      assert await sandbox.health() is None
      with pytest.raises(ValueError, match="timeout must be"):
        sandbox.run("pass", timeout=0)
      with pytest.raises(ValueError, match="cpu_share_weight"):
        sandbox.run("pass", cpu_share_weight=0)

      def add(context: RpcContext, /, a: int, b: int) -> int:
        return a + b

      async def multiply(context: RpcContext, /, a: int, b: int) -> int:
        await asyncio.sleep(0)
        return a * b

      queue_gate = asyncio.Event()

      async def wait_for_queue_gate(context: RpcContext, /) -> None:
        await queue_gate.wait()

      sandbox.expose("add", add)
      sandbox.expose("multiply", multiply)
      sandbox.expose("wait_for_queue_gate", wait_for_queue_gate)
      rpc_result = await sandbox.execute(
        'print(await call("add", 2, b=5), flush=True)\n'
        'print(await call("multiply", 3, 4), flush=True)',
        rpc_methods=["add", "multiply"],
      )
      assert rpc_result.error is None
      assert rpc_result.stdout == b"7\n12\n"

      limited_rpc = await sandbox.execute(
        'await call("add", 2, 5)',
        rpc_methods=["add"],
        max_guest_rpc_bytes=1,
      )
      assert limited_rpc.error is not None
      assert "guest RPC payload exceeded 1 bytes" in (limited_rpc.error or "")

      result = await sandbox.execute(
        "value = globals().get('value', 0) + 1\nprint(value, flush=True)"
      )
      assert result.error is None
      assert result.stdout == b"1\n"
      assert [(event.source, event.data) for event in result.output] == [
        ("stdout", b"1\n")
      ]

      result = await sandbox.execute("value += 1\nprint(value, flush=True)")
      assert result.error is None
      assert result.stdout == b"2\n"

      isolated = await sandbox.execute(
        "print(globals().get('value', 'missing'), flush=True)",
        worker_id=1,
      )
      assert isolated.error is None
      assert isolated.stdout == b"missing\n"

      result = await sandbox.execute("value += 1\nprint(value, flush=True)")
      assert result.error is None
      assert result.stdout == b"3\n"

      execution = sandbox.run(
        "def scale(number, factor=2):\n"
        "  return (value + number) * factor\n"
        "async def async_scale(number):\n"
        "  await call('multiply', 1, 1)\n"
        "  return value + number\n"
        "def large_result():\n"
        "  return 'too large'\n"
        'print("spin-ready", flush=True)\n'
        "await spin(concurrent=True)",
        worker_id=0,
        rpc_methods=["multiply"],
      )
      for _ in range(1_000):
        if b"".join(event.data for event in execution.output) == b"spin-ready\n":
          break
        await _core.sleep(10)
      assert [(event.source, event.data) for event in execution.output] == [
        ("stdout", b"spin-ready\n")
      ]
      assert await execution.call(("scale",), None, 5, factor=3) == 24
      assert await execution.call(("async_scale",), None, 7) == 10
      await execution.set_limits(max_guest_rpc_bytes=1)
      with pytest.raises(RuntimeError, match="guest RPC payload exceeded 1 bytes"):
        await execution.call(("large_result",), None)
      await execution.add_fuel(0, cap=1)
      exhausted = await asyncio.wait_for(execution.result(), timeout=2)
      assert exhausted.error is not None

      execution = sandbox.run(
        'print("busy-ready", flush=True)\nwhile True:\n  pass',
        worker_id=1,
      )
      for _ in range(1_000):
        if b"".join(event.data for event in execution.output) == b"busy-ready\n":
          break
        await _core.sleep(10)
      assert [(event.source, event.data) for event in execution.output] == [
        ("stdout", b"busy-ready\n")
      ]
      await execution.set_fuel(1)
      exhausted = await asyncio.wait_for(execution.result(), timeout=2)
      assert exhausted.error is not None

      timed_out = await sandbox.execute(
        "while True:\n  pass",
        worker_id=2,
        timeout=0.05,
      )
      assert timed_out.error is not None

      busy = sandbox.run(
        'print("queue-ready", flush=True)\nawait call("wait_for_queue_gate")',
        worker_id=3,
        rpc_methods=["wait_for_queue_gate"],
      )
      for _ in range(1_000):
        if b"".join(event.data for event in busy.output) == b"queue-ready\n":
          break
        await _core.sleep(10)
      queued = sandbox.run("print('queued', flush=True)", worker_id=3)
      await _core.sleep(50)
      overloaded = await asyncio.wait_for(
        sandbox.execute("pass", worker_id=3),
        timeout=2,
      )
      assert overloaded.reason == TerminationReason.INFRASTRUCTURE_ERROR.value
      assert overloaded.error == "worker command queue is full"

      queue_gate.set()
      busy_result = await asyncio.wait_for(busy.result(), timeout=2)
      assert busy_result.error is None
      queued_result = await asyncio.wait_for(queued.result(), timeout=2)
      assert queued_result.error is None
      assert queued_result.stdout == b"queued\n"

      assert await sandbox.close() is None
