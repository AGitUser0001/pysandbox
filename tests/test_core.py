import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path

from pysandbox import _core


class CoreTests(unittest.IsolatedAsyncioTestCase):
  def test_protocol_version(self) -> None:
    self.assertEqual(_core.protocol_version(), 1)

  async def test_tokio_awaitable(self) -> None:
    self.assertIsNone(await _core.sleep(0))

  async def test_sandbox_process_health_and_shutdown(self) -> None:
    project_root = Path(__file__).parents[1]

    with tempfile.TemporaryDirectory() as directory:
      if sys.platform in {"linux", "win32"}:
        socket_name = f"pysandbox-test-{os.getpid()}"
      else:
        socket_name = str(Path(directory) / "sandbox.sock")

      sandbox = await _core.start_sandbox(
        sys.executable,
        socket_name,
        project_root / "pysandbox" / "_runtime" / "pysandbox.wasm",
        project_root / "vendor" / "cpython" / "Lib",
        executable_arguments=["-m", "pysandbox._sandboxd"],
        worker_queue_capacity=1,
      )
      self.assertIsNone(await sandbox.health())
      with self.assertRaisesRegex(ValueError, "timeout must be"):
        sandbox.run("pass", timeout=0)

      def add(a: int, b: int) -> int:
        return a + b

      async def multiply(a: int, b: int) -> int:
        await asyncio.sleep(0)
        return a * b

      queue_gate = asyncio.Event()

      async def wait_for_queue_gate() -> None:
        await queue_gate.wait()

      sandbox.expose("add", add)
      sandbox.expose("multiply", multiply)
      sandbox.expose("wait_for_queue_gate", wait_for_queue_gate)
      rpc_result = await sandbox.execute(
        'print(await call("add", 2, b=5), flush=True)\n'
        'print(await call("multiply", 3, 4), flush=True)'
      )
      self.assertIsNone(rpc_result.error)
      self.assertEqual(rpc_result.stdout, b"7\n12\n")

      limited_rpc = await sandbox.execute(
        'await call("add", 2, 5)',
        max_guest_rpc_bytes=1,
      )
      self.assertIsNotNone(limited_rpc.error)
      self.assertIn(
        "guest RPC payload exceeded 1 bytes",
        limited_rpc.error or "",
      )

      result = await sandbox.execute(
        "value = globals().get('value', 0) + 1\nprint(value, flush=True)"
      )
      self.assertIsNone(result.error)
      self.assertEqual(result.stdout, b"1\n")
      self.assertEqual(
        [(event.source, event.data) for event in result.output],
        [("stdout", b"1\n")],
      )

      result = await sandbox.execute("value += 1\nprint(value, flush=True)")
      self.assertIsNone(result.error)
      self.assertEqual(result.stdout, b"2\n")

      isolated = await sandbox.execute(
        "print(globals().get('value', 'missing'), flush=True)",
        worker_id=1,
      )
      self.assertIsNone(isolated.error)
      self.assertEqual(isolated.stdout, b"missing\n")

      result = await sandbox.execute("value += 1\nprint(value, flush=True)")
      self.assertIsNone(result.error)
      self.assertEqual(result.stdout, b"3\n")

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
      )
      for _ in range(1_000):
        if b"".join(event.data for event in execution.output) == b"spin-ready\n":
          break
        await _core.sleep(10)
      self.assertEqual(
        [(event.source, event.data) for event in execution.output],
        [("stdout", b"spin-ready\n")],
      )
      self.assertEqual(await execution.call(("scale",), None, 5, factor=3), 24)
      self.assertEqual(await execution.call(("async_scale",), None, 7), 10)
      await execution.set_limits(max_guest_rpc_bytes=1)
      with self.assertRaisesRegex(RuntimeError, "guest RPC payload exceeded 1 bytes"):
        await execution.call(("large_result",), None)
      await execution.add_fuel(0, cap=1)
      exhausted = await asyncio.wait_for(execution.result(), timeout=2)
      self.assertIsNotNone(exhausted.error)

      execution = sandbox.run(
        'print("busy-ready", flush=True)\nwhile True:\n  pass',
        worker_id=1,
      )
      for _ in range(1_000):
        if b"".join(event.data for event in execution.output) == b"busy-ready\n":
          break
        await _core.sleep(10)
      self.assertEqual(
        [(event.source, event.data) for event in execution.output],
        [("stdout", b"busy-ready\n")],
      )
      await execution.set_fuel(1)
      exhausted = await asyncio.wait_for(execution.result(), timeout=2)
      self.assertIsNotNone(exhausted.error)

      timed_out = await sandbox.execute(
        "while True:\n  pass",
        worker_id=2,
        timeout=0.05,
      )
      self.assertIsNotNone(timed_out.error)

      busy = sandbox.run(
        'print("queue-ready", flush=True)\nawait call("wait_for_queue_gate")',
        worker_id=3,
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
      self.assertEqual(overloaded.reason, "infrastructure_error")
      self.assertEqual(overloaded.error, "worker command queue is full")

      queue_gate.set()
      busy_result = await asyncio.wait_for(busy.result(), timeout=2)
      self.assertIsNone(busy_result.error)
      queued_result = await asyncio.wait_for(queued.result(), timeout=2)
      self.assertIsNone(queued_result.error)
      self.assertEqual(queued_result.stdout, b"queued\n")

      self.assertIsNone(await sandbox.close())
