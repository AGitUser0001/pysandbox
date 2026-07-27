import asyncio
import unittest

from pysandbox import (
  AddFuel,
  Output,
  PythonRuntime,
  RuntimeExecutionError,
  RuntimeLimits,
  WorkerCallOptions,
)


class FacadeTests(unittest.IsolatedAsyncioTestCase):
  async def test_execution_rpc_worker_and_output(self) -> None:
    runtime = PythonRuntime()

    @runtime.rpc.expose
    def add(a: int, b: int) -> int:
      return a + b

    @runtime.rpc.expose("host_upper")
    async def upper(value: str) -> str:
      await asyncio.sleep(0)
      return value.upper()

    @runtime.rpc.expose
    async def host_wait(delay: float) -> str:
      await asyncio.sleep(delay)
      return "finished"

    try:
      result = await runtime.execute(
        'print(await add(2, 5), flush=True)\n'
        'print(await host_upper("hello"), flush=True)',
      )
      self.assertIsNone(result.error)
      self.assertEqual(result.stdout, b"7\nHELLO\n")
      self.assertEqual(result.text, "7\nHELLO\n")
      self.assertIsInstance(result.output[:1], Output)

      failed = await runtime.execute("raise ValueError('guest failure')")
      self.assertIn("ValueError: guest failure", failed.error or "")
      with self.assertRaises(RuntimeExecutionError):
        failed.raise_for_error()

      worker = runtime.run(
        "value = 10\n"
        "def increment(amount=1):\n"
        "  global value\n"
        "  value += amount\n"
        "  return value\n"
        "async def add_on_host(amount):\n"
        "  return await add(value, amount)\n"
        "async def slow_call(delay):\n"
        "  return await host_wait(delay)\n"
        'print("ready", flush=True)',
        limits=RuntimeLimits(timeout=30),
      )
      for _ in range(1_000):
        if worker.output.stdout == b"ready\n":
          break
        await asyncio.sleep(0.01)
      self.assertEqual(worker.output.stdout, b"ready\n")
      self.assertEqual(await worker.call(("increment",), None, 5), 15)
      self.assertEqual(await worker.call(("increment",), None, amount=2), 17)
      call_options = WorkerCallOptions(
        fuel=AddFuel(1_000, cap=2**64 - 1),
        timeout=5,
      )
      self.assertEqual(
        await worker.call(("add_on_host",), call_options, 3),
        20,
      )
      with self.assertRaises(TimeoutError):
        await worker.call(
          ("slow_call",),
          WorkerCallOptions(timeout=0.01),
          0.1,
        )
      await asyncio.sleep(0.15)
      self.assertEqual(await worker.call(("increment",), None), 18)
      await worker.set_limits(max_guest_rpc_bytes=1024)
      await worker.add_fuel(1_000, cap=2**64 - 1)
      await worker.close()
      self.assertTrue(worker.task.done())
    finally:
      await runtime.close()
