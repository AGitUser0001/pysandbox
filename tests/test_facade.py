import asyncio
import unittest

from pysandbox import (
  AddFuel,
  Output,
  PythonRuntime,
  RuntimeExecutionError,
  RuntimeLimits,
  TerminationReason,
  WorkerCallOptions,
)


class FacadeTests(unittest.IsolatedAsyncioTestCase):
  def test_worker_queue_capacity_validation(self) -> None:
    with self.assertRaisesRegex(ValueError, "worker_queue_capacity"):
      PythonRuntime(worker_queue_capacity=0)

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
      self.assertEqual(failed.reason, TerminationReason.GUEST_ERROR)
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
      self.assertEqual(await worker.call(("increment",), None, 5), 15)
      for _ in range(1_000):
        if worker.output.stdout == b"ready\n":
          break
        await asyncio.sleep(0.01)
      self.assertEqual(worker.output.stdout, b"ready\n")
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

  async def test_crossed_worker_rpc(self) -> None:
    runtime = PythonRuntime()

    @runtime.rpc.expose
    async def host_echo(worker: str, value: int) -> tuple[str, int]:
      await asyncio.sleep(0)
      return worker, value

    first = runtime.run(
      "async def relay(value):\n  return await host_echo('first', value)\n",
    )
    second = runtime.run(
      "async def relay(value):\n  return await host_echo('second', value)\n",
    )
    try:
      calls = [
        worker.call(("relay",), None, value)
        for value in range(32)
        for worker in (first, second)
      ]
      results = await asyncio.gather(*calls)
      self.assertEqual(
        results,
        [[name, value] for value in range(32) for name in ("first", "second")],
      )
    finally:
      await asyncio.gather(first.close(), second.close())
      await runtime.close()

  async def test_subprocess_death_and_lazy_reopen(self) -> None:
    runtime = PythonRuntime()

    @runtime.rpc.expose
    async def wait_forever() -> None:
      await asyncio.Event().wait()

    worker = runtime.run("await wait_forever()")
    try:
      await asyncio.wait_for(asyncio.shield(worker._execution), timeout=5)
      sandbox = await runtime._get_sandbox()
      self.assertTrue(runtime.is_open)
      await sandbox.terminate()
      with self.assertRaises(RuntimeError):
        await asyncio.wait_for(worker.task, timeout=5)
      self.assertFalse(runtime.is_open)

      recovered = await runtime.execute("print('recovered', flush=True)")
      self.assertEqual(recovered.reason, TerminationReason.COMPLETED)
      self.assertEqual(recovered.stdout, b"recovered\n")
      self.assertTrue(runtime.is_open)
    finally:
      await runtime.close()

  async def test_structured_termination_reasons(self) -> None:
    runtime = PythonRuntime()
    try:
      timed_out = await runtime.execute(
        "while True:\n  pass",
        limits=RuntimeLimits(timeout=0.05),
      )
      self.assertEqual(timed_out.reason, TerminationReason.TIMEOUT)

      fuel = await runtime.execute(
        "while True:\n  pass",
        limits=RuntimeLimits(fuel=1),
      )
      self.assertEqual(fuel.reason, TerminationReason.FUEL_EXHAUSTED)
    finally:
      await runtime.close()
