import asyncio
import time
import unittest

from pysandbox import (
  AddFuel,
  CpuShareConfig,
  Output,
  PythonRuntime,
  RpcContext,
  RuntimeLimits,
  RuntimeResult,
  TerminationReason,
  WorkerCallOptions,
  WorkerStoppedError,
)

DISPATCH_TEST_TIMEOUT = 30


class FacadeTests(unittest.IsolatedAsyncioTestCase):
  def test_worker_queue_capacity_validation(self) -> None:
    with self.assertRaisesRegex(ValueError, "worker_queue_capacity"):
      PythonRuntime(worker_queue_capacity=0)
    with self.assertRaisesRegex(ValueError, "host_dispatch_concurrency"):
      PythonRuntime(host_dispatch_concurrency=0)
    with self.assertRaisesRegex(ValueError, "host_dispatch_queue_capacity"):
      PythonRuntime(host_dispatch_queue_capacity=0)
    with self.assertRaisesRegex(
      ValueError,
      "guest_dispatch_request_concurrency",
    ):
      RuntimeLimits(guest_dispatch_request_concurrency=0)
    with self.assertRaisesRegex(
      ValueError,
      "guest_dispatch_request_queue_capacity",
    ):
      RuntimeLimits(guest_dispatch_request_queue_capacity=0)
    with self.assertRaisesRegex(ValueError, "cpu_share_weight"):
      RuntimeLimits(cpu_share_weight=0)
    with self.assertRaisesRegex(ValueError, "sample_interval"):
      CpuShareConfig(sample_interval=0)
    with self.assertRaisesRegex(ValueError, "activity_timeout"):
      CpuShareConfig(activity_timeout=float("inf"))
    with self.assertRaisesRegex(ValueError, "limit_percent"):
      CpuShareConfig(enabled=True, limit_percent=0)

  async def test_execution_rpc_worker_and_output(self) -> None:
    runtime = PythonRuntime()

    @runtime.rpc.expose
    def add(context: RpcContext, /, a: int, b: int) -> int:
      return a + b

    @runtime.rpc.expose("host_upper")
    async def upper(context: RpcContext, /, value: str) -> str:
      await asyncio.sleep(0)
      return value.upper()

    @runtime.rpc.expose
    async def host_wait(context: RpcContext, /, delay: float) -> str:
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

      main_module = await runtime.execute(
        "value = 42\n"
        "import __main__\n"
        "print(__main__.value, __main__.__dict__ is globals(), flush=True)\n"
      )
      self.assertIsNone(main_module.error)
      self.assertEqual(main_module.stdout, b"42 True\n")

      failed = await runtime.execute("raise ValueError('guest failure')")
      self.assertIn("ValueError: guest failure", failed.error or "")
      self.assertEqual(failed.reason, TerminationReason.GUEST_ERROR)

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
      await worker.set_limits(cpu_share_weight=2)
      with self.assertRaisesRegex(ValueError, "cpu_share_weight"):
        await worker.set_limits(cpu_share_weight=0)
      await worker.add_fuel(1_000, cap=2**64 - 1)
      await worker.close()
      self.assertTrue(worker.task.done())
    finally:
      await runtime.close()

  async def test_crossed_worker_rpc(self) -> None:
    runtime = PythonRuntime()
    expected_worker_ids: dict[str, int] = {}
    request_ids: set[int] = set()

    @runtime.rpc.expose
    async def host_echo(
      context: RpcContext,
      /,
      worker: str,
      value: int,
    ) -> tuple[str, int]:
      self.assertEqual(context.worker_id, expected_worker_ids[worker])
      self.assertNotIn(context.request_id, request_ids)
      request_ids.add(context.request_id)
      await asyncio.sleep(0)
      return worker, value

    first = runtime.run(
      "async def relay(value):\n  return await host_echo('first', value)\n",
    )
    second = runtime.run(
      "async def relay(value):\n  return await host_echo('second', value)\n",
    )
    expected_worker_ids.update(first=first.worker_id, second=second.worker_id)
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
      self.assertEqual(len(request_ids), 64)
    finally:
      await asyncio.gather(first.close(), second.close())
      await runtime.close()

  async def test_concurrent_worker_calls(self) -> None:
    runtime = PythonRuntime()
    worker = runtime.run(
      "import asyncio\n"
      "active = 0\n"
      "maximum_active = 0\n"
      "release = asyncio.Event()\n"
      "async def held(value):\n"
      "  global active, maximum_active\n"
      "  active += 1\n"
      "  maximum_active = max(maximum_active, active)\n"
      "  try:\n"
      "    await release.wait()\n"
      "    return value\n"
      "  finally:\n"
      "    active -= 1\n"
      "def release_calls():\n"
      "  release.set()\n"
      "def observed_concurrency():\n"
      "  return maximum_active\n",
      spin_concurrent=True,
    )
    first: asyncio.Task[object] | None = None
    second: asyncio.Task[object] | None = None
    try:
      self.assertEqual(await worker.call(("observed_concurrency",), None), 0)
      first = asyncio.create_task(worker.call(("held",), None, "first"))
      second = asyncio.create_task(worker.call(("held",), None, "second"))
      await asyncio.sleep(0.05)
      await asyncio.wait_for(worker.call(("release_calls",), None), timeout=2)
      self.assertEqual(await asyncio.gather(first, second), ["first", "second"])
      self.assertEqual(await worker.call(("observed_concurrency",), None), 2)
    finally:
      for task in (first, second):
        if task is not None:
          task.cancel()
      await worker.close()
      await runtime.close()

  async def test_worker_calls_fail_after_execution_stops(self) -> None:
    runtime = PythonRuntime()
    worker = runtime.run("raise ValueError('stopped')", spin=False)
    try:
      result = await worker.task
      self.assertEqual(result.reason, TerminationReason.GUEST_ERROR)
      with self.assertRaisesRegex(
        WorkerStoppedError,
        r"worker execution has stopped \(guest_error\)",
      ):
        await worker.call(("missing",), None)
    finally:
      await worker.close()
      await runtime.close()

  async def test_sequential_worker_calls(self) -> None:
    runtime = PythonRuntime()
    worker = runtime.run(
      "def echo(value):\n  return value\n",
      spin_concurrent=False,
    )
    try:
      self.assertEqual(await worker.call(("echo",), None, 1), 1)
      self.assertEqual(await worker.call(("echo",), None, 2), 2)
    finally:
      await worker.close()
      await runtime.close()

  async def test_rpc_preserves_shared_values_in_both_directions(self) -> None:
    runtime = PythonRuntime()

    @runtime.rpc.expose
    def preserve_sharing(
      context: RpcContext,
      /,
      value: list[list[object]],
    ) -> list[list[object]]:
      self.assertIs(value[0], value[1])
      shared: list[object] = []
      return [shared, shared]

    guest_to_host = await runtime.execute(
      "shared = []\n"
      "result = await preserve_sharing([shared, shared])\n"
      "assert result[0] is result[1]\n"
    )
    self.assertEqual(guest_to_host.reason, TerminationReason.COMPLETED)

    worker = runtime.run(
      "def preserve_sharing(value):\n"
      "  assert value[0] is value[1]\n"
      "  shared = []\n"
      "  return [shared, shared]\n"
    )
    try:
      shared: list[object] = []
      result = await worker.call(
        ("preserve_sharing",),
        None,
        [shared, shared],
      )
      if not isinstance(result, list):
        self.fail(f"expected list result, received {type(result).__name__}")
      self.assertIs(result[0], result[1])
    finally:
      await worker.close()
      await runtime.close()

  async def test_rpc_context_and_per_execution_methods(self) -> None:
    runtime = PythonRuntime()
    contexts: list[RpcContext] = []
    denied_called = False

    @runtime.rpc.expose
    def allowed(context: RpcContext, /) -> tuple[int, int]:
      contexts.append(context)
      return context.worker_id, context.request_id

    @runtime.rpc.expose
    def denied(context: RpcContext, /) -> None:
      nonlocal denied_called
      denied_called = True

    @runtime.rpc.expose("method/name")
    def non_identifier(context: RpcContext, /) -> str:
      return "explicit"

    try:
      selected = await runtime.execute(
        "assert 'allowed' in globals()\n"
        "assert 'denied' not in globals()\n"
        "worker_id, request_id = await allowed()\n"
        "assert worker_id > 0 and request_id > 0\n",
        rpc_methods={"allowed"},
      )
      self.assertEqual(selected.reason, TerminationReason.COMPLETED)
      self.assertEqual(len(contexts), 1)
      self.assertEqual(contexts[0].worker_id, 1)
      self.assertGreater(contexts[0].request_id, 0)

      bypass = await runtime.execute(
        "await call('denied')",
        rpc_methods={"allowed"},
      )
      self.assertEqual(bypass.reason, TerminationReason.GUEST_ERROR)
      self.assertIn("RPC method is not available: denied", bypass.error or "")
      self.assertFalse(denied_called)

      explicit = await runtime.execute(
        "assert 'method/name' not in globals()\n"
        "assert await call('method/name') == 'explicit'\n",
        rpc_methods={"method/name"},
      )
      self.assertEqual(explicit.reason, TerminationReason.COMPLETED)

      with self.assertRaisesRegex(ValueError, "unknown RPC methods: missing"):
        await runtime.execute("pass", rpc_methods={"missing"})
    finally:
      await runtime.close()

  async def test_pending_call_fails_when_worker_stops(self) -> None:
    runtime = PythonRuntime()
    worker = runtime.run(
      "import asyncio\n"
      "started = asyncio.Event()\n"
      "release = asyncio.Event()\n"
      "async def held():\n"
      "  started.set()\n"
      "  await release.wait()\n"
      "def is_started():\n"
      "  return started.is_set()\n",
    )
    pending: asyncio.Task[object] | None = None
    try:
      self.assertFalse(await worker.call(("is_started",), None))
      pending = asyncio.create_task(worker.call(("held",), None))
      for _ in range(100):
        if await worker.call(("is_started",), None):
          break
        await asyncio.sleep(0.01)
      await worker.close()
      with self.assertRaises(WorkerStoppedError):
        await asyncio.wait_for(pending, timeout=2)
    finally:
      if pending is not None:
        pending.cancel()
      await worker.close()
      await runtime.close()

  async def test_host_dispatch_concurrency(self) -> None:
    runtime = PythonRuntime(host_dispatch_concurrency=2)
    active = 0
    maximum_active = 0
    two_active = asyncio.Event()
    release = asyncio.Event()

    @runtime.rpc.expose
    async def held_call(context: RpcContext, /) -> None:
      nonlocal active, maximum_active
      active += 1
      maximum_active = max(maximum_active, active)
      if active == 2:
        two_active.set()
      try:
        await release.wait()
      finally:
        active -= 1

    try:
      executions = [
        asyncio.create_task(runtime.execute("await held_call()")) for _ in range(6)
      ]
      await asyncio.wait_for(two_active.wait(), timeout=DISPATCH_TEST_TIMEOUT)
      await asyncio.sleep(0.05)
      self.assertEqual(maximum_active, 2)
      release.set()
      results = await asyncio.gather(*executions)
      self.assertTrue(all(result.error is None for result in results))
      self.assertEqual(maximum_active, 2)
    finally:
      release.set()
      await runtime.close()

  async def test_guest_dispatch_request_concurrency(self) -> None:
    runtime = PythonRuntime(host_dispatch_concurrency=8)
    active = 0
    maximum_active = 0
    two_active = asyncio.Event()
    release = asyncio.Event()

    @runtime.rpc.expose
    async def locally_held(context: RpcContext, /) -> None:
      nonlocal active, maximum_active
      active += 1
      maximum_active = max(maximum_active, active)
      if active == 2:
        two_active.set()
      try:
        await release.wait()
      finally:
        active -= 1

    execution: asyncio.Task[RuntimeResult] | None = None
    try:
      execution = asyncio.create_task(
        runtime.execute(
          "import asyncio\nawait asyncio.gather(*(locally_held() for _ in range(6)))",
          limits=RuntimeLimits(guest_dispatch_request_concurrency=2),
        )
      )
      await asyncio.wait_for(two_active.wait(), timeout=DISPATCH_TEST_TIMEOUT)
      await asyncio.sleep(0.05)
      self.assertEqual(maximum_active, 2)
      release.set()
      result = await execution
      self.assertIsNone(result.error)
      self.assertEqual(maximum_active, 2)
    finally:
      release.set()
      if execution is not None:
        await asyncio.gather(execution, return_exceptions=True)
      await runtime.close()

  async def test_guest_dispatch_overload_is_isolated_per_worker(self) -> None:
    runtime = PythonRuntime(
      host_dispatch_concurrency=8,
      host_dispatch_queue_capacity=32,
    )
    first_started = asyncio.Event()
    release = asyncio.Event()

    @runtime.rpc.expose
    async def locally_saturated(
      context: RpcContext,
      /,
      value: str,
    ) -> str:
      if value == "hold":
        first_started.set()
        await release.wait()
      return value

    saturated: asyncio.Task[RuntimeResult] | None = None
    try:
      saturated = asyncio.create_task(
        runtime.execute(
          "import asyncio\n"
          "results = await asyncio.gather(\n"
          "  locally_saturated('hold'),\n"
          "  locally_saturated('queued'),\n"
          "  locally_saturated('overload'),\n"
          "  return_exceptions=True,\n"
          ")\n"
          "print(*(str(result) for result in results), sep='\\n', flush=True)",
          limits=RuntimeLimits(
            guest_dispatch_request_concurrency=1,
            guest_dispatch_request_queue_capacity=1,
          ),
        )
      )
      await asyncio.wait_for(first_started.wait(), timeout=DISPATCH_TEST_TIMEOUT)

      unaffected = await asyncio.wait_for(
        runtime.execute("print(await locally_saturated('other'), flush=True)"),
        timeout=DISPATCH_TEST_TIMEOUT,
      )
      self.assertIsNone(unaffected.error)
      self.assertEqual(unaffected.stdout, b"other\n")

      release.set()
      saturated_result = await saturated
      self.assertIsNone(saturated_result.error)
      self.assertIn(
        b"guest dispatch request queue is full",
        saturated_result.stdout,
      )
    finally:
      release.set()
      if saturated is not None:
        await asyncio.gather(saturated, return_exceptions=True)
      await runtime.close()

  async def test_host_dispatch_overload_does_not_block_connection(self) -> None:
    runtime = PythonRuntime(
      host_dispatch_concurrency=1,
      host_dispatch_queue_capacity=1,
    )
    first_started = asyncio.Event()
    make_worker_call = asyncio.Event()
    worker_call_finished = asyncio.Event()
    release = asyncio.Event()
    target = runtime.run("def ping():\n  return 'pong'\n")
    first: asyncio.Task[RuntimeResult] | None = None
    second: asyncio.Task[RuntimeResult] | None = None

    @runtime.rpc.expose
    async def saturated_call(context: RpcContext, /, name: str) -> str:
      if name == "first":
        first_started.set()
        await make_worker_call.wait()
        self.assertEqual(await target.call(("ping",), None), "pong")
        worker_call_finished.set()
        await release.wait()
      return name

    try:
      self.assertEqual(await target.call(("ping",), None), "pong")
      first = asyncio.create_task(runtime.execute("await saturated_call('first')"))
      await asyncio.wait_for(first_started.wait(), timeout=DISPATCH_TEST_TIMEOUT)

      second = asyncio.create_task(runtime.execute("await saturated_call('second')"))
      await asyncio.sleep(0.05)
      overloaded = await asyncio.wait_for(
        runtime.execute("await saturated_call('third')"),
        timeout=DISPATCH_TEST_TIMEOUT,
      )
      self.assertIn("host dispatch queue is full", overloaded.error or "")

      make_worker_call.set()
      await asyncio.wait_for(
        worker_call_finished.wait(),
        timeout=DISPATCH_TEST_TIMEOUT,
      )
      release.set()
      results = await asyncio.gather(first, second)
      self.assertTrue(all(result.error is None for result in results))
    finally:
      make_worker_call.set()
      release.set()
      pending = [task for task in (first, second) if task is not None]
      if pending:
        await asyncio.gather(*pending, return_exceptions=True)
      await target.close()
      await runtime.close()

  async def test_worker_call_queue_overload(self) -> None:
    runtime = PythonRuntime(worker_queue_capacity=1)
    host_call_started = asyncio.Event()
    release = asyncio.Event()

    @runtime.rpc.expose
    async def wait_on_host(context: RpcContext, /) -> None:
      host_call_started.set()
      await release.wait()

    worker = runtime.run(
      "async def held_call():\n  await wait_on_host()\n  return 'done'\n",
      spin_concurrent=False,
    )
    try:
      first = asyncio.create_task(worker.call(("held_call",), None))
      await asyncio.wait_for(
        host_call_started.wait(),
        timeout=DISPATCH_TEST_TIMEOUT,
      )
      second = asyncio.create_task(worker.call(("held_call",), None))
      await asyncio.sleep(0.05)
      with self.assertRaisesRegex(RuntimeError, "worker call queue is full"):
        await worker.call(("held_call",), None)

      release.set()
      self.assertEqual(await asyncio.gather(first, second), ["done", "done"])
    finally:
      release.set()
      await worker.close()
      await runtime.close()

  async def test_subprocess_death_and_lazy_reopen(self) -> None:
    runtime = PythonRuntime()

    @runtime.rpc.expose
    async def wait_forever(context: RpcContext, /) -> None:
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
      completed = await runtime.execute("pass")
      self.assertEqual(completed.reason, TerminationReason.COMPLETED)

      guest_error = await runtime.execute("raise ValueError('guest failure')")
      self.assertEqual(guest_error.reason, TerminationReason.GUEST_ERROR)

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

      output_limited = await runtime.execute(
        "print('a' * 1025)",
        limits=RuntimeLimits(max_output_bytes=1024),
      )
      self.assertEqual(output_limited.reason, TerminationReason.OUTPUT_LIMIT)
      self.assertEqual(output_limited.error, "guest output exceeded 1024 bytes")
      self.assertEqual(len(output_limited.stdout), 1024)

      memory_limited = await runtime.execute(
        "bytearray(256 * 1024 * 1024)",
        limits=RuntimeLimits(max_memory_bytes=128 * 1024 * 1024),
      )
      self.assertEqual(memory_limited.reason, TerminationReason.MEMORY_LIMIT)
      self.assertEqual(
        memory_limited.error,
        "guest memory exceeded 134217728 bytes",
      )

      runtime_error = await runtime.execute("import os\nos.abort()")
      self.assertEqual(runtime_error.reason, TerminationReason.RUNTIME_ERROR)

      cancelled_worker = runtime.run("while True:\n  pass", spin=False)
      await asyncio.wait_for(
        asyncio.shield(cancelled_worker._execution),
        timeout=5,
      )
      await cancelled_worker.cancel()
      cancelled = await asyncio.wait_for(cancelled_worker.task, timeout=5)
      self.assertEqual(cancelled.reason, TerminationReason.CANCELLED)
    finally:
      await runtime.close()

  async def test_timeout_interrupts_running_and_waiting_guest(self) -> None:
    runtime = PythonRuntime()
    try:
      await runtime.execute("pass")
      for program in ("while True:\n  pass", "import time\ntime.sleep(10)"):
        with self.subTest(program=program):
          started = time.monotonic()
          result = await runtime.execute(
            program,
            limits=RuntimeLimits(timeout=0.1),
          )
          elapsed = time.monotonic() - started

          self.assertEqual(result.reason, TerminationReason.TIMEOUT)
          self.assertLess(elapsed, 2)
    finally:
      await runtime.close()

  async def test_immediate_dynamic_timeout_waits_until_execution_is_ready(self) -> None:
    runtime = PythonRuntime()
    worker = runtime.run("import time\ntime.sleep(10)", spin=False)
    try:
      await worker.set_limits(timeout=0.1)
      started = time.monotonic()
      result = await asyncio.wait_for(worker.task, timeout=5)

      self.assertEqual(result.reason, TerminationReason.TIMEOUT)
      self.assertLess(time.monotonic() - started, 2)
    finally:
      await worker.close()
      await runtime.close()
