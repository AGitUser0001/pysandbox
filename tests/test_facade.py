import asyncio
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

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

FREE_THREADED: bool = not getattr(sys, "_is_gil_enabled", lambda: True)()


async def wait_for_event_or_execution(
  event: asyncio.Event,
  execution: asyncio.Task[RuntimeResult],
) -> None:
  event_wait = asyncio.create_task(event.wait())
  done, _ = await asyncio.wait(
    (event_wait, execution),
    return_when=asyncio.FIRST_COMPLETED,
  )
  if event_wait in done:
    return

  event_wait.cancel()
  await asyncio.gather(event_wait, return_exceptions=True)
  if execution in done:
    result = await execution
    pytest.fail(
      "execution stopped before reaching expected concurrency: "
      f"reason={result.reason.value}, error={result.error!r}"
    )
  pytest.fail("execution did not reach expected concurrency")


class TestFacade:
  @pytest.mark.skipif(not FREE_THREADED, reason="requires free-threaded CPython")
  async def test_shared_runtime_across_python_threads(self) -> None:
    runtime = PythonRuntime()
    await runtime.reopen()
    thread_count = 4
    executions_per_thread = 4
    barrier = Barrier(thread_count)

    def execute_from_thread(thread_index: int) -> list[int]:
      async def execute_all() -> list[int]:
        barrier.wait()
        results = await asyncio.gather(
          *(
            runtime.execute(f"print({thread_index * 100 + index}, flush=True)")
            for index in range(executions_per_thread)
          )
        )
        assert all(result.reason is TerminationReason.COMPLETED for result in results)
        return [int(result.stdout) for result in results]

      return asyncio.run(execute_all())

    try:
      with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [
          executor.submit(execute_from_thread, thread_index)
          for thread_index in range(thread_count)
        ]
        thread_values = await asyncio.gather(
          *(asyncio.wrap_future(future) for future in futures)
        )
        values = [value for result in thread_values for value in result]

      assert sorted(values) == sorted(
        [
          thread_index * 100 + index
          for thread_index in range(thread_count)
          for index in range(executions_per_thread)
        ]
      )
    finally:
      await runtime.close()

  def test_worker_queue_capacity_validation(self) -> None:
    with pytest.raises(ValueError, match="worker_queue_capacity"):
      PythonRuntime(worker_queue_capacity=0)
    with pytest.raises(ValueError, match="host_dispatch_concurrency"):
      PythonRuntime(host_dispatch_concurrency=0)
    with pytest.raises(ValueError, match="host_dispatch_queue_capacity"):
      PythonRuntime(host_dispatch_queue_capacity=0)
    with pytest.raises(ValueError, match="guest_dispatch_request_concurrency"):
      RuntimeLimits(guest_dispatch_request_concurrency=0)
    with pytest.raises(ValueError, match="guest_dispatch_request_queue_capacity"):
      RuntimeLimits(guest_dispatch_request_queue_capacity=0)
    with pytest.raises(ValueError, match="cpu_share_weight"):
      RuntimeLimits(cpu_share_weight=0)
    with pytest.raises(ValueError, match="sample_interval"):
      CpuShareConfig(sample_interval=0)
    with pytest.raises(ValueError, match="activity_timeout"):
      CpuShareConfig(activity_timeout=float("inf"))
    with pytest.raises(ValueError, match="limit_percent"):
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
      assert result.error is None
      assert result.stdout == b"7\nHELLO\n"
      assert result.text == "7\nHELLO\n"
      assert isinstance(result.output[:1], Output)

      main_module = await runtime.execute(
        "value = 42\n"
        "import __main__\n"
        "print(__main__.value, __main__.__dict__ is globals(), flush=True)\n"
      )
      assert main_module.error is None
      assert main_module.stdout == b"42 True\n"

      failed = await runtime.execute("raise ValueError('guest failure')")
      assert "ValueError: guest failure" in (failed.error or "")
      assert failed.reason == TerminationReason.GUEST_ERROR

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
      assert await worker.call(("increment",), None, 5) == 15
      for _ in range(1_000):
        if worker.output.stdout == b"ready\n":
          break
        await asyncio.sleep(0.01)
      assert worker.output.stdout == b"ready\n"
      assert await worker.call(("increment",), None, amount=2) == 17
      call_options = WorkerCallOptions(
        fuel=AddFuel(1_000, cap=2**64 - 1),
        timeout=5,
      )
      assert await worker.call(("add_on_host",), call_options, 3) == 20
      with pytest.raises(TimeoutError):
        await worker.call(
          ("slow_call",),
          WorkerCallOptions(timeout=0.01),
          0.1,
        )
      await asyncio.sleep(0.15)
      assert await worker.call(("increment",), None) == 18
      await worker.set_limits(max_guest_rpc_bytes=1024)
      await worker.set_limits(cpu_share_weight=2)
      with pytest.raises(ValueError, match="cpu_share_weight"):
        await worker.set_limits(cpu_share_weight=0)
      await worker.add_fuel(1_000, cap=2**64 - 1)
      await worker.close()
      assert worker.task.done()
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
      assert context.worker_id == expected_worker_ids[worker]
      assert context.request_id not in request_ids
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
      assert results == [
        [name, value] for value in range(32) for name in ("first", "second")
      ]
      assert len(request_ids) == 64
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
      assert await worker.call(("observed_concurrency",), None) == 0
      first = asyncio.create_task(worker.call(("held",), None, "first"))
      second = asyncio.create_task(worker.call(("held",), None, "second"))
      await asyncio.sleep(0.05)
      await asyncio.wait_for(worker.call(("release_calls",), None), timeout=2)
      assert await asyncio.gather(first, second) == ["first", "second"]
      assert await worker.call(("observed_concurrency",), None) == 2
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
      assert result.reason == TerminationReason.GUEST_ERROR
      with pytest.raises(
        WorkerStoppedError, match=r"worker execution has stopped \(guest_error\)"
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
      assert await worker.call(("echo",), None, 1) == 1
      assert await worker.call(("echo",), None, 2) == 2
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
      assert value[0] is value[1]
      shared: list[object] = []
      return [shared, shared]

    guest_to_host = await runtime.execute(
      "shared = []\n"
      "result = await preserve_sharing([shared, shared])\n"
      "assert result[0] is result[1]\n"
    )
    assert guest_to_host.reason == TerminationReason.COMPLETED

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
        pytest.fail(f"expected list result, received {type(result).__name__}")
      assert result[0] is result[1]
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
      assert selected.reason == TerminationReason.COMPLETED
      assert len(contexts) == 1
      assert contexts[0].worker_id == 1
      assert contexts[0].request_id > 0

      bypass = await runtime.execute(
        "await call('denied')",
        rpc_methods={"allowed"},
      )
      assert bypass.reason == TerminationReason.GUEST_ERROR
      assert "RPC method is not available: denied" in (bypass.error or "")
      assert not denied_called

      explicit = await runtime.execute(
        "assert 'method/name' not in globals()\n"
        "assert await call('method/name') == 'explicit'\n",
        rpc_methods={"method/name"},
      )
      assert explicit.reason == TerminationReason.COMPLETED

      with pytest.raises(ValueError, match="unknown RPC methods: missing"):
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
      assert not await worker.call(("is_started",), None)
      pending = asyncio.create_task(worker.call(("held",), None))
      for _ in range(100):
        if await worker.call(("is_started",), None):
          break
        await asyncio.sleep(0.01)
      await worker.close()
      with pytest.raises(WorkerStoppedError):
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
      await two_active.wait()
      assert maximum_active == 2
      release.set()
      results = await asyncio.gather(*executions)
      assert all(result.error is None for result in results)
      assert maximum_active == 2
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
      await wait_for_event_or_execution(two_active, execution)
      assert maximum_active == 2
      release.set()
      result = await execution
      assert result.error is None
      assert maximum_active == 2
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
      await first_started.wait()

      unaffected = await runtime.execute(
        "print(await locally_saturated('other'), flush=True)"
      )
      assert unaffected.error is None
      assert unaffected.stdout == b"other\n"

      release.set()
      saturated_result = await saturated
      assert saturated_result.error is None
      assert b"guest dispatch request queue is full" in saturated_result.stdout
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
    fillers: list[asyncio.Task[RuntimeResult]] = []

    @runtime.rpc.expose
    async def saturated_call(context: RpcContext, /, name: str) -> str:
      if name == "first":
        first_started.set()
        await make_worker_call.wait()
        assert await target.call(("ping",), None) == "pong"
        worker_call_finished.set()
        await release.wait()
      return name

    try:
      assert await target.call(("ping",), None) == "pong"
      first = asyncio.create_task(runtime.execute("await saturated_call('first')"))
      await first_started.wait()

      fillers = [
        asyncio.create_task(runtime.execute(f"await saturated_call('{name}')"))
        for name in ("second", "third", "fourth")
      ]
      completed, _ = await asyncio.wait(
        fillers,
        return_when=asyncio.FIRST_COMPLETED,
      )
      completed_results = await asyncio.gather(*completed)
      assert any(
        "host dispatch queue is full" in (result.error or "")
        for result in completed_results
      ), completed_results

      make_worker_call.set()
      await worker_call_finished.wait()
      release.set()
      results = await asyncio.gather(first, *fillers)
      assert all(
        result.error is None or "host dispatch queue is full" in result.error
        for result in results
      )
    finally:
      make_worker_call.set()
      release.set()
      pending = [first] if first is not None else []
      pending.extend(fillers)
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
    first: asyncio.Task[object] | None = None
    fillers: list[asyncio.Task[object]] = []
    try:
      first = asyncio.create_task(worker.call(("held_call",), None))
      await host_call_started.wait()
      fillers = [
        asyncio.create_task(worker.call(("held_call",), None)) for _ in range(3)
      ]
      completed, _ = await asyncio.wait(
        fillers,
        return_when=asyncio.FIRST_COMPLETED,
      )
      completed_results = await asyncio.gather(*completed, return_exceptions=True)
      assert any(
        isinstance(result, RuntimeError) and "worker call queue is full" in str(result)
        for result in completed_results
      ), completed_results

      release.set()
      results = await asyncio.gather(first, *fillers, return_exceptions=True)
      assert results[0] == "done"
      assert all(
        result == "done"
        or (
          isinstance(result, RuntimeError)
          and "worker call queue is full" in str(result)
        )
        for result in results[1:]
      )
    finally:
      release.set()
      pending = [first] if first is not None else []
      pending.extend(fillers)
      if pending:
        await asyncio.gather(*pending, return_exceptions=True)
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
      assert runtime.is_open
      await sandbox.terminate()
      with pytest.raises(RuntimeError):
        await asyncio.wait_for(worker.task, timeout=5)
      assert not runtime.is_open

      recovered = await runtime.execute("print('recovered', flush=True)")
      assert recovered.reason == TerminationReason.COMPLETED
      assert recovered.stdout == b"recovered\n"
      assert runtime.is_open
    finally:
      await runtime.close()

  async def test_structured_termination_reasons(self) -> None:
    runtime = PythonRuntime()
    try:
      completed = await runtime.execute("pass")
      assert completed.reason == TerminationReason.COMPLETED

      guest_error = await runtime.execute("raise ValueError('guest failure')")
      assert guest_error.reason == TerminationReason.GUEST_ERROR

      timed_out = await runtime.execute(
        "while True:\n  pass",
        limits=RuntimeLimits(timeout=0.05),
      )
      assert timed_out.reason == TerminationReason.TIMEOUT

      fuel = await runtime.execute(
        "while True:\n  pass",
        limits=RuntimeLimits(fuel=1),
      )
      assert fuel.reason == TerminationReason.FUEL_EXHAUSTED

      output_limited = await runtime.execute(
        "print('a' * 1025)",
        limits=RuntimeLimits(max_output_bytes=1024),
      )
      assert output_limited.reason == TerminationReason.OUTPUT_LIMIT
      assert output_limited.error == "guest output exceeded 1024 bytes"
      assert len(output_limited.stdout) == 1024

      memory_limited = await runtime.execute(
        "bytearray(256 * 1024 * 1024)",
        limits=RuntimeLimits(max_memory_bytes=128 * 1024 * 1024),
      )
      assert memory_limited.reason == TerminationReason.MEMORY_LIMIT
      assert memory_limited.error == "guest memory exceeded 134217728 bytes"

      runtime_error = await runtime.execute("import os\nos.abort()")
      assert runtime_error.reason == TerminationReason.RUNTIME_ERROR

      cancelled_worker = runtime.run("while True:\n  pass", spin=False)
      await asyncio.wait_for(
        asyncio.shield(cancelled_worker._execution),
        timeout=5,
      )
      await cancelled_worker.cancel()
      cancelled = await asyncio.wait_for(cancelled_worker.task, timeout=5)
      assert cancelled.reason == TerminationReason.CANCELLED
    finally:
      await runtime.close()

  async def test_guest_exit_codes(self) -> None:
    runtime = PythonRuntime()
    try:
      for program, exit_code, stderr in (
        ("raise SystemExit", 0, b""),
        ("raise SystemExit(7)", 1, b""),
        ("raise SystemExit('guest exit')", 1, b"guest exit\n"),
        ("import os\nos._exit(9)", 1, b""),
      ):
        result = await runtime.execute(program)

        assert result.reason == TerminationReason.EXITED
        assert result.exit_code == exit_code
        assert result.error is None
        assert result.stderr == stderr
    finally:
      await runtime.close()

  async def test_guest_base_exceptions_are_guest_errors(self) -> None:
    runtime = PythonRuntime()
    try:
      for exception in (
        "BaseException('base')",
        "KeyboardInterrupt()",
        "GeneratorExit()",
      ):
        result = await runtime.execute(f"raise {exception}")

        assert result.reason == TerminationReason.GUEST_ERROR
        assert result.exit_code is None
        assert result.error is not None
        assert exception.partition("(")[0] in result.error
    finally:
      await runtime.close()

  async def test_concurrent_worker_call_base_exception_stops_worker(self) -> None:
    runtime = PythonRuntime()
    try:
      for exception, reason in (
        ("KeyboardInterrupt()", TerminationReason.GUEST_ERROR),
        ("SystemExit(4)", TerminationReason.EXITED),
      ):
        worker = runtime.run(
          f"def stop():\n  raise {exception}\n\nawait spin(concurrent=True)"
        )
        with pytest.raises(WorkerStoppedError):
          await worker.call(("stop",), None)

        result = await worker.task
        assert result.reason == reason, result.error
        assert result.exit_code == (1 if reason is TerminationReason.EXITED else None)
    finally:
      await runtime.close()

  @pytest.mark.parametrize(
    "program",
    ("while True:\n  pass", "import time\ntime.sleep(10)"),
  )
  async def test_timeout_interrupts_running_and_waiting_guest(
    self,
    program: str,
  ) -> None:
    runtime = PythonRuntime()
    try:
      await runtime.execute("pass")
      started = time.monotonic()
      result = await runtime.execute(
        program,
        limits=RuntimeLimits(timeout=0.1),
      )
      elapsed = time.monotonic() - started

      assert result.reason == TerminationReason.TIMEOUT
      assert elapsed < 2
    finally:
      await runtime.close()

  async def test_immediate_dynamic_timeout_waits_until_execution_is_ready(self) -> None:
    runtime = PythonRuntime()
    worker = runtime.run("import time\ntime.sleep(10)", spin=False)
    try:
      await worker.set_limits(timeout=0.1)
      started = time.monotonic()
      result = await asyncio.wait_for(worker.task, timeout=5)

      assert result.reason == TerminationReason.TIMEOUT
      assert time.monotonic() - started < 2
    finally:
      await worker.close()
      await runtime.close()
