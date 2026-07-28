import ast
import asyncio
import inspect
import sys
import traceback
import types

import cbor2
import wit_world
from componentize_py_types import Err
from wit_world.imports import host

sys.dont_write_bytecode = True


async def handle_worker_call(
  namespace: dict[str, object],
  request: object,
) -> None:
  try:
    target: object = namespace
    for part in request.path:
      target = target[part] if isinstance(target, dict) else getattr(target, part)
    args, kwargs = cbor2.loads(request.arguments)
    value = target(*args, **kwargs)
    if inspect.isawaitable(value):
      value = await value
    await host.worker_response(request.request_id, cbor2.dumps(value), None)
  except Exception:
    await host.worker_response(request.request_id, b"", traceback.format_exc())


async def spin(namespace: dict[str, object], concurrent: bool) -> None:
  tasks: set[asyncio.Task[None]] = set()
  try:
    while (event := await host.spin_next()) is not None:
      if event.call is None:
        continue
      if not concurrent:
        await handle_worker_call(namespace, event.call)
        continue
      task = asyncio.create_task(handle_worker_call(namespace, event.call))
      tasks.add(task)
      task.add_done_callback(tasks.discard)
      await asyncio.sleep(0)
  finally:
    for task in tasks:
      task.cancel()
    if tasks:
      await asyncio.gather(*tasks, return_exceptions=True)


async def call(method: str, *args: object, **kwargs: object) -> object:
  arguments = cbor2.dumps((args, kwargs))
  result = await host.call(method, arguments)
  return cbor2.loads(result)


class WitWorld(wit_world.WitWorld):
  def __init__(self) -> None:
    self.main_module = types.ModuleType("__main__")
    self.namespace: dict[str, object] = self.main_module.__dict__
    self.namespace.update(
      {
        "cbor2": cbor2,
        "call": call,
        "spin": lambda concurrent: spin(self.namespace, concurrent),
      }
    )
    sys.modules["__main__"] = self.main_module

  async def run(self) -> None:
    try:
      if "/" not in sys.path:
        sys.path.insert(0, "/")
      program = host.program()
      code = compile(
        program,
        "<guest>",
        "exec",
        flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT,
        dont_inherit=True,
      )
      value = eval(code, self.namespace)
      if inspect.isawaitable(value):
        await value
    except Exception:
      raise Err(traceback.format_exc())
