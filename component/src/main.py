# type: ignore
import ast
import asyncio
import inspect
import sys
import traceback
import types

import wit_world
from componentize_py_types import Err
from wit_world.imports import host

sys.dont_write_bytecode = True


class UnsupportedExtensionFinder:
  @staticmethod
  def find_spec(
    fullname: str,
    path: object = None,
    target: object = None,
  ) -> None:
    del path, target
    if fullname == "_cbor2":
      raise ModuleNotFoundError(
        "the _cbor2 native extension is unavailable in the WASI runtime"
      )


def configure_sys_path() -> None:
  sys.path[:] = [
    path
    for path in sys.path
    if path not in {"/world", "/bundled"}
    and not (path.startswith("/") and path[1:].isdigit())
  ]
  site_packages = "/python/lib/python3.14/site-packages"
  if site_packages in sys.path:
    sys.path.remove(site_packages)
  sys.path.insert(0, site_packages)


async def handle_worker_call(
  namespace: dict[str, object],
  request: object,
) -> None:
  try:
    target: object = namespace
    for part in request.path:
      target = target[part] if isinstance(target, dict) else getattr(target, part)
    import cbor2

    args, kwargs = cbor2.loads(request.arguments)
    value = target(*args, **kwargs)
    if inspect.isawaitable(value):
      value = await value
    await host.worker_response(
      request.request_id,
      cbor2.dumps(value, value_sharing=True),
      None,
    )
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
  import cbor2

  arguments = cbor2.dumps((args, kwargs), value_sharing=True)
  result = await host.call(method, arguments)
  return cbor2.loads(result)


class WitWorld(wit_world.WitWorld):
  def __init__(self) -> None:
    self.main_module = types.ModuleType("__main__")
    self.namespace: dict[str, object] = self.main_module.__dict__
    self.namespace.update(
      {
        "call": call,
        "spin": lambda concurrent: spin(self.namespace, concurrent),
      }
    )
    sys.modules["__main__"] = self.main_module

  def initialize(self) -> None:
    try:
      configure_sys_path()
      sys.meta_path.insert(0, UnsupportedExtensionFinder())
      __import__("cbor2")
    except Exception:
      raise Err(traceback.format_exc())

  async def run(self) -> None:
    try:
      configure_sys_path()
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
