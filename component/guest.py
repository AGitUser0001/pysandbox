import ast
import inspect
import traceback

import cbor2
import wit_world
from componentize_py_types import Err
from wit_world.imports import host


async def spin(namespace: dict[str, object]) -> None:
  while (event := await host.spin_next()) is not None:
    if event.call is None:
      continue
    request = event.call
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


async def call(method: str, *args: object, **kwargs: object) -> object:
  arguments = cbor2.dumps((args, kwargs))
  result = await host.call(method, arguments)
  return cbor2.loads(result)


class WitWorld(wit_world.WitWorld):
  def __init__(self) -> None:
    self.namespace: dict[str, object] = {
      "__name__": "__main__",
      "cbor2": cbor2,
      "call": call,
      "spin": lambda: spin(self.namespace),
    }

  async def run(self) -> None:
    try:
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
