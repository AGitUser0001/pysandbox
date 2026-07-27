from collections.abc import Callable
from typing import overload

__all__ = ["RpcHandler", "RpcHost"]

type RpcHandler = Callable[..., object]
type RpcHandlerDecorator = Callable[[RpcHandler], RpcHandler]


class RpcHost:
  def __init__(self, register: Callable[[str, RpcHandler], None]) -> None:
    self._register = register
    self._handlers: dict[str, RpcHandler] = {}

  @overload
  def expose(self, handler: RpcHandler, /) -> RpcHandler: ...

  @overload
  def expose(self, method: str, /) -> RpcHandlerDecorator: ...

  @overload
  def expose(self, method: str, handler: RpcHandler, /) -> RpcHandler: ...

  def expose(
    self,
    method: str | RpcHandler,
    handler: RpcHandler | None = None,
    /,
  ) -> RpcHandler | RpcHandlerDecorator:
    if callable(method):
      if handler is not None:
        raise TypeError("handler cannot be provided twice")
      return self._expose(method.__name__, method)

    if handler is None:
      return lambda decorated: self._expose(method, decorated)

    return self._expose(method, handler)

  def _expose(self, method: str, handler: RpcHandler) -> RpcHandler:
    if not method:
      raise ValueError("RPC method cannot be empty")
    self._handlers[method] = handler
    self._register(method, handler)
    return handler

  @property
  def methods(self) -> tuple[str, ...]:
    return tuple(self._handlers)

  def handlers(self) -> tuple[tuple[str, RpcHandler], ...]:
    return tuple(self._handlers.items())
