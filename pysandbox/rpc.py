from collections.abc import Callable
from typing import Concatenate, Protocol, overload

from . import _core

__all__ = ["RpcContext", "RpcHandler", "RpcHost"]

RpcContext = _core.RpcContext
type RpcHandler[**Parameters] = Callable[
  Concatenate[RpcContext, Parameters],
  object,
]


class RpcHandlerDecorator(Protocol):
  def __call__[**Parameters](
    self,
    handler: RpcHandler[Parameters],
    /,
  ) -> RpcHandler[Parameters]: ...


class RpcHost:
  def __init__(self, register: Callable[[str, Callable[..., object]], None]) -> None:
    self._register = register
    self._handlers: dict[str, Callable[..., object]] = {}

  @overload
  def expose[**Parameters](
    self,
    handler: RpcHandler[Parameters],
    /,
  ) -> RpcHandler[Parameters]: ...

  @overload
  def expose(self, method: str, /) -> RpcHandlerDecorator: ...

  @overload
  def expose[**Parameters](
    self,
    method: str,
    handler: RpcHandler[Parameters],
    /,
  ) -> RpcHandler[Parameters]: ...

  def expose(
    self,
    method: object,
    handler: object | None = None,
    /,
  ) -> object:
    if callable(method):
      if handler is not None:
        raise TypeError("handler cannot be provided twice")
      return self._expose(method.__name__, method)

    if handler is None:
      if not isinstance(method, str):
        raise TypeError("method must be a string or callable")

      def decorator[**Parameters](
        decorated: RpcHandler[Parameters],
        /,
      ) -> RpcHandler[Parameters]:
        self._expose(method, decorated)
        return decorated

      return decorator

    if not isinstance(method, str):
      raise TypeError("method must be a string")
    if not callable(handler):
      raise TypeError("handler must be callable")

    return self._expose(method, handler)

  def _expose(
    self,
    method: str,
    handler: Callable[..., object],
  ) -> Callable[..., object]:
    if not method:
      raise ValueError("RPC method cannot be empty")
    self._handlers[method] = handler
    self._register(method, handler)
    return handler

  @property
  def methods(self) -> tuple[str, ...]:
    return tuple(self._handlers)

  def handlers(self) -> tuple[tuple[str, Callable[..., object]], ...]:
    return tuple(self._handlers.items())
