import keyword
import os
from collections.abc import Mapping
from typing import Any

from messaging.messanger import Messanger
from messaging.transports import FileTransport


class HostCallError(Exception):
    pass


_messanger: Messanger | None = None
_next_request_id = 0
__all__: list[str] = []


def configure(messanger: Messanger) -> None:
    global _messanger
    _messanger = messanger


def configure_from_environment() -> None:
    rpc_dir = os.environ["PYSANDBOX_RPC_DIR"]
    configure(
        Messanger(
            FileTransport(
                read_paths=(
                    os.path.join(rpc_dir, "response", "0"),
                    os.path.join(rpc_dir, "response", "1"),
                ),
                write_paths=(
                    os.path.join(rpc_dir, "request", "0"),
                    os.path.join(rpc_dir, "request", "1"),
                ),
            )
        )
    )


def call(method: str, params: Mapping[str, Any] | None = None) -> Any:
    messanger = get_messanger()
    request_id = next_request_id()
    messanger.post_message(
        {
            "type": "request",
            "id": request_id,
            "method": method,
            "params": dict(params or {}),
        }
    )
    message = receive_response(messanger, request_id)

    if not isinstance(message, dict):
        raise HostCallError("invalid host response")

    if message.get("ok"):
        return message.get("result")

    error = message.get("error")
    raise HostCallError(str(error))


def receive_response(messanger: Messanger, request_id: int) -> object:
    while True:
        message = messanger.receive_message()
        if not isinstance(message, dict):
            continue

        if message.get("type") != "response":
            continue

        if message.get("id") != request_id:
            continue

        return message


def get_messanger() -> Messanger:
    global _messanger

    if _messanger is None:
        configure_from_environment()

    if _messanger is None:
        raise HostCallError("host messenger is not configured")

    return _messanger


def next_request_id() -> int:
    global _next_request_id

    request_id = _next_request_id
    _next_request_id += 1
    return request_id


def make_proxy(method: str):
    def proxy(**params: Any) -> Any:
        return call(method, params)

    proxy.__name__ = method
    proxy.__qualname__ = method
    proxy.__doc__ = f"Call the host {method!r} RPC method."
    return proxy


def configure_api_from_environment() -> None:
    methods = os.environ.get("PYSANDBOX_RPC_METHODS", "")
    for method in parse_methods(methods):
        globals()[method] = make_proxy(method)
        __all__.append(method)


def parse_methods(methods: str) -> list[str]:
    parsed: list[str] = []
    for method in methods.split(","):
        method = method.strip()
        if not method:
            continue

        if not method.isidentifier() or keyword.iskeyword(method):
            raise HostCallError(f"invalid RPC method name: {method!r}")

        parsed.append(method)

    return parsed


configure_api_from_environment()
