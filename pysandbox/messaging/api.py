import keyword
import os
import builtins
from collections.abc import Mapping
from types import CellType, CodeType
from typing import Any

from .messenger import Messenger
from .transports import FileTransport


class HostTraceback(Exception):
    def __init__(self, traceback: str) -> None:
        super().__init__()
        self.traceback = traceback

    def __str__(self) -> str:
        return self.traceback


class HostCallError(Exception):
    def __init__(
        self,
        message: str,
        *,
        error_type: str | None = None,
        traceback: str | None = None,
        error: object = None,
    ) -> None:
        super().__init__(message)
        self.type = error_type
        self.traceback = traceback
        self.error = error
        if traceback is not None:
            self.__cause__ = HostTraceback(traceback)

    @classmethod
    def from_error(
        cls,
        error: object,
        *,
        traceback: object = None,
    ) -> "HostCallError":
        if not isinstance(error, dict):
            return cls(
                str(error),
                traceback=traceback if isinstance(traceback, str) else None,
                error=error,
            )

        message = error.get("message")
        error_type = error.get("type")
        nested_traceback = error.get("traceback")
        traceback_text = traceback if isinstance(traceback, str) else nested_traceback
        sanitized_error = dict(error)
        sanitized_error.pop("traceback", None)

        return cls(
            str(message),
            error_type=error_type if isinstance(error_type, str) else None,
            traceback=traceback_text if isinstance(traceback_text, str) else None,
            error=sanitized_error,
        )


_messenger: Messenger | None = None
_next_request_id = 0
__all__: list[str] = ["spin"]


def configure(messenger: Messenger) -> None:
    global _messenger
    _messenger = messenger


def configure_from_environment() -> None:
    rpc_dir = os.environ["PYSANDBOX_RPC_DIR"]
    configure(
        Messenger(
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


def call(method: str, *args: Any, **kwargs: Any) -> Any:
    messenger = get_messenger()
    request_id = next_request_id()
    messenger.post_message(
        {
            "type": "request",
            "id": request_id,
            "method": method,
            "args": args,
            "kwargs": kwargs,
        }
    )
    message = receive_response(messenger, request_id)

    if not isinstance(message, dict):
        raise HostCallError("invalid host response")

    if message.get("ok"):
        return message.get("result")

    raise HostCallError.from_error(
        message.get("error"),
        traceback=message.get("traceback"),
    )


def receive_response(messenger: Messenger, request_id: int) -> object:
    while True:
        message = messenger.receive_message()
        if not isinstance(message, dict):
            continue

        if message.get("type") != "response":
            continue

        if message.get("id") != request_id:
            continue

        return message


def spin() -> None:
    import sys

    namespace = sys._getframe(1).f_globals
    messenger = get_messenger()

    while True:
        message = messenger.receive_message()
        response = worker_response_for_message(message, namespace)
        if response is not None:
            messenger.post_message(response)


def worker_response_for_message(
    message: object,
    namespace: dict[str, Any],
) -> dict[str, Any] | None:
    if not isinstance(message, dict):
        return None

    if message.get("type") != "worker_call":
        return None

    request_id = message.get("id")
    fn_path = parse_fn_path(message.get("fn_path"))
    args = message.get("args", ())
    kwargs = message.get("kwargs", {})

    if fn_path is None:
        return worker_error_response(request_id, "invalid function path")

    if not isinstance(args, list | tuple):
        return worker_error_response(request_id, "invalid positional args")

    if not isinstance(kwargs, dict):
        return worker_error_response(request_id, "invalid keyword args")

    try:
        fn = resolve_function(fn_path, namespace)
        result = fn(*args, **kwargs)
        if is_coroutine(result):
            close = getattr(result, "close", None)
            if callable(close):
                close()

            return worker_error_response(
                request_id,
                "async worker functions are not supported in WASI",
                error_type="AsyncWorkerFunctionError",
            )
    except BaseException as exc:
        return worker_error_response(
            request_id,
            str(exc),
            error_type=type(exc).__name__,
        )

    return {
        "type": "worker_response",
        "id": request_id,
        "ok": True,
        "result": result,
    }


def worker_error_response(
    request_id: object,
    message: str,
    *,
    error_type: str = "WorkerCallError",
) -> dict[str, Any]:
    return {
        "type": "worker_response",
        "id": request_id,
        "ok": False,
        "error": {
            "type": error_type,
            "message": message,
        },
    }


def parse_fn_path(fn_path: object) -> tuple[str, ...] | None:
    if not isinstance(fn_path, list | tuple):
        return None

    if not all(isinstance(part, str) and part for part in fn_path):
        return None

    return tuple(fn_path)


def resolve_function(
    fn_path: tuple[str, ...],
    namespace: dict[str, Any],
) -> Any:
    if not fn_path:
        raise LookupError("empty function path")

    if fn_path == ("exec",):
        return make_exec(namespace)

    if fn_path == ("eval",):
        return make_eval(namespace)

    if fn_path[0] in namespace:
        value = namespace[fn_path[0]]
    else:
        value = getattr(builtins, fn_path[0])

    for part in fn_path[1:]:
        if isinstance(value, Mapping):
            value = value[part]
        else:
            value = getattr(value, part)

    if not callable(value):
        raise TypeError("resolved object is not callable")

    return value


def make_exec(namespace: dict[str, Any]):
    def exec_in_namespace(
        source: str | bytes | CodeType,
        globals: dict[str, Any] | None = None,
        locals: dict[str, Any] | None = None,
        *,
        closure: tuple[CellType, ...] | None = None,
    ) -> None:
        if globals is None:
            globals = namespace

        if closure is None:
            builtins.exec(source, globals, locals)
        else:
            builtins.exec(source, globals, locals, closure=closure)

    return exec_in_namespace


def make_eval(namespace: dict[str, Any]):
    def eval_in_namespace(
        source: str | bytes | CodeType,
        globals: dict[str, Any] | None = None,
        locals: dict[str, Any] | None = None,
    ) -> Any:
        if globals is None:
            globals = namespace

        return builtins.eval(source, globals, locals)

    return eval_in_namespace


def is_coroutine(value: object) -> bool:
    return hasattr(value, "send") and hasattr(value, "throw")


def get_messenger() -> Messenger:
    global _messenger

    if _messenger is None:
        configure_from_environment()

    if _messenger is None:
        raise HostCallError("host messenger is not configured")

    return _messenger


def next_request_id() -> int:
    global _next_request_id

    request_id = _next_request_id
    _next_request_id += 1
    return request_id


def make_proxy(method: str):
    def proxy(*args: Any, **kwargs: Any) -> Any:
        return call(method, *args, **kwargs)

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
