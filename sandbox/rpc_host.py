import asyncio
import inspect
import threading
import time
import traceback
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from messaging.messanger import Messanger
from messaging.transports import FileTransport


RpcHandler = Callable[[Mapping[str, Any]], Any]


@dataclass
class RpcHost:
    handlers: dict[str, RpcHandler] = field(default_factory=dict)

    def expose(self, method: str, handler: RpcHandler) -> RpcHandler:
        self.handlers[method] = handler
        return handler

    @property
    def methods(self) -> tuple[str, ...]:
        return tuple(self.handlers)

    def dispatch_file_forever(
        self,
        request_files: tuple[Path, Path],
        response_files: tuple[Path, Path],
        stop: threading.Event,
        *,
        max_request_bytes: int,
        on_oversized_request: Callable[[], None],
        poll_interval: float = 0.001,
    ) -> None:
        transport = FileTransport(
            read_paths=request_files,
            write_paths=response_files,
            poll_interval=poll_interval,
        )
        messanger = Messanger(transport)

        while not stop.is_set():
            try:
                unread_size = transport.total_available_bytes()
            except OSError:
                time.sleep(poll_interval)
                continue

            if unread_size > max_request_bytes:
                on_oversized_request()
                return

            if unread_size <= 0:
                time.sleep(poll_interval)
                continue

            self.dispatch_next_with(messanger)

    def dispatch_next_with(self, messanger: Messanger) -> None:
        message = messanger.receive_message()
        response = self.response_for_message(message)
        if response is not None:
            messanger.post_message(response)

    def response_for_message(self, message: object) -> dict[str, Any] | None:
        if not isinstance(message, dict):
            return None

        if message.get("type") != "request":
            return None

        request_id = message.get("id")
        method = message.get("method")
        params = message.get("params", {})

        if not isinstance(method, str):
            return self.error_response(request_id, "invalid request method")

        if not isinstance(params, dict):
            return self.error_response(request_id, "invalid request params")

        handler = self.handlers.get(method)
        if handler is None:
            return self.error_response(request_id, f"unknown method: {method}")

        try:
            result = resolve_handler_result(handler(params))
        except BaseException as exc:
            return self.error_response(
                request_id,
                str(exc),
                error_type=type(exc).__name__,
                traceback_text=traceback.format_exc(),
            )

        return self.result_response(request_id, result)

    def result_response(self, request_id: object, result: Any) -> dict[str, Any]:
        return {
            "type": "response",
            "id": request_id,
            "ok": True,
            "result": result,
        }

    def error_response(
        self,
        request_id: object,
        message: str,
        *,
        error_type: str = "RpcHostError",
        traceback_text: str | None = None,
    ) -> dict[str, Any]:
        error: dict[str, Any] = {
            "type": error_type,
            "message": message,
        }
        if traceback_text is not None:
            error["traceback"] = traceback_text

        return {
            "type": "response",
            "id": request_id,
            "ok": False,
            "error": error,
        }


def resolve_handler_result(result: Any) -> Any:
    if not inspect.iscoroutine(result):
        return result

    return asyncio.run(result)
