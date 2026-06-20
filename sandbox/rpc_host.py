import asyncio
import inspect
import os
import struct
import threading
import time
import traceback
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import cbor2

from messaging.messanger import Messanger, MessangerClosedError


RpcHandler = Callable[[Mapping[str, Any]], Any]
FRAME_HEADER_SIZE = 8
FRAME_HEADER_FORMAT = ">Q"


class RpcHostError(Exception):
    pass


@dataclass
class RpcHost:
    messanger: Messanger | None = None
    handlers: dict[str, RpcHandler] = field(default_factory=dict)

    def expose(self, method: str, handler: RpcHandler) -> RpcHandler:
        self.handlers[method] = handler
        return handler

    @property
    def methods(self) -> tuple[str, ...]:
        return tuple(self.handlers)

    def bind(self, messanger: Messanger) -> "RpcHost":
        return RpcHost(
            messanger=messanger,
            handlers=self.handlers,
        )

    def dispatch_next(self) -> None:
        if self.messanger is None:
            raise RpcHostError("RPC host is not bound to a messanger")

        message = self.messanger.receive_message()
        response = self.response_for_message(message)
        if response is not None:
            self.messanger.post_message(response)

    def dispatch_forever(self) -> None:
        while True:
            try:
                self.dispatch_next()
            except (MessangerClosedError, OSError, ValueError):
                return

    def dispatch_files_forever(
        self,
        request_dir: Path,
        response_dir: Path,
        stop: threading.Event,
        *,
        poll_interval: float = 0.001,
    ) -> None:
        while not stop.is_set():
            handled = False
            for request_path in sorted(request_dir.glob("*.cbor")):
                handled = True
                response_path = response_dir / request_path.name
                response_tmp_path = response_dir / f"{request_path.stem}.tmp"

                try:
                    message = cbor2.loads(request_path.read_bytes())
                except OSError:
                    continue

                request_path.unlink(missing_ok=True)
                response = self.response_for_message(message)
                if response is None:
                    continue

                response_tmp_path.write_bytes(cbor2.dumps(response))
                os.replace(response_tmp_path, response_path)

            if not handled:
                time.sleep(poll_interval)

    def dispatch_file_forever(
        self,
        request_file: Path,
        response_file: Path,
        stop: threading.Event,
        *,
        max_request_bytes: int,
        on_oversized_request: Callable[[], None],
        poll_interval: float = 0.001,
    ) -> None:
        seen_generation = 0

        while not stop.is_set():
            try:
                if request_file.stat().st_size > max_request_bytes:
                    on_oversized_request()
                    return

                data = request_file.read_bytes()
            except OSError:
                time.sleep(poll_interval)
                continue

            if frame_payload_size(data) > max_request_bytes:
                on_oversized_request()
                return

            frame = decode_file_frame(data)
            if frame is None:
                time.sleep(poll_interval)
                continue

            generation, message = frame
            if generation <= seen_generation:
                time.sleep(poll_interval)
                continue

            seen_generation = generation
            response = self.response_for_message(message)
            if response is None:
                continue

            response_file.write_bytes(encode_file_frame(generation, response))

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

    def send_result(self, request_id: object, result: Any) -> None:
        if self.messanger is None:
            raise RpcHostError("RPC host is not bound to a messanger")

        self.messanger.post_message(self.result_response(request_id, result))

    def send_error(
        self,
        request_id: object,
        message: str,
        *,
        error_type: str = "RpcHostError",
        traceback_text: str | None = None,
    ) -> None:
        if self.messanger is None:
            raise RpcHostError("RPC host is not bound to a messanger")

        self.messanger.post_message(
            self.error_response(
                request_id,
                message,
                error_type=error_type,
                traceback_text=traceback_text,
            )
        )

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


def encode_file_frame(generation: int, message: object) -> bytes:
    payload = cbor2.dumps(message)
    return (
        struct.pack(FRAME_HEADER_FORMAT, generation)
        + struct.pack(FRAME_HEADER_FORMAT, len(payload))
        + payload
    )


def decode_file_frame(data: bytes) -> tuple[int, object] | None:
    if len(data) < FRAME_HEADER_SIZE * 2:
        return None

    generation = struct.unpack(FRAME_HEADER_FORMAT, data[:FRAME_HEADER_SIZE])[0]
    size = struct.unpack(
        FRAME_HEADER_FORMAT,
        data[FRAME_HEADER_SIZE : FRAME_HEADER_SIZE * 2],
    )[0]
    end = FRAME_HEADER_SIZE * 2 + size
    if len(data) < end:
        return None

    return generation, cbor2.loads(data[FRAME_HEADER_SIZE * 2 : end])


def frame_payload_size(data: bytes) -> int:
    if len(data) < FRAME_HEADER_SIZE * 2:
        return 0

    return struct.unpack(
        FRAME_HEADER_FORMAT,
        data[FRAME_HEADER_SIZE : FRAME_HEADER_SIZE * 2],
    )[0]


def resolve_handler_result(result: Any) -> Any:
    if not inspect.iscoroutine(result):
        return result

    return asyncio.run(result)
