import asyncio
import ctypes
import inspect
import os
import select
import threading
import traceback
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO

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
        waiter = request_waiter(request_files, poll_interval=poll_interval)

        try:
            while not stop.is_set():
                try:
                    unread_size = transport.total_available_bytes()
                except OSError:
                    waiter.wait(stop)
                    continue

                if unread_size > max_request_bytes:
                    on_oversized_request()
                    return

                if unread_size <= 0:
                    waiter.wait(stop)
                    continue

                self.dispatch_next_with(messanger)
        finally:
            waiter.close()
            messanger.close()

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


class RequestWaiter:
    def wait(self, stop: threading.Event) -> None:
        raise NotImplementedError

    def close(self) -> None:
        return


@dataclass
class PollingRequestWaiter(RequestWaiter):
    poll_interval: float

    def wait(self, stop: threading.Event) -> None:
        stop.wait(self.poll_interval)


@dataclass
class KqueueRequestWaiter(RequestWaiter):
    paths: tuple[Path, Path]
    poll_interval: float
    streams: list[BinaryIO] = field(default_factory=list)
    queue: select.kqueue | None = None

    def __post_init__(self) -> None:
        self.queue = select.kqueue()
        events: list[select.kevent] = []
        flags = select.KQ_EV_ADD | select.KQ_EV_ENABLE | select.KQ_EV_CLEAR
        fflags = (
            select.KQ_NOTE_WRITE
            | select.KQ_NOTE_EXTEND
            | select.KQ_NOTE_DELETE
            | select.KQ_NOTE_RENAME
            | select.KQ_NOTE_ATTRIB
        )

        for path in self.paths:
            stream = path.open("rb", buffering=0)
            self.streams.append(stream)
            events.append(
                select.kevent(
                    stream.fileno(),
                    filter=select.KQ_FILTER_VNODE,
                    flags=flags,
                    fflags=fflags,
                )
            )

        self.queue.control(events, 0, 0)

    def wait(self, stop: threading.Event) -> None:
        if self.queue is None:
            stop.wait(self.poll_interval)
            return

        while not stop.is_set():
            events = self.queue.control(None, 1, self.poll_interval)
            if events:
                return

    def close(self) -> None:
        if self.queue is not None:
            self.queue.close()
            self.queue = None

        for stream in self.streams:
            stream.close()

        self.streams.clear()


def request_waiter(
    request_files: tuple[Path, Path],
    *,
    poll_interval: float,
) -> RequestWaiter:
    if os.name == "nt":
        with suppress(OSError):
            return WindowsDirectoryChangesRequestWaiter(
                paths=request_files,
                poll_interval=poll_interval,
            )

    if hasattr(select, "kqueue"):
        with suppress(OSError):
            return KqueueRequestWaiter(
                paths=request_files,
                poll_interval=poll_interval,
            )

    return PollingRequestWaiter(poll_interval=poll_interval)


FILE_LIST_DIRECTORY = 0x0001
FILE_SHARE_READ = 0x00000001
FILE_SHARE_WRITE = 0x00000002
FILE_SHARE_DELETE = 0x00000004
OPEN_EXISTING = 3
FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
FILE_FLAG_OVERLAPPED = 0x40000000
FILE_NOTIFY_CHANGE_FILE_NAME = 0x00000001
FILE_NOTIFY_CHANGE_SIZE = 0x00000008
FILE_NOTIFY_CHANGE_LAST_WRITE = 0x00000010
WAIT_OBJECT_0 = 0
WAIT_TIMEOUT = 0x00000102
WAIT_FAILED = 0xFFFFFFFF
ERROR_IO_PENDING = 997
ERROR_OPERATION_ABORTED = 995
INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
_windows_kernel32: ctypes.CDLL | None = None


class WindowsOverlapped(ctypes.Structure):
    _fields_ = [
        ("Internal", ctypes.c_void_p),
        ("InternalHigh", ctypes.c_void_p),
        ("Offset", ctypes.c_uint32),
        ("OffsetHigh", ctypes.c_uint32),
        ("hEvent", ctypes.c_void_p),
    ]


@dataclass
class WindowsDirectoryChangesRequestWaiter(RequestWaiter):
    paths: tuple[Path, Path]
    poll_interval: float
    directory: Path | None = None
    handle: int | None = None
    event: int | None = None

    def __post_init__(self) -> None:
        directories = {path.parent.resolve() for path in self.paths}
        if len(directories) != 1:
            raise OSError("Windows request files must be in one directory")

        self.directory = directories.pop()
        kernel32 = windows_kernel32()
        handle = kernel32.CreateFileW(
            str(self.directory),
            FILE_LIST_DIRECTORY,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OVERLAPPED,
            None,
        )
        if handle == INVALID_HANDLE_VALUE:
            raise windows_error()

        event = kernel32.CreateEventW(None, True, False, None)
        if not event:
            kernel32.CloseHandle(handle)
            raise windows_error()

        self.handle = handle
        self.event = event

    def wait(self, stop: threading.Event) -> None:
        if self.handle is None or self.event is None:
            stop.wait(self.poll_interval)
            return

        kernel32 = windows_kernel32()
        kernel32.ResetEvent(self.event)
        buffer = ctypes.create_string_buffer(4096)
        overlapped = WindowsOverlapped()
        overlapped.hEvent = self.event
        bytes_returned = ctypes.c_uint32()
        ok = kernel32.ReadDirectoryChangesW(
            self.handle,
            buffer,
            len(buffer),
            False,
            FILE_NOTIFY_CHANGE_FILE_NAME
            | FILE_NOTIFY_CHANGE_SIZE
            | FILE_NOTIFY_CHANGE_LAST_WRITE,
            ctypes.byref(bytes_returned),
            ctypes.byref(overlapped),
            None,
        )

        if not ok:
            error = windows_last_error()
            if error != ERROR_IO_PENDING:
                raise windows_error(error)

        try:
            while not stop.is_set():
                timeout_ms = max(1, int(self.poll_interval * 1000))
                result = kernel32.WaitForSingleObject(self.event, timeout_ms)
                if result == WAIT_OBJECT_0:
                    return

                if result == WAIT_TIMEOUT:
                    continue

                if result == WAIT_FAILED:
                    raise windows_error()
        finally:
            if stop.is_set():
                kernel32.CancelIoEx(self.handle, ctypes.byref(overlapped))

    def close(self) -> None:
        kernel32 = windows_kernel32()

        if self.handle is not None:
            kernel32.CancelIoEx(self.handle, None)
            kernel32.CloseHandle(self.handle)
            self.handle = None

        if self.event is not None:
            kernel32.CloseHandle(self.event)
            self.event = None


def windows_kernel32() -> ctypes.CDLL:
    global _windows_kernel32

    if _windows_kernel32 is not None:
        return _windows_kernel32

    windll: type[ctypes.CDLL] = getattr(ctypes, "WinDLL")
    kernel32 = windll("kernel32", use_last_error=True)
    kernel32.CreateFileW.restype = ctypes.c_void_p
    kernel32.CreateEventW.restype = ctypes.c_void_p
    kernel32.ReadDirectoryChangesW.restype = ctypes.c_int
    kernel32.WaitForSingleObject.restype = ctypes.c_uint32
    kernel32.ResetEvent.restype = ctypes.c_int
    kernel32.CancelIoEx.restype = ctypes.c_int
    kernel32.CloseHandle.restype = ctypes.c_int
    _windows_kernel32 = kernel32
    return kernel32


def windows_last_error() -> int:
    get_last_error: Callable[[], int] = getattr(ctypes, "get_last_error")
    return int(get_last_error())


def windows_error(error: int | None = None) -> OSError:
    win_error: Callable[..., OSError] = getattr(ctypes, "WinError")
    if error is None:
        return win_error()

    return win_error(error)
