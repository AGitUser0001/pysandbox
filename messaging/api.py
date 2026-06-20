import os
import keyword
import struct
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import cbor2

from messaging.messanger import Messanger


FRAME_HEADER_SIZE = 8
FRAME_HEADER_FORMAT = ">Q"


class HostCallError(Exception):
    pass


_messanger: Messanger | None = None
_file_transport: "FileTransport | None" = None
_next_request_id = 0
__all__: list[str] = []


def configure(messanger: Messanger) -> None:
    global _messanger
    _messanger = messanger


def configure_file_transport(request_file: str, response_file: str) -> None:
    global _file_transport
    _file_transport = FileTransport(
        request_file=request_file,
        response_file=response_file,
    )


def configure_from_environment() -> None:
    request_fd = os.environ.get("PYSANDBOX_RPC_REQUEST_FD")
    response_fd = os.environ.get("PYSANDBOX_RPC_RESPONSE_FD")
    if request_fd is not None and response_fd is not None:
        reader = os.fdopen(int(response_fd), "rb", buffering=0)
        writer = os.fdopen(int(request_fd), "wb", buffering=0)
        configure(Messanger(reader=reader, writer=writer))
        return

    request_file = os.environ.get("PYSANDBOX_RPC_REQUEST_FILE")
    response_file = os.environ.get("PYSANDBOX_RPC_RESPONSE_FILE")
    if request_file is not None and response_file is not None:
        configure_file_transport(request_file, response_file)
        return

    requests_path = os.environ["PYSANDBOX_RPC_REQUESTS"]
    responses_path = os.environ["PYSANDBOX_RPC_RESPONSES"]
    reader = open(responses_path, "rb", buffering=0)
    writer = open(requests_path, "wb", buffering=0)
    configure(Messanger(reader=reader, writer=writer))


def call(method: str, params: Mapping[str, Any] | None = None) -> Any:
    request_id = next_request_id()
    request = {
        "type": "request",
        "id": request_id,
        "method": method,
        "params": dict(params or {}),
    }

    file_transport = get_file_transport()
    if file_transport is not None:
        message = file_transport.call(request_id, request)
    else:
        messanger = get_messanger()
        messanger.post_message(request)
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


def get_file_transport() -> "FileTransport | None":
    global _file_transport

    if _messanger is None and _file_transport is None:
        configure_from_environment()

    return _file_transport


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


@dataclass(frozen=True)
class FileTransport:
    request_file: str
    response_file: str
    poll_interval: float = 0.001

    def call(self, request_id: int, request: Mapping[str, Any]) -> object:
        generation = request_id + 1

        with open(self.request_file, "r+b") as stream:
            stream.truncate(0)
            stream.seek(0)
            stream.write(encode_file_frame(generation, dict(request)))

        while True:
            try:
                with open(self.response_file, "rb") as stream:
                    data = stream.read()
            except FileNotFoundError:
                time.sleep(self.poll_interval)
                continue

            frame = decode_file_frame(data)
            if frame is None:
                time.sleep(self.poll_interval)
                continue

            response_generation, message = frame
            if response_generation == generation:
                return message

            time.sleep(self.poll_interval)


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


configure_api_from_environment()
