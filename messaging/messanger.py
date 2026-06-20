import struct
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, TypeAlias

import cbor2


Message: TypeAlias = object
MessageHandler: TypeAlias = Callable[[Message], None]

HEADER_SIZE = 8
HEADER_FORMAT = ">Q"
DEFAULT_MAX_MESSAGE_BYTES = 256 * 1024


class Transport(Protocol):
    def read(self, size: int | None = -1, /) -> bytes:
        raise NotImplementedError

    def write(self, packet: bytes, /) -> object:
        raise NotImplementedError


class MessangerError(Exception):
    """Base error for messenger failures."""


class MessangerClosedError(MessangerError):
    """Raised when a message stream closes before a full frame is read."""


class MessangerMessageTooLargeError(MessangerError):
    """Raised when a frame exceeds the configured message size limit."""


@dataclass
class Messanger:
    transport: Transport
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES

    def __post_init__(self) -> None:
        if self.max_message_bytes <= 0:
            raise ValueError("max_message_bytes must be positive")

        self._handlers: list[MessageHandler] = []

    def post_message(self, message: Message) -> None:
        payload = cbor2.dumps(message)
        if len(payload) > self.max_message_bytes:
            raise MessangerMessageTooLargeError(
                f"message exceeds {self.max_message_bytes} bytes"
            )

        self.transport.write(struct.pack(HEADER_FORMAT, len(payload)) + payload)

    def on_message(self, handler: MessageHandler) -> MessageHandler:
        self._handlers.append(handler)
        return handler

    def remove_message_handler(self, handler: MessageHandler) -> None:
        self._handlers.remove(handler)

    def receive_message(self) -> Message:
        header = self._read_exact(HEADER_SIZE)
        (size,) = struct.unpack(HEADER_FORMAT, header)

        if size > self.max_message_bytes:
            raise MessangerMessageTooLargeError(
                f"incoming message exceeds {self.max_message_bytes} bytes"
            )

        return cbor2.loads(self._read_exact(size))

    def dispatch_next(self) -> Message:
        message = self.receive_message()

        for handler in tuple(self._handlers):
            handler(message)

        return message

    def dispatch_forever(self) -> None:
        while True:
            self.dispatch_next()

    def close(self) -> None:
        close = getattr(self.transport, "close", None)
        if callable(close):
            close()

    def _read_exact(self, size: int) -> bytes:
        chunks = bytearray()

        while len(chunks) < size:
            chunk = self.transport.read(size - len(chunks))
            if not chunk:
                raise MessangerClosedError("message stream closed")

            chunks.extend(chunk)

        return bytes(chunks)
