import struct
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO


__all__ = [
    "FileTransport",
    "FileTransportError",
    "FileTransportFileTooLargeError",
    "FileTransportFrameTooLargeError",
    "FileTransportStoppedError",
]


DATA_FRAME = b"D"
SWITCH_FRAME = b"S"
TRUNCATE_FRAME = b"T"
FRAME_HEADER_SIZE = 9
FRAME_HEADER_FORMAT = ">Q"
DEFAULT_ROTATE_AFTER_BYTES = 1024 * 1024
DEFAULT_MAX_FILE_BYTES = DEFAULT_ROTATE_AFTER_BYTES * 2


class FileTransportError(Exception):
    """Base error for file transport failures."""


class FileTransportFrameTooLargeError(FileTransportError):
    """Raised when a file transport frame exceeds the configured limit."""


class FileTransportFileTooLargeError(FileTransportError):
    """Raised when a transport direction exceeds its combined file quota."""


class FileTransportStoppedError(FileTransportError):
    """Raised when a file transport read is stopped before a frame completes."""


@dataclass
class FileTransport:
    read_paths: tuple[str | Path, str | Path]
    write_paths: tuple[str | Path, str | Path]
    poll_interval: float = 0.001
    rotate_after_bytes: int = DEFAULT_ROTATE_AFTER_BYTES
    max_frame_bytes: int | Callable[[], int | None] | None = None
    max_file_bytes: int | Callable[[], int | None] | None = DEFAULT_MAX_FILE_BYTES
    stop: threading.Event | None = None
    read_index: int = 0
    write_index: int = 0
    offsets: list[int] = field(default_factory=lambda: [0, 0])
    _buffer: bytes = b""
    _read_streams: list[BinaryIO | None] = field(default_factory=lambda: [None, None])
    _write_streams: list[BinaryIO | None] = field(default_factory=lambda: [None, None])

    def read(self, size: int | None = -1, /) -> bytes:
        while not self._buffer:
            self._buffer = self.read_data_frame()

        if size is None or size < 0:
            size = len(self._buffer)

        chunk = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return chunk

    def write(self, packet: bytes, /) -> object:
        self.rotate_writer_if_needed(len(packet))
        self.append_frame(DATA_FRAME, packet)
        return None

    @property
    def read_file(self) -> Path:
        return Path(self.read_paths[self.read_index])

    @property
    def write_file(self) -> Path:
        return Path(self.write_paths[self.write_index])

    @property
    def read_stream(self) -> BinaryIO:
        stream = self._read_streams[self.read_index]
        if stream is None:
            stream = self.read_file.open("rb", buffering=0)
            self._read_streams[self.read_index] = stream

        return stream

    @property
    def write_stream(self) -> BinaryIO:
        stream = self._write_streams[self.write_index]
        if stream is None:
            stream = self.write_file.open("ab", buffering=0)
            self._write_streams[self.write_index] = stream

        return stream

    def read_data_frame(self) -> bytes:
        while True:
            self.check_stopped()
            frame = self.read_next_frame()
            if frame is None:
                self.check_stopped()
                time.sleep(self.poll_interval)
                continue

            frame_type, payload = frame
            if frame_type == DATA_FRAME:
                return payload

            if frame_type == SWITCH_FRAME:
                previous_index = self.read_index
                self.read_index = 1 - self.read_index
                if not self.truncate_read_file(previous_index):
                    self.request_remote_truncate(previous_index)

                self.offsets[previous_index] = 0
                continue

            if frame_type == TRUNCATE_FRAME:
                self.truncate_requested_write_file(payload)
                continue

    def read_next_frame(self) -> tuple[bytes, bytes] | None:
        offset = self.offsets[self.read_index]

        try:
            stream = self.read_stream
            stream.seek(offset)
            header = stream.read(FRAME_HEADER_SIZE)
            if len(header) < FRAME_HEADER_SIZE:
                return None

            frame_type = header[:1]
            size = struct.unpack(FRAME_HEADER_FORMAT, header[1:])[0]
            max_frame_bytes = self.current_limit(self.max_frame_bytes)
            if max_frame_bytes is not None and size > max_frame_bytes:
                raise FileTransportFrameTooLargeError(
                    f"file transport frame exceeds {max_frame_bytes} bytes"
                )

            payload = stream.read(size)
            if len(payload) < size:
                return None
        except FileNotFoundError:
            return None

        self.offsets[self.read_index] += FRAME_HEADER_SIZE + size
        return frame_type, payload

    def rotate_writer_if_needed(self, packet_size: int) -> None:
        if self.rotate_after_bytes <= 0:
            return

        current_size = self.file_size(self.write_file)
        if current_size + self.frame_size(packet_size) < self.rotate_after_bytes:
            return

        if self.file_size(Path(self.write_paths[1 - self.write_index])) > 0:
            return

        self.append_frame(SWITCH_FRAME, b"")
        self.write_index = 1 - self.write_index

    def append_frame(self, frame_type: bytes, payload: bytes) -> None:
        self.check_write_file_size_limit(self.frame_size(len(payload)))
        stream = self.write_stream
        stream.write(
            frame_type
            + struct.pack(FRAME_HEADER_FORMAT, len(payload))
            + payload
        )
        stream.flush()

    def available_bytes(self) -> int:
        return max(0, self.file_size(self.read_file) - self.offsets[self.read_index])

    def total_available_bytes(self) -> int:
        total = 0
        for index, path in enumerate(self.read_paths):
            total += max(0, self.file_size(Path(path)) - self.offsets[index])

        return total

    def total_file_bytes(self) -> int:
        return sum(self.file_size(Path(path)) for path in self.read_paths)

    def total_write_file_bytes(self) -> int:
        return sum(self.file_size(Path(path)) for path in self.write_paths)

    def check_file_size_limit(self) -> None:
        max_file_bytes = self.current_limit(self.max_file_bytes)
        if max_file_bytes is None:
            return

        total = self.total_file_bytes()
        if total > max_file_bytes:
            raise FileTransportFileTooLargeError(
                f"file transport files exceed {max_file_bytes} bytes"
            )

    def check_write_file_size_limit(self, additional_bytes: int) -> None:
        max_file_bytes = self.current_limit(self.max_file_bytes)
        if max_file_bytes is None:
            return

        total = self.total_write_file_bytes() + additional_bytes
        if total > max_file_bytes:
            raise FileTransportFileTooLargeError(
                f"file transport files would exceed {max_file_bytes} bytes"
            )

    def check_stopped(self) -> None:
        if self.stop is not None and self.stop.is_set():
            raise FileTransportStoppedError("file transport read stopped")

    def close(self) -> None:
        for stream in (*self._read_streams, *self._write_streams):
            if stream is not None:
                stream.close()

    @staticmethod
    def frame_size(payload_size: int) -> int:
        return FRAME_HEADER_SIZE + payload_size

    @staticmethod
    def current_limit(
        limit: int | Callable[[], int | None] | None,
    ) -> int | None:
        if callable(limit):
            limit = limit()

        if limit is None:
            return None

        if limit <= 0:
            raise ValueError("transport limits must be positive")

        return limit

    @staticmethod
    def file_size(path: Path) -> int:
        try:
            return path.stat().st_size
        except FileNotFoundError:
            return 0

    @staticmethod
    def truncate_file(path: Path) -> None:
        with path.open("r+b") as stream:
            stream.truncate(0)

    def request_remote_truncate(self, index: int) -> None:
        self.append_frame(TRUNCATE_FRAME, bytes((index,)))

    def truncate_requested_write_file(self, payload: bytes) -> None:
        if len(payload) != 1:
            return

        index = payload[0]
        if index not in {0, 1}:
            return

        path = Path(self.write_paths[index])
        self.truncate_file(path)

        stream = self._write_streams[index]
        if stream is not None:
            stream.seek(0)

    def truncate_read_file(self, index: int) -> bool:
        path = Path(self.read_paths[index])
        try:
            self.truncate_file(path)
        except PermissionError:
            return False

        stream = self._read_streams[index]
        if stream is not None:
            stream.seek(0)

        return True
