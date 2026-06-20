import struct
import time
from dataclasses import dataclass, field
from pathlib import Path


DATA_FRAME = b"D"
SWITCH_FRAME = b"S"
FRAME_HEADER_SIZE = 9
FRAME_HEADER_FORMAT = ">Q"
DEFAULT_ROTATE_AFTER_BYTES = 1024 * 1024


@dataclass
class FileTransport:
    read_paths: tuple[str | Path, str | Path]
    write_paths: tuple[str | Path, str | Path]
    poll_interval: float = 0.001
    rotate_after_bytes: int = DEFAULT_ROTATE_AFTER_BYTES
    read_index: int = 0
    write_index: int = 0
    offsets: list[int] = field(default_factory=lambda: [0, 0])
    _buffer: bytes = b""

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
        self.append_frame(self.write_file, DATA_FRAME, packet)
        return None

    @property
    def read_file(self) -> Path:
        return Path(self.read_paths[self.read_index])

    @property
    def write_file(self) -> Path:
        return Path(self.write_paths[self.write_index])

    def read_data_frame(self) -> bytes:
        while True:
            frame = self.read_next_frame()
            if frame is None:
                time.sleep(self.poll_interval)
                continue

            frame_type, payload = frame
            if frame_type == DATA_FRAME:
                return payload

            if frame_type == SWITCH_FRAME:
                previous_index = self.read_index
                self.read_index = 1 - self.read_index
                self.truncate_file(Path(self.read_paths[previous_index]))
                self.offsets[previous_index] = 0
                continue

    def read_next_frame(self) -> tuple[bytes, bytes] | None:
        path = self.read_file
        offset = self.offsets[self.read_index]

        try:
            with path.open("rb") as stream:
                stream.seek(offset)
                header = stream.read(FRAME_HEADER_SIZE)
                if len(header) < FRAME_HEADER_SIZE:
                    return None

                frame_type = header[:1]
                size = struct.unpack(FRAME_HEADER_FORMAT, header[1:])[0]
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

        self.append_frame(self.write_file, SWITCH_FRAME, b"")
        self.write_index = 1 - self.write_index

    def append_frame(self, path: Path, frame_type: bytes, payload: bytes) -> None:
        with path.open("ab") as stream:
            stream.write(
                frame_type
                + struct.pack(FRAME_HEADER_FORMAT, len(payload))
                + payload
            )

    def available_bytes(self) -> int:
        return max(0, self.file_size(self.read_file) - self.offsets[self.read_index])

    def total_available_bytes(self) -> int:
        total = 0
        for index, path in enumerate(self.read_paths):
            total += max(0, self.file_size(Path(path)) - self.offsets[index])

        return total

    def close(self) -> None:
        return None

    @staticmethod
    def frame_size(payload_size: int) -> int:
        return FRAME_HEADER_SIZE + payload_size

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
