import io
import runpy
from collections.abc import Buffer
from pathlib import Path
from unittest.mock import patch


def test_atomic_bytecode_write_handles_short_writes(tmp_path: Path) -> None:
  compiler = runpy.run_path(
    str(Path(__file__).parents[1] / "rust/pysandbox-sandboxd/src/compile_stdlib.py")
  )
  file_io = io.FileIO

  class ShortWriteFile(io.RawIOBase):
    def __init__(self, descriptor: int, mode: str) -> None:
      super().__init__()
      self.raw = file_io(descriptor, mode)

    def writable(self) -> bool:
      return True

    def write(self, data: Buffer) -> int:
      return self.raw.write(memoryview(data)[:65536])

    def close(self) -> None:
      self.raw.close()
      super().close()

  destination = tmp_path / "module.pyc"
  bytecode = bytes(range(256)) * 1025
  with patch.object(io, "FileIO", ShortWriteFile):
    compiler["write_atomic"](str(destination), bytecode)

  assert destination.read_bytes() == bytecode
  assert list(tmp_path.iterdir()) == [destination]
