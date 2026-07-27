from collections.abc import Awaitable, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

__all__ = [
  "VfsDirectoryEntry",
  "VfsMetadata",
  "VirtualFileSystem",
]


@dataclass(frozen=True, slots=True)
class VfsMetadata:
  kind: Literal["file", "directory"]
  size: int = 0


@dataclass(frozen=True, slots=True)
class VfsDirectoryEntry:
  name: str
  kind: Literal["file", "directory"]
  size: int = 0


class VirtualFileSystem(Protocol):
  def stat(self, path: str) -> VfsMetadata | Awaitable[VfsMetadata]: ...

  def read(self, path: str) -> bytes | Awaitable[bytes]: ...

  def list(
    self,
    path: str,
  ) -> Sequence[VfsDirectoryEntry] | Awaitable[Sequence[VfsDirectoryEntry]]: ...
