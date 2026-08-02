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
  read: bool = True
  write: bool = False


@dataclass(frozen=True, slots=True)
class VfsDirectoryEntry:
  name: str
  kind: Literal["file", "directory"]
  size: int = 0
  read: bool = True
  write: bool = False


class VirtualFileSystem(Protocol):
  """Host-backed filesystem.

  Implementations may provide ``append(path, data)``, ``truncate(path, size)``,
  and ``rename(source, destination)`` for native operations. File truncation and
  rename have non-atomic fallbacks; directory rename requires the native method.
  """

  def stat(self, path: str) -> VfsMetadata | Awaitable[VfsMetadata]: ...

  def read(self, path: str) -> bytes | Awaitable[bytes]: ...

  def write(
    self,
    path: str,
    data: bytes,
    offset: int | None,
  ) -> None | Awaitable[None]: ...

  def delete(self, path: str) -> None | Awaitable[None]: ...

  def mkdir(self, path: str) -> None | Awaitable[None]: ...

  def list(
    self,
    path: str,
  ) -> Sequence[VfsDirectoryEntry] | Awaitable[Sequence[VfsDirectoryEntry]]: ...
