from abc import ABC, abstractmethod
from collections.abc import Awaitable, Sequence
from dataclasses import dataclass
from typing import Literal

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


class VirtualFileSystem(ABC):
  """Host-backed filesystem.

  ``stat``, ``read``, and ``list`` are required. The remaining operations are
  optional, but determine which guest filesystem features are available:

  - ``write`` enables file creation and writes. It is also required by every
    fallback below.
  - ``delete`` enables file and directory removal and is required by the file
    rename fallback.
  - ``mkdir`` enables directory creation.
  - ``append`` is optional when ``write`` is available; the fallback writes at
    the size reported by ``stat``.
  - ``truncate`` is optional when ``read`` and ``write`` are available; its
    fallback reads and rewrites the complete file.
  - ``rename`` is optional for files when ``read``, ``write``, and ``delete``
    are available. That fallback is non-atomic. Directory rename requires an
    explicit ``rename`` implementation.

  Unsupported optional operations may retain their default implementations,
  which raise ``NotImplementedError``.
  """

  @abstractmethod
  def stat(self, path: str) -> VfsMetadata | Awaitable[VfsMetadata]:
    raise NotImplementedError

  @abstractmethod
  def read(self, path: str) -> bytes | Awaitable[bytes]:
    raise NotImplementedError

  def write(
    self,
    path: str,
    data: bytes,
    offset: int | None,
  ) -> None | Awaitable[None]:
    raise NotImplementedError

  def delete(self, path: str) -> None | Awaitable[None]:
    raise NotImplementedError

  def mkdir(self, path: str) -> None | Awaitable[None]:
    raise NotImplementedError

  def append(self, path: str, data: bytes) -> None | Awaitable[None]:
    raise NotImplementedError

  def truncate(self, path: str, size: int) -> None | Awaitable[None]:
    raise NotImplementedError

  def rename(self, source: str, destination: str) -> None | Awaitable[None]:
    raise NotImplementedError

  @abstractmethod
  def list(
    self,
    path: str,
  ) -> Sequence[VfsDirectoryEntry] | Awaitable[Sequence[VfsDirectoryEntry]]:
    raise NotImplementedError
