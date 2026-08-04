import asyncio
import errno
from pathlib import Path
from tempfile import TemporaryDirectory

from pysandbox import (
  PythonRuntime,
  RpcContext,
  RuntimeResult,
  VfsDirectoryEntry,
  VfsMetadata,
  VirtualFileSystem,
)


class MemoryVfs(VirtualFileSystem):
  def __init__(self) -> None:
    self.files = {"/hello.py": b"value = 42\n"}
    self.directories = {"/"}
    self.writable: set[str] = set()
    self.unreadable: set[str] = set()
    self.root_write = False
    self.read_errors: set[str] = set()
    self.calls: list[tuple[str, str]] = []

  async def stat(self, path: str) -> VfsMetadata:
    self.calls.append(("stat", path))
    if path in self.directories:
      return VfsMetadata("directory", write=self.root_write)
    if path in self.files:
      data = self.files[path]
      return VfsMetadata(
        "file",
        len(data),
        read=path not in self.unreadable,
        write=path in self.writable,
      )
    raise FileNotFoundError(path)

  async def read(self, path: str) -> bytes:
    self.calls.append(("read", path))
    if path in self.read_errors:
      raise FileNotFoundError(path)
    try:
      return self.files[path]
    except KeyError:
      raise FileNotFoundError(path) from None

  async def write(self, path: str, data: bytes, offset: int | None) -> None:
    self.calls.append(("write", path))
    if path not in self.files:
      if not self.root_write:
        raise PermissionError(path)
      self.files[path] = b""
      self.writable.add(path)
    elif path not in self.writable:
      raise PermissionError(path)
    if offset is None:
      self.files[path] = data
      return
    contents = bytearray(self.files[path])
    if offset > len(contents):
      contents.extend(b"\0" * (offset - len(contents)))
    end = offset + len(data)
    if end > len(contents):
      contents.extend(b"\0" * (end - len(contents)))
    contents[offset:end] = data
    self.files[path] = bytes(contents)

  async def delete(self, path: str) -> None:
    self.calls.append(("delete", path))
    if path in self.files:
      del self.files[path]
      self.writable.discard(path)
      return
    if path in self.directories:
      prefix = path.rstrip("/") + "/"
      entries = set(self.files) | self.directories
      if any(name != path and name.startswith(prefix) for name in entries):
        raise OSError(errno.ENOTEMPTY, "directory not empty", path)
      self.directories.remove(path)
      return
    raise FileNotFoundError(path)

  async def mkdir(self, path: str) -> None:
    self.calls.append(("mkdir", path))
    if path in self.files or path in self.directories:
      raise FileExistsError(path)
    self.directories.add(path)

  async def list(self, path: str) -> list[VfsDirectoryEntry]:
    self.calls.append(("list", path))
    if path != "/":
      raise NotADirectoryError(path)
    return [
      VfsDirectoryEntry(name.removeprefix("/"), "file", len(data))
      for name, data in self.files.items()
      if "/" not in name.removeprefix("/")
    ]


class DirectoryVfs(VirtualFileSystem):
  def __init__(self, root: Path) -> None:
    self.root = root.resolve()

  def stat(self, path: str) -> VfsMetadata:
    local = self._path(path)
    if local.is_dir():
      return VfsMetadata("directory")
    if local.is_file():
      return VfsMetadata("file", local.stat().st_size)
    raise FileNotFoundError(path)

  def read(self, path: str) -> bytes:
    return self._path(path).read_bytes()

  def write(self, path: str, data: bytes, offset: int | None) -> None:
    raise PermissionError(path)

  def delete(self, path: str) -> None:
    raise PermissionError(path)

  def mkdir(self, path: str) -> None:
    raise PermissionError(path)

  def list(self, path: str) -> list[VfsDirectoryEntry]:
    local = self._path(path)
    if not local.is_dir():
      raise NotADirectoryError(path)
    return [
      VfsDirectoryEntry(
        entry.name,
        "directory" if entry.is_dir() else "file",
        0 if entry.is_dir() else entry.stat().st_size,
      )
      for entry in local.iterdir()
    ]

  def _path(self, path: str) -> Path:
    local = (self.root / path.lstrip("/")).resolve()
    if not local.is_relative_to(self.root):
      raise PermissionError(path)
    return local


class NativeMutationVfs(MemoryVfs):
  async def append(self, path: str, data: bytes) -> None:
    self.calls.append(("append", path))
    self.files[path] += data

  async def truncate(self, path: str, size: int) -> None:
    self.calls.append(("truncate", path))
    data = self.files[path]
    self.files[path] = data[:size].ljust(size, b"\0")

  async def rename(self, source: str, destination: str) -> None:
    self.calls.append(("rename", source))
    if source in self.files:
      self.files[destination] = self.files.pop(source)
      if source in self.writable:
        self.writable.remove(source)
        self.writable.add(destination)
      return
    if source in self.directories:
      self.directories.remove(source)
      self.directories.add(destination)
      return
    raise FileNotFoundError(source)


class TestVfs:
  async def test_file_mutations_use_fallbacks(self) -> None:
    vfs = MemoryVfs()
    vfs.root_write = True
    vfs.files["/source.bin"] = b"abcdef"
    vfs.writable.add("/source.bin")
    runtime = PythonRuntime(vfs=vfs, cache_vfs=True)
    try:
      result = await runtime.execute(
        "import os\n"
        "with open('/source.bin', 'r+b') as file:\n"
        "  file.truncate(3)\n"
        "assert 'source.bin' in os.listdir('/')\n"
        "os.rename('/source.bin', '/renamed.bin')\n"
        "assert 'source.bin' not in os.listdir('/')\n"
        "assert 'renamed.bin' in os.listdir('/')\n"
        "os.remove('/renamed.bin')\n"
        "assert 'renamed.bin' not in os.listdir('/')\n"
        "os.mkdir('/empty')\n"
        "os.rmdir('/empty')\n",
      )
      assert result.error is None, result.error
      assert "/source.bin" not in vfs.files
      assert "/renamed.bin" not in vfs.files
      assert "/empty" not in vfs.directories
      assert ("read", "/source.bin") in vfs.calls
      assert ("write", "/renamed.bin") in vfs.calls
      assert ("delete", "/source.bin") in vfs.calls
      assert ("mkdir", "/empty") in vfs.calls
    finally:
      await runtime.close()

  async def test_native_mutations_support_directory_rename(self) -> None:
    vfs = NativeMutationVfs()
    vfs.root_write = True
    vfs.files["/data.bin"] = b"abcdef"
    vfs.writable.add("/data.bin")
    vfs.directories.add("/before")
    runtime = PythonRuntime(vfs=vfs)
    try:
      result = await runtime.execute(
        "import os\n"
        "with open('/data.bin', 'r+b') as file:\n"
        "  file.truncate(2)\n"
        "os.rename('/before', '/after')\n",
      )
      assert result.error is None, result.error
      assert vfs.files["/data.bin"] == b"ab"
      assert "/before" not in vfs.directories
      assert "/after" in vfs.directories
      assert ("truncate", "/data.bin") in vfs.calls
      assert ("rename", "/before") in vfs.calls
    finally:
      await runtime.close()

  async def test_mutations_raise_standard_filesystem_errors(self) -> None:
    vfs = MemoryVfs()
    vfs.root_write = True
    vfs.files["/file"] = b"data"
    vfs.directories.update({"/directory", "/nonempty"})
    vfs.files["/nonempty/child"] = b"data"
    runtime = PythonRuntime(vfs=vfs, cache_vfs=True)
    try:
      result = await runtime.execute(
        "import errno, os\n"
        "try:\n"
        "  os.mkdir('/directory')\n"
        "except FileExistsError:\n"
        "  pass\n"
        "else:\n"
        "  raise AssertionError('mkdir did not raise FileExistsError')\n"
        "try:\n"
        "  os.rmdir('/file')\n"
        "except NotADirectoryError:\n"
        "  pass\n"
        "else:\n"
        "  raise AssertionError('rmdir did not raise NotADirectoryError')\n"
        "try:\n"
        "  os.remove('/directory')\n"
        "except IsADirectoryError:\n"
        "  pass\n"
        "else:\n"
        "  raise AssertionError('remove did not raise IsADirectoryError')\n"
        "try:\n"
        "  os.rmdir('/nonempty')\n"
        "except OSError as error:\n"
        "  assert error.errno == errno.ENOTEMPTY, error\n"
        "else:\n"
        "  raise AssertionError('rmdir did not raise ENOTEMPTY')\n",
      )
      assert result.error is None, result.error

      vfs.root_write = False
      await runtime.invalidate_vfs("/")
      denied = await runtime.execute("import os\nos.mkdir('/denied')")
      assert "PermissionError" in (denied.error or "")
    finally:
      await runtime.close()

  async def test_write_permissions_append_and_cache_invalidation(self) -> None:
    vfs = MemoryVfs()
    vfs.root_write = True
    vfs.files["/editable.txt"] = b"abc"
    vfs.writable.add("/editable.txt")
    vfs.files["/write-only.txt"] = b"secret"
    vfs.writable.add("/write-only.txt")
    vfs.unreadable.add("/write-only.txt")
    runtime = PythonRuntime(vfs=vfs, cache_vfs=True)
    try:
      result = await runtime.execute(
        "with open('/editable.txt', 'r+') as file:\n"
        "  file.seek(1)\n"
        "  file.write('Z')\n"
        "with open('/editable.txt', 'a') as file:\n"
        "  file.write('!')\n"
        "with open('/created.txt', 'w') as file:\n"
        "  file.write('new')\n"
        "with open('/write-only.txt', 'w') as file:\n"
        "  file.write('changed')\n",
      )
      assert result.error is None, result.error
      assert vfs.files["/editable.txt"] == b"aZc!"
      assert vfs.files["/created.txt"] == b"new"
      assert vfs.files["/write-only.txt"] == b"changed"
      assert vfs.calls.count(("write", "/editable.txt")) >= 2

      vfs.root_write = False
      await runtime.invalidate_vfs("/")
      truncate = await runtime.execute(
        "with open('/editable.txt', 'w') as file:\n  file.write('replacement')\n",
      )
      assert truncate.error is None
      assert vfs.files["/editable.txt"] == b"replacement"
      denied_create = await runtime.execute("open('/denied.txt', 'w')")
      assert "PermissionError" in (denied_create.error or "")

      denied = await runtime.execute("open('/write-only.txt').read()")
      assert "PermissionError" in (denied.error or "")

      refreshed = await runtime.execute(
        "print(open('/editable.txt').read(), flush=True)",
      )
      assert refreshed.error is None
      assert refreshed.output.stdout == b"replacement\n"
      assert vfs.calls.count(("stat", "/editable.txt")) >= 2
    finally:
      await runtime.close()

  async def test_overload_error_is_not_cached(self) -> None:
    vfs = MemoryVfs()
    runtime = PythonRuntime(
      vfs=vfs,
      cache_vfs=True,
      host_dispatch_concurrency=1,
      host_dispatch_queue_capacity=1,
    )
    first_started = asyncio.Event()
    release = asyncio.Event()
    first: asyncio.Task[RuntimeResult] | None = None
    fillers: list[asyncio.Task[RuntimeResult]] = []

    @runtime.rpc.expose
    async def held(context: RpcContext, /, name: str) -> str:
      if name == "first":
        first_started.set()
        await release.wait()
      return name

    try:
      first = asyncio.create_task(runtime.execute("await held('first')"))
      await first_started.wait()
      fillers = [
        asyncio.create_task(runtime.execute("await held('filler')")) for _ in range(3)
      ]
      completed, _ = await asyncio.wait(
        fillers,
        return_when=asyncio.FIRST_COMPLETED,
      )
      completed_results = await asyncio.gather(*completed)
      assert any(
        "host dispatch queue is full" in (result.error or "")
        for result in completed_results
      ), completed_results

      overloaded = await runtime.execute("import hello")
      assert overloaded.error is not None
      assert vfs.calls == []

      release.set()
      await asyncio.gather(first, *fillers)
      recovered = await runtime.execute(
        "import hello\nprint(hello.value, flush=True)",
      )
      assert recovered.error is None
      assert recovered.output.stdout == b"42\n"
    finally:
      release.set()
      pending = [first] if first is not None else []
      pending.extend(fillers)
      if pending:
        await asyncio.gather(*pending, return_exceptions=True)
      await runtime.close()

  async def test_negative_cache_policy(self) -> None:
    vfs = MemoryVfs()
    vfs.files["/late.py"] = b"value = 7\n"
    vfs.read_errors.add("/late.py")
    runtime = PythonRuntime(vfs=vfs, cache_vfs=True)
    try:
      missing = await runtime.execute("import late")
      assert missing.error is not None

      vfs.read_errors.remove("/late.py")
      available = await runtime.execute(
        "import late\nprint(late.value, flush=True)",
      )
      assert available.error is None
      assert available.output.stdout == b"7\n"
    finally:
      await runtime.close()

    cached_vfs = MemoryVfs()
    cached_vfs.files["/late.py"] = b"value = 8\n"
    cached_vfs.read_errors.add("/late.py")
    cached_runtime = PythonRuntime(
      vfs=cached_vfs,
      cache_vfs=True,
      cache_vfs_negative=True,
    )
    try:
      missing = await cached_runtime.execute("import late")
      assert missing.error is not None

      cached_vfs.read_errors.remove("/late.py")
      still_missing = await cached_runtime.execute("import late")
      assert still_missing.error is not None

      await cached_runtime.invalidate_vfs("/late.py")
      available = await cached_runtime.execute(
        "import late\nprint(late.value, flush=True)",
      )
      assert available.error is None
      assert available.output.stdout == b"8\n"
    finally:
      await cached_runtime.close()

  async def test_import_cache_and_invalidation(self) -> None:
    vfs = MemoryVfs()
    runtime = PythonRuntime(vfs=vfs, cache_vfs=True)
    try:
      result = await runtime.execute(
        "import sys\n"
        "import hello\n"
        "print(hello.value, sys.dont_write_bytecode, flush=True)\n",
      )
      assert result.error is None, vfs.calls
      assert result.output.stdout == b"42 True\n"

      initial_reads = vfs.calls.count(("read", "/hello.py"))
      second = await runtime.execute(
        "import sys\nimport hello\nprint(hello.value, flush=True)\n",
      )
      assert second.error is None
      assert vfs.calls.count(("read", "/hello.py")) == initial_reads
      # The directory listing populated the same stat cache used by read/open.
      assert vfs.calls.count(("stat", "/hello.py")) == 0

      vfs.files["/hello.py"] = b"value = 84\n"
      await runtime.invalidate_vfs("/hello.py")
      third = await runtime.execute(
        "import sys\nimport hello\nprint(hello.value, flush=True)\n",
      )
      assert third.error is None
      assert third.output.stdout == b"84\n"
      assert vfs.calls.count(("read", "/hello.py")) == initial_reads + 1

      readonly = await runtime.execute("open('/created.txt', 'w')")
      assert "PermissionError" in (readonly.error or "")
    finally:
      await runtime.close()

  async def test_directory_invalidation_evicts_descendants(self) -> None:
    with TemporaryDirectory() as directory:
      root = Path(directory)
      package = root / "package"
      package.mkdir()
      (package / "__init__.py").write_text(
        "from .value import value\n",
        encoding="utf-8",
      )
      value_file = package / "value.py"
      value_file.write_text("value = 1\n", encoding="utf-8")

      runtime = PythonRuntime(vfs=DirectoryVfs(root), cache_vfs=True)
      try:
        first = await runtime.execute(
          "import package\nprint(package.value, flush=True)",
        )
        assert first.error is None
        assert first.output.stdout == b"1\n"

        value_file.write_text("value = 2\n", encoding="utf-8")
        cached = await runtime.execute(
          "import package\nprint(package.value, flush=True)",
        )
        assert cached.error is None
        assert cached.output.stdout == b"1\n"

        await runtime.invalidate_vfs("/package")
        refreshed = await runtime.execute(
          "import package\nprint(package.value, flush=True)",
        )
        assert refreshed.error is None
        assert refreshed.output.stdout == b"2\n"
      finally:
        await runtime.close()
