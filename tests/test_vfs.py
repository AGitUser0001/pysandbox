import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pysandbox import (
  PythonRuntime,
  RpcContext,
  RuntimeResult,
  VfsDirectoryEntry,
  VfsMetadata,
)

DISPATCH_TEST_TIMEOUT = 30


class MemoryVfs:
  def __init__(self) -> None:
    self.files = {"/hello.py": b"value = 42\n"}
    self.read_errors: set[str] = set()
    self.calls: list[tuple[str, str]] = []

  async def stat(self, path: str) -> VfsMetadata:
    self.calls.append(("stat", path))
    if path == "/":
      return VfsMetadata("directory")
    if path in self.files:
      data = self.files[path]
      return VfsMetadata("file", len(data))
    raise FileNotFoundError(path)

  async def read(self, path: str) -> bytes:
    self.calls.append(("read", path))
    if path in self.read_errors:
      raise FileNotFoundError(path)
    try:
      return self.files[path]
    except KeyError:
      raise FileNotFoundError(path) from None

  async def list(self, path: str) -> list[VfsDirectoryEntry]:
    self.calls.append(("list", path))
    if path != "/":
      raise NotADirectoryError(path)
    return [
      VfsDirectoryEntry(name.removeprefix("/"), "file", len(data))
      for name, data in self.files.items()
    ]


class DirectoryVfs:
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


class VfsTests(unittest.IsolatedAsyncioTestCase):
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
    second: asyncio.Task[RuntimeResult] | None = None

    @runtime.rpc.expose
    async def held(context: RpcContext, /, name: str) -> str:
      if name == "first":
        first_started.set()
        await release.wait()
      return name

    try:
      first = asyncio.create_task(runtime.execute("await held('first')"))
      await asyncio.wait_for(first_started.wait(), timeout=DISPATCH_TEST_TIMEOUT)
      second = asyncio.create_task(runtime.execute("await held('second')"))
      await asyncio.sleep(0.05)

      overloaded = await asyncio.wait_for(
        runtime.execute("import hello"),
        timeout=DISPATCH_TEST_TIMEOUT,
      )
      self.assertIn("OSError", overloaded.error or "")

      release.set()
      await asyncio.gather(first, second)
      recovered = await runtime.execute(
        "import hello\nprint(hello.value, flush=True)",
      )
      self.assertIsNone(recovered.error)
      self.assertEqual(recovered.stdout, b"42\n")
    finally:
      release.set()
      pending = [task for task in (first, second) if task is not None]
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
      self.assertIsNotNone(missing.error)

      vfs.read_errors.remove("/late.py")
      available = await runtime.execute(
        "import late\nprint(late.value, flush=True)",
      )
      self.assertIsNone(available.error)
      self.assertEqual(available.stdout, b"7\n")
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
      self.assertIsNotNone(missing.error)

      cached_vfs.read_errors.remove("/late.py")
      still_missing = await cached_runtime.execute("import late")
      self.assertIsNotNone(still_missing.error)

      await cached_runtime.invalidate_vfs("/late.py")
      available = await cached_runtime.execute(
        "import late\nprint(late.value, flush=True)",
      )
      self.assertIsNone(available.error)
      self.assertEqual(available.stdout, b"8\n")
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
      self.assertIsNone(result.error, vfs.calls)
      self.assertEqual(result.stdout, b"42 True\n")

      initial_reads = vfs.calls.count(("read", "/hello.py"))
      second = await runtime.execute(
        "import sys\nimport hello\nprint(hello.value, flush=True)\n",
      )
      self.assertIsNone(second.error)
      self.assertEqual(vfs.calls.count(("read", "/hello.py")), initial_reads)
      self.assertEqual(vfs.calls.count(("stat", "/hello.py")), 1)

      vfs.files["/hello.py"] = b"value = 84\n"
      await runtime.invalidate_vfs("/hello.py")
      third = await runtime.execute(
        "import sys\nimport hello\nprint(hello.value, flush=True)\n",
      )
      self.assertIsNone(third.error)
      self.assertEqual(third.stdout, b"84\n")
      self.assertEqual(vfs.calls.count(("read", "/hello.py")), initial_reads + 1)

      readonly = await runtime.execute("open('/created.txt', 'w')")
      self.assertIn("PermissionError", readonly.error or "")
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
        self.assertIsNone(first.error)
        self.assertEqual(first.stdout, b"1\n")

        value_file.write_text("value = 2\n", encoding="utf-8")
        cached = await runtime.execute(
          "import package\nprint(package.value, flush=True)",
        )
        self.assertIsNone(cached.error)
        self.assertEqual(cached.stdout, b"1\n")

        await runtime.invalidate_vfs("/package")
        refreshed = await runtime.execute(
          "import package\nprint(package.value, flush=True)",
        )
        self.assertIsNone(refreshed.error)
        self.assertEqual(refreshed.stdout, b"2\n")
      finally:
        await runtime.close()


if __name__ == "__main__":
  unittest.main()
