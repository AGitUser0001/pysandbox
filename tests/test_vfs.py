import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pysandbox import (
  PythonRuntime,
  RuntimeLimits,
  VfsDirectoryEntry,
  VfsMetadata,
)


class MemoryVfs:
  def __init__(self) -> None:
    self.files = {"/hello.py": b"value = 42\n"}
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

  async def test_panel_sympy_import_tree(self) -> None:
    root = Path.home() / "Desktop/Panel/data/.runtime/fs"
    if not (root / "sympy").is_dir():
      self.skipTest("Panel SymPy runtime tree is unavailable")

    runtime = PythonRuntime(vfs=DirectoryVfs(root), cache_vfs=True)
    try:
      result = await runtime.execute(
        "import sys, types\n"
        "ctypes = types.ModuleType('ctypes')\n"
        "ctypes.c_long = int\n"
        "ctypes.sizeof = lambda _: 8\n"
        "ctypes.Union = ctypes.Structure = ctypes.Array = object\n"
        "sys.modules['ctypes'] = ctypes\n"
        "import sympy as sp\n"
        "from sympy.parsing.sympy_parser import parse_expr\n"
        "print(sp.factor(parse_expr('x**2 - 1')), flush=True)\n",
        limits=RuntimeLimits(
          max_memory_bytes=256 * 1024 * 1024,
          max_output_bytes=1024 * 1024,
          timeout=120,
        ),
      )
      self.assertIsNone(result.error)
      self.assertEqual(result.stdout, b"(x - 1)*(x + 1)\n")
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
