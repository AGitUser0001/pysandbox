import base64
import csv
import hashlib
import shutil
import zipfile
from pathlib import Path

import pytest
from packaging.version import Version

from pysandbox import (
  DEFAULT_PACKAGE_CACHE,
  DEFAULT_PACKAGE_INDEX,
  Package,
  PackageCache,
  PackageEnvironment,
  PackageError,
  PackageManager,
  PythonRuntime,
  TerminationReason,
)
from pysandbox.packages import PackageArtifact


class LocalIndex:
  def __init__(self, distributions: list[Path]) -> None:
    self.distributions = distributions
    self.downloads = 0

  def artifacts(self, project: str) -> list[PackageArtifact]:
    normalized = project.replace("_", "-").lower()
    return [
      PackageArtifact(
        name=normalized,
        version=Version(path.name.split("-")[1]),
        filename=path.name,
        url=path.as_uri(),
        sha256=file_hash(path),
        size=path.stat().st_size,
      )
      for path in self.distributions
      if path.name.split("-")[0].replace("_", "-").lower() == normalized
    ]

  def download(
    self,
    artifact: PackageArtifact,
    destination: Path,
    max_bytes: int | None,
  ) -> Path:
    self.downloads += 1
    source = next(path for path in self.distributions if path.name == artifact.filename)
    if max_bytes is not None and source.stat().st_size > max_bytes:
      raise PackageError(f"{source.name} exceeds the {max_bytes}-byte size limit")
    target = destination / source.name
    shutil.copy2(source, target)
    return target


class TestPackages:
  @pytest.fixture(autouse=True)
  def package_root(self, tmp_path: Path) -> None:
    self.root = tmp_path

  async def test_default_package_dependencies_are_explicit(self) -> None:
    runtime = PythonRuntime()

    assert runtime.packages.cache is DEFAULT_PACKAGE_CACHE
    assert runtime.packages.index is DEFAULT_PACKAGE_INDEX

    manager = PackageManager(cache=None, index=DEFAULT_PACKAGE_INDEX)
    with pytest.raises(PackageError, match="package cache is unavailable"):
      await manager.resolve("example")

  async def test_resolves_dependencies_and_reuses_version_layers(self) -> None:
    dependency = make_wheel(self.root, "dependency", "2.0")
    application = make_wheel(
      self.root,
      "application",
      "1.0",
      requires=("dependency>=2",),
    )
    index = LocalIndex([application, dependency])
    cache = PackageCache(self.root / "cache")
    manager = PackageManager(cache=cache, index=index)

    first = await manager.resolve("application==1.0")
    second = await manager.resolve("application==1.0")

    assert {package.name for package in first.packages} == {"application", "dependency"}
    assert first.paths == second.paths
    assert all(path.exists() for path in first.paths)
    assert index.downloads == 2

  async def test_can_disable_dependencies(self) -> None:
    dependency = make_wheel(self.root, "dependency", "2.0")
    application = make_wheel(
      self.root,
      "application",
      "1.0",
      requires=("dependency>=2",),
    )
    manager = PackageManager(
      cache=PackageCache(self.root / "cache"),
      index=LocalIndex([application, dependency]),
    )

    environment = await manager.resolve(
      Package("application==1.0", include_dependencies=False)
    )

    assert [package.name for package in environment.packages] == ["application"]

  async def test_resolves_dependencies_selected_by_extra(self) -> None:
    dependency = make_wheel(self.root, "dependency", "2.0")
    application = make_wheel(
      self.root,
      "application",
      "1.0",
      requires=("dependency>=2; extra == 'feature'",),
    )
    manager = PackageManager(
      cache=PackageCache(self.root / "cache"),
      index=LocalIndex([application, dependency]),
    )

    environment = await manager.resolve("application[feature]==1.0")

    assert {package.name for package in environment.packages} == {
      "application",
      "dependency",
    }

  async def test_never_cache_owns_temporary_layers(self) -> None:
    wheel = make_wheel(self.root, "example", "1.0")
    manager = PackageManager(
      cache=PackageCache(self.root / "cache"),
      index=LocalIndex([wheel]),
    )

    environment = await manager.resolve("example==1.0", cache="never")
    paths = environment.paths
    assert all(path.exists() for path in paths)
    environment.close()
    assert all(not path.exists() for path in paths)

  async def test_direct_wheel_bypasses_index(self) -> None:
    wheel = make_wheel(self.root, "example", "1.0")
    index = LocalIndex([])
    manager = PackageManager(
      cache=PackageCache(self.root / "cache"),
      index=index,
    )

    environment = await manager.resolve(Package("example==1.0", source=wheel))

    assert isinstance(environment, PackageEnvironment)
    assert index.downloads == 0

  async def test_source_directory_is_built(self) -> None:
    source = make_source_project(self.root)
    manager = PackageManager(
      cache=PackageCache(self.root / "cache"), index=DEFAULT_PACKAGE_INDEX
    )

    environment = await manager.resolve(Package("example==1.0", source=source))

    assert [package.name for package in environment.packages] == ["example"]
    module = next(path for path in environment.paths if path.name == "example")
    assert (module / "__init__.py").read_text() == "VALUE = 42\n"
    assert not any(source.parent.glob("example-1.0-*.whl"))

  async def test_source_directory_requires_build(self) -> None:
    source = make_source_project(self.root)
    manager = PackageManager(
      cache=PackageCache(self.root / "cache"), index=DEFAULT_PACKAGE_INDEX
    )

    with pytest.raises(PackageError):
      await manager.resolve(Package("example==1.0", source=source, build=False))

  async def test_source_directory_size_limit(self) -> None:
    source = make_source_project(self.root)
    manager = PackageManager(
      cache=PackageCache(self.root / "cache"), index=DEFAULT_PACKAGE_INDEX
    )

    with pytest.raises(PackageError, match="size limit"):
      await manager.resolve(Package("example==1.0", source=source, max_size=1))

  async def test_wheel_decompressed_size_limit(self) -> None:
    wheel = make_wheel(
      self.root,
      "example",
      "1.0",
      additional_files={"example/payload": b"x" * (1024 * 1024)},
      compression=zipfile.ZIP_DEFLATED,
    )
    assert wheel.stat().st_size < 10000

    with pytest.raises(PackageError, match="size limit"):
      await PackageCache(self.root / "cache").add(
        wheel,
        build=False,
        max_size=10_000,
      )

  async def test_wheel_file_limit(self) -> None:
    wheel = make_wheel(self.root, "example", "1.0")

    with pytest.raises(PackageError, match="file limit"):
      await PackageCache(self.root / "cache").add(
        wheel,
        build=False,
        max_files=3,
      )

  async def test_dependency_count_limit(self) -> None:
    first = make_wheel(self.root, "first", "1.0")
    second = make_wheel(self.root, "second", "1.0")
    application = make_wheel(
      self.root,
      "application",
      "1.0",
      requires=("first", "second"),
    )
    manager = PackageManager(
      cache=PackageCache(self.root / "cache"),
      index=LocalIndex([application, first, second]),
    )

    with pytest.raises(PackageError, match="dependency limit"):
      await manager.resolve(Package("application==1.0", max_dependencies=1))

  async def test_dependency_size_limit(self) -> None:
    dependency = make_wheel(
      self.root,
      "dependency",
      "1.0",
      additional_files={"dependency/payload": b"x" * 1024},
    )
    application = make_wheel(
      self.root,
      "application",
      "1.0",
      requires=("dependency",),
    )
    manager = PackageManager(
      cache=PackageCache(self.root / "cache"),
      index=LocalIndex([application, dependency]),
    )

    with pytest.raises(PackageError):
      await manager.resolve(Package("application==1.0", max_dependency_size=1))

  async def test_dependency_file_limit(self) -> None:
    dependency = make_wheel(self.root, "dependency", "1.0")
    application = make_wheel(
      self.root,
      "application",
      "1.0",
      requires=("dependency",),
    )
    manager = PackageManager(
      cache=PackageCache(self.root / "cache"),
      index=LocalIndex([application, dependency]),
    )

    with pytest.raises(PackageError, match="file limit"):
      await manager.resolve(Package("application==1.0", max_dependency_files=3))

  async def test_wheel_record_is_validated(self) -> None:
    wheel = make_wheel(self.root, "example", "1.0")
    with zipfile.ZipFile(wheel, "a") as archive:
      archive.writestr("example/unrecorded.py", b"VALUE = 1\n")

    with pytest.raises(PackageError, match="not mentioned in RECORD"):
      await PackageCache(self.root / "cache").add(wheel, build=False)

  async def test_package_directory_is_importable_by_guest(self) -> None:
    await self._check_guest_import(single_file=False)

  async def test_single_file_module_is_importable_by_guest(self) -> None:
    await self._check_guest_import(single_file=True)

  async def test_cbor2_uses_canonical_site_packages(self) -> None:
    runtime = PythonRuntime()
    try:
      result = await runtime.execute(
        """import cbor2
import sys

print(cbor2.__file__)
print(
  [
    path
    for path in sys.path
    if path in {"/world", "/bundled"}
    or (path.startswith("/") and path[1:].isdigit())
  ]
)
""",
      )
    finally:
      await runtime.close()

    assert result.reason == TerminationReason.COMPLETED
    assert result.text == "/python/lib/python3.14/site-packages/cbor2/__init__.py\n[]\n"

  async def _check_guest_import(self, *, single_file: bool) -> None:
    wheel = make_wheel(self.root, "example", "1.0", single_file=single_file)
    runtime = PythonRuntime(
      package_cache=PackageCache(self.root / "cache"),
    )
    try:
      environment = await runtime.packages.resolve(
        Package("example==1.0", source=wheel)
      )
      result = await runtime.execute(
        "import example\nprint(example.__version__)",
        packages=environment,
      )
    finally:
      await runtime.close()

    assert result.reason == TerminationReason.COMPLETED
    assert result.text == "1.0\n"


def make_wheel(
  root: Path,
  name: str,
  version: str,
  *,
  requires: tuple[str, ...] = (),
  single_file: bool = False,
  additional_files: dict[str, bytes] | None = None,
  compression: int = zipfile.ZIP_STORED,
) -> Path:
  normalized = name.replace("-", "_")
  dist_info = f"{normalized}-{version}.dist-info"
  wheel = root / f"{normalized}-{version}-py3-none-any.whl"
  metadata = [
    "Metadata-Version: 2.1",
    f"Name: {name}",
    f"Version: {version}",
    *(f"Requires-Dist: {requirement}" for requirement in requires),
    "",
  ]
  module_path = f"{normalized}.py" if single_file else f"{normalized}/__init__.py"
  files = {
    module_path: f"__version__ = {version!r}\n".encode(),
    f"{dist_info}/METADATA": "\n".join(metadata).encode(),
    f"{dist_info}/WHEEL": (
      b"Wheel-Version: 1.0\nGenerator: tests\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
    ),
    **(additional_files or {}),
  }
  records = []
  for path, data in files.items():
    digest = (
      base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
    )
    records.append((path, f"sha256={digest}", str(len(data))))
  record_path = f"{dist_info}/RECORD"
  records.append((record_path, "", ""))
  output = []
  writer = csv.writer(_ListWriter(output), lineterminator="\n")
  writer.writerows(records)
  files[record_path] = "".join(output).encode()
  with zipfile.ZipFile(wheel, "w", compression=compression) as archive:
    for path, data in files.items():
      archive.writestr(path, data)
  return wheel


def make_source_project(root: Path) -> Path:
  source = root / "example-1.0"
  source.mkdir()
  (source / "pyproject.toml").write_text(
    """[build-system]
requires = []
build-backend = "backend"
backend-path = ["."]
"""
  )
  (source / "backend.py").write_text(
    '''import base64
import csv
import hashlib
import io
import zipfile
from pathlib import Path


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
  filename = "example-1.0-py3-none-any.whl"
  dist_info = "example-1.0.dist-info"
  files = {
    "example/__init__.py": b"VALUE = 42\\n",
    f"{dist_info}/METADATA": (
      b"Metadata-Version: 2.1\\nName: example\\nVersion: 1.0\\n"
    ),
    f"{dist_info}/WHEEL": (
      b"Wheel-Version: 1.0\\nRoot-Is-Purelib: true\\nTag: py3-none-any\\n"
    ),
  }
  record_path = f"{dist_info}/RECORD"
  output = io.StringIO(newline="")
  writer = csv.writer(output, lineterminator="\\n")
  records = []
  for path, data in files.items():
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
    records.append((path, f"sha256={digest.decode()}", str(len(data))))
  writer.writerows((*records, (record_path, "", "")))
  files[record_path] = output.getvalue().encode()
  target = Path(wheel_directory) / filename
  with zipfile.ZipFile(target, "w") as archive:
    for path, data in files.items():
      archive.writestr(path, data)
  return filename
'''
  )
  return source


class _ListWriter:
  def __init__(self, output: list[str]) -> None:
    self.output = output

  def write(self, value: str) -> int:
    self.output.append(value)
    return len(value)


def file_hash(path: Path) -> str:
  return hashlib.sha256(path.read_bytes()).hexdigest()
