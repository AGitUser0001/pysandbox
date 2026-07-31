import base64
import csv
import hashlib
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path

from packaging.version import Version

from pysandbox import (
  Package,
  PackageCache,
  PackageEnvironment,
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
      )
      for path in self.distributions
      if path.name.split("-")[0].replace("_", "-").lower() == normalized
    ]

  def download(self, artifact: PackageArtifact, destination: Path) -> Path:
    self.downloads += 1
    source = next(path for path in self.distributions if path.name == artifact.filename)
    target = destination / source.name
    shutil.copy2(source, target)
    return target


class PackageTests(unittest.IsolatedAsyncioTestCase):
  async def asyncSetUp(self) -> None:
    self.temporary = tempfile.TemporaryDirectory()
    self.root = Path(self.temporary.name)

  async def asyncTearDown(self) -> None:
    self.temporary.cleanup()

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

    self.assertEqual(
      {package.name for package in first.packages}, {"application", "dependency"}
    )
    self.assertEqual(first.paths, second.paths)
    self.assertTrue(all(path.exists() for path in first.paths))
    self.assertEqual(index.downloads, 2)

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

    self.assertEqual(
      [package.name for package in environment.packages], ["application"]
    )

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

    self.assertEqual(
      {package.name for package in environment.packages},
      {"application", "dependency"},
    )

  async def test_never_cache_owns_temporary_layers(self) -> None:
    wheel = make_wheel(self.root, "example", "1.0")
    manager = PackageManager(
      cache=PackageCache(self.root / "cache"),
      index=LocalIndex([wheel]),
    )

    environment = await manager.resolve("example==1.0", cache="never")
    paths = environment.paths
    self.assertTrue(all(path.exists() for path in paths))
    environment.close()
    self.assertTrue(all(not path.exists() for path in paths))

  async def test_direct_wheel_bypasses_index(self) -> None:
    wheel = make_wheel(self.root, "example", "1.0")
    index = LocalIndex([])
    manager = PackageManager(
      cache=PackageCache(self.root / "cache"),
      index=index,
    )

    environment = await manager.resolve(Package("example==1.0", source=wheel))

    self.assertIsInstance(environment, PackageEnvironment)
    self.assertEqual(index.downloads, 0)

  async def test_package_directory_is_importable_by_guest(self) -> None:
    await self.assert_guest_import(single_file=False)

  async def test_single_file_module_is_importable_by_guest(self) -> None:
    await self.assert_guest_import(single_file=True)

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

    self.assertEqual(result.reason, TerminationReason.COMPLETED)
    self.assertEqual(
      result.text,
      "/python/lib/python3.14/site-packages/cbor2/__init__.py\n[]\n",
    )

  async def assert_guest_import(self, *, single_file: bool) -> None:
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

    self.assertEqual(result.reason, TerminationReason.COMPLETED)
    self.assertEqual(result.text, "1.0\n")


def make_wheel(
  root: Path,
  name: str,
  version: str,
  *,
  requires: tuple[str, ...] = (),
  single_file: bool = False,
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
  with zipfile.ZipFile(wheel, "w") as archive:
    for path, data in files.items():
      archive.writestr(path, data)
  return wheel


class _ListWriter:
  def __init__(self, output: list[str]) -> None:
    self.output = output

  def write(self, value: str) -> int:
    self.output.append(value)
    return len(value)


def file_hash(path: Path) -> str:
  return hashlib.sha256(path.read_bytes()).hexdigest()
