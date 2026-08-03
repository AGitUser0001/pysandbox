import asyncio
import hashlib
import json
import os
import shutil
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
import zipfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from email.parser import BytesParser
from email.policy import compat32
from pathlib import Path
from typing import BinaryIO, Literal, Protocol, Self, cast

from build import ProjectBuilder
from build.env import DefaultIsolatedEnv
from installer import install
from installer.destinations import SchemeDictionaryDestination
from installer.sources import WheelFile
from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.utils import (
  canonicalize_name,
  parse_sdist_filename,
  parse_wheel_filename,
)
from packaging.version import Version
from platformdirs import user_cache_path
from resolvelib import AbstractProvider, BaseReporter, Resolver

__all__ = [
  "DEFAULT_PACKAGE_CACHE",
  "DEFAULT_PACKAGE_INDEX",
  "CachePolicy",
  "CachedPackage",
  "Package",
  "PackageCache",
  "PackageEnvironment",
  "PackageError",
  "PackageIndex",
  "PackageManager",
  "PyPIIndex",
]

type CachePolicy = Literal["by_version", "never"]


class PackageError(Exception):
  """Raised when packages cannot be resolved or installed safely."""


@dataclass(frozen=True, slots=True)
class Package:
  requirement: str
  source: Path | None = None
  build: bool = True
  include_dependencies: bool = True
  max_size: int | None = None
  max_files: int | None = None
  max_dependencies: int | None = None
  max_dependency_size: int | None = None
  max_dependency_files: int | None = None

  def __post_init__(self) -> None:
    if self.max_size is not None and self.max_size < 0:
      raise ValueError("max_size must be non-negative")
    if self.max_files is not None and self.max_files < 0:
      raise ValueError("max_files must be non-negative")
    if self.max_dependencies is not None and self.max_dependencies < 0:
      raise ValueError("max_dependencies must be non-negative")
    if self.max_dependency_size is not None and self.max_dependency_size < 0:
      raise ValueError("max_dependency_size must be non-negative")
    if self.max_dependency_files is not None and self.max_dependency_files < 0:
      raise ValueError("max_dependency_files must be non-negative")

  def parsed_requirement(self) -> Requirement:
    try:
      return Requirement(self.requirement)
    except ValueError as error:
      raise PackageError(f"invalid package requirement: {self.requirement}") from error


@dataclass(frozen=True, slots=True)
class PackageArtifact:
  name: str
  version: Version
  filename: str
  url: str
  sha256: str | None = None
  requires_python: str | None = None
  yanked: bool = False
  size: int | None = None

  @property
  def is_wheel(self) -> bool:
    return self.filename.endswith(".whl")


class PackageIndex(Protocol):
  def artifacts(self, project: str) -> Sequence[PackageArtifact]: ...

  def download(
    self,
    artifact: PackageArtifact,
    destination: Path,
    max_bytes: int | None,
  ) -> Path: ...


class PyPIIndex:
  def __init__(self, url: str = "https://pypi.org/simple") -> None:
    self.url = url.rstrip("/")

  def artifacts(self, project: str) -> Sequence[PackageArtifact]:
    normalized = canonicalize_name(project)
    request = urllib.request.Request(
      f"{self.url}/{normalized}/",
      headers={"Accept": "application/vnd.pypi.simple.v1+json"},
    )
    try:
      with urllib.request.urlopen(request) as response:
        document = json.load(response)
    except (OSError, urllib.error.HTTPError, json.JSONDecodeError) as error:
      raise PackageError(
        f"failed to query package index for {project}: {error}"
      ) from error

    artifacts: list[PackageArtifact] = []
    for item in document.get("files", ()):
      filename = item.get("filename")
      url = item.get("url")
      if not isinstance(filename, str) or not isinstance(url, str):
        continue
      parsed = distribution_from_filename(filename)
      if parsed is None or parsed[0] != normalized:
        continue
      hashes = item.get("hashes")
      sha256 = hashes.get("sha256") if isinstance(hashes, dict) else None
      size = item.get("size")
      artifacts.append(
        PackageArtifact(
          name=parsed[0],
          version=parsed[1],
          filename=filename,
          url=url,
          sha256=sha256 if isinstance(sha256, str) else None,
          requires_python=item.get("requires-python"),
          yanked=bool(item.get("yanked", False)),
          size=size if isinstance(size, int) and size >= 0 else None,
        )
      )
    return artifacts

  def download(
    self,
    artifact: PackageArtifact,
    destination: Path,
    max_bytes: int | None,
  ) -> Path:
    ensure_size(artifact.size, max_bytes, artifact.filename)
    path = destination / artifact.filename
    try:
      with urllib.request.urlopen(artifact.url) as response, path.open("wb") as output:
        copy_limited(response, output, max_bytes, artifact.filename)
    except (OSError, urllib.error.HTTPError) as error:
      raise PackageError(f"failed to download {artifact.filename}: {error}") from error
    if artifact.sha256 is not None and file_sha256(path) != artifact.sha256:
      path.unlink(missing_ok=True)
      raise PackageError(f"hash mismatch for {artifact.filename}")
    return path


@dataclass(frozen=True, slots=True)
class CachedPackage:
  name: str
  version: Version
  root: Path
  paths: tuple[Path, ...]
  sha256: str


@dataclass(slots=True)
class PackageEnvironment:
  packages: tuple[CachedPackage, ...]
  paths: tuple[Path, ...]
  _temporary_directory: tempfile.TemporaryDirectory[str] | None = field(
    default=None,
    repr=False,
  )

  def close(self) -> None:
    if self._temporary_directory is not None:
      self._temporary_directory.cleanup()
      self._temporary_directory = None

  def __enter__(self) -> Self:
    return self

  def __exit__(self, *args: object) -> None:
    self.close()


class PackageCache:
  def __init__(self, root: Path) -> None:
    self.root = root
    self._locks: dict[tuple[str, Version], asyncio.Lock] = {}

  async def add(
    self,
    distribution: Path,
    *,
    build: bool = True,
    max_size: int | None = None,
    max_files: int | None = None,
  ) -> CachedPackage:
    return await self.install(
      Path(distribution),
      build=build,
      policy="by_version",
      max_size=max_size,
      max_files=max_files,
    )

  async def resolve(self, name: str, version: str | Version) -> CachedPackage | None:
    return await asyncio.to_thread(
      self._resolve, name, Version(str(version)), self.root
    )

  async def remove(self, name: str, version: str | Version) -> bool:
    normalized = canonicalize_name(name)
    path = self.root / normalized / str(Version(str(version)))
    if not path.exists():
      return False
    await asyncio.to_thread(shutil.rmtree, path)
    return True

  async def packages(self) -> tuple[CachedPackage, ...]:
    return await asyncio.to_thread(self._packages)

  async def install(
    self,
    distribution: Path,
    *,
    build: bool,
    policy: CachePolicy,
    temporary_root: Path | None = None,
    max_size: int | None = None,
    max_files: int | None = None,
  ) -> CachedPackage:
    distribution = Path(distribution)
    build_directory = (
      tempfile.TemporaryDirectory(prefix="pysandbox-package-build-")
      if distribution.is_dir()
      else None
    )
    try:
      wheel = await asyncio.to_thread(
        prepare_wheel,
        distribution,
        build,
        Path(build_directory.name) if build_directory is not None else None,
        max_size,
        max_files,
      )
      name, version, _, _ = parse_wheel_filename(wheel.name)
      key = (canonicalize_name(name), version)
      lock = self._locks.setdefault(key, asyncio.Lock())
      async with lock:
        root = self.root if policy == "by_version" else temporary_root
        if root is None:
          raise PackageError("temporary package root is missing")
        return await asyncio.to_thread(
          self._add_wheel,
          wheel,
          root,
          max_size,
          max_files,
        )
    finally:
      if build_directory is not None:
        build_directory.cleanup()

  def _add_wheel(
    self,
    wheel: Path,
    root: Path,
    max_size: int | None,
    max_files: int | None,
  ) -> CachedPackage:
    ensure_compatible_wheel(wheel)
    parsed_name, version, _, _ = parse_wheel_filename(wheel.name)
    name = canonicalize_name(parsed_name)
    digest = file_sha256(wheel)
    destination = root / name / str(version)
    existing = self._resolve(name, version, root)
    if existing is not None:
      if existing.sha256 != digest:
        raise PackageError(
          f"cached {name}=={version} has different distribution contents"
        )
      return existing

    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{version}-", dir=destination.parent))
    try:
      site_packages = staging / "site-packages"
      site_packages.mkdir()
      install_wheel(wheel, site_packages, max_size, max_files)
      paths = tuple(sorted(site_packages.iterdir(), key=lambda path: path.name))
      manifest = {
        "name": name,
        "version": str(version),
        "sha256": digest,
        "paths": [path.name for path in paths],
      }
      (staging / "package.json").write_text(json.dumps(manifest, indent=2) + "\n")
      try:
        staging.replace(destination)
      except FileExistsError:
        existing = self._resolve(name, version, root)
        if existing is None or existing.sha256 != digest:
          raise PackageError(f"conflicting concurrent install of {name}=={version}")
        return existing
      return self._resolve_required(name, version, root)
    finally:
      if staging.exists():
        shutil.rmtree(staging)

  def _resolve(
    self,
    name: str,
    version: Version,
    root: Path,
  ) -> CachedPackage | None:
    normalized = canonicalize_name(name)
    directory = root / normalized / str(version)
    manifest_path = directory / "package.json"
    if not manifest_path.is_file():
      return None
    try:
      manifest = json.loads(manifest_path.read_text())
      paths = tuple(directory / "site-packages" / item for item in manifest["paths"])
      if not all(path.exists() for path in paths):
        return None
      return CachedPackage(
        name=normalized,
        version=version,
        root=directory,
        paths=paths,
        sha256=manifest["sha256"],
      )
    except (KeyError, TypeError, json.JSONDecodeError):
      return None

  def _resolve_required(self, name: str, version: Version, root: Path) -> CachedPackage:
    package = self._resolve(name, version, root)
    if package is None:
      raise PackageError(f"failed to publish cached package {name}=={version}")
    return package

  def _packages(self) -> tuple[CachedPackage, ...]:
    if not self.root.is_dir():
      return ()
    packages: list[CachedPackage] = []
    for project in self.root.iterdir():
      if not project.is_dir():
        continue
      for version_path in project.iterdir():
        try:
          version = Version(version_path.name)
        except ValueError:
          continue
        package = self._resolve(project.name, version, self.root)
        if package is not None:
          packages.append(package)
    return tuple(sorted(packages, key=lambda package: (package.name, package.version)))


@dataclass(frozen=True, slots=True)
class _PackageRequirement:
  requirement: Requirement
  build: bool
  include_dependencies: bool
  max_size: int | None
  max_files: int | None
  max_dependencies: int | None
  max_dependency_size: int | None
  max_dependency_files: int | None


@dataclass(slots=True)
class _Candidate:
  artifact: PackageArtifact
  package: Package
  manager: "PackageManager"
  download_directory: Path
  distribution: Path | None = None
  allow_cached: bool = True
  wheel: Path | None = None
  cached: CachedPackage | None = None
  dependencies: tuple[_PackageRequirement, ...] | None = None
  extras: frozenset[str] = frozenset()

  @property
  def name(self) -> str:
    return self.artifact.name

  @property
  def version(self) -> Version:
    return self.artifact.version

  def prepare(self) -> None:
    if self.wheel is None and self.cached is None:
      ensure_size(self.artifact.size, self.package.max_size, self.artifact.filename)
      cache = self.manager._require_cache()
      cached = (
        cache._resolve(
          self.name,
          self.version,
          cache.root,
        )
        if self.allow_cached
        else None
      )
      if cached is not None and (
        self.artifact.sha256 is None or cached.sha256 == self.artifact.sha256
      ):
        self.cached = cached
      else:
        distribution = self.distribution or self.manager.index.download(
          self.artifact,
          self.download_directory,
          self.package.max_size,
        )
        self.wheel = prepare_wheel(
          distribution,
          self.package.build,
          self.download_directory,
          self.package.max_size,
          self.package.max_files,
        )
    if self.dependencies is not None:
      return
    metadata = (
      cached_package_metadata(self.cached)
      if self.cached is not None
      else wheel_metadata(
        self.wheel_required(),
        self.package.max_size,
        self.package.max_files,
      )
    )
    self.dependencies = dependencies_from_metadata(
      metadata,
      build=self.package.build,
      extras=self.extras,
      max_dependencies=self.package.max_dependencies,
      max_dependency_size=self.package.max_dependency_size,
      max_dependency_files=self.package.max_dependency_files,
    )

  def wheel_required(self) -> Path:
    if self.wheel is None:
      raise PackageError(
        f"prepared {self.name}=={self.version} has neither a wheel nor cache layer"
      )
    return self.wheel


class _Provider(AbstractProvider[_PackageRequirement, _Candidate, str]):
  def __init__(
    self,
    manager: "PackageManager",
    packages: Sequence[Package],
    download_directory: Path,
    allow_cached: bool,
  ) -> None:
    self.manager = manager
    self.packages: dict[str, Package] = {
      canonicalize_name(package.parsed_requirement().name): package
      for package in packages
    }
    self.download_directory = download_directory
    self.allow_cached = allow_cached
    self._candidates: dict[str, list[_Candidate]] = {}
    self._dependency_identifiers: set[str] = set()
    limits = [
      package.max_dependencies
      for package in packages
      if package.max_dependencies is not None
    ]
    self.max_dependencies = sum(limits) if len(limits) == len(packages) else None

  def identify(self, requirement_or_candidate: _PackageRequirement | _Candidate) -> str:
    if isinstance(requirement_or_candidate, _Candidate):
      return requirement_or_candidate.name
    return canonicalize_name(requirement_or_candidate.requirement.name)

  def get_preference(
    self,
    identifier: str,
    resolutions: Mapping[str, _Candidate],
    candidates: Mapping[str, Iterator[_Candidate]],
    information: Mapping[str, Iterator[object]],
    backtrack_causes: Sequence[object],
  ) -> tuple[bool, str]:
    return (identifier in resolutions, identifier)

  def find_matches(
    self,
    identifier: str,
    requirements: Mapping[str, Iterator[_PackageRequirement]],
    incompatibilities: Mapping[str, Iterator[_Candidate]],
  ) -> Iterable[_Candidate]:
    if identifier not in self.packages:
      self._dependency_identifiers.add(identifier)
      if (
        self.max_dependencies is not None
        and len(self._dependency_identifiers) > self.max_dependencies
      ):
        raise PackageError(
          f"package resolution exceeds the {self.max_dependencies}-dependency limit"
        )
    required = list(requirements[identifier])
    incompatible = {(item.name, item.version) for item in incompatibilities[identifier]}
    candidates = self._candidates.get(identifier)
    if candidates is None:
      root = self.packages.get(identifier)
      package = Package(
        requirement=str(root.parsed_requirement()) if root is not None else identifier,
        source=root.source if root is not None else None,
        build=all(item.build for item in required),
        include_dependencies=any(item.include_dependencies for item in required),
        max_size=minimum_limit(item.max_size for item in required),
        max_files=minimum_limit(item.max_files for item in required),
        max_dependencies=minimum_limit(item.max_dependencies for item in required),
        max_dependency_size=minimum_limit(
          item.max_dependency_size for item in required
        ),
        max_dependency_files=minimum_limit(
          item.max_dependency_files for item in required
        ),
      )
      candidates = self.manager._candidates(
        package,
        self.download_directory,
        self.allow_cached,
      )
      self._candidates[identifier] = candidates
    for candidate in candidates:
      extras = frozenset(
        extra for item in required for extra in item.requirement.extras
      )
      if candidate.extras != extras:
        candidate.extras = extras
        candidate.dependencies = None
      candidate.package = Package(
        requirement=candidate.package.requirement,
        source=candidate.package.source,
        build=all(item.build for item in required),
        include_dependencies=any(item.include_dependencies for item in required),
        max_size=minimum_limit(item.max_size for item in required),
        max_files=minimum_limit(item.max_files for item in required),
        max_dependencies=minimum_limit(item.max_dependencies for item in required),
        max_dependency_size=minimum_limit(
          item.max_dependency_size for item in required
        ),
        max_dependency_files=minimum_limit(
          item.max_dependency_files for item in required
        ),
      )
    return [
      candidate
      for candidate in candidates
      if (candidate.name, candidate.version) not in incompatible
      and all(self.is_satisfied_by(requirement, candidate) for requirement in required)
      and (all(item.build for item in required) or candidate.artifact.is_wheel)
    ]

  def is_satisfied_by(
    self,
    requirement: _PackageRequirement,
    candidate: _Candidate,
  ) -> bool:
    return (
      canonicalize_name(requirement.requirement.name) == candidate.name
      and candidate.version in requirement.requirement.specifier
    )

  def get_dependencies(self, candidate: _Candidate) -> Iterable[_PackageRequirement]:
    if not candidate.package.include_dependencies:
      return ()
    candidate.prepare()
    return candidate.dependencies or ()


# Cache discovery can fail in restricted or incompletely configured environments.
try:
  DEFAULT_PACKAGE_CACHE: PackageCache | None = PackageCache(
    user_cache_path("pysandbox") / "packages"
  )
except Exception:
  DEFAULT_PACKAGE_CACHE = None

DEFAULT_PACKAGE_INDEX: PackageIndex = PyPIIndex()


class PackageManager:
  def __init__(
    self,
    *,
    cache: PackageCache | None = DEFAULT_PACKAGE_CACHE,
    index: PackageIndex = DEFAULT_PACKAGE_INDEX,
  ) -> None:
    self.cache = cache
    self.index = index

  async def resolve(
    self,
    *packages: Package | str,
    cache: CachePolicy = "by_version",
  ) -> PackageEnvironment:
    if cache not in {"by_version", "never"}:
      raise ValueError("cache must be 'by_version' or 'never'")
    package_cache = self._require_cache()
    requested = tuple(normalize_package(package) for package in packages)
    temporary: tempfile.TemporaryDirectory[str] | None = tempfile.TemporaryDirectory(
      prefix="pysandbox-packages-"
    )
    download_directory = Path(temporary.name) / "downloads"
    download_directory.mkdir()
    try:
      candidates = await asyncio.to_thread(
        self._resolve,
        requested,
        download_directory,
        cache == "by_version",
      )
      installed = []
      temporary_layers = Path(temporary.name) / "layers"
      for candidate in candidates:
        candidate.prepare()
        if candidate.cached is not None and cache == "by_version":
          installed.append(candidate.cached)
          continue
        if candidate.wheel is None:
          raise PackageError(f"failed to prepare {candidate.name}=={candidate.version}")
        installed.append(
          await package_cache.install(
            candidate.wheel,
            build=False,
            policy=cache,
            temporary_root=temporary_layers,
            max_size=candidate.package.max_size,
            max_files=candidate.package.max_files,
          )
        )
      paths = compose_paths(installed)
      if cache == "by_version":
        temporary.cleanup()
        temporary = None
      return PackageEnvironment(
        packages=tuple(installed),
        paths=paths,
        _temporary_directory=temporary,
      )
    except BaseException:
      if temporary is not None:
        temporary.cleanup()
      raise

  def _require_cache(self) -> PackageCache:
    if self.cache is None:
      raise PackageError("package cache is unavailable")
    return self.cache

  def _resolve(
    self,
    packages: Sequence[Package],
    download_directory: Path,
    allow_cached: bool,
  ) -> tuple[_Candidate, ...]:
    requirements = [
      _PackageRequirement(
        package.parsed_requirement(),
        build=package.build,
        include_dependencies=package.include_dependencies,
        max_size=package.max_size,
        max_files=package.max_files,
        max_dependencies=package.max_dependencies,
        max_dependency_size=package.max_dependency_size,
        max_dependency_files=package.max_dependency_files,
      )
      for package in packages
    ]
    provider = _Provider(self, packages, download_directory, allow_cached)
    try:
      result = Resolver(provider, BaseReporter()).resolve(requirements)
    except Exception as error:
      raise PackageError(f"package resolution failed: {error}") from error
    return tuple(result.mapping.values())

  def _candidates(
    self,
    package: Package,
    download_directory: Path,
    allow_cached: bool,
  ) -> list[_Candidate]:
    requirement = package.parsed_requirement()
    distribution = package.source
    candidate_allow_cached = allow_cached
    if distribution is not None:
      is_directory = distribution.is_dir()
      filename = f"{distribution.name}.tar.gz" if is_directory else distribution.name
      parsed = distribution_from_filename(filename)
      if parsed is None:
        raise PackageError(f"unsupported distribution filename: {filename}")
      artifact = PackageArtifact(
        name=parsed[0],
        version=parsed[1],
        filename=filename,
        url=distribution.as_uri(),
        sha256=None if is_directory else file_sha256(distribution),
        size=None if is_directory else distribution.stat().st_size,
      )
      if artifact.name != canonicalize_name(requirement.name):
        raise PackageError(f"{filename} does not provide {requirement.name}")
      artifacts = (artifact,)
      candidate_allow_cached = allow_cached and not is_directory
    else:
      artifacts = self.index.artifacts(requirement.name)
    candidates = [
      _Candidate(
        artifact=artifact,
        package=package,
        manager=self,
        download_directory=download_directory,
        distribution=distribution,
        allow_cached=candidate_allow_cached,
      )
      for artifact in artifacts
      if not artifact.yanked
      and artifact.version in requirement.specifier
      and artifact_matches_python(artifact)
      and size_within_limit(artifact.size, package.max_size)
      and (package.build or artifact.is_wheel)
      and (not artifact.is_wheel or compatible_wheel_filename(artifact.filename))
    ]
    candidates.sort(
      key=lambda candidate: (candidate.version, candidate.artifact.is_wheel),
      reverse=True,
    )
    return candidates


def prepare_wheel(
  distribution: Path,
  build: bool,
  output_directory: Path | None = None,
  max_size: int | None = None,
  max_files: int | None = None,
) -> Path:
  if distribution.name.endswith(".whl"):
    ensure_compatible_wheel(distribution)
    validate_zip_file(distribution, max_size, max_files)
    return distribution
  if not build:
    raise PackageError(f"building is disabled for {distribution.name}")
  with tempfile.TemporaryDirectory(prefix="pysandbox-build-") as directory:
    source = Path(directory) / "source"
    if distribution.is_dir():
      validate_directory(distribution, max_size, max_files)
      shutil.copytree(distribution, source)
    else:
      ensure_file_size(distribution, max_size)
      unpack_distribution(distribution, source, max_size, max_files)
    output = Path(directory) / "wheel"
    output.mkdir()
    with DefaultIsolatedEnv() as environment:
      builder = ProjectBuilder.from_isolated_env(environment, source)
      environment.install(builder.build_system_requires)
      environment.install(builder.get_requires_for_build("wheel"))
      wheel = Path(builder.build("wheel", output))
    target = (output_directory or distribution.parent) / wheel.name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(wheel, target)
  ensure_compatible_wheel(target)
  validate_zip_file(target, max_size, max_files)
  return target


def normalize_package(package: Package | str) -> Package:
  if isinstance(package, str):
    return Package(package)
  return Package(
    requirement=package.requirement,
    source=package.source.resolve() if package.source is not None else None,
    build=package.build,
    include_dependencies=package.include_dependencies,
    max_size=package.max_size,
    max_files=package.max_files,
    max_dependencies=package.max_dependencies,
    max_dependency_size=package.max_dependency_size,
    max_dependency_files=package.max_dependency_files,
  )


def unpack_distribution(
  distribution: Path,
  destination: Path,
  max_size: int | None = None,
  max_files: int | None = None,
) -> None:
  archive_root = destination.parent / "archive"
  archive_root.mkdir()
  if distribution.name.endswith((".tar.gz", ".tar.bz2", ".tar.xz", ".tar")):
    with tarfile.open(distribution) as archive:
      validate_archive_members(
        ((member.size, member.isfile()) for member in archive.getmembers()),
        max_size,
        max_files,
        distribution.name,
      )
      archive.extractall(archive_root, filter="data")
  elif distribution.name.endswith(".zip"):
    with zipfile.ZipFile(distribution) as archive:
      validate_zip_archive(archive, max_size, max_files, distribution.name)
      archive.extractall(archive_root)
  else:
    raise PackageError(f"unsupported source distribution: {distribution.name}")
  entries = list(archive_root.iterdir())
  if len(entries) == 1 and entries[0].is_dir():
    entries[0].replace(destination)
    return
  destination.mkdir()
  for entry in entries:
    entry.replace(destination / entry.name)


def install_wheel(
  wheel: Path,
  site_packages: Path,
  max_size: int | None = None,
  max_files: int | None = None,
) -> None:
  validate_zip_file(wheel, max_size, max_files)
  schemes = {
    "purelib": str(site_packages),
    "platlib": str(site_packages),
    "scripts": str(site_packages / ".scripts"),
    "data": str(site_packages / ".data"),
    "headers": str(site_packages / ".headers"),
  }
  destination = SchemeDictionaryDestination(
    schemes,
    interpreter=sys.executable,
    script_kind="posix",
    bytecode_optimization_levels=(),
  )
  try:
    with WheelFile.open(wheel) as source:
      source.validate_record()
      install(
        source=source,
        destination=destination,
        additional_metadata={"INSTALLER": b"pysandbox"},
      )
  except ValueError as error:
    raise PackageError(f"invalid wheel {wheel.name}: {error}") from error


def wheel_metadata(
  wheel: Path,
  max_size: int | None = None,
  max_files: int | None = None,
):
  with zipfile.ZipFile(wheel) as archive:
    validate_zip_archive(archive, max_size, max_files, wheel.name)
    names = [
      name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
    ]
    if len(names) != 1:
      raise PackageError(f"wheel must contain exactly one METADATA file: {wheel.name}")
    return BytesParser(policy=compat32).parsebytes(archive.read(names[0]))


def cached_package_metadata(package: CachedPackage):
  metadata = [
    path / "METADATA"
    for path in package.paths
    if path.name.endswith(".dist-info") and path.is_dir()
  ]
  if len(metadata) != 1 or not metadata[0].is_file():
    raise PackageError(f"cached {package.name}=={package.version} has invalid metadata")
  return BytesParser(policy=compat32).parsebytes(metadata[0].read_bytes())


def dependencies_from_metadata(
  metadata,
  *,
  build: bool,
  extras: frozenset[str],
  max_dependencies: int | None,
  max_dependency_size: int | None,
  max_dependency_files: int | None,
) -> tuple[_PackageRequirement, ...]:
  return tuple(
    _PackageRequirement(
      Requirement(value),
      build=build,
      include_dependencies=True,
      max_size=max_dependency_size,
      max_files=max_dependency_files,
      max_dependencies=max_dependencies,
      max_dependency_size=max_dependency_size,
      max_dependency_files=max_dependency_files,
    )
    for value in metadata.get_all("Requires-Dist", ())
    if requirement_applies(value, extras)
  )


def requirement_applies(value: str, extras: frozenset[str]) -> bool:
  requirement = Requirement(value)
  if requirement.marker is None:
    return True
  environment = cast(dict[str, str], default_environment())
  return any(
    requirement.marker.evaluate({**environment, "extra": extra})
    for extra in extras or frozenset({""})
  )


def ensure_compatible_wheel(path: Path) -> None:
  if not compatible_wheel_filename(path.name):
    raise PackageError(f"wheel is not a compatible pure-Python wheel: {path.name}")


def compatible_wheel_filename(filename: str) -> bool:
  try:
    _, _, _, tags = parse_wheel_filename(filename)
  except ValueError:
    return False
  return any(
    tag.interpreter.startswith("py3") and tag.abi == "none" and tag.platform == "any"
    for tag in tags
  )


def artifact_matches_python(artifact: PackageArtifact) -> bool:
  if artifact.requires_python is None:
    return True
  try:
    from packaging.specifiers import SpecifierSet

    return Version(".".join(map(str, sys.version_info[:3]))) in SpecifierSet(
      artifact.requires_python
    )
  except ValueError:
    return False


def distribution_from_filename(filename: str) -> tuple[str, Version] | None:
  try:
    if filename.endswith(".whl"):
      name, version, _, _ = parse_wheel_filename(filename)
    else:
      name, version = parse_sdist_filename(filename)
    return canonicalize_name(name), version
  except ValueError:
    return None


def compose_paths(packages: Sequence[CachedPackage]) -> tuple[Path, ...]:
  paths: dict[str, Path] = {}
  for package in packages:
    for path in package.paths:
      previous = paths.get(path.name)
      if previous is not None and previous != path:
        raise PackageError(
          f"package paths conflict at site-packages/{path.name}: {previous} and {path}"
        )
      paths[path.name] = path
  return tuple(paths[name] for name in sorted(paths))


def minimum_limit(limits: Iterable[int | None]) -> int | None:
  bounded = [limit for limit in limits if limit is not None]
  return min(bounded) if bounded else None


def size_within_limit(size: int | None, max_bytes: int | None) -> bool:
  return size is None or max_bytes is None or size <= max_bytes


def ensure_size(size: int | None, max_bytes: int | None, name: str) -> None:
  if not size_within_limit(size, max_bytes):
    raise PackageError(f"{name} exceeds the {max_bytes}-byte size limit")


def ensure_file_size(path: Path, max_bytes: int | None) -> None:
  ensure_size(path.stat().st_size, max_bytes, path.name)


def copy_limited(
  source: BinaryIO,
  destination: BinaryIO,
  max_bytes: int | None,
  name: str,
) -> None:
  if max_bytes is None:
    shutil.copyfileobj(source, destination)
    return
  copied = 0
  while chunk := source.read(min(1024 * 1024, max_bytes - copied + 1)):
    copied += len(chunk)
    if copied > max_bytes:
      raise PackageError(f"{name} exceeds the {max_bytes}-byte size limit")
    destination.write(chunk)


def validate_archive_members(
  members: Iterable[tuple[int, bool]],
  max_bytes: int | None,
  max_files: int | None,
  name: str,
) -> None:
  if max_bytes is None and max_files is None:
    return
  size = 0
  files = 0
  for member_size, is_file in members:
    if not is_file:
      continue
    size += member_size
    files += 1
    ensure_size(size, max_bytes, name)
    if max_files is not None and files > max_files:
      raise PackageError(f"{name} exceeds the {max_files}-file limit")


def validate_zip_archive(
  archive: zipfile.ZipFile,
  max_bytes: int | None,
  max_files: int | None,
  name: str,
) -> None:
  validate_archive_members(
    ((member.file_size, not member.is_dir()) for member in archive.infolist()),
    max_bytes,
    max_files,
    name,
  )


def validate_zip_file(
  path: Path,
  max_bytes: int | None,
  max_files: int | None,
) -> None:
  if max_bytes is None and max_files is None:
    return
  ensure_file_size(path, max_bytes)
  with zipfile.ZipFile(path) as archive:
    validate_zip_archive(archive, max_bytes, max_files, path.name)


def validate_directory(
  path: Path,
  max_bytes: int | None,
  max_files: int | None,
) -> None:
  if max_bytes is None and max_files is None:
    return
  size = 0
  files = 0

  def visit(directory: Path, ancestors: frozenset[tuple[int, int]]) -> None:
    nonlocal size, files
    metadata = directory.stat()
    identity = (metadata.st_dev, metadata.st_ino)
    if identity in ancestors:
      raise PackageError(
        f"directory source contains a symbolic-link cycle: {directory}"
      )
    descendants = ancestors | {identity}
    with os.scandir(directory) as entries:
      for entry in entries:
        if entry.is_dir(follow_symlinks=True):
          visit(Path(entry.path), descendants)
          continue
        metadata = entry.stat(follow_symlinks=True)
        size += metadata.st_size
        files += 1
        ensure_size(size, max_bytes, path.name)
        if max_files is not None and files > max_files:
          raise PackageError(f"{path.name} exceeds the {max_files}-file limit")

  visit(path, frozenset())


def file_sha256(path: Path) -> str:
  digest = hashlib.sha256()
  with path.open("rb") as file:
    for chunk in iter(lambda: file.read(1024 * 1024), b""):
      digest.update(chunk)
  return digest.hexdigest()
