import hashlib
import json
import os
import re
import shutil
import tarfile
import tempfile
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


__all__ = [
    "Asset",
    "AssetDigestError",
    "AssetError",
    "AssetNotFoundError",
    "GitHubReleaseAsset",
]


class AssetError(Exception):
    """Base error for asset download and installation failures."""


class AssetNotFoundError(AssetError):
    """Raised when a requested GitHub release asset cannot be found."""


class AssetDigestError(AssetError):
    """Raised when a downloaded asset does not match its expected digest."""


@dataclass(frozen=True)
class GitHubReleaseAsset:
    name: str
    url: str
    digest: str | None = None
    size: int | None = None

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> "GitHubReleaseAsset":
        name = data.get("name")
        url = data.get("browser_download_url")

        if not isinstance(name, str) or not name:
            raise AssetError("GitHub release asset is missing a name")

        if not isinstance(url, str) or not url:
            raise AssetError(f"GitHub release asset {name!r} is missing a download URL")

        digest = data.get("digest")
        size = data.get("size")

        return cls(
            name=name,
            url=url,
            digest=digest if isinstance(digest, str) else None,
            size=size if isinstance(size, int) else None,
        )


@dataclass(frozen=True)
class Asset:
    repo: str
    filename: str | re.Pattern[str] | None = None
    tag: str | re.Pattern[str] | None = None
    source: bool = False
    extract: bool = False
    strip_single_root: bool = True
    extract_subdir: str | None = None
    user_agent: str = "pysandbox"
    timeout: float = 60

    def __post_init__(self) -> None:
        if "/" not in self.repo:
            raise ValueError("repo must look like 'owner/name'")

        if not self.source and self.filename is None:
            raise ValueError("filename is required unless source=True")

    def fetch(self, path: Path) -> Path:
        selected = self.find()
        return self.download(selected, path)

    def find(self) -> GitHubReleaseAsset:
        for release in self.fetch_releases():
            if self.source:
                source_asset = self.source_asset_from_release(release)
                if source_asset is not None:
                    return source_asset

            assets = release.get("assets")
            if not isinstance(assets, list):
                assets = []

            for item in assets:
                if not isinstance(item, dict):
                    continue

                if self.matches(item):
                    return GitHubReleaseAsset.from_api(item)

        raise AssetNotFoundError(f"no asset matching {self.description} in {self.repo}")

    def fetch_releases(self) -> list[dict[str, Any]]:
        if isinstance(self.tag, str):
            release = self.get_json(
                f"https://api.github.com/repos/{self.repo}/releases/tags/{self.tag}"
            )
            return [release]

        data = self.get_json(f"https://api.github.com/repos/{self.repo}/releases")
        if not isinstance(data, list):
            raise AssetError("GitHub releases response was not a list")

        releases: list[dict[str, Any]] = []
        for item in data:
            if isinstance(item, dict) and self.matches_release_tag(item):
                releases.append(item)

        return releases

    def matches_release_tag(self, release: dict[str, Any]) -> bool:
        if self.tag is None:
            return True

        tag_name = release.get("tag_name")
        if not isinstance(tag_name, str):
            return False

        if isinstance(self.tag, re.Pattern):
            return self.tag.fullmatch(tag_name) is not None

        return tag_name == self.tag

    def matches(self, asset: dict[str, Any]) -> bool:
        name = asset.get("name")
        if not isinstance(name, str):
            return False

        return self.matches_name(name)

    def matches_source_asset(self, asset: GitHubReleaseAsset) -> bool:
        return self.matches_name(asset.name)

    def matches_name(self, name: str) -> bool:
        if self.filename is None:
            return False

        if isinstance(self.filename, re.Pattern):
            return self.filename.fullmatch(name) is not None

        return name == self.filename

    def source_asset_from_release(
        self,
        release: dict[str, Any],
    ) -> GitHubReleaseAsset | None:
        tag_name = release.get("tag_name")
        zipball_url = release.get("zipball_url")

        if not isinstance(tag_name, str) or not isinstance(zipball_url, str):
            return None

        repo_name = self.repo.rsplit("/", 1)[-1]
        return GitHubReleaseAsset(
            name=f"{repo_name}-{tag_name}.zip",
            url=zipball_url,
        )

    @property
    def description(self) -> str:
        if self.source:
            if isinstance(self.tag, re.Pattern):
                return f"source tag regex {self.tag.pattern!r}"

            if isinstance(self.tag, str):
                return f"source tag {self.tag!r}"

            return "source"

        if isinstance(self.filename, re.Pattern):
            return f"regex {self.filename.pattern!r}"

        return repr(self.filename)

    def download(self, release_asset: GitHubReleaseAsset, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(
            prefix=f".download-{destination.name}-",
            dir=destination.parent,
        ) as tmpdir:
            tmpdir_path = Path(tmpdir)
            download_path = tmpdir_path / release_asset.name

            self.download_file(release_asset.url, download_path)
            self.verify_digest(release_asset, download_path)

            if self.extract:
                install_root = tmpdir_path / "install"
                install_root.mkdir()
                extract_archive(download_path, install_root)

                source = normalize_extracted_root(
                    install_root,
                    strip_single_root=self.strip_single_root,
                )
                if self.extract_subdir is not None:
                    source = source / self.extract_subdir

                if not source.exists():
                    raise AssetError(
                        f"{release_asset.name} does not contain {self.extract_subdir!r}"
                    )

                replace_path(source, destination)
            else:
                replace_path(download_path, destination)

        return destination

    def get_json(self, url: str) -> Any:
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "User-Agent": self.user_agent,
            },
        )

        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def download_file(self, url: str, destination: Path) -> None:
        request = urllib.request.Request(
            url,
            headers={"User-Agent": self.user_agent},
        )

        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            with destination.open("wb") as file:
                shutil.copyfileobj(response, file)

    @staticmethod
    def verify_digest(asset: GitHubReleaseAsset, path: Path) -> None:
        if asset.digest is None:
            return

        algorithm, separator, expected = asset.digest.partition(":")
        if separator != ":":
            return

        if algorithm.lower() != "sha256":
            return

        actual = sha256_file(path)
        if actual.lower() != expected.lower():
            raise AssetDigestError(
                f"sha256 mismatch for {asset.name}: expected {expected}, got {actual}"
            )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def extract_archive(archive: Path, destination: Path) -> None:
    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as zip_file:
            for member in zip_file.infolist():
                assert_archive_member_path(destination, member.filename)

            zip_file.extractall(destination)
        return

    if tarfile.is_tarfile(archive):
        with tarfile.open(archive) as tar_file:
            for member in tar_file.getmembers():
                assert_archive_member_path(destination, member.name)
                if member.issym() or member.islnk():
                    raise AssetError(
                        f"{archive.name} contains unsupported link {member.name!r}"
                    )

            tar_file.extractall(destination)
        return

    raise AssetError(f"{archive.name} is not a supported archive")


def normalize_extracted_root(root: Path, *, strip_single_root: bool) -> Path:
    if not strip_single_root:
        return root

    children = list(root.iterdir())
    if len(children) == 1 and children[0].is_dir():
        return children[0]

    return root


def replace_path(source: Path, destination: Path) -> None:
    old = destination.parent / f".old-{destination.name}"

    remove_path(old)

    if destination.exists() or destination.is_symlink():
        destination.rename(old)

    try:
        source.rename(destination)
    except Exception:
        if not destination.exists() and old.exists():
            old.rename(destination)
        raise
    finally:
        remove_path(old)


def remove_path(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return

    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        os.remove(path)


def assert_archive_member_path(destination: Path, member_name: str) -> None:
    target = (destination / member_name).resolve()
    root = destination.resolve()

    try:
        common = os.path.commonpath([root, target])
    except ValueError as exc:
        raise AssetError(f"archive member escapes destination: {member_name!r}") from exc

    if common != str(root):
        raise AssetError(f"archive member escapes destination: {member_name!r}")
