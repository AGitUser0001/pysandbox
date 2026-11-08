import os
import shutil
import subprocess
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class RuntimeBuildHook(BuildHookInterface):
  """Packages the platform-independent WASI runtime into `pysandbox/_runtime`.

  The runtime is either copied from `PYSANDBOX_PREBUILT_RUNTIME` or generated
  with the `pysandbox-runtime-build` crate.
  """

  def initialize(self, version, build_data):
    root = Path(self.root)
    destination = root / "pysandbox" / "_runtime"
    prebuilt = os.environ.get("PYSANDBOX_PREBUILT_RUNTIME")
    if prebuilt:
      self.copy_prebuilt(root / prebuilt, destination)
    else:
      self.generate(root, destination)

  def copy_prebuilt(self, source: Path, destination: Path):
    if not (source / "pysandbox.wasm").is_file() or not (source / "runtime").is_dir():
      raise RuntimeError(
        f"prebuilt runtime must contain pysandbox.wasm and runtime/: {source}"
      )
    shutil.rmtree(destination, ignore_errors=True)
    shutil.copytree(source, destination)

  def generate(self, root: Path, destination: Path):
    shutil.rmtree(destination, ignore_errors=True)
    subprocess.run(
      [
        "cargo",
        "run",
        "--release",
        "--package",
        "pysandbox-runtime-build",
        "--",
        str(destination),
      ],
      cwd=root,
      check=True,
    )
