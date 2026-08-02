import json
import os
import subprocess
import sys
from pathlib import Path


def interpreter_key(executable: Path) -> str | None:
  probe = (
    "import json, sys; "
    "print(json.dumps([sys.version_info.major, sys.version_info.minor, "
    "not getattr(sys, '_is_gil_enabled', lambda: True)()]))"
  )
  try:
    output = subprocess.run(
      [executable, "-I", "-c", probe],
      check=True,
      capture_output=True,
      text=True,
      timeout=10,
    ).stdout
    major, minor, free_threaded = json.loads(output)
  except (OSError, subprocess.SubprocessError, ValueError, json.JSONDecodeError):
    return None
  return f"python{major}{minor}{'t' if free_threaded else ''}"


def candidate_interpreters() -> list[Path]:
  names = ("python.exe",) if os.name == "nt" else ("python", "python3")
  candidates = [Path(sys.executable)]
  for directory in os.environ.get("PATH", "").split(os.pathsep):
    if not directory:
      continue
    candidates.extend(Path(directory) / name for name in names)
  return candidates


def main() -> None:
  required = set(sys.argv[1:])
  found: dict[str, Path] = {}
  seen: set[Path] = set()
  for candidate in candidate_interpreters():
    if not candidate.is_file():
      continue
    resolved = candidate.resolve()
    if resolved in seen:
      continue
    seen.add(resolved)
    key = interpreter_key(resolved)
    if key in required and key not in found:
      found[key] = resolved

  missing = required - found.keys()
  if missing:
    raise RuntimeError(f"could not find interpreters: {', '.join(sorted(missing))}")

  output_path = os.environ.get("GITHUB_OUTPUT")
  if output_path is None:
    raise RuntimeError("GITHUB_OUTPUT is not set")
  with Path(output_path).open("a", encoding="utf-8") as output:
    for key in sorted(found):
      print(f"{key}={found[key]}", file=output)
      print(f"{key}: {found[key]}")


if __name__ == "__main__":
  main()
