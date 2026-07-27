import sys
from pathlib import Path

from . import _core


def main() -> None:
  if len(sys.argv) != 7:
    raise SystemExit(
      "usage: python -m pysandbox._sandboxd "
      "<socket-name> <component> <python-root> <max-ipc-frame-bytes> "
      "<worker-queue-capacity> <cache-vfs>"
    )
  _core.run_sandboxd(
    sys.argv[1],
    Path(sys.argv[2]),
    Path(sys.argv[3]),
    int(sys.argv[4]),
    int(sys.argv[5]),
    sys.argv[6] == "true",
  )


if __name__ == "__main__":
  main()
