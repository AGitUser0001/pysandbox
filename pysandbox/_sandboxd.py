import signal
import sys
from pathlib import Path


def main() -> None:
  ignore_terminal_signals()

  from . import _core

  if len(sys.argv) != 12:
    raise SystemExit(
      "usage: python -m pysandbox._sandboxd "
      "<socket-name> <component> <python-root> <max-ipc-frame-bytes> "
      "<worker-queue-capacity> <cache-vfs> <cache-vfs-negative>"
      " <cpu-share-enabled> <cpu-share-limit-percent>"
      " <cpu-share-sample-interval-ms> <cpu-share-activity-timeout-ms>"
    )
  _core.run_sandboxd(
    sys.argv[1],
    Path(sys.argv[2]),
    Path(sys.argv[3]),
    int(sys.argv[4]),
    int(sys.argv[5]),
    sys.argv[6] == "true",
    sys.argv[7] == "true",
    sys.argv[8] == "true",
    None if sys.argv[9] == "none" else float(sys.argv[9]),
    int(sys.argv[10]),
    int(sys.argv[11]),
  )


def ignore_terminal_signals() -> None:
  for name in ("SIGINT", "SIGHUP"):
    terminal_signal = getattr(signal, name, None)
    if terminal_signal is not None:
      signal.signal(terminal_signal, signal.SIG_IGN)


if __name__ == "__main__":
  main()
