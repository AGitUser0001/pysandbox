import importlib._bootstrap_external
import io
import os
import py_compile
import sys


def write_atomic(path, data, mode=0o666):
  temporary = f"{path}.{id(path)}"
  descriptor = os.open(temporary, os.O_EXCL | os.O_CREAT | os.O_WRONLY, mode & 0o666)
  try:
    # WASI file streams can accept only part of an unbuffered write.
    with io.FileIO(descriptor, "wb") as raw, io.BufferedWriter(raw) as file:
      file.write(data)
    os.replace(temporary, path)
  except OSError:
    try:
      os.unlink(temporary)
    except OSError:
      pass
    raise


def compile_files(paths):
  sys.dont_write_bytecode = True
  importlib._bootstrap_external._write_atomic = write_atomic  # pyright: ignore[reportAttributeAccessIssue]
  for path in paths:
    py_compile.compile(path, doraise=True)
