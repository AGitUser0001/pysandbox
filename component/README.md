# Python Component Runtime

This is the build input for the Rust-hosted componentized Python runtime.

Build the component with:

```sh
sh component/build.sh
```

The async `run` export asks the trusted host for one program, compiles it with
top-level await enabled, and runs it in persistent `__main__` globals. The
vendored pure-Python `cbor2` package is included in the component through
componentize-py's Python dependency path. The matching CPython standard library
is sourced from `vendor/cpython/Lib` and packaged as the read-only `/python`
mount.

The daemon shares one Wasmtime Engine, Linker, and compiled Component. Each
worker owns an independent Store, component instance, Python namespace, memory,
WASI context, limits, and output stream. Guest stdout and stderr are captured
through Wasmtime's WASI stream API and forwarded as labelled socket frames.

Run the Rust and Python integration tests with `./static_checks.sh`.
