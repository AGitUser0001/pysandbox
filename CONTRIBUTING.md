# Contributing

## Requirements

- Python 3.12 or newer
- [uv](https://docs.astral.sh/uv/)
- Stable Rust 1.94 or newer
- Node.js and `npx` for Pyright
- `cargo-deny` for dependency policy checks

Clone the repository with its vendored submodules:

```sh
git clone --recurse-submodules https://github.com/AGitUser0001/pysandbox.git
cd pysandbox
```

If the repository was cloned without them:

```sh
git submodule update --init --recursive
```

Install the Python environment and Rust tools:

```sh
uv sync --group dev
rustup component add rustfmt
cargo install cargo-deny --locked
```

Activate `.venv` before running the repository scripts, or otherwise make its
executables available on `PATH`.

## Checks

Static checks validate and then format the source:

```sh
./static_checks.sh
```

This runs Pyright, Ruff, `cargo deny`, Ruff formatting, and `cargo fmt`.

Run the Rust and installed Python integration tests separately:

```sh
./dynamic_checks.sh
```

The integration tests start the packaged sandbox subprocess and exercise the
component runtime, RPC, limits, VFS, packages, concurrency, and recovery.

## Building

### Standard Build

Build a wheel and source distribution through the configured Maturin backend
with uv:

```sh
uv build
```

Or build only a wheel through pip's isolated PEP 517 build environment:

```sh
python -m pip wheel . --wheel-dir dist
```

To build and install directly from the checkout:

```sh
python -m pip install .
```

The native build script generates two packaged runtime inputs:

- `pysandbox.wasm`, the componentized Python guest
- `runtime/`, the CPython standard library and guest dependencies mounted at
  `/python`

Runtime generation invokes `componentize-py`, then starts the generated WASI
component to compile the copied standard library with the matching interpreter.
The first build is therefore substantially slower than installing a published
wheel.

### Build with a Component Runtime Artifact

The component runtime is platform-independent. Download the
`pysandbox-component-runtime-<tag>.tar.gz` asset from a GitHub Release, then
extract it and point the native build at it:

```sh
mkdir -p tmp/component-runtime
RELEASE_TAG="v$(python3 -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])')"
tar -xzf "pysandbox-component-runtime-${RELEASE_TAG}.tar.gz" -C tmp/component-runtime
PYSANDBOX_PREBUILT_RUNTIME="$PWD/tmp/component-runtime" uv build
```

The pip equivalent is:

```sh
PYSANDBOX_PREBUILT_RUNTIME="$PWD/tmp/component-runtime" \
  python -m pip wheel . --wheel-dir dist
```

The extracted directory must contain `pysandbox.wasm` and `runtime/`. This path
skips componentization and standard-library compilation, and works even on a
platform where `componentize-py` cannot run.

To generate the same reusable artifact locally instead of downloading it:

```sh
cargo run --release --package pysandbox-runtime-build -- target/pysandbox-runtime
PYSANDBOX_PREBUILT_RUNTIME=target/pysandbox-runtime uv build
```

CI uses this mechanism to build every native wheel from one shared WASI runtime
artifact.

To rebuild only the component during guest development:

```sh
sh component/build.sh
```

## Repository Layout

- `pysandbox/`: public Python facade and packaged native extension
- `component/`: componentized guest bootstrap and WIT interface
- `rust/pysandbox-native/`: PyO3 supervisor and Python-facing async objects
- `rust/pysandbox-sandboxd/`: sandbox daemon, worker actors, Wasmtime host, and VFS
- `rust/pysandbox-protocol/`: shared framed IPC protocol
- `rust/pysandbox-runtime-build/`: component and standard-library generator
- `vendor/`: pinned CPython, cbor2, and VFS sources
- `tests/`: Python integration and regression tests

See [DOCS.md](https://github.com/AGitUser0001/pysandbox/blob/main/DOCS.md) for
the runtime architecture.

## Releases

Update the version in `pyproject.toml` and refresh `uv.lock`. Then manually run
the `CI` workflow from the commit being released and set `release_tag` to the
matching tag, such as `v1.5.1`. An ordinary push or pull-request run builds and
tests artifacts but does not publish them.

Release preflight verifies that the requested tag matches the project version.
If the tag already exists, it must resolve to the selected workflow commit. The
workflow builds the shared component runtime and every native wheel, runs static
checks and native installed-wheel tests, and validates the built wheel versions
again before publishing.

The `release` GitHub environment requires approval and holds the publishing
boundary. No PyPI token is stored in the repository. After approval, the job
attests and publishes the artifacts to PyPI through OIDC Trusted Publishing,
creates the Git tag and GitHub Release when absent, and attaches the wheels and
component-runtime archive. A failed release can be retried by dispatching the
same commit with the same `release_tag`.
