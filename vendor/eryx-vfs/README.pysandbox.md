This directory vendors the filesystem binding layer from `eryx-vfs` 0.5.0.
Its WASI dependencies are aligned with pysandbox's Wasmtime 48. The checked-in
`wit-template/` is not directly parseable WIT: the build script fills its WASI
version placeholder into Cargo's `OUT_DIR` using the prebuilt Python component's
filesystem import. Builds without a prebuilt component use 0.2.9 as a bootstrap
version. This vendored copy is redistributed under the original MIT option.
