This directory vendors the filesystem binding layer from `eryx-vfs` 0.5.0.
Its WASI dependency versions are aligned with pysandbox's Wasmtime 46 and its
generated WIT interfaces target `wasi:filesystem@0.2.4`, matching the Python
component. This vendored copy is redistributed under the original MIT option.
