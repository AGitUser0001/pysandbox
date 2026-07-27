#!/bin/sh
set -eu

cd "$(dirname "$0")"
uv run componentize-py \
  -d wit \
  -w python \
  componentize guest \
  -p . \
  -p ../vendor/cbor2 \
  -o pysandbox.wasm
