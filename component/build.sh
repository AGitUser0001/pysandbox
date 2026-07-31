#!/bin/sh
set -eu

cd "$(dirname "$0")"
uv run componentize-py \
  -d wit \
  -w python \
  componentize main \
  -p src \
  -o pysandbox.wasm
