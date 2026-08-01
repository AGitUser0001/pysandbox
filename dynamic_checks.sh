#!/bin/sh
cd "$(dirname "$0")" || exit 1

echo 'rust tests:'
cargo test --workspace; rust_status=$?

echo 'python tests:'
uv run python -m unittest discover -s tests -v; python_status=$?

if [ $rust_status -ne 0 ] || [ $python_status -ne 0 ]; then
  exit 1
fi
