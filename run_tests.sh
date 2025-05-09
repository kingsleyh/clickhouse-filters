#!/bin/bash
set -e

# Run only integration tests with output
echo "Running integration tests with improved container handling..."
cargo test integration:: -- --nocapture

# Check exit status
if [ $? -eq 0 ]; then
  echo "✅ Tests passed successfully!"
else
  echo "❌ Tests failed."
  exit 1
fi