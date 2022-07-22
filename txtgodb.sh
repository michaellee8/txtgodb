#!/bin/bash

SCRIPT_SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_SRC_DIR" || exit

go run ./cmd/txtgodb \
  -spec "./pkg/testdata/specs" \
  -data "./pkg/testdata/data" \
  -driver "sqlite" \
  -dsn "file:./pkg/testdata/tmp/txtgodb.db" \
  -parallel
